"""
Implanta a Fase 1 (Fundação) do DW no Microsoft Fabric pela API REST.

Cria (ou atualiza, se já existirem):
  - workspaces DW_DEV e DW_PROD na capacidade F4
  - lakehouses Bronze, Silver, Gold
  - notebooks nb_setup_controle, nb_preparar_ciclo, nb_orquestrador (Python notebooks)
  - pipeline pl_mestre_dw (sem agendamento)

Uso (no seu computador, com Python 3.10+):
  pip install azure-identity requests truststore
  python implantar_fundacao.py --listar-conexoes     # descobre o ID da conexão do gateway do GS
  python implantar_fundacao.py                       # implanta conforme config_fundacao.json
  python implantar_fundacao.py --executar-setup      # implanta e roda nb_setup_controle

O login abre o navegador com a sua conta Microsoft (admin do Fabric). Pode rodar de novo
quantas vezes quiser: itens existentes são atualizados, não duplicados.
"""
import argparse, base64, json, pathlib, sys, time
import requests

try:  # usa os certificados do Windows (rede com inspeção TLS/proxy da empresa)
    import truststore
    truststore.inject_into_ssl()
except ImportError:
    pass

RAIZ = pathlib.Path(__file__).parent
API = "https://api.fabric.microsoft.com/v1"
NOTEBOOKS = ["nb_setup_controle", "nb_preparar_ciclo", "nb_orquestrador"]
LAKEHOUSES = ["Bronze", "Silver", "Gold"]
NOMES_ANTIGOS = {"Bronze": "lh_bronze", "Silver": "lh_silver", "Gold": "lh_gold"}
PIPELINE = "pl_mestre_dw"

sys.path.insert(0, str(RAIZ))
import pipeline_mestre  # noqa: E402

# ---------------------------------------------------------------- autenticação e chamadas
class Fabric:
    def __init__(self):
        from azure.identity import (AuthenticationRecord, InteractiveBrowserCredential,
                                    TokenCachePersistenceOptions)
        # sessão guardada no cache criptografado do Windows: o navegador só abre no primeiro login
        registro = pathlib.Path.home() / ".dw_fabric_login.json"
        rec = AuthenticationRecord.deserialize(registro.read_text()) if registro.exists() else None
        self.cred = InteractiveBrowserCredential(
            cache_persistence_options=TokenCachePersistenceOptions(name="dw_fabric"),
            authentication_record=rec)
        if rec is None:
            registro.write_text(self.cred.authenticate(
                scopes=["https://api.fabric.microsoft.com/.default"]).serialize())
        self._token, self._exp = None, 0

    def _headers(self):
        if time.time() > self._exp - 300:
            t = self.cred.get_token("https://api.fabric.microsoft.com/.default")
            self._token, self._exp = t.token, t.expires_on
        return {"Authorization": f"Bearer {self._token}", "Content-Type": "application/json"}

    def chamar(self, metodo, url, corpo=None):
        url = url if url.startswith("http") else API + url
        for _ in range(8):
            r = requests.request(metodo, url, headers=self._headers(), json=corpo, timeout=120)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", 30)))
                continue
            if r.status_code == 202:
                return self._aguardar(r)
            if r.status_code >= 400:
                raise RuntimeError(f"{metodo} {url} -> {r.status_code}: {r.text}")
            return r.json() if r.content else {}
        raise RuntimeError(f"{metodo} {url}: limite de tentativas (429)")

    def _aguardar(self, r):
        local = r.headers.get("Location")
        if not local:
            return {}
        while True:
            time.sleep(int(r.headers.get("Retry-After", 5)))
            r = requests.get(local, headers=self._headers(), timeout=120)
            if r.status_code == 200:
                estado = r.json().get("status")
                if estado == "Succeeded":
                    try:
                        return self.chamar("GET", local.rstrip("/") + "/result")
                    except RuntimeError:
                        return r.json()
                if estado in ("Failed", "Cancelled"):
                    raise RuntimeError(f"Operação falhou: {r.text}")
            elif r.status_code >= 400:
                raise RuntimeError(f"Operação falhou: {r.status_code} {r.text}")

    def listar(self, url, chave="value"):
        itens, token = [], None
        while True:
            sep = "&" if "?" in url else "?"
            dados = self.chamar("GET", url + (f"{sep}continuationToken={token}" if token else ""))
            itens += dados.get(chave, [])
            token = dados.get("continuationToken")
            if not token:
                return itens

# ---------------------------------------------------------------- etapas
def b64(texto):
    return base64.b64encode(texto.encode("utf-8")).decode()

def capacidade(fab, nome):
    caps = fab.listar("/capacities")
    for c in caps:
        if c["displayName"].lower() == nome.lower() or c["id"] == nome:
            return c["id"]
    print("Capacidades visíveis:")
    for c in caps:
        print(f"  {c['displayName']}  sku={c['sku']}  estado={c['state']}  id={c['id']}")
    sys.exit(f"Capacidade '{nome}' não encontrada. Ajuste 'capacidade' no config.")

def workspace(fab, nome, cap_id, descricao, ws_id=None):
    if ws_id:  # workspace já existente, informado pelo ID em "workspaces_existentes"
        w = fab.chamar("GET", f"/workspaces/{ws_id}")
        if w.get("capacityId", "").lower() != cap_id.lower():
            fab.chamar("POST", f"/workspaces/{ws_id}/assignToCapacity", {"capacityId": cap_id})
        print(f"  workspace {nome}: usando existente '{w['displayName']}' ({ws_id})")
        return ws_id
    for w in fab.listar("/workspaces"):
        if w["displayName"] == nome:
            if w.get("capacityId", "").lower() != cap_id.lower():
                fab.chamar("POST", f"/workspaces/{w['id']}/assignToCapacity", {"capacityId": cap_id})
            print(f"  workspace {nome}: já existe ({w['id']})")
            return w["id"]
    w = fab.chamar("POST", "/workspaces", {"displayName": nome, "capacityId": cap_id,
                                           "description": descricao})
    print(f"  workspace {nome}: criado ({w['id']})")
    return w["id"]

def item(fab, ws, nome, tipo, partes=None, rota=None):
    existentes = {i["displayName"]: i["id"] for i in fab.listar(f"/workspaces/{ws}/items?type={tipo}")}
    definicao = None
    if partes:
        definicao = {"parts": [{"path": p, "payload": b64(c), "payloadType": "InlineBase64"}
                               for p, c in partes.items()]}
        if tipo == "Notebook":
            definicao["format"] = "ipynb"
    if nome in existentes:
        if definicao:
            fab.chamar("POST", f"/workspaces/{ws}/items/{existentes[nome]}/updateDefinition",
                       {"definition": definicao})
            print(f"  {tipo} {nome}: atualizado")
        else:
            print(f"  {tipo} {nome}: já existe")
        return existentes[nome]
    corpo = {"displayName": nome, "type": tipo}
    if definicao:
        corpo["definition"] = definicao
    novo = fab.chamar("POST", rota or f"/workspaces/{ws}/items", corpo)
    if not novo.get("id"):  # algumas criações assíncronas não devolvem o item
        novo = {"id": {i["displayName"]: i["id"] for i in
                       fab.listar(f"/workspaces/{ws}/items?type={tipo}")}[nome]}
    print(f"  {tipo} {nome}: criado ({novo['id']})")
    return novo["id"]

def renomear_antigos(fab, ws):
    """Renomeia lh_bronze/lh_silver/lh_gold de implantações anteriores, preservando os dados."""
    atuais = {i["displayName"]: i["id"] for i in fab.listar(f"/workspaces/{ws}/items?type=Lakehouse")}
    for novo, antigo in NOMES_ANTIGOS.items():
        if antigo in atuais and novo not in atuais:
            fab.chamar("PATCH", f"/workspaces/{ws}/lakehouses/{atuais[antigo]}", {"displayName": novo})
            print(f"  Lakehouse {antigo}: renomeado para {novo}")

def executar_job(fab, ws, item_id, nome, tipo_job="RunNotebook", corpo=None):
    print(f"  executando {nome} ...")
    r = requests.post(f"{API}/workspaces/{ws}/items/{item_id}/jobs/instances?jobType={tipo_job}",
                      headers=fab._headers(), json=corpo or {}, timeout=120)
    if r.status_code != 202:
        raise RuntimeError(f"Falha ao iniciar {nome}: {r.status_code} {r.text}")
    local = r.headers["Location"]
    while True:
        time.sleep(15)
        st = requests.get(local, headers=fab._headers(), timeout=120).json()
        if st.get("status") in ("Completed", "Failed", "Cancelled", "Deduped"):
            print(f"  {nome}: {st['status']} {st.get('failureReason') or ''}")
            return st

# ---------------------------------------------------------------- principal
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(RAIZ / "config_fundacao.json"))
    ap.add_argument("--listar-conexoes", action="store_true")
    ap.add_argument("--executar-setup", action="store_true")
    ap.add_argument("--executar-ciclo", choices=["AUTO", "COMPLETO", "FATOS"],
                    help="depois de implantar, roda o pl_mestre_dw com este ciclo")
    ap.add_argument("--origens", help="limita o pipeline a estas origens, ex.: GS ou GS,4S")
    ap.add_argument("--somente-itens", action="store_true",
                    help="não cria os workspaces que ficam fora de implantar_itens_em (ex.: DW_PROD)")
    args = ap.parse_args()
    cfg = json.loads(pathlib.Path(args.config).read_text(encoding="utf-8"))
    if args.origens:
        filtro = {o.strip().upper() for o in args.origens.split(",")}
        cfg["origens"] = [o for o in cfg["origens"] if o["origem"].upper() in filtro]
    if args.somente_itens:
        cfg["workspaces"] = {n: d for n, d in cfg["workspaces"].items() if n in cfg["implantar_itens_em"]}
    fab = Fabric()

    if args.listar_conexoes:
        for g in fab.listar("/gateways"):
            print(f"gateway {g['id']}  {g.get('displayName', ''):<40} {g.get('type', '')}")
        print()
        for c in fab.listar("/connections"):
            det = c.get("connectionDetails", {})
            print(f"{c['id']}  {c.get('displayName') or '':<40} {c.get('connectivityType') or '':<22} "
                  f"{det.get('type') or ''}: {det.get('path') or ''}")
        return

    print("1/5 Capacidade")
    cap = capacidade(fab, cfg["capacidade"])

    print("2/5 Workspaces")
    existentes = cfg.get("workspaces_existentes", {})
    ids_ws = {nome: workspace(fab, nome, cap, desc, existentes.get(nome))
              for nome, desc in cfg["workspaces"].items()}

    manifesto = {}
    for nome_ws in cfg["implantar_itens_em"]:
        ws = ids_ws[nome_ws]
        print(f"3/5 Lakehouses em {nome_ws}")
        renomear_antigos(fab, ws)
        lh = {n: item(fab, ws, n, "Lakehouse", rota=f"/workspaces/{ws}/lakehouses") for n in LAKEHOUSES}

        print(f"4/5 Notebooks em {nome_ws}")
        nb = {}
        for n in NOTEBOOKS:
            conteudo = (RAIZ / "notebooks" / f"{n}.ipynb").read_text(encoding="utf-8")
            nb[n] = item(fab, ws, n, "Notebook", {"notebook-content.ipynb": conteudo})

        print(f"5/5 Pipeline em {nome_ws}")
        definicao = pipeline_mestre.gerar(ws, lh["Bronze"], nb["nb_preparar_ciclo"],
                                          nb["nb_orquestrador"], cfg["origens"], cfg.get("paralelismo", 4))
        (RAIZ / f"{PIPELINE}_{nome_ws}.json").write_text(json.dumps(definicao, indent=2, ensure_ascii=False))
        pl = item(fab, ws, PIPELINE, "DataPipeline",
                  {"pipeline-content.json": json.dumps(definicao, ensure_ascii=False)})

        manifesto[nome_ws] = {"workspace": ws, "lakehouses": lh, "notebooks": nb, "pipeline": pl}
        if args.executar_setup:
            st = executar_job(fab, ws, nb["nb_setup_controle"], "nb_setup_controle")
            if st.get("status") != "Completed":
                sys.exit("nb_setup_controle falhou: confira o notebook no workspace.")
        if args.executar_ciclo:
            executar_job(fab, ws, pl, PIPELINE, "Pipeline",
                         {"executionData": {"parameters": {"ciclo": args.executar_ciclo}}})

    (RAIZ / "manifesto_implantacao.json").write_text(json.dumps(
        {"workspaces": ids_ws, "itens": manifesto}, indent=2, ensure_ascii=False))
    print("\nConcluído. IDs gravados em manifesto_implantacao.json")

if __name__ == "__main__":
    main()
