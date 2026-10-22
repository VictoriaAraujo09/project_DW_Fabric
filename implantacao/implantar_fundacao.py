"""
Implanta a Fase 1 (Fundação) do DW no Microsoft Fabric pela API REST.

Cria (ou atualiza, se já existirem):
  - workspaces DW_DEV e DW_PROD na capacidade F4
  - lakehouses lh_bronze, lh_silver, lh_gold
  - notebooks nb_setup_controle, nb_preparar_ciclo, nb_orquestrador (Python notebooks)
  - pipeline pl_mestre_dw (sem agendamento)

Uso (no seu computador, com Python 3.10+):
  pip install azure-identity requests
  python implantar_fundacao.py --listar-conexoes     # descobre o ID da conexão do gateway do GS
  python implantar_fundacao.py                       # implanta conforme config_fundacao.json
  python implantar_fundacao.py --executar-setup      # implanta e roda nb_setup_controle

O login abre o navegador com a sua conta Microsoft (admin do Fabric). Pode rodar de novo
quantas vezes quiser: itens existentes são atualizados, não duplicados.
"""
import argparse, base64, json, pathlib, sys, time
import requests

RAIZ = pathlib.Path(__file__).parent
API = "https://api.fabric.microsoft.com/v1"
NOTEBOOKS = ["nb_setup_controle", "nb_preparar_ciclo", "nb_orquestrador"]
LAKEHOUSES = ["lh_bronze", "lh_silver", "lh_gold"]
PIPELINE = "pl_mestre_dw"

sys.path.insert(0, str(RAIZ))
import pipeline_mestre  # noqa: E402

# ---------------------------------------------------------------- autenticação e chamadas
class Fabric:
    def __init__(self):
        from azure.identity import InteractiveBrowserCredential
        self.cred = InteractiveBrowserCredential()
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

def workspace(fab, nome, cap_id, descricao):
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

def workspace_existente(fab, ws_id, cap_id):
    """Usa um workspace que já existe (pelo ID). Não renomeia nem troca a capacidade sozinho."""
    w = fab.chamar("GET", f"/workspaces/{ws_id}")
    if w.get("capacityId", "").lower() != cap_id.lower():
        sys.exit(f"O workspace '{w['displayName']}' não está na capacidade configurada. "
                 "Atribua-o à F4 em Configurações do workspace > Licença e rode de novo.")
    print(f"  workspace {w['displayName']}: existente ({ws_id})")
    return ws_id

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

def executar_notebook(fab, ws, nb_id, nome):
    print(f"  executando {nome} ...")
    r = requests.post(f"{API}/workspaces/{ws}/items/{nb_id}/jobs/instances?jobType=RunNotebook",
                      headers=fab._headers(), json={}, timeout=120)
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
    args = ap.parse_args()
    cfg = json.loads(pathlib.Path(args.config).read_text(encoding="utf-8"))
    fab = Fabric()

    if args.listar_conexoes:
        for c in fab.listar("/connections"):
            det = c.get("connectionDetails", {})
            print(f"{c['id']}  {c['displayName']:<40} {c.get('connectivityType',''):<22} "
                  f"{det.get('type','')}: {det.get('path','')}")
        return

    print("1/5 Capacidade")
    cap = capacidade(fab, cfg["capacidade"])

    print("2/5 Workspaces")
    ids_ws = {}
    for nome, w in cfg["workspaces"].items():
        if isinstance(w, dict) and w.get("id"):
            ids_ws[nome] = workspace_existente(fab, w["id"], cap)
        else:
            desc = w.get("descricao", "") if isinstance(w, dict) else w
            ids_ws[nome] = workspace(fab, nome, cap, desc)

    manifesto = {}
    for nome_ws in cfg["implantar_itens_em"]:
        ws = ids_ws[nome_ws]
        print(f"3/5 Lakehouses em {nome_ws}")
        lh = {n: item(fab, ws, n, "Lakehouse", rota=f"/workspaces/{ws}/lakehouses") for n in LAKEHOUSES}

        print(f"4/5 Notebooks em {nome_ws}")
        nb = {}
        for n in NOTEBOOKS:
            conteudo = (RAIZ / "notebooks" / f"{n}.ipynb").read_text(encoding="utf-8")
            nb[n] = item(fab, ws, n, "Notebook", {"notebook-content.ipynb": conteudo})

        print(f"5/5 Pipeline em {nome_ws}")
        definicao = pipeline_mestre.gerar(ws, lh["lh_bronze"], nb["nb_preparar_ciclo"],
                                          nb["nb_orquestrador"], cfg["origens"], cfg.get("paralelismo", 4))
        (RAIZ / f"{PIPELINE}_{nome_ws}.json").write_text(json.dumps(definicao, indent=2, ensure_ascii=False))
        pl = item(fab, ws, PIPELINE, "DataPipeline",
                  {"pipeline-content.json": json.dumps(definicao, ensure_ascii=False)})

        manifesto[nome_ws] = {"workspace": ws, "lakehouses": lh, "notebooks": nb, "pipeline": pl}
        if args.executar_setup:
            executar_notebook(fab, ws, nb["nb_setup_controle"], "nb_setup_controle")

    (RAIZ / "manifesto_implantacao.json").write_text(json.dumps(
        {"workspaces": ids_ws, "itens": manifesto}, indent=2, ensure_ascii=False))
    print("\nConcluído. IDs gravados em manifesto_implantacao.json")

if __name__ == "__main__":
    main()
