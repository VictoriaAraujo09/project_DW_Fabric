# %% [markdown]
# # nb_ingestao_api
# Ingestão das origens **API** (hoje: Nexti) na Bronze. Roda no pipeline mestre em paralelo às cópias
# dos bancos e devolve ao orquestrador a mesma lista `id|novo_watermark|linhas` que o pipeline monta
# para as origens SQL. O orquestrador cuida da Silver, dos watermarks e do log.
#
# - Credenciais no Azure Key Vault (`configuracao_api`), nunca no código.
# - Só os campos de config/nexti.yml são gravados; credenciais ficam fora. Dados pessoais e de saúde entram
#   como no DW do Airflow (decisão de 26/10/2026): CID, médico e justificativa nas ausências; CPF, e-mail,
#   telefone, PIS e geolocalização nas marcações, colaboradores e postos.
# - Incremental por janela (data de alteração ou data de referência), com recuperação de histórico em
#   blocos de até `max_dias_por_execucao` dias por ciclo.

# %% tags=["parameters"]
lote = "manual"
ciclo = "COMPLETO"            # já resolvido pelo nb_preparar_ciclo (COMPLETO | FATOS)
origens = ""                  # filtro opcional, ex.: "NEXTI"
tabelas = ""                  # filtro opcional de endpoints, ex.: "ABSENCES,WORKPLACETRANSFERS" (rotinas)
configuracao_api = "{}"       # {"NEXTI": {"key_vault": "https://<cofre>.vault.azure.net/"}}
inicio_historico = "2025-01-01"
max_dias_por_execucao = 45
janelas_paralelas = 1         # janelas buscadas ao mesmo tempo; com 4 a Nexti segurou as chamadas (26/10/2026)

# %%
#@@COMUM@@

# %%
import time, requests
from concurrent.futures import ThreadPoolExecutor

NEXTI_URL = "https://api.nexti.com"
FMT_NEXTI = "%d%m%Y%H%M%S"
TAMANHO_PAGINA = 60000  # um dia de marcações (~45 mil) numa chamada: o limite da API é por chamada pesada

# Endpoints e campos gravados ficam em config/nexti.yml (no Fabric: lakehouse Gold → Files/config);
# endpoint novo = bloco novo no YAML + linha em config/tabelas.yml, sem mexer neste notebook.
_NEXTI = ler_config("nexti.yml")["endpoints"]
ENDPOINTS_NEXTI = {k: {c: v for c, v in ep.items() if c not in ("campos", "rotina", "inicio_historico")}
                   for k, ep in _NEXTI.items()}
COLUNAS_NEXTI = {k: ep["campos"] for k, ep in _NEXTI.items()}
# Tabelas com rotina dedicada (nb_rotina_nexti, a cada poucos minutos): o ciclo geral não as carrega,
# para a mesma janela não ser copiada duas vezes. Só entram quando pedidas em `tabelas`.
TABELAS_ROTINA = {k for k, ep in _NEXTI.items() if ep.get("rotina")}
# Início do histórico por endpoint, quando difere de `inicio_historico`.
INICIO_HISTORICO_TABELA = {k: str(ep["inicio_historico"]) for k, ep in _NEXTI.items() if ep.get("inicio_historico")}

TIPO_SPARK = {"inteiro": "bigint", "numero": "double", "texto": "string", "json": "string",
              "booleano": "boolean", "data_hora": "timestamp"}
DATAS_FICTICIAS = ("01011900", "01011940", "01011970")  # "sem data" na Nexti

# %%
def segredo(cfg_origem, nome, variavel_local):
    """Key Vault no Fabric; variável de ambiente nos testes locais."""
    if _LOCAL:
        return os.environ[variavel_local]
    return notebookutils.credentials.getSecret(cfg_origem["key_vault"], nome)

class ClienteNexti:
    def __init__(self, cfg_origem):
        self.cfg, self.cabecalho, self.expira = cfg_origem, None, 0

    def _token(self):
        if time.time() > self.expira - 600:
            r = requests.post(f"{NEXTI_URL}/security/oauth/token", data={"grant_type": "client_credentials"},
                              auth=(segredo(self.cfg, "nexti-client-id", "NEXTI_CLIENT_ID"),
                                    segredo(self.cfg, "nexti-client-secret", "NEXTI_CLIENT_SECRET")),
                              timeout=60)
            r.raise_for_status()
            j = r.json()
            self.cabecalho = {"Authorization": f"Bearer {j['access_token']}"}
            self.expira = time.time() + int(j.get("expires_in", 3600))
        return self.cabecalho

    def get(self, caminho, params):
        for tentativa in range(10):
            inicio = time.time()
            url = caminho if caminho.startswith("http") else f"{NEXTI_URL}{caminho}"
            r = requests.get(url, headers=self._token(), params=params, timeout=900)
            if r.status_code == 429 or r.status_code >= 500:
                espera = int(r.headers.get("Retry-After", 30 * (tentativa + 1)))
                print(f"  {caminho} página {params.get('page', '-')}: HTTP {r.status_code}, nova tentativa em {espera}s")
                time.sleep(espera)
                continue
            print(f"  {caminho} página {params.get('page', '-')}: {time.time() - inicio:.0f}s")
            if r.status_code == 409 and "nenhum dado" in r.text:   # a API responde 409 para período vazio
                return {}
            if r.status_code >= 400:
                raise RuntimeError(f"Nexti {caminho} -> {r.status_code}: {r.text[:300]}")
            return r.json()
        raise RuntimeError(f"Nexti {caminho}: limite de tentativas (429/5xx)")

    def itens(self, ep, ini=None, fim=None):
        """Gera os itens de um endpoint, página a página (ou de uma vez, se não for paginado)."""
        fmt_ini = ini.strftime(FMT_NEXTI) if ini else None
        fmt_fim = fim.strftime(FMT_NEXTI) if fim else None
        caminho = ep["caminho"].format(ini=fmt_ini, fim=fmt_fim)
        params = {k: v.format(ini=fmt_ini, fim=fmt_fim) for k, v in ep.get("params", {}).items()}
        if ep["itens"] == "value":                      # resposta única, sem paginação
            yield from (self.get(caminho, params).get("value") or [])
            return
        pagina = 0
        while True:
            j = self.get(caminho, {**params, "page": pagina, "size": ep.get("tamanho", TAMANHO_PAGINA)})
            yield from (j.get("content") or [])
            if j.get("last", True) or not j.get("content"):
                return
            pagina += 1

def converter(valor, tipo, utc=False):
    if valor is None or valor == "" or valor == [] or valor == {}:
        return None
    if tipo == "data_hora":
        s = str(valor)
        if s.startswith(DATAS_FICTICIAS):
            return None
        d = dt.datetime.strptime(s, FMT_NEXTI if len(s) == 14 else "%d%m%Y")
        if utc:
            d = d.replace(tzinfo=dt.timezone.utc).astimezone(FUSO_BRT).replace(tzinfo=None)
        # horário de Brasília gravado como está, rotulado UTC (mesma convenção da cópia dos bancos)
        return d.replace(tzinfo=dt.timezone.utc)
    if tipo == "json":
        return json.dumps(valor, ensure_ascii=False)
    if tipo == "inteiro":
        return int(valor)
    if tipo == "numero":
        return float(valor)
    if tipo == "booleano":
        return bool(valor)
    return str(valor)

def para_tabela(chave, itens):
    """DataFrame com os campos de config/nexti.yml e as colunas técnicas. Devolve (DataFrame, linhas)."""
    colunas, utc = COLUNAS_NEXTI[chave], ENDPOINTS_NEXTI[chave].get("utc", False)
    schema = ", ".join([f"`{c}` {TIPO_SPARK[t]}" for c, t in colunas.items()]
                       + ["_origem string", "_lote string", "_dt_carga timestamp"])
    agora = agora_utc()
    linhas = [tuple(converter(i.get(c), t, utc) for c, t in colunas.items()) + ("NEXTI", lote, agora)
              for i in itens]
    return criar(linhas, schema), len(linhas)

def janelas(ini, fim, dias, por_dia):
    """Divide [ini, fim] em blocos de até `dias` dias (limite da API)."""
    passo = dt.timedelta(days=dias) - (dt.timedelta(0) if por_dia else dt.timedelta(seconds=1))
    while ini <= fim:
        bloco_fim = min(fim, ini + passo)
        yield ini, bloco_fim
        ini = bloco_fim + (dt.timedelta(days=1) if por_dia else dt.timedelta(seconds=1))

def periodo(cfg, agora_brt):
    """Janela do ciclo: do último watermark (menos a margem de segurança) até agora, limitada a
    max_dias_por_execucao a partir do watermark para recuperar o histórico em blocos."""
    wm = (cfg["ultimo_watermark"] or "").strip()
    inicio = INICIO_HISTORICO_TABELA.get(cfg["tabela_origem"], inicio_historico)
    base = dt.datetime.fromisoformat(wm[:19]) if wm else dt.datetime.fromisoformat(inicio)
    ini = base - dt.timedelta(minutes=cfg["janela_seguranca_min"] or 0) if wm else base
    fim = min(agora_brt, base + dt.timedelta(days=int(max_dias_por_execucao)))
    return ini, fim

def carregar(cliente, cfg):
    """Copia um endpoint para a Bronze (append, em blocos). Devolve (novo watermark, linhas)."""
    ep = ENDPOINTS_NEXTI[cfg["tabela_origem"]]
    destino = nome_bronze(cfg["tabela_destino"])
    fuso = dt.timezone.utc if ep.get("utc") else FUSO_BRT       # fuso da janela e do watermark
    agora = dt.datetime.now(fuso).replace(tzinfo=None, microsecond=0)
    if cfg["modo_carga"] == "FULL":
        tabela, n = para_tabela(cfg["tabela_origem"], cliente.itens(ep))
        gravar(LH_BRONZE, destino, tabela)
        return agora.isoformat(), n
    por_dia = ep.get("por_dia", False)
    ini, fim = periodo(cfg, agora)
    if por_dia:
        ini, fim = ini.replace(hour=0, minute=0, second=0), fim.replace(hour=0, minute=0, second=0)
    def buscar(janela):
        a, b = janela
        return para_tabela(cfg["tabela_origem"], cliente.itens(ep, a, b.replace(hour=23, minute=59, second=59)
                                                                if por_dia else b))
    # janelas buscadas em `janelas_paralelas` ao mesmo tempo (a API leva ~2 min por dia de marcações);
    # a gravação segue na ordem, e o watermark só avança se todas as janelas derem certo
    total = 0
    with ThreadPoolExecutor(max_workers=max(1, int(janelas_paralelas))) as pool:
        for tabela, n in pool.map(buscar, list(janelas(ini, fim, ep["janela_dias"], por_dia))):
            if n:
                gravar(LH_BRONZE, destino, tabela)
                total += n
    return fim.isoformat(), total

# %%
cfg_api = json.loads(configuracao_api or "{}")
filtro = {o.strip().upper() for o in origens.split(",") if o.strip()}
so_tabelas = {t.strip().upper() for t in tabelas.split(",") if t.strip()}
tipos = {"DIM", "FATO"} if ciclo == "COMPLETO" else {"FATO"}

def escolhida(cfg):
    if so_tabelas:
        return cfg["tabela_origem"] in so_tabelas
    return cfg["tipo"] in tipos and cfg["tabela_origem"] not in TABELAS_ROTINA
ativas = {o["origem"] for o in coletar(ler(LH_BRONZE, "ctl_origens"))
          if o["ativo"] and o["dialeto"] == "API" and (not filtro or o["origem"] in filtro)}

resultados = []
for origem in sorted(ativas):
    if origem != "NEXTI":
        raise ValueError(f"Origem API sem conector implementado: {origem}")
    cliente = ClienteNexti(cfg_api.get(origem, {}))
    for cfg in coletar(ler(LH_BRONZE, "ctl_tabelas")):
        if cfg["origem"] == origem and cfg["ativo"] and escolhida(cfg):
            inicio = time.time()
            # falha de uma tabela não impede as outras: vai para o orquestrador como FALHA (watermark parado)
            try:
                novo, linhas = carregar(cliente, cfg)
            except Exception as e:
                print(f"{cfg['id']}: FALHA {e}")
                resultados.append(f"{cfg['id']}|FALHA|0|{str(e)[:500]}")
                continue
            print(f"{cfg['id']}: {linhas} linhas até {novo} ({time.time() - inicio:.0f}s)")
            resultados.append(f"{cfg['id']}|{novo}|{linhas}")

sair(json.dumps(resultados))
