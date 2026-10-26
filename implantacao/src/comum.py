# %% [markdown]
# ### Configuração comum (não alterar)
# Funciona em DW_DEV e DW_PROD sem mudança: o caminho do OneLake usa o nome do workspace atual.

# %%
import os, json, datetime as dt
import pyarrow as pa
import duckdb
from deltalake import DeltaTable, write_deltalake

# Não renomear os lakehouses: o banco do SQL endpoint guarda o nome da criação e a sincronização falha
LH_BRONZE, LH_SILVER, LH_GOLD = "Bronze", "Silver", "Gold"
FUSO_BRT = dt.timezone(dt.timedelta(hours=-3))

_LOCAL = os.environ.get("DW_LOCAL_ROOT")  # usado só em testes fora do Fabric
if _LOCAL:
    def _opts():
        return None
    def caminho(lakehouse, tabela):
        return f"{_LOCAL}/{lakehouse}/{tabela}"
    def sair(valor):
        globals()["_EXIT_VALUE"] = valor
else:
    import notebookutils
    _WS = notebookutils.runtime.context["currentWorkspaceName"]
    def _opts():
        return {"bearer_token": notebookutils.credentials.getToken("storage"),
                "use_fabric_endpoint": "true"}
    def caminho(lakehouse, tabela):
        return f"abfss://{_WS}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/Tables/{tabela}"
    def sair(valor):
        notebookutils.notebook.exit(valor)

def existe(lakehouse, tabela):
    try:
        DeltaTable(caminho(lakehouse, tabela), storage_options=_opts())
        return True
    except Exception:
        return False

def ler(lakehouse, tabela, filtros=None, colunas=None):
    dt_ = DeltaTable(caminho(lakehouse, tabela), storage_options=_opts())
    kw = {k: v for k, v in (("filters", filtros), ("columns", colunas)) if v}
    return dt_.to_pyarrow_table(**kw)

def gravar(lakehouse, tabela, dados, modo="append"):
    if modo == "append":
        acrescentar_colunas(lakehouse, tabela, dados.schema)
    kw = {"schema_mode": "overwrite"} if modo == "overwrite" else {}
    write_deltalake(caminho(lakehouse, tabela), dados, mode=modo,
                    storage_options=_opts(), **kw)

def acrescentar_colunas(lakehouse, tabela, schema):
    """Coluna nova na origem (ex.: campo novo da API): a tabela é regravada com a coluna, vazia nas linhas
    antigas, antes do append ou do MERGE. Não depende da versão do deltalake suportar evolução de schema."""
    if not existe(lakehouse, tabela):
        return
    atual = tabela_delta(lakehouse, tabela)
    nomes = {f.name for f in atual.schema().fields}
    novas = [c for c in schema if c.name not in nomes]
    if not novas:
        return
    dados = atual.to_pyarrow_table()
    for c in novas:
        dados = dados.append_column(c, pa.nulls(dados.num_rows, type=c.type))
    write_deltalake(caminho(lakehouse, tabela), dados, mode="overwrite", schema_mode="overwrite",
                    storage_options=_opts())
    print(f"{tabela}: colunas novas {[c.name for c in novas]}")

def tabela_delta(lakehouse, tabela):
    return DeltaTable(caminho(lakehouse, tabela), storage_options=_opts())

def para_arrow(rel):
    """Converte resultado DuckDB em pyarrow.Table (compatível com várias versões)."""
    r = rel.fetch_arrow_table() if hasattr(rel, "fetch_arrow_table") else rel.arrow()
    return r.read_all() if hasattr(r, "read_all") else r

def nome_bronze(tabela_destino):
    """ctl_tabelas guarda o nome base (fato_contas_receber_gs); cada camada recebe o seu prefixo."""
    return f"bronze_{tabela_destino}"

def nome_silver(tabela_destino):
    return f"silver_{tabela_destino}"

def agora_utc():
    return dt.datetime.now(dt.timezone.utc)

def watermark_gold(gold, fonte):
    """Último _dt_carga da Silver já levado para esta Gold (tabela ctl_gold na Bronze). As Golds são
    incrementais por esta marca, e não pelo lote: se uma Gold falhar, a próxima execução retoma dali."""
    if not existe(LH_BRONZE, "ctl_gold"):
        return None
    for r in ler(LH_BRONZE, "ctl_gold").to_pylist():
        if r["gold"] == gold and r["fonte"] == fonte:
            return r["ultimo_dt_carga"]
    return None

def gravar_watermark_gold(gold, fonte, valor):
    linha = pa.Table.from_pylist([{"gold": gold, "fonte": fonte, "ultimo_dt_carga": valor,
                                   "dt_atualizacao": agora_utc()}],
                                 schema=pa.schema([("gold", pa.string()), ("fonte", pa.string()),
                                                   ("ultimo_dt_carga", pa.timestamp("us", tz="UTC")),
                                                   ("dt_atualizacao", pa.timestamp("us", tz="UTC"))]))
    if not existe(LH_BRONZE, "ctl_gold"):
        gravar(LH_BRONZE, "ctl_gold", linha, modo="overwrite")
        return
    (tabela_delta(LH_BRONZE, "ctl_gold")
        .merge(linha, predicate="t.gold = s.gold AND t.fonte = s.fonte", source_alias="s", target_alias="t")
        .when_matched_update_all().when_not_matched_insert_all().execute())

def consulta(nome):
    """Texto de uma consulta da pasta `consultas` (no Fabric: lakehouse Gold → Files/consultas/<nome>.sql,
    copiada de implantacao/consultas pela implantação). Nos testes locais, lê de DW_CONSULTAS."""
    if _LOCAL:
        with open(os.path.join(os.environ["DW_CONSULTAS"], f"{nome}.sql"), encoding="utf-8") as arq:
            return arq.read()
    return notebookutils.fs.head(
        f"abfss://{_WS}@onelake.dfs.fabric.microsoft.com/{LH_GOLD}.Lakehouse/Files/consultas/{nome}.sql", 10_000_000)

def cabecalho(sql):
    """Configuração no topo do .sql, em linhas `-- @chave valor` (ex.: `-- @destino gold_fato_coberturas`)."""
    cfg = {}
    for linha in sql.splitlines():
        linha = linha.strip()
        if linha.startswith("-- @"):
            chave, _, valor = linha[4:].partition(" ")
            cfg[chave] = valor.strip()
    return cfg

def ler_config(nome):
    """Arquivo YAML da pasta config (no Fabric: lakehouse Gold → Files/config/<nome>, copiado de config/ pela
    implantação). Nos testes locais, lê de DW_CONFIG."""
    import yaml
    if _LOCAL:
        with open(os.path.join(os.environ["DW_CONFIG"], nome), encoding="utf-8") as arq:
            return yaml.safe_load(arq)
    return yaml.safe_load(notebookutils.fs.head(
        f"abfss://{_WS}@onelake.dfs.fabric.microsoft.com/{LH_GOLD}.Lakehouse/Files/config/{nome}", 10_000_000))
