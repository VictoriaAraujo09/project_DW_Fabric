# %% [markdown]
# ### Configuração comum (não alterar)
# Funciona em DW_DEV e DW_PROD sem mudança: o caminho do OneLake usa o nome do workspace atual.

# %%
import os, json, datetime as dt
import pyarrow as pa
import duckdb
from deltalake import DeltaTable, write_deltalake

LH_BRONZE, LH_SILVER, LH_GOLD = "lh_bronze", "lh_silver", "lh_gold"
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

def ler(lakehouse, tabela, filtros=None):
    dt_ = DeltaTable(caminho(lakehouse, tabela), storage_options=_opts())
    return dt_.to_pyarrow_table(filters=filtros) if filtros else dt_.to_pyarrow_table()

def gravar(lakehouse, tabela, dados, modo="append"):
    kw = {"schema_mode": "overwrite"} if modo == "overwrite" else {}
    write_deltalake(caminho(lakehouse, tabela), dados, mode=modo,
                    storage_options=_opts(), **kw)

def tabela_delta(lakehouse, tabela):
    return DeltaTable(caminho(lakehouse, tabela), storage_options=_opts())

def para_arrow(rel):
    """Converte resultado DuckDB em pyarrow.Table (compatível com várias versões)."""
    r = rel.fetch_arrow_table() if hasattr(rel, "fetch_arrow_table") else rel.arrow()
    return r.read_all() if hasattr(r, "read_all") else r

def agora_utc():
    return dt.datetime.now(dt.timezone.utc)
