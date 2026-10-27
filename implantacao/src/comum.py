# %% [markdown]
# ### Configuração comum (não alterar)
# Funciona em DW_DEV e DW_PROD sem mudança: o caminho do OneLake usa o nome do workspace atual.
# Motor: PySpark com Delta (Fabric Runtime 2.0: Spark 4.1, Delta 4.2).

# %%
import os, json, time, datetime as dt
from pyspark.sql import SparkSession, DataFrame, Window, functions as F, types as T
from delta.tables import DeltaTable

# Não renomear os lakehouses: o banco do SQL endpoint guarda o nome da criação e a sincronização falha
LH_BRONZE, LH_SILVER, LH_GOLD = "Bronze", "Silver", "Gold"
FUSO_BRT = dt.timezone(dt.timedelta(hours=-3))

# processo e sessão em UTC: as datas da Bronze são horário de Brasília rotulado UTC e não podem ser
# deslocadas na leitura, no CAST para DATE nem na volta para o Python (collect devolve hora local)
os.environ["TZ"] = "UTC"
time.tzset()
spark = SparkSession.builder.getOrCreate()   # no Fabric a sessão já existe
spark.conf.set("spark.sql.session.timeZone", "UTC")
# coluna nova na origem entra no MERGE e no append sem recriar a tabela
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

_LOCAL = os.environ.get("DW_LOCAL_ROOT")  # usado só em testes fora do Fabric
if _LOCAL:
    def caminho(lakehouse, tabela):
        return f"{_LOCAL}/{lakehouse}/{tabela}"
    def sair(valor):
        globals()["_EXIT_VALUE"] = valor
    spark.sparkContext.setCheckpointDir(f"{_LOCAL}/_checkpoints")
else:
    import notebookutils
    # IDs, e não nomes, no endereço do OneLake: o ABFS do Spark não aceita os espaços do nome do
    # workspace ("Squad Dados - DW Dev")
    _WS = notebookutils.runtime.context["currentWorkspaceId"]
    _LH = {}
    def raiz(lakehouse):
        """abfss://<workspace>@onelake.../<lakehouse>: + /Tables/<tabela> ou + /Files/<caminho>."""
        if lakehouse not in _LH:
            item = notebookutils.lakehouse.get(lakehouse)
            _LH[lakehouse] = item["id"] if isinstance(item, dict) else item.id
        return f"abfss://{_WS}@onelake.dfs.fabric.microsoft.com/{_LH[lakehouse]}"
    def caminho(lakehouse, tabela):
        return f"{raiz(lakehouse)}/Tables/{tabela}"
    def sair(valor):
        notebookutils.notebook.exit(valor)
    # resultados intermediários de materializar(), uma pasta por sessão (a rotina diária apaga as antigas)
    CHECKPOINTS = f"{raiz(LH_BRONZE)}/Files/_checkpoints"
    spark.sparkContext.setCheckpointDir(f"{CHECKPOINTS}/{spark.sparkContext.applicationId}")

def existe(lakehouse, tabela):
    try:
        return DeltaTable.isDeltaTable(spark, caminho(lakehouse, tabela))
    except Exception:
        return False

def ler(lakehouse, tabela, filtros=None, colunas=None):
    """DataFrame da tabela. filtros: [(coluna, operador, valor)], operadores =, >, >=, <, <=, in."""
    df = spark.read.format("delta").load(caminho(lakehouse, tabela))
    for coluna, op, valor in filtros or []:
        c = F.col(f"`{coluna}`")
        df = df.where(c.isin(list(valor)) if op == "in" else
                      {"=": c == valor, ">": c > valor, ">=": c >= valor, "<": c < valor, "<=": c <= valor}[op])
    return df.select(*[f"`{c}`" for c in colunas]) if colunas else df

def gravar(lakehouse, tabela, dados, modo="append"):
    """append acrescenta colunas novas (mergeSchema); overwrite troca dados e schema."""
    (dados.write.format("delta").mode(modo)
        .option("overwriteSchema" if modo == "overwrite" else "mergeSchema", "true")
        .save(caminho(lakehouse, tabela)))

def tabela_delta(lakehouse, tabela):
    return DeltaTable.forPath(spark, caminho(lakehouse, tabela))

def mesclar(lakehouse, tabela, dados, chaves):
    """MERGE pela chave: atualiza as linhas existentes e insere as novas (cria a tabela se não existir)."""
    if not existe(lakehouse, tabela):
        gravar(lakehouse, tabela, dados, modo="overwrite")
        return
    (tabela_delta(lakehouse, tabela).alias("t")
        .merge(dados.alias("s"), " AND ".join(f"t.`{c}` = s.`{c}`" for c in chaves))
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

def apagar(lakehouse, tabela, coluna, valores, extra=None):
    """DELETE das linhas com `coluna` em `valores`, em blocos (listas grandes de ids)."""
    alvo = tabela_delta(lakehouse, tabela)
    valores = list(valores)
    for i in range(0, len(valores), 5000):
        cond = F.col(f"`{coluna}`").isin(valores[i:i + 5000])
        alvo.delete(cond & F.expr(extra) if extra else cond)

def linhas_deletadas(lakehouse, tabela):
    """Linhas apagadas pela última operação (o DELETE do Spark não devolve métricas)."""
    m = tabela_delta(lakehouse, tabela).history(1).select("operationMetrics").first()[0] or {}
    return int(m.get("numDeletedRows") or 0)

def criar(linhas, schema):
    """DataFrame a partir de dicts. schema em DDL, ex.: "id string, n bigint, dt timestamp"."""
    return spark.createDataFrame(linhas, schema=schema, verifySchema=False)

def _utc(v):
    return v.replace(tzinfo=dt.timezone.utc) if isinstance(v, dt.datetime) and v.tzinfo is None else v

def coletar(df):
    """Linhas como dicts, com timestamp em UTC com fuso (o collect devolve sem fuso, em UTC)."""
    return [{k: _utc(v) for k, v in r.asDict().items()} for r in df.collect()]

def materializar(df):
    """Executa já e grava o resultado em arquivo: necessário antes de apagar ou regravar a tabela que o
    originou. Em arquivo, e não na memória (localCheckpoint): um executor desligado no meio da execução
    levava o resultado junto (CHECKPOINT_RDD_BLOCK_ID_NOT_FOUND)."""
    return df.checkpoint(eager=True)

def nome_bronze(tabela_destino):
    """ctl_tabelas guarda o nome base (fato_contas_receber_gs); cada camada recebe o seu prefixo."""
    return f"bronze_{tabela_destino}"

def nome_silver(tabela_destino):
    return f"silver_{tabela_destino}"

def agora_utc():
    return dt.datetime.now(dt.timezone.utc)

def maximo(df, coluna):
    return _utc(df.agg(F.max(f"`{coluna}`")).first()[0])

def watermark_gold(gold, fonte):
    """Último _dt_carga da Silver já levado para esta Gold (tabela ctl_gold na Bronze). As Golds são
    incrementais por esta marca, e não pelo lote: se uma Gold falhar, a próxima execução retoma dali."""
    if not existe(LH_BRONZE, "ctl_gold"):
        return None
    r = ler(LH_BRONZE, "ctl_gold").where((F.col("gold") == gold) & (F.col("fonte") == fonte)).first()
    return _utc(r["ultimo_dt_carga"]) if r else None

def gravar_watermark_gold(gold, fonte, valor):
    linha = criar([{"gold": gold, "fonte": fonte, "ultimo_dt_carga": valor, "dt_atualizacao": agora_utc()}],
                  "gold string, fonte string, ultimo_dt_carga timestamp, dt_atualizacao timestamp")
    com_nova_tentativa(lambda: mesclar(LH_BRONZE, "ctl_gold", linha, ["gold", "fonte"]))

def com_nova_tentativa(acao, tentativas=5):
    """Execuções diferentes podem gravar a mesma tabela de controle ao mesmo tempo: o commit que perde a
    disputa é refeito sobre a versão nova da tabela."""
    for n in range(tentativas):
        try:
            return acao()
        except Exception as e:
            msg = str(e).lower()
            if n == tentativas - 1 or not any(p in msg for p in ("concurrent", "conflict", "commit")):
                raise
            time.sleep(5 * (n + 1))

def consulta(nome):
    """Texto de uma consulta da pasta `consultas` (no Fabric: lakehouse Gold → Files/consultas/<nome>.sql,
    copiada de implantacao/consultas pela implantação). Nos testes locais, lê de DW_CONSULTAS."""
    if _LOCAL:
        with open(os.path.join(os.environ["DW_CONSULTAS"], f"{nome}.sql"), encoding="utf-8") as arq:
            return arq.read()
    return notebookutils.fs.head(
        f"{raiz(LH_GOLD)}/Files/consultas/{nome}.sql", 10_000_000)

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
        f"{raiz(LH_GOLD)}/Files/config/{nome}", 10_000_000))
