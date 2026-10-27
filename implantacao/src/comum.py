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

# ---------------------------------------------------------------- aviso no Teams por execução
# Resultado de cada ciclo (nb_orquestrador) e de cada rotina (nb_rotina_nexti) vai para o Teams pelo
# mesmo Workflow do nb_monitoramento. Config: config/qualidade.yml, seção `monitoramento.notificacao`.
_ICONE_STATUS = {"SUCESSO": "✅", "PARCIAL": "⚠️", "FALHA": "❌"}
_ICONE_COMP = {"NORMAL": "🟢", "ATENÇÃO": "🟡", "ALERTA": "🔴"}
_COR_COMP = {"NORMAL": "Good", "ATENÇÃO": "Warning", "ALERTA": "Attention"}

def _config_notificacao():
    return ((ler_config("qualidade.yml").get("monitoramento") or {}).get("notificacao")) or {}

def incrementar_contador(chave):
    """Contador persistente por chave (ctl_contador na Bronze); devolve o novo valor. Usado para avisar
    só 1 a cada N execuções das rotinas de alta frequência."""
    atual = 0
    if existe(LH_BRONZE, "ctl_contador"):
        r = ler(LH_BRONZE, "ctl_contador").where(F.col("chave") == chave).first()
        atual = int(r["n"]) if r else 0
    linha = criar([{"chave": chave, "n": atual + 1, "dt": agora_utc()}], "chave string, n bigint, dt timestamp")
    com_nova_tentativa(lambda: mesclar(LH_BRONZE, "ctl_contador", linha, ["chave"]))
    return atual + 1

def enviar_teams(corpo_cartao):
    """Posta um Adaptive Card no Workflow do Teams (URL no Key Vault, cofre e segredo em qualidade.yml).
    Não lança: uma notificação que falha não derruba a execução. Devolve o status do envio em texto."""
    notif = _config_notificacao()
    if _LOCAL:
        return "local"
    if not notif.get("teams_workflow_segredo") or not notif.get("key_vault"):
        return "sem workflow"
    try:
        import requests
        url = notebookutils.credentials.getSecret(notif["key_vault"], notif["teams_workflow_segredo"])
        payload = {"type": "message", "destinatario": notif.get("destinatario"),
                   "attachments": [{"contentType": "application/vnd.microsoft.card.adaptive", "content": {
                       "$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "type": "AdaptiveCard",
                       "version": "1.4", "body": corpo_cartao}}]}
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
        return f"enviado para {notif.get('destinatario') or 'o Workflow'}"
    except Exception as e:   # sem Workflow configurado ou fora do ar: a execução segue normal
        return f"não notificado: {str(e)[:200]}"

def comportamento_execucao(status, duracao_s, limite_atencao=None):
    """🔴 ALERTA: a base não atualizou (FALHA/PARCIAL). 🟡 ATENÇÃO: concluiu, mas acima do tempo esperado
    (`limite_atencao`). 🟢 NORMAL: concluiu no tempo normal."""
    if status in ("FALHA", "PARCIAL"):
        return "ALERTA"
    if limite_atencao and duracao_s and duracao_s > limite_atencao:
        return "ATENÇÃO"
    return "NORMAL"

def avisar_execucao(base, objeto, status, duracao_s, erro="", resumo="", tipo="CICLO", chave=None):
    """Aviso no Teams do resultado de uma execução (ciclo do pipeline ou rotina). As rotinas da Nexti só
    avisam o sucesso NORMAL 1 vez a cada `nexti_cada` execuções (config); falha, PARCIAL e ATENÇÃO avisam
    sempre. Devolve o status do envio ('enviado...', 'local', 'agrupado', 'desligado', 'sem workflow')."""
    notif = _config_notificacao()
    if not notif.get("avisar_execucoes", True):
        return "desligado"
    comp = comportamento_execucao(status, duracao_s, (notif.get("duracao_atencao_s") or {}).get(tipo))
    cada = int(notif.get("nexti_cada", 1)) if str(base).upper() == "NEXTI" else 1
    if comp == "NORMAL" and cada > 1 and chave and incrementar_contador(f"AVISO_{chave}") % cada != 1:
        return "agrupado"
    fatos = [("BASE", base), ("Notebook/Pipeline", objeto),
             ("Status", f"{_ICONE_STATUS.get(status, '•')} {status}")]
    if erro:
        fatos.append(("Erro", erro[:800]))
    fatos.append(("Horário", agora_utc().astimezone(FUSO_BRT).strftime("%d/%m/%Y %H:%M")))
    fatos.append(("Comportamento", f"{_ICONE_COMP[comp]} {comp}"))
    corpo = [{"type": "TextBlock", "size": "Medium", "weight": "Bolder", "color": _COR_COMP[comp],
              "text": f"{_ICONE_COMP[comp]} DW Fabric — {comp}"}]
    if resumo:
        corpo.append({"type": "TextBlock", "isSubtle": True, "wrap": True, "spacing": "None", "text": resumo[:300]})
    corpo.append({"type": "FactSet", "facts": [{"title": t, "value": str(v)} for t, v in fatos]})
    return enviar_teams(corpo)
