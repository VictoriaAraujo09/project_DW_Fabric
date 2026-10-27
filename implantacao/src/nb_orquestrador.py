# %% [markdown]
# # nb_orquestrador
# Execução única por ciclo, chamada no fim do pipeline mestre:
# 1. Bronze → Silver: MERGE pela chave de negócio só das tabelas que receberam linhas neste lote
# 2. Gold: recalcula as tabelas Gold afetadas (notebooks cadastrados em `GOLD_NOTEBOOKS`, Fase 4)
# 3. Avança o `ultimo_watermark` **somente se tudo deu certo**
# 4. Grava o log do ciclo em `ctl_log`
#
# Com `status_ciclo = "FALHA"` (caminho de erro do pipeline), só registra o log e não mexe em nada.

# %% tags=["parameters"]
lote = "manual"
ciclo = "FATOS"
resultados = "[]"       # lista "id|novo_watermark|linhas_copiadas", montada pelo pipeline
resultados_api = "[]"   # mesma lista, devolvida pelo nb_ingestao_api (origens API); "FALHOU" se ele quebrou
planejadas = "[]"       # ids das tabelas de banco que o nb_preparar_ciclo mandou copiar
status_ciclo = "SUCESSO"
mensagem = ""
origens = ""            # origens deste ciclo, ex.: "GS,4S" (vazio = todas): limita a retenção a elas
notificar = "SIM"       # aviso do ciclo no Teams; "NAO" quando chamado por uma rotina (ela faz o próprio)

# %%
#@@COMUM@@

# %%
# Tabela Silver -> lista de notebooks Gold a executar quando ela muda (preencher na Fase 4)
GOLD_NOTEBOOKS = {
    # "gs_lan": ["nb_gold_f_lancamentos"],
}

# Retenção da Bronze (dias de lotes mantidos), aplicada só no ciclo COMPLETO.
# FULL: cada lote é uma cópia inteira, basta guardar poucos dias para reprocessar.
RETENCAO_DIAS = {"FULL": 3, "JANELA": 3, "INCREMENTAL": 30}

INICIO = agora_utc()
_log = []
FILTRO = {o.strip().upper() for o in origens.split(",") if o.strip()}

SCHEMA_LOG = ("lote string, ciclo string, tabela_id string, etapa string, linhas bigint, status string, "
              "mensagem string, dt_registro timestamp")

def log(tabela_id, etapa, linhas, status, msg=""):
    _log.append({"lote": lote, "ciclo": ciclo, "tabela_id": tabela_id, "etapa": etapa,
                 "linhas": int(linhas or 0), "status": status, "mensagem": (msg or "")[:4000],
                 "dt_registro": agora_utc()})

def gravar_log():
    if _log:
        com_nova_tentativa(lambda: gravar(LH_BRONZE, "ctl_log", criar(_log, SCHEMA_LOG)))

def interpretar(resultados):
    """Itens "id|novo_watermark|linhas[|mensagem]". novo_watermark = FALHA (cópia falhou) ou SEM_MUDANCA
    (incremental sem alteração na origem, cópia pulada)."""
    itens = []
    for s in json.loads(resultados or "[]"):
        tid, novo, linhas, msg = (s.split("|", 3) + ["", "", "0", ""])[:4]
        itens.append({"id": tid, "novo": novo, "linhas": int(float(linhas or 0)), "msg": msg})
    return itens

def q(nome):
    return "`" + nome.replace("`", "``") + "`"

def padronizar(df):
    """Regras da Silver (docs/02_levantamento_silver.md): float -> DECIMAL(18,6), com NaN/infinito
    -> NULL; texto sem espaços no fim e vazio -> NULL; binários removidos. Colunas técnicas (_*) e
    demais tipos ficam como estão. Devolve (DataFrame, aviso com as conversões de NaN/infinito)."""
    flutuantes = [f.name for f in df.schema if not f.name.startswith("_")
                  and isinstance(f.dataType, (T.FloatType, T.DoubleType))]
    def nao_finito(c):
        return F.isnan(F.col(q(c))) | (F.abs(F.col(q(c))) == float("inf"))
    nao_finitos = []
    if flutuantes:
        contagem = df.select([F.sum(F.when(nao_finito(c), 1).otherwise(0)).alias(c) for c in flutuantes]).first()
        nao_finitos = [f"{c}={contagem[c]}" for c in flutuantes if contagem[c]]
    exprs = []
    for f in df.schema:
        c = F.col(q(f.name))
        if f.name.startswith("_"):
            exprs.append(c)
        elif f.name in flutuantes:
            exprs.append(F.when(~nao_finito(f.name), c).cast("decimal(18,6)").alias(f.name))
        elif isinstance(f.dataType, T.StringType):
            exprs.append(F.when(F.rtrim(c) != "", F.rtrim(c)).alias(f.name))
        elif not isinstance(f.dataType, T.BinaryType):
            exprs.append(c)
    return df.select(exprs), ("NaN/infinito -> NULL: " + ", ".join(nao_finitos)) if nao_finitos else ""

def carregar_silver(cfg, linhas_copiadas):
    """Leva o lote da Bronze para a Silver. Devolve quantas linhas foram gravadas."""
    destino = nome_silver(cfg["tabela_destino"])
    if not existe(LH_BRONZE, nome_bronze(cfg["tabela_destino"])):  # origem API sem nenhuma linha ainda
        return 0, ""
    bruto = ler(LH_BRONZE, nome_bronze(cfg["tabela_destino"]), filtros=[("_lote", "=", lote)])
    if bruto.isEmpty():
        return 0, ""
    # nomes sem diferenciar maiúsculas: o Firebird (4S) devolve EMP_CODIGO para EMP_Codigo
    por_nome = {c.upper(): c for c in bruto.columns}
    chaves = [por_nome.get(c.strip().upper(), c.strip()) for c in cfg["chave_negocio"].split(",") if c.strip()]
    faltando = [c for c in chaves if c not in bruto.columns]
    if faltando:
        raise ValueError(f"chave_negocio não encontrada na Bronze: {faltando}")
    ordem = por_nome.get((cfg["coluna_watermark"] or "").upper(), "_dt_carga")
    janela = Window.partitionBy(*[q(c) for c in chaves]).orderBy(F.col(q(ordem)).desc_nulls_last())
    unico = (bruto.withColumn("_rn", F.row_number().over(janela)).where("_rn = 1").drop("_rn")
             .withColumn("_ativo", F.lit(True)).withColumn("_dt_atualizacao", F.current_timestamp()))
    unico, aviso = padronizar(unico)
    unico = materializar(unico)
    n = unico.count()

    if cfg["modo_carga"] == "FULL" or not existe(LH_SILVER, destino):
        # dimensões: retrato completo do dia substitui a versão anterior
        gravar(LH_SILVER, destino, unico, modo="overwrite")
    else:
        mesclar(LH_SILVER, destino, unico, chaves)
    return n, aviso

def aplicar_retencao(ctl):
    """Apaga da Bronze os lotes mais antigos que RETENCAO_DIAS, compacta os arquivos e remove
    fisicamente o que não é mais referenciado (após 7 dias). Falha aqui não derruba o ciclo."""
    for cfg in ctl.values():
        nome = nome_bronze(cfg["tabela_destino"])
        # só as origens deste ciclo: outra origem pode estar gravando a Bronze em paralelo
        if FILTRO and cfg["origem"] not in FILTRO:
            continue
        if not cfg["ativo"] or not existe(LH_BRONZE, nome):
            continue
        limite = agora_utc() - dt.timedelta(days=RETENCAO_DIAS.get(cfg["modo_carga"], 30))
        try:
            alvo = tabela_delta(LH_BRONZE, nome)
            alvo.delete(F.col("_dt_carga") < F.lit(limite))
            apagadas = linhas_deletadas(LH_BRONZE, nome)
            alvo.optimize().executeCompaction()
            alvo.vacuum(168)
            log(cfg["id"], "RETENCAO", apagadas, "SUCESSO")
        except Exception as e:
            log(cfg["id"], "RETENCAO", 0, "FALHA", str(e))

def rodar_gold(tabelas_alteradas):
    for nb in dict.fromkeys(n for t in tabelas_alteradas for n in GOLD_NOTEBOOKS.get(t, [])):
        notebookutils.notebook.run(nb, 1800, {"lote": lote})
        log(nb, "GOLD", 0, "SUCESSO")

def avancar_watermarks(itens, ctl):
    agora = agora_utc()
    novos = [{"id": i["id"], "novo": i["novo"], "dt": agora} for i in itens
             if i["novo"] and ctl[i["id"]]["modo_carga"] == "INCREMENTAL"]
    if not novos:
        return
    fonte = criar(novos, "id string, novo string, dt timestamp")
    com_nova_tentativa(lambda: tabela_delta(LH_BRONZE, "ctl_tabelas").alias("t")
        .merge(fonte.alias("s"), "t.id = s.id")
        .whenMatchedUpdate(set={"ultimo_watermark": "s.novo", "dt_atualizacao": "s.dt"})
        .execute())

# %%
if status_ciclo != "SUCESSO":
    log("CICLO", "CICLO", 0, "FALHA", mensagem or "Falha em etapa do pipeline")
    gravar_log()
    if notificar == "SIM":
        avisar_execucao(origens or "todas", f"Ciclo {ciclo} (pl_mestre_dw)", "FALHA",
                        (agora_utc() - INICIO).total_seconds(),
                        erro=mensagem or "Falha antes do orquestrador", tipo="CICLO")
    sair("FALHA registrada")
else:
    # Cada tabela é independente: a que falhou fica com o watermark parado e vai para o log; as outras
    # seguem para a Silver normalmente. No fim, havendo falha, o notebook falha (pipeline vermelho).
    falhas = []
    itens = interpretar(resultados)
    if resultados_api == "FALHOU":
        falhas.append("Ingestao_API: notebook falhou antes de devolver os resultados")
        log("Ingestao_API", "BRONZE", 0, "FALHA", "notebook nb_ingestao_api falhou")
    else:
        itens += interpretar(resultados_api)
    ctl = {r["id"]: r for r in coletar(ler(LH_BRONZE, "ctl_tabelas"))}
    reportadas = {i["id"] for i in itens}
    for tid in json.loads(planejadas or "[]"):
        if tid not in reportadas:   # o ForEach não chegou a registrar: a cópia falhou
            falhas.append(f"{tid}: cópia não concluída")
            log(tid, "BRONZE", 0, "FALHA", "cópia não concluída (ver a execução do pipeline)")
    ok, alteradas = [], []
    for i in itens:
        cfg = ctl.get(i["id"])
        if cfg is None:
            falhas.append(f"{i['id']}: não está em ctl_tabelas")
            continue
        if i["novo"] == "FALHA":
            falhas.append(f"{i['id']}: {i['msg']}")
            log(i["id"], "BRONZE", 0, "FALHA", i["msg"])
            continue
        if i["novo"] == "SEM_MUDANCA":
            continue
        log(i["id"], "BRONZE", i["linhas"], "SUCESSO")
        try:
            n, aviso = carregar_silver(cfg, i["linhas"])
            log(i["id"], "SILVER", n, "SUCESSO", aviso)
            ok.append(i)
            if n:
                alteradas.append(cfg["tabela_destino"])
        except Exception as e:
            falhas.append(f"{i['id']}: {e}")
            log(i["id"], "SILVER", 0, "FALHA", str(e))

    try:
        rodar_gold(alteradas)
    except Exception as e:
        falhas.append(f"GOLD: {e}")
        log("GOLD", "GOLD", 0, "FALHA", str(e))

    avancar_watermarks(ok, ctl)              # só as tabelas que chegaram à Silver
    if ciclo == "COMPLETO":
        aplicar_retencao(ctl)
    duracao = (agora_utc() - INICIO).total_seconds()
    status = "SUCESSO" if not falhas else ("PARCIAL" if ok else "FALHA")
    log("CICLO", "CICLO", sum(i["linhas"] for i in ok), status,
        f"Origens {origens or 'todas'}: {len(ok)} tabelas na Silver, {len(alteradas)} alteradas, "
        f"{len(falhas)} com falha, {duracao:.0f}s no notebook" + (" | " + " | ".join(falhas) if falhas else ""))
    gravar_log()
    if notificar == "SIM":
        avisar_execucao(origens or "todas", f"Ciclo {ciclo} (pl_mestre_dw)", status, duracao,
                        erro=" | ".join(falhas),
                        resumo=f"{len(ok)} tabelas na Silver, {len(alteradas)} Gold(s) afetada(s)", tipo="CICLO")
    if falhas:
        raise RuntimeError(f"Ciclo {status}: {len(ok)} tabelas atualizadas; watermark parado nas que falharam:\n"
                           + "\n".join(falhas))
    sair(json.dumps({"tabelas": len(ok), "alteradas": alteradas}))
