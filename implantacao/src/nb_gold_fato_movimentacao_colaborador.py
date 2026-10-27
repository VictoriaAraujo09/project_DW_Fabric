# %% [markdown]
# # nb_gold_fato_movimentacao_colaborador
# Posto e cliente de cada colaborador **em cada dia** (Nexti), a partir das transferências de posto.
# Mesma regra do `gold_fato_movimentacao_colaborador` do Airflow:
# - uma transferência vale do dia dela até a véspera da próxima, limitada a hoje e à demissão;
# - no mesmo dia, vale a transferência alterada por último;
# - chave: CodColaborador + DataReferencia + SistemaOrigem.
#
# Incremental: recalcula só os colaboradores com transferência nova desde a última execução. Na primeira
# execução do dia (ou com `modo = COMPLETO`) recalcula todos, para estender as linhas até hoje.
# Devolve quantos colaboradores mudaram, para a Gold de ausências refazer os dias deles.

# %% tags=["parameters"]
lote = "manual"
modo = "INCREMENTAL"      # INCREMENTAL | COMPLETO

# %%
#@@COMUM@@

# %%
GOLD = "gold_fato_movimentacao_colaborador"
SILVER_TRANSF = nome_silver("fato_transferencias_posto_nexti")
SILVER_COLAB = nome_silver("dim_colaboradores_nexti")
SILVER_EMP = nome_silver("dim_empresas_nexti")
SILVER_POSTOS = nome_silver("dim_postos_nexti")

# %%
inicio = agora_utc()
hoje = dt.datetime.now(FUSO_BRT).date()
transf = ler(LH_SILVER, SILVER_TRANSF)
wm = watermark_gold(GOLD, SILVER_TRANSF)
novo_wm = maximo(transf, "_dt_carga") or wm

transf.createOrReplaceTempView("transf")
ler(LH_SILVER, SILVER_COLAB).createOrReplaceTempView("src_colab")
ler(LH_SILVER, SILVER_EMP).createOrReplaceTempView("src_emp")
ler(LH_SILVER, SILVER_POSTOS).createOrReplaceTempView("src_postos")

# virada do dia: o primeiro recálculo do dia é completo, para estender as linhas de todos até hoje
ultima_virada = watermark_gold(GOLD, "virada_dia")
completo = (modo == "COMPLETO" or wm is None or not existe(LH_GOLD, GOLD) or ultima_virada is None
            or ultima_virada.astimezone(FUSO_BRT).date() < hoje)

alterados = transf.select(F.col("personId").alias("CodColaborador"))
if not completo:
    alterados = alterados.where(F.col("_dt_carga") > F.lit(wm))
alterados = materializar(alterados.distinct())
alterados.createOrReplaceTempView("alterados")
n_alterados = alterados.count()

# %%
if n_alterados == 0:
    print(f"{GOLD}: nenhum colaborador alterado")
    sair(json.dumps({"gold": GOLD, "colaboradores": 0, "linhas": 0, "completo": False}))
else:
    linhas = materializar(spark.sql(consulta(GOLD).replace("{hoje}", hoje.isoformat())))
    n_linhas = linhas.count()
    ids = [r[0] for r in alterados.collect()]

    if completo:
        gravar(LH_GOLD, GOLD, linhas, modo="overwrite")
    else:
        # troca as linhas dos colaboradores alterados: as que sumiram saem, as novas entram
        apagar(LH_GOLD, GOLD, "CodColaborador", ids, extra="SistemaOrigem = 'NEXTI'")
        if n_linhas:
            gravar(LH_GOLD, GOLD, linhas)
    if novo_wm is not None:
        gravar_watermark_gold(GOLD, SILVER_TRANSF, novo_wm)
    # marca da escrita: a Gold de ausências refaz os dias dos colaboradores gravados depois dela
    gravar_watermark_gold(GOLD, "escrita", inicio)
    if completo:
        gravar_watermark_gold(GOLD, "virada_dia", inicio)
    duracao = (agora_utc() - inicio).total_seconds()
    print(f"{GOLD}: {'COMPLETO' if completo else 'INCREMENTAL'} | {n_alterados} colaboradores | "
          f"{n_linhas} linhas | {duracao:.0f}s")
    sair(json.dumps({"gold": GOLD, "colaboradores": int(n_alterados), "linhas": n_linhas,
                     "completo": completo}))
