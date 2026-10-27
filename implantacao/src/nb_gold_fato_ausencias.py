# %% [markdown]
# # nb_gold_fato_ausencias
# Uma linha por ausência e por dia (Nexti), com posto e cliente do dia. Mesma regra do `gold_fato_ausencias`
# do Airflow:
# - colaborador e empresa ativos (join interno); posto e cliente vêm da `gold_fato_movimentacao_colaborador`;
# - `FlagAusenciaAutomatica`: registrada pelo usuário "Sistema" com uma das justificativas automáticas;
# - `FlagDiaPrevistoTrabalho`: 0 até a rotina do previsto entrar;
# - ausência removida na Nexti fica com `Ativo = 0`; dia que sumiu do cálculo também (com `DataInativacao`);
# - chave: IdAusencia + DataCalendario + SistemaOrigem.
#
# Incremental: refaz só as ausências alteradas desde a última execução e as dos colaboradores cujo posto
# mudou na Gold de movimentação. Sem nada alterado, termina sem tocar na Gold.

# %% tags=["parameters"]
lote = "manual"
modo = "INCREMENTAL"      # INCREMENTAL | COMPLETO

# %%
#@@COMUM@@

# %%
GOLD = "gold_fato_ausencias"
GOLD_MOV = "gold_fato_movimentacao_colaborador"
SILVER_AUS = nome_silver("fato_ausencias_nexti")
SILVER_COLAB = nome_silver("dim_colaboradores_nexti")
SILVER_EMP = nome_silver("dim_empresas_nexti")
SILVER_USU = nome_silver("dim_usuarios_nexti")

JUSTIFICATIVAS_AUTOMATICAS = ["AUSÊNCIA DE MARCAÇÃO", "Marcação não identificada",
                              "Falta automática devido o não registro de ponto"]

COLUNAS = ["PK_Colaborador", "IdAusencia", "CodColaborador", "CodEmpresa", "CodCliente", "CodPosto",
           "CodSituacao", "DataCalendario", "DataInicio", "DataFim", "InicioMinuto", "FimMinuto", "Justificativa",
           "CodMedico", "Medico", "CRM", "CidId", "CodCid", "CidDescricao", "CodUsuarioRegistro",
           "FlagAusenciaAutomatica", "FlagDiaPrevistoTrabalho", "NomeEmpresa", "CNPJEmpresaOrigem",
           "SistemaOrigem", "DataIngestao", "Ativo", "DataAtualizacao", "DataInativacao"]

# %%
inicio = agora_utc()
wm_aus = watermark_gold(GOLD, SILVER_AUS)
wm_mov = watermark_gold(GOLD, GOLD_MOV)             # última escrita da movimentação já considerada aqui
escrita_mov = watermark_gold(GOLD_MOV, "escrita")   # última escrita da movimentação
virada_mov = watermark_gold(GOLD_MOV, "virada_dia")  # último recálculo completo da movimentação
# completo: primeira vez, pedido explícito ou movimentação recalculada inteira (virada do dia, 1x por dia)
completo = (modo == "COMPLETO" or wm_aus is None or not existe(LH_GOLD, GOLD)
            or (virada_mov is not None and (wm_mov is None or virada_mov > wm_mov)))

# Incremental lê só o necessário (filtros empurrados para o Delta): as ausências alteradas, as dos
# colaboradores cujo posto mudou e a movimentação apenas desses colaboradores.
if completo:
    aus = ler(LH_SILVER, SILVER_AUS)
    mov = ler(LH_GOLD, GOLD_MOV) if existe(LH_GOLD, GOLD_MOV) else None
else:
    aus = ler(LH_SILVER, SILVER_AUS, filtros=[("_dt_carga", ">", wm_aus)])
    if existe(LH_GOLD, GOLD_MOV) and escrita_mov is not None and (wm_mov is None or escrita_mov > wm_mov):
        mudou = ler(LH_GOLD, GOLD_MOV, filtros=[("DataAtualizacao", ">", wm_mov)] if wm_mov else None)
        pessoas_mov = [r[0] for r in mudou.select("CodColaborador").distinct().collect()]
        if pessoas_mov:
            aus = aus.unionByName(ler(LH_SILVER, SILVER_AUS, filtros=[("personId", "in", pessoas_mov)]))
    aus = materializar(aus)
    pessoas = [r[0] for r in aus.select("personId").distinct().collect() if r[0] is not None]
    mov = (ler(LH_GOLD, GOLD_MOV, filtros=[("CodColaborador", "in", pessoas)])
           if pessoas and existe(LH_GOLD, GOLD_MOV) else None)

# a versão mais recente de cada ausência
janela = Window.partitionBy("id").orderBy(F.col("_dt_carga").desc())
aus.withColumn("_rn", F.row_number().over(janela)).where("_rn = 1").drop("_rn").createOrReplaceTempView("aus")
if mov is not None:
    materializar(mov).createOrReplaceTempView("mov")
else:
    criar([], "CodColaborador bigint, DataReferencia date, CodCliente bigint, CodPosto bigint, "
              "DataAtualizacao timestamp").createOrReplaceTempView("mov")
alteradas = materializar(spark.sql("SELECT DISTINCT id FROM aus"))
alteradas.createOrReplaceTempView("alteradas")
n_alteradas = alteradas.count()
maior = maximo(aus, "_dt_carga")
novo_wm_aus = max(x for x in (wm_aus, maior) if x is not None) if (wm_aus or maior) else None
novo_wm_mov = escrita_mov or wm_mov

# %%
if n_alteradas == 0:
    print(f"{GOLD}: nenhuma ausência alterada")
    sair(json.dumps({"gold": GOLD, "ausencias": 0, "linhas": 0}))
else:
    ler(LH_SILVER, SILVER_COLAB).createOrReplaceTempView("src_colab")
    ler(LH_SILVER, SILVER_EMP).createOrReplaceTempView("src_emp")
    if existe(LH_SILVER, SILVER_USU):
        ler(LH_SILVER, SILVER_USU).createOrReplaceTempView("src_usu")
    else:
        criar([], "id bigint, name string").createOrReplaceTempView("src_usu")
    criar([(j,) for j in JUSTIFICATIVAS_AUTOMATICAS], "justificativa string").createOrReplaceTempView("automaticas")
    novas = spark.sql(consulta(GOLD))

    # Dias que já estavam na Gold e não voltaram no cálculo ficam inativos (Ativo = 0), como no Airflow
    resultado = novas.select(COLUNAS)
    if existe(LH_GOLD, GOLD):
        ids_alt = [r[0] for r in alteradas.collect()]
        gold = ler(LH_GOLD, GOLD) if completo else ler(LH_GOLD, GOLD, filtros=[("IdAusencia", "in", ids_alt)])
        agora = F.current_timestamp()
        sumidas = (gold.join(novas.select("IdAusencia", "DataCalendario"), ["IdAusencia", "DataCalendario"], "left_anti")
                   .withColumns({"Ativo": F.lit(False),
                                 "DataAtualizacao": F.when(F.col("Ativo"), agora).otherwise(F.col("DataAtualizacao")),
                                 "DataInativacao": F.when(F.col("Ativo"), agora).otherwise(F.col("DataInativacao"))}))
        tipos = dict(resultado.dtypes)
        resultado = resultado.unionByName(sumidas.select([F.col(c).cast(tipos[c]) for c in COLUNAS]))
    # calculado antes de apagar ou regravar a Gold, que também é fonte das linhas inativadas
    resultado = materializar(resultado)
    n_linhas = resultado.count()
    ativas = resultado.where("Ativo").count()

    if completo:
        if ativas == 0:
            raise RuntimeError("Nenhuma ausência ativa calculada: Gold não substituída para não esvaziar a tabela")
        gravar(LH_GOLD, GOLD, resultado, modo="overwrite")
    else:
        # troca as linhas das ausências recalculadas (novas + dias que ficaram inativos)
        ids = sorted(r[0] for r in resultado.select("IdAusencia").distinct().collect())
        apagar(LH_GOLD, GOLD, "IdAusencia", ids)
        if n_linhas:
            gravar(LH_GOLD, GOLD, resultado)

    if novo_wm_aus is not None:
        gravar_watermark_gold(GOLD, SILVER_AUS, novo_wm_aus)
    if novo_wm_mov is not None:
        gravar_watermark_gold(GOLD, GOLD_MOV, novo_wm_mov)
    duracao = (agora_utc() - inicio).total_seconds()
    print(f"{GOLD}: {'COMPLETO' if completo else 'INCREMENTAL'} | {n_alteradas} ausências | "
          f"{n_linhas} linhas ({ativas} ativas) | {duracao:.0f}s")
    sair(json.dumps({"gold": GOLD, "ausencias": int(n_alteradas), "linhas": n_linhas}))
