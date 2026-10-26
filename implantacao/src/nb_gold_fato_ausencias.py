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
import pyarrow.compute as pc

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
    partes = [ler(LH_SILVER, SILVER_AUS, filtros=[("_dt_carga", ">", wm_aus)])]
    if existe(LH_GOLD, GOLD_MOV) and escrita_mov is not None and (wm_mov is None or escrita_mov > wm_mov):
        mudou = ler(LH_GOLD, GOLD_MOV, filtros=[("DataAtualizacao", ">", wm_mov)] if wm_mov else None)
        pessoas_mov = pc.unique(mudou.column("CodColaborador")).to_pylist() if mudou.num_rows else []
        if pessoas_mov:
            partes.append(ler(LH_SILVER, SILVER_AUS, filtros=[("personId", "in", pessoas_mov)]))
    aus = pa.concat_tables(partes, promote_options="default") if len(partes) > 1 else partes[0]
    pessoas = pc.unique(aus.column("personId")).drop_null().to_pylist() if aus.num_rows else []
    mov = (ler(LH_GOLD, GOLD_MOV, filtros=[("CodColaborador", "in", pessoas)])
           if pessoas and existe(LH_GOLD, GOLD_MOV) else None)

con = duckdb.connect()
# as datas da Bronze são horário de Brasília rotulado UTC: converter em UTC para não recuar um dia
con.execute("SET TimeZone = 'UTC'").fetchall()
con.register("aus_lidas", aus)
con.sql("CREATE VIEW aus AS SELECT * FROM aus_lidas "
        "QUALIFY row_number() OVER (PARTITION BY id ORDER BY _dt_carga DESC) = 1")
if mov is not None:
    con.register("mov", mov)
else:
    con.sql("CREATE TABLE mov (CodColaborador BIGINT, DataReferencia DATE, CodCliente BIGINT, CodPosto BIGINT, "
            "DataAtualizacao TIMESTAMPTZ)")
con.sql("CREATE TABLE alteradas AS SELECT DISTINCT id FROM aus")
n_alteradas = con.execute("SELECT count(*) FROM alteradas").fetchall()[0][0]
maior = pc.max(aus.column("_dt_carga")).as_py() if aus.num_rows else None
novo_wm_aus = max(x for x in (wm_aus, maior) if x is not None) if (wm_aus or maior) else None
novo_wm_mov = escrita_mov or wm_mov

# %%
if n_alteradas == 0:
    print(f"{GOLD}: nenhuma ausência alterada")
    sair(json.dumps({"gold": GOLD, "ausencias": 0, "linhas": 0}))
else:
    con.register("src_colab", ler(LH_SILVER, SILVER_COLAB))
    con.register("src_emp", ler(LH_SILVER, SILVER_EMP))
    if existe(LH_SILVER, SILVER_USU):
        con.register("src_usu", ler(LH_SILVER, SILVER_USU))
    else:
        con.sql("CREATE TABLE src_usu (id BIGINT, name VARCHAR)")
    con.execute("SET VARIABLE automaticas = ?", [JUSTIFICATIVAS_AUTOMATICAS]).fetchall()
    novas = para_arrow(con.sql(consulta(GOLD)))

    # Dias que já estavam na Gold e não voltaram no cálculo ficam inativos (Ativo = 0), como no Airflow
    if existe(LH_GOLD, GOLD):
        con.register("novas", novas)
        filtro = "" if completo else "WHERE IdAusencia IN (SELECT id FROM alteradas)"
        ids_alt = [r[0] for r in con.execute("SELECT id FROM alteradas").fetchall()]
        con.register("gold", ler(LH_GOLD, GOLD) if completo else
                     ler(LH_GOLD, GOLD, filtros=[("IdAusencia", "in", ids_alt)]))
        sumidas = para_arrow(con.sql(f"""
            SELECT g.* REPLACE (FALSE AS Ativo,
                                CASE WHEN g.Ativo THEN now() ELSE g.DataAtualizacao END AS DataAtualizacao,
                                CASE WHEN g.Ativo THEN now() ELSE g.DataInativacao END AS DataInativacao)
            FROM (SELECT * FROM gold {filtro}) g
            ANTI JOIN novas n ON n.IdAusencia = g.IdAusencia AND n.DataCalendario = g.DataCalendario"""))
    else:
        sumidas = None
    con.close()

    resultado = novas.select(COLUNAS)
    if sumidas is not None and sumidas.num_rows:
        resultado = pa.concat_tables([resultado, sumidas.select(COLUNAS).cast(resultado.schema)])
    ativas = pc.sum(pc.cast(resultado.column("Ativo"), pa.int64())).as_py() or 0

    if completo:
        if ativas == 0:
            raise RuntimeError("Nenhuma ausência ativa calculada: Gold não substituída para não esvaziar a tabela")
        gravar(LH_GOLD, GOLD, resultado, modo="overwrite")
    else:
        # troca as linhas das ausências recalculadas (novas + dias que ficaram inativos)
        ids = sorted(int(i) for i in pc.unique(resultado.column("IdAusencia")).to_pylist())
        alvo = tabela_delta(LH_GOLD, GOLD)
        for i in range(0, len(ids), 5000):
            alvo.delete(f"IdAusencia IN ({', '.join(str(x) for x in ids[i:i + 5000])})")
        if resultado.num_rows:
            gravar(LH_GOLD, GOLD, resultado)

    if novo_wm_aus is not None:
        gravar_watermark_gold(GOLD, SILVER_AUS, novo_wm_aus)
    if novo_wm_mov is not None:
        gravar_watermark_gold(GOLD, GOLD_MOV, novo_wm_mov)
    duracao = (agora_utc() - inicio).total_seconds()
    print(f"{GOLD}: {'COMPLETO' if completo else 'INCREMENTAL'} | {n_alteradas} ausências | "
          f"{resultado.num_rows} linhas ({ativas} ativas) | {duracao:.0f}s")
    sair(json.dumps({"gold": GOLD, "ausencias": int(n_alteradas), "linhas": resultado.num_rows}))
