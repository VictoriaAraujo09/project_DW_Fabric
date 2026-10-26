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
resultados_api = "[]"   # mesma lista, devolvida pelo nb_ingestao_api (origens API)
status_ciclo = "SUCESSO"
mensagem = ""
origens = ""            # origens deste ciclo, ex.: "GS,4S" (vazio = todas): limita a retenção a elas

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

def com_nova_tentativa(acao, tentativas=5):
    """Ciclos de origens diferentes podem rodar juntos e gravar ctl_tabelas/ctl_log ao mesmo tempo:
    o commit que perde a disputa é refeito sobre a versão nova da tabela."""
    import time
    for n in range(tentativas):
        try:
            return acao()
        except Exception as e:
            if n == tentativas - 1 or ("commit" not in str(e).lower() and "conflict" not in str(e).lower()):
                raise
            time.sleep(5 * (n + 1))

def log(tabela_id, etapa, linhas, status, msg=""):
    _log.append({"lote": lote, "ciclo": ciclo, "tabela_id": tabela_id, "etapa": etapa,
                 "linhas": int(linhas or 0), "status": status, "mensagem": (msg or "")[:4000],
                 "dt_registro": agora_utc()})

def gravar_log():
    if _log:
        schema = ler(LH_BRONZE, "ctl_log").schema
        com_nova_tentativa(lambda: gravar(LH_BRONZE, "ctl_log", pa.Table.from_pylist(_log, schema=schema)))

def interpretar(resultados):
    itens = []
    for s in json.loads(resultados or "[]"):
        tid, novo, linhas = (s.split("|") + ["", "", "0"])[:3]
        itens.append({"id": tid, "novo": novo, "linhas": int(float(linhas or 0))})
    return itens

def q(nome):
    return '"' + nome.replace('"', '""') + '"'

def padronizar(tabela):
    """Regras da Silver (docs/02_levantamento_silver.md): float -> DECIMAL(18,6), com NaN/infinito
    -> NULL; texto sem espaços no fim e vazio -> NULL; binários removidos. Colunas técnicas (_*) e
    demais tipos ficam como estão. Devolve (tabela, aviso com as conversões de NaN/infinito)."""
    import pyarrow.compute as pc
    exprs, nao_finitos = [], []
    for f in tabela.schema:
        c = q(f.name)
        if f.name.startswith("_"):
            exprs.append(c)
        elif pa.types.is_floating(f.type):
            n = pc.sum(pc.invert(pc.is_finite(tabela.column(f.name)))).as_py()
            if n:
                nao_finitos.append(f"{f.name}={n}")
            exprs.append(f"CAST(CASE WHEN isfinite({c}) THEN {c} END AS DECIMAL(18,6)) AS {c}")
        elif pa.types.is_string(f.type) or pa.types.is_large_string(f.type):
            exprs.append(f"NULLIF(rtrim({c}), '') AS {c}")
        elif not (pa.types.is_binary(f.type) or pa.types.is_large_binary(f.type)):
            exprs.append(c)
    con = duckdb.connect()
    con.register("tbl", tabela)
    saida = para_arrow(con.sql(f"SELECT {', '.join(exprs)} FROM tbl"))
    con.close()
    return saida, ("NaN/infinito -> NULL: " + ", ".join(nao_finitos)) if nao_finitos else ""

def carregar_silver(cfg, linhas_copiadas):
    """Leva o lote da Bronze para a Silver. Devolve quantas linhas foram gravadas."""
    destino = nome_silver(cfg["tabela_destino"])
    if not existe(LH_BRONZE, nome_bronze(cfg["tabela_destino"])):  # origem API sem nenhuma linha ainda
        return 0, ""
    bruto = ler(LH_BRONZE, nome_bronze(cfg["tabela_destino"]), filtros=[("_lote", "=", lote)])
    if bruto.num_rows == 0:
        return 0, ""
    # nomes sem diferenciar maiúsculas: o Firebird (4S) devolve EMP_CODIGO para EMP_Codigo
    por_nome = {c.upper(): c for c in bruto.column_names}
    chaves = [por_nome.get(c.strip().upper(), c.strip()) for c in cfg["chave_negocio"].split(",") if c.strip()]
    faltando = [c for c in chaves if c not in bruto.column_names]
    if faltando:
        raise ValueError(f"chave_negocio não encontrada na Bronze: {faltando}")
    ordem = por_nome.get((cfg["coluna_watermark"] or "").upper(), "_dt_carga")
    con = duckdb.connect()
    con.register("lote_tbl", bruto)
    unico = para_arrow(con.sql(f"""
        SELECT * EXCLUDE (_rn), TRUE AS _ativo, now() AS _dt_atualizacao
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {", ".join(q(c) for c in chaves)}
                                           ORDER BY {q(ordem)} DESC NULLS LAST) AS _rn
              FROM lote_tbl)
        WHERE _rn = 1"""))
    con.close()
    unico, aviso = padronizar(unico)

    if cfg["modo_carga"] == "FULL" or not existe(LH_SILVER, destino):
        # dimensões: retrato completo do dia substitui a versão anterior
        gravar(LH_SILVER, destino, unico, modo="overwrite")
    else:
        acrescentar_colunas(LH_SILVER, destino, unico.schema)
        alvo = tabela_delta(LH_SILVER, destino)
        (alvo.merge(unico,
                    predicate=" AND ".join(f"t.{q(c)} = s.{q(c)}" for c in chaves),
                    source_alias="s", target_alias="t")
             .when_matched_update_all()
             .when_not_matched_insert_all()
             .execute())
    return unico.num_rows, aviso

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
            r = alvo.delete(f"_dt_carga < '{limite:%Y-%m-%d %H:%M:%S}'")
            alvo.optimize.compact()
            alvo.vacuum(retention_hours=168, enforce_retention_duration=False, dry_run=False)
            log(cfg["id"], "RETENCAO", r.get("num_deleted_rows", 0), "SUCESSO")
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
    fonte = pa.Table.from_pylist(novos, schema=pa.schema(
        [("id", pa.string()), ("novo", pa.string()), ("dt", pa.timestamp("us", tz="UTC"))]))
    com_nova_tentativa(lambda: tabela_delta(LH_BRONZE, "ctl_tabelas")
        .merge(fonte, predicate="t.id = s.id", source_alias="s", target_alias="t")
        .when_matched_update(updates={"ultimo_watermark": "s.novo", "dt_atualizacao": "s.dt"})
        .execute())

# %%
if status_ciclo != "SUCESSO":
    log("CICLO", "CICLO", 0, "FALHA", mensagem or "Falha em etapa do pipeline")
    gravar_log()
    sair("FALHA registrada")
else:
    itens = interpretar(resultados) + interpretar(resultados_api)
    ctl = {r["id"]: r for r in ler(LH_BRONZE, "ctl_tabelas").to_pylist()}
    erros, alteradas = [], []
    for i in itens:
        cfg = ctl.get(i["id"])
        if cfg is None:
            erros.append(f"{i['id']}: não está em ctl_tabelas")
            continue
        log(i["id"], "BRONZE", i["linhas"], "SUCESSO")
        try:
            n, aviso = carregar_silver(cfg, i["linhas"])
            log(i["id"], "SILVER", n, "SUCESSO", aviso)
            if n:
                alteradas.append(cfg["tabela_destino"])
        except Exception as e:
            erros.append(f"{i['id']}: {e}")
            log(i["id"], "SILVER", 0, "FALHA", str(e))

    if not erros:
        try:
            rodar_gold(alteradas)
        except Exception as e:
            erros.append(f"GOLD: {e}")
            log("GOLD", "GOLD", 0, "FALHA", str(e))

    if erros:
        log("CICLO", "CICLO", 0, "FALHA", " | ".join(erros))
        gravar_log()
        raise RuntimeError("Ciclo com falha; watermarks NÃO avançados:\n" + "\n".join(erros))

    avancar_watermarks(itens, ctl)
    if ciclo == "COMPLETO":
        aplicar_retencao(ctl)
    duracao = (agora_utc() - INICIO).total_seconds()
    log("CICLO", "CICLO", sum(i["linhas"] for i in itens), "SUCESSO",
        f"Origens {origens or 'todas'}: {len(itens)} tabelas copiadas, {len(alteradas)} alteradas na Silver, "
        f"{duracao:.0f}s no notebook")
    gravar_log()
    sair(json.dumps({"tabelas": len(itens), "alteradas": alteradas}))
