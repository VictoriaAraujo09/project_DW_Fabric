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
status_ciclo = "SUCESSO"
mensagem = ""

# %%
#@@COMUM@@

# %%
# Tabela Silver -> lista de notebooks Gold a executar quando ela muda (preencher na Fase 4)
GOLD_NOTEBOOKS = {
    # "silver_fato_contas_receber_gs": ["nb_gold_fato_contas_receber"],
}

INICIO = agora_utc()
_log = []

def log(tabela_id, etapa, linhas, status, msg=""):
    _log.append({"lote": lote, "ciclo": ciclo, "tabela_id": tabela_id, "etapa": etapa,
                 "linhas": int(linhas or 0), "status": status, "mensagem": (msg or "")[:4000],
                 "dt_registro": agora_utc()})

def gravar_log():
    if _log:
        schema = ler(LH_BRONZE, "ctl_log").schema
        gravar(LH_BRONZE, "ctl_log", pa.Table.from_pylist(_log, schema=schema))

def interpretar(resultados):
    itens = []
    for s in json.loads(resultados or "[]"):
        tid, novo, linhas = (s.split("|") + ["", "", "0"])[:3]
        itens.append({"id": tid, "novo": novo, "linhas": int(float(linhas or 0))})
    return itens

def q(nome):
    return '"' + nome.replace('"', '""') + '"'

def carregar_silver(cfg, linhas_copiadas):
    """Leva o lote da Bronze para a Silver. Devolve quantas linhas foram gravadas."""
    destino = cfg["tabela_silver"]
    bruto = ler(LH_BRONZE, cfg["tabela_bronze"], filtros=[("_lote", "=", lote)])
    if bruto.num_rows == 0:
        return 0
    chaves = [c.strip() for c in cfg["chave_negocio"].split(",") if c.strip()]
    faltando = [c for c in chaves if c not in bruto.column_names]
    if faltando:
        raise ValueError(f"chave_negocio não encontrada na Bronze: {faltando}")
    ordem = cfg["coluna_watermark"] if cfg["coluna_watermark"] in bruto.column_names else "_dt_carga"
    con = duckdb.connect()
    con.register("lote_tbl", bruto)
    unico = para_arrow(con.sql(f"""
        SELECT * EXCLUDE (_rn), TRUE AS _ativo, now() AS _dt_atualizacao
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {", ".join(q(c) for c in chaves)}
                                           ORDER BY {q(ordem)} DESC NULLS LAST) AS _rn
              FROM lote_tbl)
        WHERE _rn = 1"""))
    con.close()

    if cfg["modo_carga"] == "FULL" or not existe(LH_SILVER, destino):
        # dimensões: retrato completo do dia substitui a versão anterior
        gravar(LH_SILVER, destino, unico, modo="overwrite")
    else:
        alvo = tabela_delta(LH_SILVER, destino)
        (alvo.merge(unico,
                    predicate=" AND ".join(f"t.{q(c)} = s.{q(c)}" for c in chaves),
                    source_alias="s", target_alias="t")
             .when_matched_update_all()
             .when_not_matched_insert_all()
             .execute())
    return unico.num_rows

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
    (tabela_delta(LH_BRONZE, "ctl_tabelas")
        .merge(fonte, predicate="t.id = s.id", source_alias="s", target_alias="t")
        .when_matched_update(updates={"ultimo_watermark": "s.novo", "dt_atualizacao": "s.dt"})
        .execute())

# %%
if status_ciclo != "SUCESSO":
    log("CICLO", "CICLO", 0, "FALHA", mensagem or "Falha em etapa do pipeline")
    gravar_log()
    sair("FALHA registrada")
else:
    itens = interpretar(resultados)
    ctl = {r["id"]: r for r in ler(LH_BRONZE, "ctl_tabelas").to_pylist()}
    erros, alteradas = [], []
    for i in itens:
        cfg = ctl.get(i["id"])
        if cfg is None:
            erros.append(f"{i['id']}: não está em ctl_tabelas")
            continue
        log(i["id"], "BRONZE", i["linhas"], "SUCESSO")
        try:
            n = carregar_silver(cfg, i["linhas"])
            log(i["id"], "SILVER", n, "SUCESSO")
            if n:
                alteradas.append(cfg["tabela_silver"])
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
    duracao = (agora_utc() - INICIO).total_seconds()
    log("CICLO", "CICLO", sum(i["linhas"] for i in itens), "SUCESSO",
        f"{len(itens)} tabelas copiadas, {len(alteradas)} alteradas na Silver, {duracao:.0f}s no notebook")
    gravar_log()
    sair(json.dumps({"tabelas": len(itens), "alteradas": alteradas}))
