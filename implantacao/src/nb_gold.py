# %% [markdown]
# # nb_gold
# Executa uma consulta Gold da pasta `consultas` (lakehouse Gold → Files/consultas/<gold>.sql) e grava o
# resultado. A regra de negócio fica toda no .sql; este notebook só cuida de ler, filtrar e gravar.
#
# Cabeçalho do .sql (linhas `-- @chave valor`):
# - `@destino`: tabela Gold gravada (lakehouse Gold);
# - `@chave`: coluna da Gold que identifica o registro de origem (apagar e reinserir por ela);
# - `@incremental`: `apelido.coluna` da fonte que comanda o incremental e cujo valor vai para `@chave`;
# - `@fontes`: `apelido=Lakehouse.tabela; ...`, as tabelas que a consulta usa pelo apelido.
#
# Incremental: a fonte de `@incremental` traz só as linhas que chegaram desde a última execução
# (`_dt_carga`); a Gold apaga esses registros e grava o recálculo. Sem linha nova, termina sem gravar.
# `modo = COMPLETO`, ou a primeira execução, recalcula e regrava a tabela inteira.

# %% tags=["parameters"]
gold = "gold_fato_coberturas"
lote = "manual"
modo = "INCREMENTAL"      # INCREMENTAL | COMPLETO

# %%
#@@COMUM@@

# %%
import pyarrow.compute as pc

LAKEHOUSES = {"Bronze": LH_BRONZE, "Silver": LH_SILVER, "Gold": LH_GOLD}
inicio = agora_utc()
sql = consulta(gold)
cfg = cabecalho(sql)
for campo in ("destino", "chave", "incremental", "fontes"):
    if not cfg.get(campo):
        raise ValueError(f"{gold}.sql sem `-- @{campo}` no cabeçalho")
destino, chave = cfg["destino"], cfg["chave"]
apelido_inc, coluna_inc = cfg["incremental"].split(".", 1)
fontes = {}
for item in cfg["fontes"].split(";"):
    apelido, _, ref = item.strip().partition("=")
    lh, _, tabela = ref.strip().partition(".")
    fontes[apelido.strip()] = (LAKEHOUSES[lh], tabela)

lh_inc, tabela_inc = fontes[apelido_inc]
wm = watermark_gold(destino, tabela_inc)
completo = modo == "COMPLETO" or wm is None or not existe(LH_GOLD, destino)

con = duckdb.connect()
# as datas da Bronze são horário de Brasília rotulado UTC: converter em UTC para não recuar um dia
con.execute("SET TimeZone = 'UTC'").fetchall()
for apelido, (lh, tabela) in fontes.items():
    if apelido == apelido_inc and not completo:
        dados = ler(lh, tabela, filtros=[("_dt_carga", ">", wm)])
    else:
        dados = ler(lh, tabela)
    if apelido == apelido_inc:
        alterados = dados
    con.register(apelido, dados)

# %%
if alterados.num_rows == 0:
    print(f"{destino}: nada novo")
    resultado = {"gold": destino, "registros": 0, "linhas": 0, "completo": False}
else:
    linhas = para_arrow(con.sql(sql))
    chaves = sorted(set(pc.unique(alterados.column(coluna_inc)).drop_null().to_pylist()))
    novo_wm = pc.max(alterados.column("_dt_carga")).as_py()
    con.close()
    if completo:
        if linhas.num_rows == 0:
            raise RuntimeError(f"{destino}: consulta sem linhas; tabela não substituída para não esvaziá-la")
        gravar(LH_GOLD, destino, linhas, modo="overwrite")
    else:
        alvo = tabela_delta(LH_GOLD, destino)
        for i in range(0, len(chaves), 5000):
            alvo.delete(f"{chave} IN ({', '.join(str(int(x)) for x in chaves[i:i + 5000])})")
        if linhas.num_rows:
            gravar(LH_GOLD, destino, linhas)
    gravar_watermark_gold(destino, tabela_inc, novo_wm)
    gravar_watermark_gold(destino, "escrita", inicio)
    duracao = (agora_utc() - inicio).total_seconds()
    print(f"{destino}: {'COMPLETO' if completo else 'INCREMENTAL'} | {len(chaves)} registros | "
          f"{linhas.num_rows} linhas | {duracao:.0f}s")
    resultado = {"gold": destino, "registros": len(chaves), "linhas": linhas.num_rows, "completo": completo}

# %%
sair(json.dumps(resultado))
