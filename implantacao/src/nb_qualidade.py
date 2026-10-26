# %% [markdown]
# # nb_qualidade
# Aplica as regras de `config/qualidade.yml` nos dados reais (lakehouse Gold → Files/config/qualidade.yml)
# e grava o resultado de cada regra em `ctl_qualidade` (lakehouse Bronze). Não falha por regra quebrada:
# o resultado vai para a tabela e o nb_monitoramento transforma as falhas em alerta.
#
# Tipos de regra: chave_unica, nao_nulo, sem_linhas, valores_permitidos, intervalo, minimo_linhas,
# sem_timestamp_sem_fuso. Ver o cabeçalho do qualidade.yml.

# %% tags=["parameters"]
grupo = ""              # vazio = todas as regras; ex.: "AUSENCIAS" roda só as do grupo
lote = "manual"

# %%
#@@COMUM@@

# %%
LAKEHOUSES = {"Bronze": LH_BRONZE, "Silver": LH_SILVER, "Gold": LH_GOLD}
hoje = dt.datetime.now(FUSO_BRT).date().isoformat()
regras = [r for r in ler_config("qualidade.yml")["regras"] if not grupo or r["grupo"] in (grupo, "GERAL")]

def q(nome):
    return '"' + nome.replace('"', '""') + '"'

def tabelas_da_camada(camada):
    lh = LAKEHOUSES[camada]
    if _LOCAL:
        raiz = os.path.join(os.environ["DW_LOCAL_ROOT"], lh)
        return sorted(n for n in os.listdir(raiz) if os.path.isdir(os.path.join(raiz, n)))
    return sorted(i.name.rstrip("/") for i in notebookutils.fs.ls(
        f"abfss://{_WS}@onelake.dfs.fabric.microsoft.com/{lh}.Lakehouse/Tables"))

def avaliar(regra):
    """Devolve (linhas com problema, amostra)."""
    camada, _, tabela = regra["tabela"].partition(".")
    tipo = regra["tipo"]
    if tipo == "sem_timestamp_sem_fuso":
        ruins = []
        for t in tabelas_da_camada(camada):
            for campo in tabela_delta(LAKEHOUSES[camada], t).schema().fields:
                if "timestamp_ntz" in str(campo.type).lower():
                    ruins.append(f"{t}.{campo.name}")
        return len(ruins), ruins[:5]
    colunas = regra.get("colunas") or ([regra["coluna"]] if regra.get("coluna") else None)
    dados = ler(LAKEHOUSES[camada], tabela, colunas=colunas)
    if tipo == "minimo_linhas":
        return (0 if dados.num_rows >= int(regra["minimo"]) else 1), [f"{dados.num_rows} linhas"]
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'").fetchall()
    con.register("t", dados)
    if tipo == "chave_unica":
        cols = ", ".join(q(c) for c in regra["colunas"])
        sql = f"SELECT {cols}, count(*) AS repeticoes FROM t GROUP BY {cols} HAVING count(*) > 1"
    elif tipo == "nao_nulo":
        sql = "SELECT * FROM t WHERE " + " OR ".join(f"{q(c)} IS NULL" for c in regra["colunas"])
    elif tipo == "sem_linhas":
        sql = "SELECT * FROM t WHERE " + regra["condicao"].replace("{hoje}", hoje)
    elif tipo == "valores_permitidos":
        valores = ", ".join(repr(v) for v in regra["valores"])
        sql = f"SELECT {q(regra['coluna'])}, count(*) AS linhas FROM t WHERE {q(regra['coluna'])} NOT IN ({valores}) GROUP BY 1"
    elif tipo == "intervalo":
        c = q(regra["coluna"])
        sql = f"SELECT * FROM t WHERE {c} < '{regra['minimo']}' OR {c} > '{regra['maximo']}'"
    else:
        raise ValueError(f"tipo de regra desconhecido: {tipo}")
    n = con.execute(f"SELECT count(*) FROM ({sql})").fetchall()[0][0]
    amostra = [str(l) for l in con.execute(f"{sql} LIMIT 5").fetchall()] if n else []
    con.close()
    return n, amostra

# %%
inicio = agora_utc()
resultados = []
for regra in regras:
    t0 = agora_utc()
    try:
        n, amostra = avaliar(regra)
        status = "OK" if n == 0 else "FALHA"
    except Exception as e:
        n, amostra, status = 0, [str(e)[:500]], "ERRO_EXECUCAO"
    resultados.append({
        "dt_execucao": inicio, "lote": lote, "grupo": regra["grupo"], "regra": regra["nome"],
        "tabela": regra["tabela"], "tipo": regra["tipo"], "severidade": regra["severidade"], "status": status,
        "linhas_problema": int(n), "amostra": json.dumps(amostra, ensure_ascii=False)[:4000],
        "segundos": round((agora_utc() - t0).total_seconds(), 1)})
    print(f"{status:<13} {regra['severidade']:<7} {regra['nome']}: {n}")

schema = pa.schema([("dt_execucao", pa.timestamp("us", tz="UTC")), ("lote", pa.string()), ("grupo", pa.string()),
                    ("regra", pa.string()), ("tabela", pa.string()), ("tipo", pa.string()),
                    ("severidade", pa.string()), ("status", pa.string()), ("linhas_problema", pa.int64()),
                    ("amostra", pa.string()), ("segundos", pa.float64())])
if resultados:
    gravar(LH_BRONZE, "ctl_qualidade", pa.Table.from_pylist(resultados, schema=schema),
           modo="append" if existe(LH_BRONZE, "ctl_qualidade") else "overwrite")

# %%
falhas_erro = [r["regra"] for r in resultados if r["status"] != "OK" and r["severidade"] == "erro"]
falhas_alerta = [r["regra"] for r in resultados if r["status"] != "OK" and r["severidade"] == "alerta"]
sair(json.dumps({"regras": len(resultados), "falhas_erro": falhas_erro, "falhas_alerta": falhas_alerta},
                ensure_ascii=False))
