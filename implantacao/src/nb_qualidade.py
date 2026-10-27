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
    return "`" + nome.replace("`", "``") + "`"

def tabelas_da_camada(camada):
    lh = LAKEHOUSES[camada]
    if _LOCAL:
        raiz = os.path.join(os.environ["DW_LOCAL_ROOT"], lh)
        return sorted(n for n in os.listdir(raiz) if os.path.isdir(os.path.join(raiz, n)))
    return sorted(i.name.rstrip("/") for i in notebookutils.fs.ls(f"{raiz(lh)}/Tables"))

def avaliar(regra):
    """Devolve (linhas com problema, amostra)."""
    camada, _, tabela = regra["tabela"].partition(".")
    tipo = regra["tipo"]
    if tipo == "sem_timestamp_sem_fuso":
        ruins = []
        for t in tabelas_da_camada(camada):
            for campo in ler(LAKEHOUSES[camada], t).schema:
                if isinstance(campo.dataType, T.TimestampNTZType):
                    ruins.append(f"{t}.{campo.name}")
        return len(ruins), ruins[:5]
    colunas = regra.get("colunas") or ([regra["coluna"]] if regra.get("coluna") else None)
    dados = ler(LAKEHOUSES[camada], tabela, colunas=colunas)
    if tipo == "minimo_linhas":
        n = dados.count()
        return (0 if n >= int(regra["minimo"]) else 1), [f"{n} linhas"]
    dados.createOrReplaceTempView("t")
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
    n = spark.sql(f"SELECT count(*) FROM ({sql})").first()[0]
    amostra = [str(tuple(l)) for l in spark.sql(f"{sql} LIMIT 5").collect()] if n else []
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

schema = ("dt_execucao timestamp, lote string, grupo string, regra string, tabela string, tipo string, "
          "severidade string, status string, linhas_problema bigint, amostra string, segundos double")
if resultados:
    gravar(LH_BRONZE, "ctl_qualidade", criar(resultados, schema),
           modo="append" if existe(LH_BRONZE, "ctl_qualidade") else "overwrite")

# %%
falhas_erro = [r["regra"] for r in resultados if r["status"] != "OK" and r["severidade"] == "erro"]
falhas_alerta = [r["regra"] for r in resultados if r["status"] != "OK" and r["severidade"] == "alerta"]
sair(json.dumps({"regras": len(resultados), "falhas_erro": falhas_erro, "falhas_alerta": falhas_alerta},
                ensure_ascii=False))
