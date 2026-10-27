# %% [markdown]
# # nb_preparar_ciclo
# Primeiro passo do pipeline mestre. Lê `ctl_tabelas` e devolve, por origem, a lista de tabelas do ciclo
# com as consultas prontas:
# - `query_max`: lida pelo Lookup na origem (novo watermark, lido **antes** da cópia)
# - `query_copy`: consulta da Copy activity, com o marcador `#NOVO#` trocado pelo pipeline
#
# Ciclo `AUTO`: antes das 09:00 (Brasília) = COMPLETO (dimensões + fatos); depois = FATOS.

# %% tags=["parameters"]
ciclo = "AUTO"        # AUTO | COMPLETO | FATOS
lote = "manual"       # o pipeline envia @pipeline().RunId
origens = ""          # filtro opcional, ex.: "NEXTI" ou "GS,4S" (vazio = todas as ativas)
hora_corte_completo = 9

# %%
#@@COMUM@@

# %%
# Alias "novo" entre aspas no Firebird para o Lookup receber a coluna em minúsculas.
MARCADOR = "#NOVO#"

SQL = {
    "SQLSERVER": {
        "agora": "SYSDATETIME()",
        "q_agora": "SELECT CONVERT(varchar(30), SYSDATETIME(), 126) AS novo",
        "q_max_data": "SELECT CONVERT(varchar(30), MAX({col}), 126) AS novo FROM {tab}",
        "q_max_id": "SELECT CAST(MAX({col}) AS varchar(30)) AS novo FROM {tab}",
        "fmt_data": "%Y-%m-%dT%H:%M:%S",
        "dias_atras": "DATEADD(DAY, -{dias}, CAST(GETDATE() AS date))",
    },
    "FIREBIRD": {
        "agora": "CURRENT_TIMESTAMP",
        "q_agora": 'SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR(30)) AS "novo" FROM RDB$DATABASE',
        "q_max_data": 'SELECT CAST(MAX({col}) AS VARCHAR(30)) AS "novo" FROM {tab}',
        "q_max_id": 'SELECT CAST(MAX({col}) AS VARCHAR(30)) AS "novo" FROM {tab}',
        "fmt_data": "%Y-%m-%d %H:%M:%S",
        "dias_atras": "DATEADD(-{dias} DAY TO CURRENT_DATE)",
    },
}

def inicio_janela(ultimo, minutos, fmt):
    """Watermark anterior menos a janela de segurança, no formato literal do dialeto."""
    base = dt.datetime.strptime(ultimo.strip()[:19].replace(" ", "T"), "%Y-%m-%dT%H:%M:%S")
    return (base - dt.timedelta(minutes=minutos or 0)).strftime(fmt)

def montar(r, dialeto):
    s = SQL[dialeto]
    tab, col = r["tabela_origem"], r["coluna_watermark"]
    modo = r["modo_carga"]
    ultimo = (r["ultimo_watermark"] or "").strip()
    colunas = r["colunas"] or "*"
    if colunas.strip() == "*" and dialeto == "FIREBIRD":
        colunas = f"{tab}.*"   # Firebird não aceita '*' seguido de outras colunas
    tecnicas = f"'{r['origem']}' AS \"_origem\", '{lote}' AS \"_lote\", {s['agora']} AS \"_dt_carga\""
    filtros = []

    if modo == "INCREMENTAL" and r["tipo_watermark"] == "ID":
        q_max = s["q_max_id"].format(col=col, tab=tab)
        if ultimo:
            filtros.append(f"{col} > {int(ultimo)}")
        filtros.append(f"{col} <= {MARCADOR}")
    elif modo == "INCREMENTAL":
        q_max = s["q_max_data"].format(col=col, tab=tab)
        if ultimo:
            filtros.append(f"{col} > '{inicio_janela(ultimo, r['janela_seguranca_min'], s['fmt_data'])}'")
            filtros.append(f"{col} <= '{MARCADOR}'")
        else:
            # primeira carga: também as linhas sem data de alteração (registros anteriores à coluna,
            # como o S_T_A_M_P_ do Protheus), que nenhuma carga incremental traria depois
            filtros.append(f"({col} <= '{MARCADOR}' OR {col} IS NULL)")
    elif modo == "JANELA":
        q_max = s["q_agora"]
        filtros.append(f"{col} >= {s['dias_atras'].format(dias=int(r['janela_dias']))}")
    else:  # FULL
        q_max = s["q_agora"]

    if r["filtro_adicional"]:
        filtros.append(f"({r['filtro_adicional']})")
    where = (" WHERE " + " AND ".join(filtros)) if filtros else ""
    return {
        "id": r["id"],
        "tabela_destino": nome_bronze(r["tabela_destino"]),   # destino da Copy activity
        "modo_carga": modo,
        "wm_atual": ultimo,
        "query_max": q_max,
        "query_copy": f"SELECT {colunas}, {tecnicas} FROM {tab}{where}",
    }

# %%
if ciclo == "AUTO":
    ciclo = "COMPLETO" if dt.datetime.now(FUSO_BRT).hour < hora_corte_completo else "FATOS"
tipos = {"DIM", "FATO"} if ciclo == "COMPLETO" else {"FATO"}

filtro = {o.strip().upper() for o in origens.split(",") if o.strip()}
origens = {o["origem"]: o for o in coletar(ler(LH_BRONZE, "ctl_origens"))}
ativas = sorted(n for n, o in origens.items() if o["ativo"])
# nome errado no filtro faria um ciclo vazio terminar com "sucesso": melhor falhar já aqui
invalidas = sorted(filtro - set(ativas))
if invalidas:
    raise ValueError(f"Origens inexistentes ou inativas: {', '.join(invalidas)}. "
                     f"Ativas: {', '.join(ativas)} (vazio = todas)")
selecionadas = sorted(filtro) if filtro else ativas
saida = {nome: [] for nome in origens}          # toda origem aparece, mesmo vazia
for r in coletar(ler(LH_BRONZE, "ctl_tabelas")):
    o = origens.get(r["origem"])
    # origens API são lidas pelo nb_ingestao_api, não pela cópia do pipeline
    if (r["ativo"] and o and o["ativo"] and o["dialeto"] != "API" and r["tipo"] in tipos
            and (not filtro or r["origem"] in filtro)):
        saida[r["origem"]].append(montar(r, o["dialeto"]))

resumo = {k: len(v) for k, v in saida.items()}
print(f"Ciclo {ciclo} | lote {lote} | origens {selecionadas} | tabelas por origem: {resumo}")
# ids planejados: o orquestrador compara com os resultados para achar cópias que falharam
ids = [t["id"] for lista in saida.values() for t in lista]
sair(json.dumps({"ciclo": ciclo, "lote": lote, "origens": saida, "selecionadas": ",".join(selecionadas),
                 "ids": ids}, ensure_ascii=False))
