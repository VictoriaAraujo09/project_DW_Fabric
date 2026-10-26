# %% [markdown]
# # nb_monitoramento
# Verifica a saúde do DW e grava os problemas em `ctl_alertas` (lakehouse Bronze):
# 1. **Rotina atrasada**: rotina do nb_rotina_nexti sem executar há mais que o limite, dentro da janela;
# 2. **Tabela parada**: tabela incremental do ciclo geral com o watermark sem avançar além do limite;
# 3. **Falhas**: etapas com FALHA/PARCIAL no `ctl_log` desde a última verificação;
# 4. **Volume**: tabela FULL do ciclo completo com queda grande em relação à média dos últimos ciclos;
# 5. **Qualidade**: regras do `ctl_qualidade` quebradas na última execução.
#
# Limites em `config/qualidade.yml` (seção `monitoramento`). O mesmo alerta não se repete dentro de
# `silenciar_horas`. Com o segredo do webhook no Key Vault, os alertas de severidade erro vão para o Teams.

# %% tags=["parameters"]
configuracao_api = '{"NEXTI": {"key_vault": "https://kv-dw-fabric-kmf.vault.azure.net/"}}'
horas_log = 24          # janela do ctl_log analisada na primeira execução

# %%
#@@COMUM@@

# %%
cfg = ler_config("qualidade.yml")["monitoramento"]
agora = agora_utc()
agora_brt = dt.datetime.now(FUSO_BRT)
alertas = []

def alerta(tipo, objeto, severidade, mensagem):
    alertas.append({"dt_alerta": agora, "tipo": tipo, "objeto": objeto, "severidade": severidade,
                    "mensagem": mensagem[:4000], "chave": f"{tipo}|{objeto}"})

# ---------------------------------------------------------------- 1. rotinas atrasadas
for rotina, lim in cfg.get("rotinas", {}).items():
    ini, fim = lim["janela"]
    if not (ini <= agora_brt.strftime("%H:%M") <= fim):
        continue
    ultima = watermark_gold(f"ROTINA_{rotina}", "execucao")
    # tolerância no começo da janela: a primeira execução do dia ainda pode não ter rodado
    inicio_janela = agora_brt.replace(hour=int(ini[:2]), minute=int(ini[3:]), second=0, microsecond=0)
    referencia = max(ultima, inicio_janela) if ultima else inicio_janela
    atraso = (agora - referencia).total_seconds() / 60
    if atraso > lim["atraso_max_min"]:
        desde = f"desde {ultima.astimezone(FUSO_BRT):%d/%m %H:%M}" if ultima else "nunca executou"
        alerta("ROTINA_ATRASADA", rotina, "erro", f"Rotina {rotina} sem executar há {atraso:.0f} min ({desde})")

# ---------------------------------------------------------------- 2. tabelas incrementais paradas
limite_h = cfg.get("tabelas_incrementais_max_horas", 30)
for t in ler(LH_BRONZE, "ctl_tabelas").to_pylist():
    if not t["ativo"] or t["modo_carga"] != "INCREMENTAL" or t["dt_atualizacao"] is None:
        continue
    horas = (agora - t["dt_atualizacao"]).total_seconds() / 3600
    if horas > limite_h:
        alerta("TABELA_PARADA", t["id"], "alerta",
               f"{t['id']} sem avançar há {horas:.0f} h (último watermark {t['ultimo_watermark']})")

# ---------------------------------------------------------------- 3. falhas no log
ultima_verificacao = watermark_gold("MONITORAMENTO", "ctl_log") or (agora - dt.timedelta(hours=horas_log))
log = ler(LH_BRONZE, "ctl_log", filtros=[("dt_registro", ">", ultima_verificacao)]).to_pylist()
for x in log:
    if x["status"] in ("FALHA", "PARCIAL") and x["etapa"] in ("CICLO", "ROTINA", "GOLD", "SILVER", "BRONZE"):
        alerta(f"FALHA_{x['etapa']}", f"{x['ciclo']}|{x['tabela_id']}", "erro",
               f"{x['ciclo']} {x['tabela_id']} ({x['etapa']}) {x['status']}: {x['mensagem'][:500]}")

# ---------------------------------------------------------------- 4. volume das tabelas FULL
queda = cfg.get("volume_queda_max_percentual", 70) / 100
completos = ler(LH_BRONZE, "ctl_log", filtros=[("ciclo", "=", "COMPLETO"), ("etapa", "=", "SILVER"),
                                               ("dt_registro", ">", agora - dt.timedelta(days=15))]).to_pylist()
por_tabela = {}
for x in sorted(completos, key=lambda x: x["dt_registro"]):
    if x["status"] == "SUCESSO":
        por_tabela.setdefault(x["tabela_id"], []).append(x)
modos = {t["id"]: t["modo_carga"] for t in ler(LH_BRONZE, "ctl_tabelas").to_pylist()}
for tid, xs in por_tabela.items():
    if modos.get(tid) != "FULL" or len(xs) < 3 or xs[-1]["dt_registro"] <= ultima_verificacao:
        continue
    media = sum(x["linhas"] for x in xs[:-1]) / len(xs[:-1])
    if media > 0 and xs[-1]["linhas"] < media * (1 - queda):
        alerta("VOLUME_BAIXO", tid, "alerta",
               f"{tid}: {xs[-1]['linhas']} linhas no último ciclo completo, média de {media:.0f}")

# ---------------------------------------------------------------- 5. qualidade
if existe(LH_BRONZE, "ctl_qualidade"):
    qual = ler(LH_BRONZE, "ctl_qualidade", filtros=[("dt_execucao", ">", ultima_verificacao)]).to_pylist()
    for x in qual:
        if x["status"] != "OK":
            alerta("QUALIDADE", f"{x['grupo']}|{x['regra']}", x["severidade"],
                   f"{x['regra']} ({x['tabela']}): {x['status']}, {x['linhas_problema']} linhas. Exemplos: {x['amostra'][:300]}")

# %%
# não repete o mesmo alerta dentro de silenciar_horas
silenciar = dt.timedelta(hours=cfg.get("silenciar_horas", 6))
recentes = set()
if existe(LH_BRONZE, "ctl_alertas"):
    recentes = {a["chave"] for a in ler(LH_BRONZE, "ctl_alertas", filtros=[("dt_alerta", ">", agora - silenciar)],
                                        colunas=["chave"]).to_pylist()}
novos = list({a["chave"]: a for a in alertas if a["chave"] not in recentes}.values())   # 1 por chave
schema = pa.schema([("dt_alerta", pa.timestamp("us", tz="UTC")), ("tipo", pa.string()), ("objeto", pa.string()),
                    ("severidade", pa.string()), ("mensagem", pa.string()), ("chave", pa.string())])
if novos:
    gravar(LH_BRONZE, "ctl_alertas", pa.Table.from_pylist(novos, schema=schema),
           modo="append" if existe(LH_BRONZE, "ctl_alertas") else "overwrite")
gravar_watermark_gold("MONITORAMENTO", "ctl_log", agora)

# Teams: só os novos de severidade erro, numa mensagem só
erros = [a for a in novos if a["severidade"] == "erro"]
if erros and not _LOCAL and cfg.get("teams_webhook_segredo"):
    try:
        cofre = json.loads(configuracao_api)["NEXTI"]["key_vault"]
        url = notebookutils.credentials.getSecret(cofre, cfg["teams_webhook_segredo"])
        import requests
        texto = "**DW Fabric: alertas**\n\n" + "\n\n".join(f"- {a['mensagem']}" for a in erros[:20])
        requests.post(url, json={"text": texto}, timeout=30)
    except Exception as e:   # sem webhook configurado ou fora do ar: o alerta continua no ctl_alertas
        print(f"Teams não notificado: {str(e)[:200]}")

for a in novos:
    print(f"{a['severidade']:<7} {a['tipo']:<16} {a['mensagem']}")
sair(json.dumps({"alertas": len(alertas), "novos": len(novos), "erros": len(erros)}))
