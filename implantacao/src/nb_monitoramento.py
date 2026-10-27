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
# `silenciar_horas`. Com a URL do Workflow do Teams no Key Vault, os alertas novos das severidades de
# `notificacao` vão para o chat do destinatário (ou canal) configurado no Workflow.

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
ctl = coletar(ler(LH_BRONZE, "ctl_tabelas"))
for t in ctl:
    if not t["ativo"] or t["modo_carga"] != "INCREMENTAL" or t["dt_atualizacao"] is None:
        continue
    horas = (agora - t["dt_atualizacao"]).total_seconds() / 3600
    if horas > limite_h:
        alerta("TABELA_PARADA", t["id"], "alerta",
               f"{t['id']} sem avançar há {horas:.0f} h (último watermark {t['ultimo_watermark']})")

# ---------------------------------------------------------------- 3. falhas no log
ultima_verificacao = watermark_gold("MONITORAMENTO", "ctl_log") or (agora - dt.timedelta(hours=horas_log))
log = coletar(ler(LH_BRONZE, "ctl_log", filtros=[("dt_registro", ">", ultima_verificacao)]))
for x in log:
    if x["status"] in ("FALHA", "PARCIAL") and x["etapa"] in ("CICLO", "ROTINA", "GOLD", "SILVER", "BRONZE"):
        alerta(f"FALHA_{x['etapa']}", f"{x['ciclo']}|{x['tabela_id']}", "erro",
               f"{x['ciclo']} {x['tabela_id']} ({x['etapa']}) {x['status']}: {x['mensagem'][:500]}")

# ---------------------------------------------------------------- 4. volume das tabelas FULL
queda = cfg.get("volume_queda_max_percentual", 70) / 100
completos = coletar(ler(LH_BRONZE, "ctl_log", filtros=[("ciclo", "=", "COMPLETO"), ("etapa", "=", "SILVER"),
                                                       ("dt_registro", ">", agora - dt.timedelta(days=15))]))
por_tabela = {}
for x in sorted(completos, key=lambda x: x["dt_registro"]):
    if x["status"] == "SUCESSO":
        por_tabela.setdefault(x["tabela_id"], []).append(x)
modos = {t["id"]: t["modo_carga"] for t in ctl}
for tid, xs in por_tabela.items():
    if modos.get(tid) != "FULL" or len(xs) < 3 or xs[-1]["dt_registro"] <= ultima_verificacao:
        continue
    media = sum(x["linhas"] for x in xs[:-1]) / len(xs[:-1])
    if media > 0 and xs[-1]["linhas"] < media * (1 - queda):
        alerta("VOLUME_BAIXO", tid, "alerta",
               f"{tid}: {xs[-1]['linhas']} linhas no último ciclo completo, média de {media:.0f}")

# ---------------------------------------------------------------- 5. qualidade
if existe(LH_BRONZE, "ctl_qualidade"):
    qual = coletar(ler(LH_BRONZE, "ctl_qualidade", filtros=[("dt_execucao", ">", ultima_verificacao)]))
    for x in qual:
        if x["status"] != "OK":
            alerta("QUALIDADE", f"{x['grupo']}|{x['regra']}", x["severidade"],
                   f"{x['regra']} ({x['tabela']}): {x['status']}, {x['linhas_problema']} linhas. Exemplos: {x['amostra'][:300]}")

# %%
# não repete o mesmo alerta dentro de silenciar_horas
silenciar = dt.timedelta(hours=cfg.get("silenciar_horas", 6))
recentes = set()
if existe(LH_BRONZE, "ctl_alertas"):
    recentes = {a["chave"] for a in coletar(ler(LH_BRONZE, "ctl_alertas", filtros=[("dt_alerta", ">", agora - silenciar)],
                                                colunas=["chave"]))}
novos = list({a["chave"]: a for a in alertas if a["chave"] not in recentes}.values())   # 1 por chave
schema = "dt_alerta timestamp, tipo string, objeto string, severidade string, mensagem string, chave string"
if novos:
    gravar(LH_BRONZE, "ctl_alertas", criar(novos, schema),
           modo="append" if existe(LH_BRONZE, "ctl_alertas") else "overwrite")
gravar_watermark_gold("MONITORAMENTO", "ctl_log", agora)

# Teams: os novos das severidades configuradas, num cartão só, pelo Workflow do Teams
notif = cfg.get("notificacao") or {}
erros = [a for a in novos if a["severidade"] in notif.get("severidades", ["erro"])]

def cartao(itens):
    """Mensagem no formato dos Workflows do Teams (cartão adaptável)."""
    corpo = [{"type": "TextBlock", "size": "Medium", "weight": "Bolder", "text": "DW Fabric: alertas"},
             {"type": "TextBlock", "isSubtle": True, "spacing": "None",
              "text": f"{agora_brt:%d/%m/%Y %H:%M} · {len(itens)} novo(s)"}]
    corpo += [{"type": "TextBlock", "wrap": True, "text": f"**{a['tipo']}** · {a['objeto']}\n\n{a['mensagem'][:500]}"}
              for a in itens[:20]]
    if len(itens) > 20:
        corpo.append({"type": "TextBlock", "text": f"... e mais {len(itens) - 20} em ctl_alertas"})
    return {"type": "message", "destinatario": notif.get("destinatario"),
            "attachments": [{"contentType": "application/vnd.microsoft.card.adaptive", "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json", "type": "AdaptiveCard",
                "version": "1.4", "body": corpo}}]}

teams = "sem alertas"
if erros and _LOCAL:
    teams = "local"
elif erros and notif.get("teams_workflow_segredo"):
    try:
        cofre = json.loads(configuracao_api)["NEXTI"]["key_vault"]
        url = notebookutils.credentials.getSecret(cofre, notif["teams_workflow_segredo"])
        import requests
        r = requests.post(url, json=cartao(erros), timeout=30)
        r.raise_for_status()
        teams = f"enviado para {notif.get('destinatario') or 'o Workflow'}"
    except Exception as e:   # sem Workflow configurado ou fora do ar: o alerta continua no ctl_alertas
        teams = f"não notificado: {str(e)[:200]}"
print(f"Teams: {teams}")

for a in novos:
    print(f"{a['severidade']:<7} {a['tipo']:<16} {a['mensagem']}")
sair(json.dumps({"alertas": len(alertas), "novos": len(novos), "erros": len(erros), "teams": teams}))
