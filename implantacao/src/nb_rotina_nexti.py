# %% [markdown]
# # nb_rotina_nexti
# Rotinas de alta frequência da Nexti, do Bronze à Gold numa única sessão de notebook. Um só agendamento
# (a cada 5 min) roda, em sequência, as rotinas de `ROTINAS` que estão ativas, dentro da janela de horário
# e com o intervalo vencido. Para cada rotina:
# 1. `nb_ingestao_api` busca na API só as tabelas da rotina, desde o último watermark;
# 2. sem nenhuma alteração na API, a rotina termina aqui (só uma linha SEM_ALTERACAO no log);
# 3. com alteração, `nb_orquestrador` leva o lote para a Silver e as Golds recalculam só o que mudou.
#
# Incluir uma rotina = uma entrada em `ROTINAS`. Rodar uma rotina sob demanda, fora do horário:
# executar o notebook com `rotina = "AUSENCIAS"`.
#
# A trava `Files/rotinas/nb_rotina_nexti.lock` (lakehouse Bronze) impede duas execuções simultâneas; a
# seguinte termina sem fazer nada. A trava expira em `trava_minutos`, caso uma execução morra sem
# liberá-la. As tabelas destas rotinas ficam fora do ciclo geral (TABELAS_ROTINA no nb_ingestao_api).

# %% tags=["parameters"]
rotina = ""                   # vazio = as rotinas no horário (agendamento); "AUSENCIAS" = só ela, a qualquer hora
modo_gold = "INCREMENTAL"     # COMPLETO recalcula as Golds inteiras; NENHUM pula as Golds (carga de histórico)
max_dias_por_execucao = 45    # janela máxima por tabela (recuperação de histórico)
configuracao_api = '{"NEXTI": {"key_vault": "https://kv-dw-fabric-kmf.vault.azure.net/"}}'
trava_minutos = 30
timeout_etapa_s = 3000

# %%
#@@COMUM@@

# %%
import uuid

# tabelas: endpoints da Nexti (ENDPOINTS_NEXTI no nb_ingestao_api)
# golds: em ordem; "nb_..." = notebook próprio, outro nome = consulta consultas/<nome>.sql executada pelo nb_gold
# intervalo_min: de quanto em quanto tempo roda; janela: horário de Brasília em que roda (inclusive)
ROTINAS = {
    "AUSENCIAS": {
        "tabelas": ["WORKPLACETRANSFERS", "ABSENCES"],
        "golds": ["nb_gold_fato_movimentacao_colaborador", "nb_gold_fato_ausencias"],
        "intervalo_min": 5, "janela": ("06:00", "20:00"), "ativo": True,
    },
    "COBERTURAS": {
        "tabelas": ["REPLACEMENTS"],
        "golds": ["gold_fato_coberturas"],
        "intervalo_min": 5, "janela": ("06:00", "20:00"), "ativo": True,
    },
    "MARCACAO": {
        "tabelas": ["CLOCKINGS"],
        "golds": ["gold_fato_marcacao_ponto"],
        "intervalo_min": 5, "janela": ("06:00", "20:00"), "ativo": False,  # ativar após a carga do histórico
    },
    # cadastros de que as Golds dependem: só sob demanda (o ciclo geral COMPLETO os atualiza às 05:00)
    "CADASTROS": {
        "tabelas": ["PERSONS", "WORKPLACES", "COMPANIES", "USERACCOUNTS"],
        "golds": [],
        "intervalo_min": None, "janela": None, "ativo": False,
    },
}
inicio = agora_utc()
_log = []

def log(lote, rot, tabela_id, etapa, linhas, status, msg=""):
    _log.append({"lote": lote, "ciclo": f"ROTINA_{rot}", "tabela_id": tabela_id, "etapa": etapa,
                 "linhas": int(linhas or 0), "status": status, "mensagem": (msg or "")[:4000],
                 "dt_registro": agora_utc()})

def gravar_log():
    if _log:
        schema = ler(LH_BRONZE, "ctl_log").schema
        gravar(LH_BRONZE, "ctl_log", pa.Table.from_pylist(_log, schema=schema))
        _log.clear()

def no_horario(nome, cfg):
    """Ativa, dentro da janela e com o intervalo vencido desde a última execução."""
    if not cfg["ativo"] or not cfg["intervalo_min"]:
        return False
    agora = dt.datetime.now(FUSO_BRT)
    ini, fim = cfg["janela"]
    if not (ini <= agora.strftime("%H:%M") <= fim):
        return False
    ultima = watermark_gold(f"ROTINA_{nome}", "execucao")
    # 1 min de folga: o agendamento não dispara exatamente no mesmo segundo
    return ultima is None or agora_utc() - ultima >= dt.timedelta(minutes=cfg["intervalo_min"] - 1)

if rotina:
    if rotina not in ROTINAS:
        raise ValueError(f"Rotina desconhecida: {rotina}. Disponíveis: {', '.join(ROTINAS)}")
    escolhidas = [rotina]
else:
    escolhidas = [n for n, c in ROTINAS.items() if no_horario(n, c)]

# %%
if _LOCAL:
    def pegar_trava():
        return True
    def soltar_trava():
        pass
    def rodar(nome, params):
        raise RuntimeError("nb_rotina_nexti só roda no Fabric (chama outros notebooks)")
else:
    TRAVA = f"abfss://{_WS}@onelake.dfs.fabric.microsoft.com/{LH_BRONZE}.Lakehouse/Files/rotinas/nb_rotina_nexti.lock"
    DONO = str(uuid.uuid4())

    def pegar_trava():
        if notebookutils.fs.exists(TRAVA):
            try:
                desde = dt.datetime.fromisoformat(notebookutils.fs.head(TRAVA, 100).split("|")[1])
            except Exception:
                desde = agora_utc()
            if agora_utc() - desde < dt.timedelta(minutes=int(trava_minutos)):
                return False
            print(f"trava vencida desde {desde:%H:%M}: assumindo a rotina")
        notebookutils.fs.put(TRAVA, f"{DONO}|{agora_utc().isoformat()}", True)
        return True

    def soltar_trava():
        try:
            if notebookutils.fs.head(TRAVA, 200).startswith(DONO):
                notebookutils.fs.rm(TRAVA)
        except Exception:
            pass

    def rodar(nome, params):
        return notebookutils.notebook.run(nome, int(timeout_etapa_s), params)

def manutencao(nome, cfg, lote):
    """Uma vez por dia: compacta os arquivos pequenos que a rotina gera a cada 5 min e remove os
    antigos (VACUUM de 7 dias). Falha aqui só vai para o log."""
    alvos = [(LH_BRONZE, "ctl_log"), (LH_BRONZE, "ctl_tabelas"), (LH_BRONZE, "ctl_gold")]
    for t in ler(LH_BRONZE, "ctl_tabelas").to_pylist():
        if t["origem"] == "NEXTI" and t["tabela_origem"] in cfg["tabelas"]:
            alvos += [(LH_BRONZE, nome_bronze(t["tabela_destino"])), (LH_SILVER, nome_silver(t["tabela_destino"]))]
    alvos += [(LH_GOLD, g.replace("nb_", "", 1)) for g in cfg["golds"]]
    for lh, tabela in alvos:
        try:
            if existe(lh, tabela):
                alvo = tabela_delta(lh, tabela)
                alvo.optimize.compact()
                alvo.vacuum(retention_hours=168, enforce_retention_duration=False, dry_run=False)
        except Exception as e:
            log(lote, nome, tabela, "MANUTENCAO", 0, "FALHA", str(e))
    gravar_watermark_gold(f"ROTINA_{nome}", "manutencao", agora_utc())

def hoje_ja(nome, marca):
    ultima = watermark_gold(f"ROTINA_{nome}", marca)
    return ultima is not None and ultima.astimezone(FUSO_BRT).date() == dt.datetime.now(FUSO_BRT).date()

def executar(nome, cfg):
    lote = f"rotina-{nome.lower()}-{uuid.uuid4()}"
    t_ini = agora_utc()
    gravar_watermark_gold(f"ROTINA_{nome}", "execucao", t_ini)
    res_api = rodar("nb_ingestao_api", {
        "lote": lote, "ciclo": "FATOS", "origens": "NEXTI", "tabelas": ",".join(cfg["tabelas"]),
        "configuracao_api": configuracao_api, "max_dias_por_execucao": int(max_dias_por_execucao)})
    copiadas = sum(int(float(s.split("|")[2] or 0)) for s in json.loads(res_api or "[]"))
    # nada mudou na API e as Golds já viraram o dia: termina sem Silver nem Gold. Os watermarks não
    # avançam; a próxima busca parte do mesmo ponto, o que só alarga um pouco a janela consultada.
    if copiadas == 0 and modo_gold != "COMPLETO" and (not cfg["golds"] or hoje_ja(nome, "golds_dia")):
        seg = (agora_utc() - t_ini).total_seconds()
        log(lote, nome, f"ROTINA.{nome}", "ROTINA", 0, "SEM_ALTERACAO", f"nenhuma alteração na API | {seg:.0f}s")
        return f"{nome}: sem alteração ({seg:.0f}s)"
    rodar("nb_orquestrador", {
        "lote": lote, "ciclo": "FATOS", "resultados": "[]", "resultados_api": res_api,
        "status_ciclo": "SUCESSO", "origens": "NEXTI"})
    resumo = [f"{copiadas} linhas da API"]
    for g in (cfg["golds"] if modo_gold != "NENHUM" else []):
        t0 = agora_utc()
        if g.startswith("nb_"):
            r = json.loads(rodar(g, {"lote": lote, "modo": modo_gold}) or "{}")
        else:
            r = json.loads(rodar("nb_gold", {"gold": g, "lote": lote, "modo": modo_gold}) or "{}")
        log(lote, nome, r.get("gold", g), "GOLD", r.get("linhas", 0), "SUCESSO",
            f"{json.dumps(r, ensure_ascii=False)} | {(agora_utc() - t0).total_seconds():.0f}s")
        resumo.append(f"{r.get('gold', g)}: {r.get('linhas', 0)} linhas")
    if cfg["golds"] and modo_gold != "NENHUM":
        gravar_watermark_gold(f"ROTINA_{nome}", "golds_dia", agora_utc())
    if not hoje_ja(nome, "manutencao"):
        manutencao(nome, cfg, lote)
        resumo.append("manutenção diária")
    seg = (agora_utc() - t_ini).total_seconds()
    log(lote, nome, f"ROTINA.{nome}", "ROTINA", copiadas, "SUCESSO", " | ".join(resumo) + f" | {seg:.0f}s")
    return f"{nome}: " + " | ".join(resumo) + f" | {seg:.0f}s"

# %%
resultado = {"rotinas": escolhidas, "status": "SEM_ROTINA_NO_HORARIO" if not escolhidas else "EM_EXECUCAO"}
falhas = []
if escolhidas and not pegar_trava():
    print("nb_rotina_nexti já em execução: nada a fazer")
elif escolhidas:
    try:
        for nome in escolhidas:
            try:
                print(executar(nome, ROTINAS[nome]))
            except Exception as e:
                falhas.append(f"{nome}: {e}")
                log("", nome, f"ROTINA.{nome}", "ROTINA", 0, "FALHA", str(e))
            gravar_log()
        resultado = {"rotinas": escolhidas, "status": "FALHA" if falhas else "SUCESSO",
                     "segundos": round((agora_utc() - inicio).total_seconds())}
    finally:
        soltar_trava()
else:
    print("nenhuma rotina no horário")

# %%
# falha de uma rotina não impede as outras; no fim, a execução fica marcada como falha no Fabric
if falhas:
    raise RuntimeError("Rotinas com falha:\n" + "\n".join(falhas))
# fora do try: no Fabric o exit pode ser tratado como exceção e cair no except
sair(json.dumps(resultado))
