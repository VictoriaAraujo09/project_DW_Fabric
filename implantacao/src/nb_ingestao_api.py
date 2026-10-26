# %% [markdown]
# # nb_ingestao_api
# Ingestão das origens **API** (hoje: Nexti) na Bronze. Roda no pipeline mestre em paralelo às cópias
# dos bancos e devolve ao orquestrador a mesma lista `id|novo_watermark|linhas` que o pipeline monta
# para as origens SQL. O orquestrador cuida da Silver, dos watermarks e do log.
#
# - Credenciais no Azure Key Vault (`configuracao_api`), nunca no código.
# - Só os campos de `COLUNAS_NEXTI` são gravados; credenciais ficam fora. Dados pessoais e de saúde entram
#   como no DW do Airflow (decisão de 26/10/2026): CID, médico e justificativa nas ausências; CPF, e-mail,
#   telefone, PIS e geolocalização nas marcações, colaboradores e postos.
# - Incremental por janela (data de alteração ou data de referência), com recuperação de histórico em
#   blocos de até `max_dias_por_execucao` dias por ciclo.

# %% tags=["parameters"]
lote = "manual"
ciclo = "COMPLETO"            # já resolvido pelo nb_preparar_ciclo (COMPLETO | FATOS)
origens = ""                  # filtro opcional, ex.: "NEXTI"
tabelas = ""                  # filtro opcional de endpoints, ex.: "ABSENCES,WORKPLACETRANSFERS" (rotinas)
configuracao_api = "{}"       # {"NEXTI": {"key_vault": "https://<cofre>.vault.azure.net/"}}
inicio_historico = "2025-01-01"
max_dias_por_execucao = 45

# %%
#@@COMUM@@

# %%
import time, requests

NEXTI_URL = "https://api.nexti.com"
FMT_NEXTI = "%d%m%Y%H%M%S"
TAMANHO_PAGINA = 60000  # um dia de marcações (~45 mil) numa chamada: o limite da API é por chamada pesada
TIPOS_MARCACAO = ",".join(str(i) for i in range(1, 11))  # sem o filtro a API devolve zero marcações

# Tabelas com rotina dedicada (nb_rotina_nexti, a cada poucos minutos): o ciclo geral não as carrega,
# para a mesma janela não ser copiada duas vezes. Só entram quando pedidas em `tabelas`.
TABELAS_ROTINA = {"ABSENCES", "WORKPLACETRANSFERS", "REPLACEMENTS", "CLOCKINGS"}

# Início do histórico por endpoint, quando difere de `inicio_historico`: a transferência antiga define o
# posto atual do colaborador, então a movimentação precisa do histórico completo.
INICIO_HISTORICO_TABELA = {"WORKPLACETRANSFERS": "2010-01-01"}

# chave (tabela_origem em ctl_tabelas): caminho, parâmetros, onde estão os itens, janela máxima por chamada.
# utc=True: o endpoint trabalha em UTC (conferido: marcação 3h à frente do firstClocking do previsto);
# a janela e o watermark ficam em UTC e as datas gravadas são convertidas para o horário de Brasília.
ENDPOINTS_NEXTI = {
    "CLOCKINGS": dict(caminho="/clockings/lastupdate/start/{ini}/finish/{fim}/types",
                      params={"types": TIPOS_MARCACAO}, itens="content", janela_dias=1, utc=True),
    "TIMETRACKINGS": dict(caminho="/timetrackings/estimatedhoursvsworkedhours",
                          params={"startDate": "{ini}", "finishDate": "{fim}"}, itens="value", janela_dias=7,
                          por_dia=True),
    "ABSENCES": dict(caminho="/absences/lastupdate/start/{ini}/finish/{fim}", itens="content", janela_dias=30,
                     tamanho=1000),
    "WORKPLACETRANSFERS": dict(caminho="/workplacetransfers/lastupdate/start/{ini}/finish/{fim}", itens="content",
                               janela_dias=30, tamanho=1000),
    "REPLACEMENTS": dict(caminho="/replacements/lastupdate/start/{ini}/finish/{fim}", itens="content",
                         janela_dias=30, tamanho=1000),
    # usuários ficam em outro host; a resposta traz senha e token, que não entram em COLUNAS_NEXTI
    "USERACCOUNTS": dict(caminho="https://pontodigital.api.nexti.com/security/useraccounts/", itens="content",
                         tamanho=1000),
    "PERSONS": dict(caminho="/persons/all", itens="content"),
    "WORKPLACES": dict(caminho="/workplaces/all", itens="content"),
    "SCHEDULES": dict(caminho="/schedules/all", itens="content"),
    "SHIFTS": dict(caminho="/shifts/all", itens="content"),
    "CLIENTS": dict(caminho="/clients/all", itens="content"),
    "COMPANIES": dict(caminho="/companies/all", itens="content"),
    "BUSINESSUNITS": dict(caminho="/businessunits/all", itens="content"),
    "CAREERS": dict(caminho="/careers/all", itens="content"),
    "ABSENCESITUATIONS": dict(caminho="/absencesituations/all", itens="content"),
}

# campo: tipo (inteiro, numero, texto, booleano, data_hora, json). Gerado de docs/origens/nexti/estrutura.csv:
# só os campos com proposta 'carregar' + nome do colaborador. Dados pessoais, de saúde e texto livre ficam fora.
COLUNAS_NEXTI = {
    "CLOCKINGS": {
        "adjustmentDate": "data_hora", "adjustmentReasonId": "inteiro", "clockingCollectorId": "inteiro",
        "clockingCollectorName": "texto", "clockingDate": "data_hora", "clockingOriginId": "inteiro",
        "clockingTypeId": "inteiro", "clockingTypeName": "texto", "companyNumber": "texto",
        "deviceCode": "texto", "deviceId": "inteiro", "directionTypeId": "inteiro", "externalPersonId": "texto",
        "id": "inteiro", "identificationMethodId": "inteiro", "identificationMethodName": "texto",
        "lastUpdate": "data_hora", "online": "booleano", "personId": "inteiro", "referenceDate": "data_hora",
        "registerDate": "data_hora", "removed": "booleano", "timezone": "texto", "userAdjustmentId": "inteiro",
        "workplaceId": "inteiro",
        # dados pessoais, igual ao DW do Airflow (decisão de 26/10/2026)
        "adjustmentObservation": "texto", "latitude": "numero", "longitude": "numero", "personPIS": "texto",
        "ticketSignature": "texto"},
    "TIMETRACKINGS": {
        "absenceSituation": "texto", "adjustmentReason": "texto", "costCenter": "texto",
        "expectedHours": "texto", "expectedMinutes": "inteiro", "firstClocking": "texto", "month": "inteiro",
        "personEnrolment": "texto", "personExternalId": "texto", "personId": "inteiro",
        "referenceDate": "data_hora", "scheduleId": "inteiro", "scheduleName": "texto",
        "timeTrackingEvents": "json", "timeTrackingPeriodId": "inteiro", "timeTrackingPeriodName": "texto",
        "workedMinutes": "inteiro", "workedMinutesWithOvertime": "inteiro", "workplaceName": "texto",
        "year": "inteiro", "personName": "texto"},
    "ABSENCES": {
        "absenceSituationExternalId": "texto", "absenceSituationId": "inteiro", "demissionReasonId": "inteiro",
        "finishDateTime": "data_hora", "finishMinute": "inteiro", "id": "inteiro", "lastUpdate": "data_hora",
        "medicalDoctorId": "inteiro", "personExternalId": "texto", "personId": "inteiro", "removed": "booleano",
        "startDateTime": "data_hora", "startMinute": "inteiro", "userRegisterId": "inteiro",
        "note": "texto", "medicalDoctorName": "texto", "medicalDoctorCrm": "texto", "cidId": "inteiro",
        "cidCode": "texto", "cidDescription": "texto"},
    "WORKPLACETRANSFERS": {
        "id": "inteiro", "personId": "inteiro", "workplaceId": "inteiro", "transferDateTime": "data_hora",
        "lastUpdate": "data_hora", "removed": "booleano", "userRegisterId": "inteiro"},
    "USERACCOUNTS": {"id": "inteiro", "name": "texto", "active": "booleano"},
    "REPLACEMENTS": {
        "id": "inteiro", "personId": "inteiro", "personName": "texto", "absenteeId": "inteiro",
        "absenteeName": "texto", "workplaceId": "inteiro", "startDateTime": "data_hora",
        "finishDateTime": "data_hora", "replacementTypeId": "inteiro", "note": "texto",
        "registerDate": "data_hora", "replacementReasonId": "inteiro", "replacementReasonName": "texto",
        "replacementResourceId": "inteiro", "replacementResourceName": "texto", "shiftId": "inteiro",
        "lastUpdate": "data_hora", "removed": "booleano"},
    "PERSONS": {
        "adminDevice": "booleano", "admissionDate": "data_hora", "allowMobileClocking": "booleano",
        "allowMobileMonthComptime": "booleano", "businessUnitId": "inteiro", "businessUnitName": "texto",
        "careerId": "inteiro", "city": "texto", "companyId": "inteiro", "country": "texto",
        "demissionDate": "data_hora", "enrolment": "texto", "externalCareerId": "texto",
        "externalCompanyId": "texto", "externalId": "texto", "externalScheduleId": "texto",
        "federatedUnitInitials": "texto", "gender": "texto", "generateNightBonusByExpectedWorkload": "booleano",
        "id": "inteiro", "ignoreTimeTracking": "booleano", "ignoreTimeTrackingDate": "data_hora",
        "ignoreValidation": "booleano", "personSituationId": "inteiro", "personTypeId": "inteiro",
        "rotationCode": "inteiro", "rotationId": "inteiro", "scheduleId": "inteiro", "state": "texto",
        "userAccountId": "inteiro", "workplaceId": "inteiro", "name": "texto",
        "cpf": "texto", "email": "texto", "phone": "texto"},
    "WORKPLACES": {
        "active": "booleano", "alterBonusForReplacement": "booleano", "businessUnitId": "inteiro",
        "checkinTolerance": "inteiro", "checkoutTolerance": "inteiro", "cityExternalId": "texto",
        "cityId": "inteiro", "cityName": "texto", "clientId": "inteiro", "clientName": "texto",
        "companyId": "inteiro", "companyNumber": "texto", "contractNumber": "inteiro",
        "contractNumberTxt": "texto", "controlLackOfEffectiveness": "booleano", "costCenter": "texto",
        "createMaximumPerimeterToRecordMarking": "booleano", "externalCompanyId": "texto",
        "federatedUnitExternalId": "texto", "federatedUnitId": "inteiro", "federatedUnitInitials": "texto",
        "federatedUnitName": "texto", "finalScheduleAdjustment": "booleano", "finishDate": "data_hora",
        "forceCompanyReplacement": "booleano", "forceCompanyTransfer": "booleano",
        "fractionalVacancy": "booleano", "generateFlexibleInconsistenciesAlert": "booleano",
        "generateVirtualClocking": "booleano", "id": "inteiro", "initialScheduleAdjustment": "booleano",
        "intervalScheduleAdjustment": "booleano", "lastUpdate": "data_hora",
        "maximumPerimeterRecordMarking": "inteiro", "name": "texto", "registerDate": "data_hora",
        "service": "texto", "serviceTypeId": "inteiro", "showWorkplaceInPersonInformationApp": "booleano",
        "startDate": "data_hora", "state": "texto", "timezone": "texto", "vacantJob": "inteiro",
        "latitude": "numero", "longitude": "numero"},
    "SCHEDULES": {
        "active": "booleano", "dailyWorkload": "inteiro", "differentiateToleranceInMarkings": "booleano",
        "externalId": "texto", "fullDescription": "texto", "holidayTimeTracking": "booleano", "id": "inteiro",
        "ignoreHolidays": "booleano", "ignoreInterval": "booleano", "ignoreValidation": "booleano",
        "lastUpdate": "data_hora", "monthlyWorkload": "inteiro", "name": "texto", "premarkedInterval": "booleano",
        "registerDate": "data_hora", "rotations": "json", "scheduleShifts": "json", "scheduleTurnId": "inteiro",
        "scheduleTypeId": "inteiro", "weeklyWorkload": "inteiro", "workedBreakOnDoubleJourney": "booleano"},
    "SHIFTS": {
        "active": "booleano", "generateFlexibleInconsistenciesAlert": "booleano", "id": "inteiro",
        "lastUpdate": "data_hora", "markings": "json", "mobilityAfter": "inteiro", "mobilityBefore": "inteiro",
        "name": "texto", "registerDate": "data_hora", "shiftTypeId": "inteiro"},
    "CLIENTS": {
        "active": "booleano", "clientTypeId": "inteiro", "companyNumber": "texto",
        "generateFlexibleInconsistenciesAlert": "booleano", "id": "inteiro", "name": "texto",
        "premarkedInterval": "booleano", "startPaycheckPeriod": "inteiro"},
    "COMPANIES": {
        "active": "booleano", "companyName": "texto", "companyNumber": "texto", "companyNumberType": "texto",
        "externalId": "texto", "fantasyName": "texto", "id": "inteiro"},
    "BUSINESSUNITS": {
        "active": "booleano", "externalId": "texto", "id": "inteiro", "name": "texto"},
    "CAREERS": {
        "active": "booleano", "careerGroupId": "inteiro", "id": "inteiro", "lastUpdate": "data_hora",
        "name": "texto", "registerDate": "data_hora"},
    "ABSENCESITUATIONS": {
        "absenceTypeId": "inteiro", "active": "booleano", "cid": "booleano", "deductWeeklyDayOff": "booleano",
        "externalId": "texto", "id": "inteiro", "medicalDoctor": "booleano", "name": "texto",
        "removeAbsence": "booleano", "removeTemplate": "booleano", "requiredReplacement": "booleano",
        "showSituation": "booleano"},
}

TIPO_ARROW = {"inteiro": pa.int64(), "numero": pa.float64(), "texto": pa.string(), "json": pa.string(),
              "booleano": pa.bool_(), "data_hora": pa.timestamp("us", tz="UTC")}
DATAS_FICTICIAS = ("01011900", "01011940", "01011970")  # "sem data" na Nexti

# %%
def segredo(cfg_origem, nome, variavel_local):
    """Key Vault no Fabric; variável de ambiente nos testes locais."""
    if _LOCAL:
        return os.environ[variavel_local]
    return notebookutils.credentials.getSecret(cfg_origem["key_vault"], nome)

class ClienteNexti:
    def __init__(self, cfg_origem):
        self.cfg, self.cabecalho, self.expira = cfg_origem, None, 0

    def _token(self):
        if time.time() > self.expira - 600:
            r = requests.post(f"{NEXTI_URL}/security/oauth/token", data={"grant_type": "client_credentials"},
                              auth=(segredo(self.cfg, "nexti-client-id", "NEXTI_CLIENT_ID"),
                                    segredo(self.cfg, "nexti-client-secret", "NEXTI_CLIENT_SECRET")),
                              timeout=60)
            r.raise_for_status()
            j = r.json()
            self.cabecalho = {"Authorization": f"Bearer {j['access_token']}"}
            self.expira = time.time() + int(j.get("expires_in", 3600))
        return self.cabecalho

    def get(self, caminho, params):
        for tentativa in range(10):
            inicio = time.time()
            url = caminho if caminho.startswith("http") else f"{NEXTI_URL}{caminho}"
            r = requests.get(url, headers=self._token(), params=params, timeout=900)
            if r.status_code == 429 or r.status_code >= 500:
                espera = int(r.headers.get("Retry-After", 30 * (tentativa + 1)))
                print(f"  {caminho} página {params.get('page', '-')}: HTTP {r.status_code}, nova tentativa em {espera}s")
                time.sleep(espera)
                continue
            print(f"  {caminho} página {params.get('page', '-')}: {time.time() - inicio:.0f}s")
            if r.status_code == 409 and "nenhum dado" in r.text:   # a API responde 409 para período vazio
                return {}
            if r.status_code >= 400:
                raise RuntimeError(f"Nexti {caminho} -> {r.status_code}: {r.text[:300]}")
            return r.json()
        raise RuntimeError(f"Nexti {caminho}: limite de tentativas (429/5xx)")

    def itens(self, ep, ini=None, fim=None):
        """Gera os itens de um endpoint, página a página (ou de uma vez, se não for paginado)."""
        fmt_ini = ini.strftime(FMT_NEXTI) if ini else None
        fmt_fim = fim.strftime(FMT_NEXTI) if fim else None
        caminho = ep["caminho"].format(ini=fmt_ini, fim=fmt_fim)
        params = {k: v.format(ini=fmt_ini, fim=fmt_fim) for k, v in ep.get("params", {}).items()}
        if ep["itens"] == "value":                      # resposta única, sem paginação
            yield from (self.get(caminho, params).get("value") or [])
            return
        pagina = 0
        while True:
            j = self.get(caminho, {**params, "page": pagina, "size": ep.get("tamanho", TAMANHO_PAGINA)})
            yield from (j.get("content") or [])
            if j.get("last", True) or not j.get("content"):
                return
            pagina += 1

def converter(valor, tipo, utc=False):
    if valor is None or valor == "" or valor == [] or valor == {}:
        return None
    if tipo == "data_hora":
        s = str(valor)
        if s.startswith(DATAS_FICTICIAS):
            return None
        d = dt.datetime.strptime(s, FMT_NEXTI if len(s) == 14 else "%d%m%Y")
        if utc:
            d = d.replace(tzinfo=dt.timezone.utc).astimezone(FUSO_BRT).replace(tzinfo=None)
        # horário de Brasília gravado como está, rotulado UTC (mesma convenção da cópia dos bancos)
        return d.replace(tzinfo=dt.timezone.utc)
    if tipo == "json":
        return json.dumps(valor, ensure_ascii=False)
    if tipo == "inteiro":
        return int(valor)
    if tipo == "numero":
        return float(valor)
    if tipo == "booleano":
        return bool(valor)
    return str(valor)

def para_tabela(chave, itens):
    colunas, utc = COLUNAS_NEXTI[chave], ENDPOINTS_NEXTI[chave].get("utc", False)
    schema = pa.schema([(c, TIPO_ARROW[t]) for c, t in colunas.items()] + [
        ("_origem", pa.string()), ("_lote", pa.string()), ("_dt_carga", pa.timestamp("us", tz="UTC"))])
    agora = agora_utc()
    linhas = [{**{c: converter(i.get(c), t, utc) for c, t in colunas.items()},
               "_origem": "NEXTI", "_lote": lote, "_dt_carga": agora} for i in itens]
    return pa.Table.from_pylist(linhas, schema=schema)

def janelas(ini, fim, dias, por_dia):
    """Divide [ini, fim] em blocos de até `dias` dias (limite da API)."""
    passo = dt.timedelta(days=dias) - (dt.timedelta(0) if por_dia else dt.timedelta(seconds=1))
    while ini <= fim:
        bloco_fim = min(fim, ini + passo)
        yield ini, bloco_fim
        ini = bloco_fim + (dt.timedelta(days=1) if por_dia else dt.timedelta(seconds=1))

def periodo(cfg, agora_brt):
    """Janela do ciclo: do último watermark (menos a margem de segurança) até agora, limitada a
    max_dias_por_execucao a partir do watermark para recuperar o histórico em blocos."""
    wm = (cfg["ultimo_watermark"] or "").strip()
    inicio = INICIO_HISTORICO_TABELA.get(cfg["tabela_origem"], inicio_historico)
    base = dt.datetime.fromisoformat(wm[:19]) if wm else dt.datetime.fromisoformat(inicio)
    ini = base - dt.timedelta(minutes=cfg["janela_seguranca_min"] or 0) if wm else base
    fim = min(agora_brt, base + dt.timedelta(days=int(max_dias_por_execucao)))
    return ini, fim

def carregar(cliente, cfg):
    """Copia um endpoint para a Bronze (append, em blocos). Devolve (novo watermark, linhas)."""
    ep = ENDPOINTS_NEXTI[cfg["tabela_origem"]]
    destino = nome_bronze(cfg["tabela_destino"])
    fuso = dt.timezone.utc if ep.get("utc") else FUSO_BRT       # fuso da janela e do watermark
    agora = dt.datetime.now(fuso).replace(tzinfo=None, microsecond=0)
    if cfg["modo_carga"] == "FULL":
        tabela = para_tabela(cfg["tabela_origem"], cliente.itens(ep))
        gravar(LH_BRONZE, destino, tabela)
        return agora.isoformat(), tabela.num_rows
    por_dia = ep.get("por_dia", False)
    ini, fim = periodo(cfg, agora)
    if por_dia:
        ini, fim = ini.replace(hour=0, minute=0, second=0), fim.replace(hour=0, minute=0, second=0)
    total = 0
    for a, b in janelas(ini, fim, ep["janela_dias"], por_dia):
        tabela = para_tabela(cfg["tabela_origem"], cliente.itens(ep, a, b.replace(hour=23, minute=59, second=59)
                                                                     if por_dia else b))
        if tabela.num_rows:
            gravar(LH_BRONZE, destino, tabela)
            total += tabela.num_rows
    return fim.isoformat(), total

# %%
cfg_api = json.loads(configuracao_api or "{}")
filtro = {o.strip().upper() for o in origens.split(",") if o.strip()}
so_tabelas = {t.strip().upper() for t in tabelas.split(",") if t.strip()}
tipos = {"DIM", "FATO"} if ciclo == "COMPLETO" else {"FATO"}

def escolhida(cfg):
    if so_tabelas:
        return cfg["tabela_origem"] in so_tabelas
    return cfg["tipo"] in tipos and cfg["tabela_origem"] not in TABELAS_ROTINA
ativas = {o["origem"] for o in ler(LH_BRONZE, "ctl_origens").to_pylist()
          if o["ativo"] and o["dialeto"] == "API" and (not filtro or o["origem"] in filtro)}

resultados = []
for origem in sorted(ativas):
    if origem != "NEXTI":
        raise ValueError(f"Origem API sem conector implementado: {origem}")
    cliente = ClienteNexti(cfg_api.get(origem, {}))
    for cfg in ler(LH_BRONZE, "ctl_tabelas").to_pylist():
        if cfg["origem"] == origem and cfg["ativo"] and escolhida(cfg):
            inicio = time.time()
            # falha de uma tabela não impede as outras: vai para o orquestrador como FALHA (watermark parado)
            try:
                novo, linhas = carregar(cliente, cfg)
            except Exception as e:
                print(f"{cfg['id']}: FALHA {e}")
                resultados.append(f"{cfg['id']}|FALHA|0|{str(e)[:500]}")
                continue
            print(f"{cfg['id']}: {linhas} linhas até {novo} ({time.time() - inicio:.0f}s)")
            resultados.append(f"{cfg['id']}|{novo}|{linhas}")

sair(json.dumps(resultados))
