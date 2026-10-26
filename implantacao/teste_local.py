# Teste local do ciclo (sem Fabric): pip install deltalake duckdb pyarrow pandas && python teste_local.py
import os, json, shutil, datetime as dt, pyarrow as pa
import tempfile; RAIZ=os.path.join(tempfile.gettempdir(),"dwtest"); shutil.rmtree(RAIZ, ignore_errors=True); os.makedirs(RAIZ)
os.environ["DW_LOCAL_ROOT"]=RAIZ
os.environ["DW_CONSULTAS"]=os.path.join(os.path.dirname(os.path.abspath(__file__)),"consultas")
import build_notebooks as b
from deltalake import write_deltalake, DeltaTable

def run(nome, ov=None, patch=None):
    code=b.script_local(nome, ov)
    if patch: code=code.replace(*patch)
    g={}; exec(code, g); return g.get("_EXIT_VALUE")

run("nb_setup_controle")
run("nb_setup_controle")  # idempotente
saida=json.loads(run("nb_preparar_ciclo", {"ciclo":"COMPLETO","lote":"L1"}))
for o,l in saida["origens"].items():
    for t in l[:2]: print(o, t["id"], t["tabela_destino"], "\n  MAX:", t["query_max"], "\n  COPY:", t["query_copy"])
# credenciais e binários fora da consulta de cópia das tabelas sensíveis
copias={t["id"]: t["query_copy"] for l in saida["origens"].values() for t in l}
for tid in ("GS.DBO.CLI","GS.DBO.EST","GS.DBO.EMP","GS.DBO.NFS","4S.EST"):
    q=copias[tid]
    assert "SELECT *" not in q and "SENHA" not in q.upper() and "TOKEN" not in q.upper() and "Logotipo" not in q, tid
assert "ImpNFEConteudoArquivoA1" not in copias["GS.DBO.EST"] and "CNPJCPF" in copias["GS.DBO.EST"]
assert "MESSUBSEQUENTEFAT" in copias["GS.DBO.CLI"] and "MESSUBSEQUENTEFAT" not in copias["4S.CLI"]
assert ", id," in copias["GS.DBO.EMP"] + "," and ", id," not in copias["4S.EMP"] + ","
print("OK: credenciais fora das consultas")

# filtro de origens: só as escolhidas recebem tabelas; nome errado ou inativo falha
assert saida["selecionadas"] == "4S,GS,NEXTI", saida["selecionadas"]
so_gs=json.loads(run("nb_preparar_ciclo", {"ciclo":"FATOS","lote":"L0","origens":"gs"}))
assert so_gs["selecionadas"] == "GS" and so_gs["origens"]["GS"] and not so_gs["origens"]["4S"]
for ruim in ("G5", "GS,PROTHEUS"):
    try: run("nb_preparar_ciclo", {"ciclo":"FATOS","lote":"L0","origens":ruim}); raise AssertionError(ruim)
    except ValueError as e: assert "inexistentes ou inativas" in str(e)
print("OK: filtro de origens")

def bronze(tab, rows):
    write_deltalake(f"{RAIZ}/Bronze/{tab}", pa.Table.from_pylist(rows), mode="append")
ts=lambda s: dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc)
bronze("bronze_fato_lancamentos_gs",[dict(Codigo=1,EMP_Codigo=1,VALOR=10.1234567,HIST="abc   ",OBS="",FOTO=b"\x00",DataUltimaAlteracao=ts("2026-10-21T10:00:00"),_origem="GS",_lote="L1",_dt_carga=ts("2026-10-21T12:00:00")),
                 dict(Codigo=2,EMP_Codigo=1,VALOR=float("nan"),HIST="def",OBS="x",FOTO=None,DataUltimaAlteracao=ts("2026-10-21T11:00:00"),_origem="GS",_lote="L1",_dt_carga=ts("2026-10-21T12:00:00"))])
hoje=dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
bronze("bronze_dim_empresas_gs",[dict(Codigo=1,NOME="Antiga",_origem="GS",_lote="L0",_dt_carga=hoje-dt.timedelta(days=10))])  # lote antigo: retenção apaga
bronze("bronze_dim_empresas_gs",[dict(Codigo=1,NOME="Matriz",_origem="GS",_lote="L1",_dt_carga=hoje)])
# 4S (Firebird): nomes de coluna em maiúsculas e CHAR completado com espaços
bronze("bronze_dim_empresas_4s",[dict(CODIGO="0001",NOME="Matriz 4S   ",_origem="4S",_lote="L1",_dt_carga=hoje)])
print(run("nb_orquestrador", {"lote":"L1","ciclo":"COMPLETO","resultados":json.dumps(["GS.DBO.LAN|2026-10-21T11:00:00.000|2","GS.DBO.EMP|2026-10-21T12:00:00.000|1","4S.EMP|2026-10-21 12:00:00|1"])}))
e4s=DeltaTable(f"{RAIZ}/Silver/silver_dim_empresas_4s").to_pandas()
assert len(e4s)==1 and e4s.loc[0,"NOME"]=="Matriz 4S", e4s
print("OK: 4S com colunas em maiúsculas")
sil=DeltaTable(f"{RAIZ}/Silver/silver_fato_lancamentos_gs").to_pyarrow_table()
assert str(sil.schema.field("VALOR").type)=="decimal128(18, 6)" and "FOTO" not in sil.column_names, sil.schema
p=sil.to_pandas().set_index("Codigo")
assert p.loc[1,"HIST"]=="abc" and p["OBS"].isna()[1] and p.loc[2,"OBS"]=="x" and str(p.loc[1,"VALOR"])=="10.123457", p
assert p["VALOR"].isna()[2], p  # NaN da origem -> NULL
lg=DeltaTable(f"{RAIZ}/Bronze/ctl_log").to_pandas().query("lote=='L1' and etapa=='SILVER' and tabela_id=='GS.DBO.LAN'")
assert lg.iloc[0]["mensagem"]=="NaN/infinito -> NULL: VALOR=1", lg
emp=DeltaTable(f"{RAIZ}/Bronze/bronze_dim_empresas_gs").to_pandas()
assert list(emp["_lote"])==["L1"], emp
print("OK: Silver padronizada (decimal, rtrim, vazio->NULL, sem binário) e retenção da Bronze")

saida=json.loads(run("nb_preparar_ciclo", {"ciclo":"FATOS","lote":"L2"}))
print("Ciclo 2:", [ (t["id"], t["wm_atual"], t["query_copy"]) for t in saida["origens"]["GS"]])
# linha 2 alterada + repetida no lote, linha 3 nova
bronze("bronze_fato_lancamentos_gs",[dict(Codigo=2,EMP_Codigo=1,VALOR=25.0,HIST="d",OBS=None,FOTO=None,DataUltimaAlteracao=ts("2026-10-21T13:00:00"),_origem="GS",_lote="L2",_dt_carga=ts("2026-10-21T14:00:00")),
                 dict(Codigo=2,EMP_Codigo=1,VALOR=22.0,HIST="d",OBS=None,FOTO=None,DataUltimaAlteracao=ts("2026-10-21T12:30:00"),_origem="GS",_lote="L2",_dt_carga=ts("2026-10-21T14:00:00")),
                 dict(Codigo=3,EMP_Codigo=1,VALOR=30.0,HIST="g  ",OBS="",FOTO=None,DataUltimaAlteracao=ts("2026-10-21T13:00:00"),_origem="GS",_lote="L2",_dt_carga=ts("2026-10-21T14:00:00"))])
print(run("nb_orquestrador", {"lote":"L2","ciclo":"FATOS","resultados":json.dumps(["GS.DBO.LAN|2026-10-21T13:00:00.000|3"])}))
print(DeltaTable(f"{RAIZ}/Silver/silver_fato_lancamentos_gs").to_pandas().sort_values("Codigo")[["Codigo","VALOR","_ativo"]])

# falha: id inexistente -> não avança watermark
try:
    run("nb_orquestrador", {"lote":"L3","ciclo":"FATOS","resultados":json.dumps(["GS.DBO.LAN|2026-10-22T00:00:00.000|0","GS.DBO.XXX|1|1"])})
except RuntimeError as e: print("FALHA esperada:", str(e).splitlines()[0])
run("nb_orquestrador", {"lote":"L4","status_ciclo":"FALHA","mensagem":"Copy falhou"})
print(DeltaTable(f"{RAIZ}/Bronze/ctl_tabelas").to_pandas().query("id in ['GS.DBO.LAN','GS.DBO.EMP']")[["id","ultimo_watermark"]])
print(DeltaTable(f"{RAIZ}/Bronze/ctl_log").to_pandas()[["lote","tabela_id","etapa","linhas","status"]])

# ---------------------------------------------------------------- Golds da rotina de ausências (Nexti)
def silver(tab, rows, modo="overwrite"):
    write_deltalake(f"{RAIZ}/Silver/{tab}", pa.Table.from_pylist(rows), mode=modo, **({"schema_mode": "overwrite"} if modo == "overwrite" else {}))
c0, c1 = ts("2026-10-20T10:00:00"), ts("2026-10-21T10:00:00")
d = lambda s: ts(s + "T00:00:00")
silver("silver_dim_colaboradores_nexti", [dict(id=10, companyId=1, demissionDate=None, _dt_carga=c0),
                                          dict(id=11, companyId=1, demissionDate=d("2026-10-05"), _dt_carga=c0)])
silver("silver_dim_empresas_nexti", [dict(id=1, companyNumber="123", companyName="GS", active=True, _dt_carga=c0)])
silver("silver_dim_postos_nexti", [dict(id=100, clientId=1000, _dt_carga=c0), dict(id=200, clientId=2000, _dt_carga=c0)])
silver("silver_dim_usuarios_nexti", [dict(id=1, name="Sistema", active=True, _dt_carga=c0)])
silver("silver_fato_transferencias_posto_nexti", [
    dict(id=1, personId=10, workplaceId=100, transferDateTime=d("2026-10-01"), lastUpdate=c0, removed=False, _dt_carga=c0),
    dict(id=2, personId=10, workplaceId=200, transferDateTime=d("2026-10-10"), lastUpdate=c0, removed=False, _dt_carga=c0),
    dict(id=3, personId=11, workplaceId=100, transferDateTime=d("2026-10-01"), lastUpdate=c0, removed=False, _dt_carga=c0)])
aus_cols = dict(startMinute=None, finishMinute=None, medicalDoctorId=None, medicalDoctorName=None, medicalDoctorCrm=None,
                cidId=None, cidCode=None, cidDescription=None, absenceSituationId=15932)
silver("silver_fato_ausencias_nexti", [
    dict(id=500, personId=10, startDateTime=d("2026-10-08"), finishDateTime=d("2026-10-11"), note="Marcação não identificada",
         userRegisterId=1, removed=False, _dt_carga=c0, **aus_cols),
    dict(id=501, personId=11, startDateTime=d("2026-10-03"), finishDateTime=None, note="x", userRegisterId=2,
         removed=True, _dt_carga=c0, **aus_cols)])

r = json.loads(run("nb_gold_fato_movimentacao_colaborador"))
mov = DeltaTable(f"{RAIZ}/Gold/gold_fato_movimentacao_colaborador").to_pandas().set_index(["CodColaborador", "DataReferencia"])
hoje = dt.datetime.now(dt.timezone(dt.timedelta(hours=-3))).date()
assert r["completo"] and mov.loc[(10, dt.date(2026, 10, 9)), "CodPosto"] == 100, mov
assert mov.loc[(10, dt.date(2026, 10, 10)), "CodCliente"] == 2000 and (10, hoje) in mov.index
assert mov.xs(11).index.max() == dt.date(2026, 10, 5), "demissão limita a vigência"
assert len(mov.loc[(10, dt.date(2026, 10, 1)), "PK_Colaborador"]) == 32
assert json.loads(run("nb_gold_fato_movimentacao_colaborador"))["colaboradores"] == 0, "sem mudança, nada a fazer"

run("nb_gold_fato_ausencias")
g = DeltaTable(f"{RAIZ}/Gold/gold_fato_ausencias").to_pandas().set_index(["IdAusencia", "DataCalendario"])
assert len(g.xs(500)) == 4 and g.loc[(500, dt.date(2026, 10, 9)), "CodPosto"] == 100
assert g.loc[(500, dt.date(2026, 10, 11)), "CodPosto"] == 200 and g.xs(500)["FlagAusenciaAutomatica"].all()
assert not g.loc[(501, dt.date(2026, 10, 3)), "Ativo"], "removida na Nexti -> Ativo = 0"
assert json.loads(run("nb_gold_fato_ausencias"))["ausencias"] == 0, "sem mudança, nada a fazer"

# ausência 500 encurtada: 08 e 11 ficam inativos, 09 e 10 continuam ativos
silver("silver_fato_ausencias_nexti", [
    dict(id=500, personId=10, startDateTime=d("2026-10-09"), finishDateTime=d("2026-10-10"), note="Marcação não identificada",
         userRegisterId=1, removed=False, _dt_carga=c1, **aus_cols),
    dict(id=501, personId=11, startDateTime=d("2026-10-03"), finishDateTime=None, note="x", userRegisterId=2,
         removed=True, _dt_carga=c0, **aus_cols)])
r = json.loads(run("nb_gold_fato_ausencias"))
g = DeltaTable(f"{RAIZ}/Gold/gold_fato_ausencias").to_pandas().set_index(["IdAusencia", "DataCalendario"])
ativos = sorted(g.xs(500).query("Ativo").index.tolist())
assert r["ausencias"] == 1 and ativos == [dt.date(2026, 10, 9), dt.date(2026, 10, 10)], g.xs(500)
assert not g.loc[(500, dt.date(2026, 10, 8)), "Ativo"] and g.loc[(500, dt.date(2026, 10, 8)), "DataInativacao"] is not None
assert len(g) == 5, g
print("OK: Golds de movimentação e ausências (vigência, demissão, posto do dia, flags, inativação, incremental)")

# coluna nova na origem: append e MERGE acrescentam a coluna, vazia nas linhas antigas
g = {}
exec(b.codigo_comum(), g)
g["gravar"]("Bronze", "bronze_teste_evolucao", pa.Table.from_pylist([dict(id=1, a="x")]), modo="overwrite")
g["gravar"]("Bronze", "bronze_teste_evolucao", pa.Table.from_pylist([dict(id=2, a="y", cid="M54")]))
t = DeltaTable(f"{RAIZ}/Bronze/bronze_teste_evolucao").to_pandas().set_index("id")
assert t.loc[2, "cid"] == "M54" and t["cid"].isna()[1], t
print("OK: colunas novas da origem entram sem recriar a tabela")

# Gold genérica (nb_gold + consultas/gold_fato_coberturas.sql)
silver("silver_dim_postos_nexti", [dict(id=100, clientId=1000, companyId=1, _dt_carga=c0),
                                   dict(id=200, clientId=2000, companyId=1, _dt_carga=c0)])
cob = dict(personName="A", absenteeName="B", replacementTypeId=1, note=None, registerDate=c0, replacementReasonId=0,
           replacementReasonName=None, replacementResourceId=None, replacementResourceName=None, shiftId=5, removed=False)
silver("silver_fato_coberturas_nexti", [
    dict(id=900, personId=10, absenteeId=11, workplaceId=100, startDateTime=d("2026-10-01"), finishDateTime=d("2026-10-03"),
         lastUpdate=c0, _dt_carga=c0, **cob),
    dict(id=901, personId=10, absenteeId=10, workplaceId=100, startDateTime=d("2026-10-01"), finishDateTime=None,
         lastUpdate=c0, _dt_carga=c0, **cob)])
r = json.loads(run("nb_gold", {"gold": "gold_fato_coberturas"}))
g = DeltaTable(f"{RAIZ}/Gold/gold_fato_coberturas").to_pandas()
assert r["completo"] and len(g) == 3 and set(g["IdCobertura"]) == {900}, g   # 901 cobre a si mesmo: fora
assert g["CodMotivoSubstituicao"].isna().all() and g["CNPJEmpresaOrigem"].eq("123").all()
assert json.loads(run("nb_gold", {"gold": "gold_fato_coberturas"}))["registros"] == 0, "sem mudança, nada a fazer"
silver("silver_fato_coberturas_nexti", [
    dict(id=900, personId=10, absenteeId=11, workplaceId=200, startDateTime=d("2026-10-02"), finishDateTime=d("2026-10-02"),
         lastUpdate=c1, _dt_carga=c1, **cob)])
r = json.loads(run("nb_gold", {"gold": "gold_fato_coberturas"}))
g = DeltaTable(f"{RAIZ}/Gold/gold_fato_coberturas").to_pandas()
assert not r["completo"] and len(g) == 1 and g.iloc[0]["CodPosto"] == 200, g     # período encolheu: sem dia órfão
print("OK: Gold genérica por consulta .sql (coberturas: autocobertura fora, incremental apaga e reinsere)")

# Gold de marcação (nb_gold + consultas/gold_fato_marcacao_ponto.sql)
mk = lambda i, tipo, rem=False, wp=100: dict(id=i, personId=10, externalPersonId="E10", workplaceId=wp, clockingTypeId=tipo,
    clockingDate=ts("2026-10-20T08:01:00"), removed=rem, latitude=-12.9, longitude=-38.5, lastUpdate=c0, _dt_carga=c0)
silver("silver_fato_marcacoes_ponto_nexti", [mk(1, 1), mk(2, 3), mk(3, 5, rem=True), mk(4, 6, wp=999)])
run("nb_gold", {"gold": "gold_fato_marcacao_ponto"})
g = DeltaTable(f"{RAIZ}/Gold/gold_fato_marcacao_ponto").to_pandas()
assert list(g["IdMarcacaoPonto"]) == [1] and g.iloc[0]["DataReferencia"] == dt.date(2026, 10, 20), g  # tipo 3, removida e posto sem empresa: fora
assert g.iloc[0]["CNPJEmpresaOrigem"] == "123" and len(g.iloc[0]["PK_Colaborador"]) == 32
print("OK: Gold de marcação (tipos 1/5/6, removidas fora, data da marcação)")

# o SQL endpoint do Fabric não aceita timestamp sem fuso (TIMESTAMP_NTZ) nas Golds
for t in ("gold_fato_ausencias", "gold_fato_coberturas", "gold_fato_movimentacao_colaborador", "gold_fato_marcacao_ponto"):
    for campo in DeltaTable(f"{RAIZ}/Gold/{t}").to_pyarrow_table().schema:
        assert not (pa.types.is_timestamp(campo.type) and campo.type.tz is None), (t, campo)
print("OK: Golds sem timestamp sem fuso (compatível com o SQL endpoint)")
