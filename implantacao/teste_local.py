# Teste local do ciclo (sem Fabric): pip install deltalake duckdb pyarrow pandas && python teste_local.py
import os, json, shutil, datetime as dt, pyarrow as pa
import tempfile; RAIZ=os.path.join(tempfile.gettempdir(),"dwtest"); shutil.rmtree(RAIZ, ignore_errors=True); os.makedirs(RAIZ)
os.environ["DW_LOCAL_ROOT"]=RAIZ
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
