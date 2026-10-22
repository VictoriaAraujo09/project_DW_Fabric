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

run("nb_setup_controle", patch=("DETALHES = {\n}", "DETALHES = {'CRE': dict(chave='NUM_LANC, FILIAL', watermark='DT_ALTERACAO'), 'EMP': dict(chave='FILIAL')}"))
run("nb_setup_controle", patch=("DETALHES = {\n}", "DETALHES = {'CRE': dict(chave='NUM_LANC, FILIAL', watermark='DT_ALTERACAO'), 'EMP': dict(chave='FILIAL')}"))  # idempotente
saida=json.loads(run("nb_preparar_ciclo", {"ciclo":"COMPLETO","lote":"L1"}))
for o,l in saida["origens"].items():
    for t in l: print(o, t["id"], "\n  MAX:", t["query_max"], "\n  COPY:", t["query_copy"])

def bronze(tab, rows):
    write_deltalake(f"{RAIZ}/lh_bronze/{tab}", pa.Table.from_pylist(rows), mode="append")
ts=lambda s: dt.datetime.fromisoformat(s).replace(tzinfo=dt.timezone.utc)
bronze("bronze_fato_contas_receber_gs",[dict(NUM_LANC=1,FILIAL="01",VALOR=10.0,DT_ALTERACAO=ts("2026-10-21T10:00:00"),_origem="GS",_lote="L1",_dt_carga=ts("2026-10-21T12:00:00")),
                 dict(NUM_LANC=2,FILIAL="01",VALOR=20.0,DT_ALTERACAO=ts("2026-10-21T11:00:00"),_origem="GS",_lote="L1",_dt_carga=ts("2026-10-21T12:00:00"))])
bronze("bronze_dim_empresas_gs",[dict(FILIAL="01",NOME="Matriz",_origem="GS",_lote="L1",_dt_carga=ts("2026-10-21T12:00:00"))])
print(run("nb_orquestrador", {"lote":"L1","ciclo":"COMPLETO","resultados":json.dumps(["GS.DBO.CRE|2026-10-21T11:00:00.000|2","GS.DBO.EMP|2026-10-21T12:00:00.000|1"])}))

saida=json.loads(run("nb_preparar_ciclo", {"ciclo":"FATOS","lote":"L2"}))
print("Ciclo 2:", [ (t["id"], t["wm_atual"], t["query_copy"]) for t in saida["origens"]["GS"]])
# linha 2 alterada + repetida no lote, linha 3 nova
bronze("bronze_fato_contas_receber_gs",[dict(NUM_LANC=2,FILIAL="01",VALOR=25.0,DT_ALTERACAO=ts("2026-10-21T13:00:00"),_origem="GS",_lote="L2",_dt_carga=ts("2026-10-21T14:00:00")),
                 dict(NUM_LANC=2,FILIAL="01",VALOR=22.0,DT_ALTERACAO=ts("2026-10-21T12:30:00"),_origem="GS",_lote="L2",_dt_carga=ts("2026-10-21T14:00:00")),
                 dict(NUM_LANC=3,FILIAL="01",VALOR=30.0,DT_ALTERACAO=ts("2026-10-21T13:00:00"),_origem="GS",_lote="L2",_dt_carga=ts("2026-10-21T14:00:00"))])
print(run("nb_orquestrador", {"lote":"L2","ciclo":"FATOS","resultados":json.dumps(["GS.DBO.CRE|2026-10-21T13:00:00.000|3"])}))
print(DeltaTable(f"{RAIZ}/lh_silver/silver_fato_contas_receber_gs").to_pandas().sort_values("NUM_LANC")[["NUM_LANC","VALOR","_ativo"]])

# falha: id inexistente -> não avança watermark
try:
    run("nb_orquestrador", {"lote":"L3","ciclo":"FATOS","resultados":json.dumps(["GS.DBO.CRE|2026-10-22T00:00:00.000|0","GS.DBO.XXX|1|1"])})
except RuntimeError as e: print("FALHA esperada:", str(e).splitlines()[0])
run("nb_orquestrador", {"lote":"L4","status_ciclo":"FALHA","mensagem":"Copy falhou"})
print(DeltaTable(f"{RAIZ}/lh_bronze/ctl_tabelas").to_pandas()[["id","ultimo_watermark"]])
print(DeltaTable(f"{RAIZ}/lh_bronze/ctl_log").to_pandas()[["lote","tabela_id","etapa","linhas","status"]])
