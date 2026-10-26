"""
Roda o pl_mestre_dw já implantado, com todas as origens ou só as escolhidas, sem reimplantar nada.

Uso:
  python executar_ciclo.py                          # ciclo AUTO, todas as origens ativas
  python executar_ciclo.py --origens GS             # só o GS
  python executar_ciclo.py --origens GS,4S --ciclo COMPLETO
  python executar_ciclo.py --origens NEXTI --sem-esperar

Origens diferentes podem rodar em execuções separadas ao mesmo tempo. Não rode a mesma origem em
duas execuções simultâneas: as duas copiariam a mesma janela.
"""
import argparse, json, pathlib, sys, time
import requests

RAIZ = pathlib.Path(__file__).parent
sys.path.insert(0, str(RAIZ))
from implantar_fundacao import API, Fabric  # noqa: E402

def ler_log(fab, ws, lh_bronze, lote):
    from deltalake import DeltaTable
    tok = fab.cred.get_token("https://storage.azure.com/.default").token
    caminho = f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{lh_bronze}/Tables/ctl_log"
    dt_ = DeltaTable(caminho, storage_options={"bearer_token": tok, "use_fabric_endpoint": "true"})
    return dt_.to_pyarrow_table(filters=[("lote", "=", lote)]).to_pylist()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ciclo", default="AUTO", choices=["AUTO", "COMPLETO", "FATOS"])
    ap.add_argument("--origens", default="", help="ex.: GS ou GS,4S,NEXTI (vazio = todas as ativas)")
    ap.add_argument("--workspace", default="DW_DEV", help="chave do workspace em manifesto_implantacao.json")
    ap.add_argument("--sem-esperar", action="store_true", help="só dispara e devolve o ID da execução")
    args = ap.parse_args()

    itens = json.loads((RAIZ / "manifesto_implantacao.json").read_text(encoding="utf-8"))["itens"][args.workspace]
    ws, pl = itens["workspace"], itens["pipeline"]
    lh_bronze = next(v for k, v in itens["lakehouses"].items() if "bronze" in k.lower())
    origens = ",".join(o.strip().upper() for o in args.origens.split(",") if o.strip())
    fab = Fabric()

    r = requests.post(f"{API}/workspaces/{ws}/items/{pl}/jobs/instances?jobType=Pipeline",
                      headers=fab._headers(), timeout=120,
                      json={"executionData": {"parameters": {"ciclo": args.ciclo, "origens": origens}}})
    if r.status_code != 202:
        sys.exit(f"Falha ao iniciar o pipeline: {r.status_code} {r.text}")
    local = r.headers["Location"]
    execucao = local.rstrip("/").split("/")[-1]
    print(f"pl_mestre_dw iniciado | ciclo {args.ciclo} | origens {origens or 'todas'} | execução {execucao}")
    if args.sem_esperar:
        return

    inicio = time.time()
    while True:
        time.sleep(30)
        st = requests.get(local, headers=fab._headers(), timeout=120).json()
        if st.get("status") in ("Completed", "Failed", "Cancelled", "Deduped"):
            break
    print(f"{st['status']} em {(time.time() - inicio) / 60:.0f} min {st.get('failureReason') or ''}")

    try:
        log = ler_log(fab, ws, lh_bronze, execucao)
    except Exception as e:  # o resumo é só conveniência: o status acima é o que vale
        print(f"(sem resumo do ctl_log: {e})")
        return
    for linha in sorted(log, key=lambda x: (x["etapa"] != "CICLO", x["tabela_id"], x["etapa"])):
        if linha["etapa"] == "CICLO" or linha["status"] != "SUCESSO":
            print(f"  {linha['tabela_id']:<32} {linha['etapa']:<9} {linha['status']:<8} {linha['mensagem'][:150]}")
    silver = [x for x in log if x["etapa"] == "SILVER"]
    print(f"  {len(silver)} tabelas na Silver, {sum(x['linhas'] for x in silver):,} linhas gravadas")
    if st["status"] != "Completed":
        sys.exit(1)

if __name__ == "__main__":
    main()
