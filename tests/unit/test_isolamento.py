"""Uma base (ou tabela) com falha não pode travar as outras.

O orquestrador recebe o resultado de cada tabela: as que deram certo vão para a Silver e avançam o
watermark; as que falharam ficam paradas e aparecem no log; no fim, o notebook falha para o pipeline
ficar vermelho. Mesma regra para a ingestão por API (nb_ingestao_api devolve FALHA por tabela).
"""
import json

import pytest
from conftest import ts


@pytest.fixture(scope="module")
def ciclo_com_falhas(lake):
    """Ciclo em que o GS.LAN copiou, o 4S.LAN falhou no ForEach (não registrou), o NEXTI.ABSENCES
    falhou na API e o GS.CPG não teve alteração na origem."""
    lake.gravar("Bronze", "bronze_fato_lancamentos_gs", [
        dict(Codigo=1, EMP_Codigo=1, VALOR=1.0, DataUltimaAlteracao=ts("2026-10-21T10:00:00"),
             _origem="GS", _lote="LF", _dt_carga=ts("2026-10-21T12:00:00"))], modo="append")
    wm_antes = {t: lake.watermark(t) for t in ("4S.LAN", "NEXTI.ABSENCES", "GS.DBO.CPG")}
    with pytest.raises(RuntimeError, match="PARCIAL") as erro:
        lake.rodar("nb_orquestrador", {
            "lote": "LF", "ciclo": "FATOS",
            "planejadas": json.dumps(["GS.DBO.LAN", "4S.LAN", "GS.DBO.CPG"]),
            "resultados": json.dumps(["GS.DBO.LAN|2026-10-21T10:00:00.000|1", "GS.DBO.CPG|SEM_MUDANCA|0"]),
            "resultados_api": json.dumps(["NEXTI.ABSENCES|FALHA|0|HTTP 500 da Nexti"])})
    return lake, wm_antes, str(erro.value)


def test_tabela_que_deu_certo_vai_para_a_silver_e_avanca(ciclo_com_falhas):
    lake, _, _ = ciclo_com_falhas
    assert len(lake.ler("Silver", "silver_fato_lancamentos_gs")) == 1
    assert lake.watermark("GS.DBO.LAN") == "2026-10-21T10:00:00.000"


def test_tabelas_que_falharam_ficam_com_watermark_parado(ciclo_com_falhas):
    lake, wm_antes, _ = ciclo_com_falhas
    assert lake.watermark("4S.LAN") == wm_antes["4S.LAN"]
    assert lake.watermark("NEXTI.ABSENCES") == wm_antes["NEXTI.ABSENCES"]


def test_sem_mudanca_na_origem_nao_e_falha(ciclo_com_falhas):
    lake, wm_antes, erro = ciclo_com_falhas
    assert "GS.DBO.CPG" not in erro and lake.watermark("GS.DBO.CPG") == wm_antes["GS.DBO.CPG"]


def test_log_registra_cada_falha_e_o_ciclo_parcial(ciclo_com_falhas):
    lake, _, erro = ciclo_com_falhas
    log = lake.log("LF")
    falhas = set(log.query("status == 'FALHA'")["tabela_id"])
    assert falhas == {"4S.LAN", "NEXTI.ABSENCES"}
    ciclo = log.query("etapa == 'CICLO'").iloc[0]
    assert ciclo["status"] == "PARCIAL" and "HTTP 500" in ciclo["mensagem"]
    assert "4S.LAN: cópia não concluída" in erro


def test_api_que_quebrou_nao_impede_os_bancos(lake):
    lake.gravar("Bronze", "bronze_fato_lancamentos_gs", [
        dict(Codigo=2, EMP_Codigo=1, VALOR=2.0, DataUltimaAlteracao=ts("2026-10-22T10:00:00"),
             _origem="GS", _lote="LA", _dt_carga=ts("2026-10-22T12:00:00"))], modo="append")
    with pytest.raises(RuntimeError, match="PARCIAL"):
        lake.rodar("nb_orquestrador", {"lote": "LA", "ciclo": "FATOS", "resultados_api": "FALHOU",
                                       "resultados": json.dumps(["GS.DBO.LAN|2026-10-22T10:00:00.000|1"])})
    assert lake.watermark("GS.DBO.LAN") == "2026-10-22T10:00:00.000"
    assert "Ingestao_API" in set(lake.log("LA").query("status == 'FALHA'")["tabela_id"])


def test_ciclo_sem_falha_termina_com_sucesso(lake):
    saida = lake.rodar_json("nb_orquestrador", {"lote": "LS", "ciclo": "FATOS",
                                                "resultados": json.dumps(["GS.DBO.CPG|SEM_MUDANCA|0"])})
    assert saida["tabelas"] == 0
    assert lake.log("LS").query("etapa == 'CICLO'").iloc[0]["status"] == "SUCESSO"
