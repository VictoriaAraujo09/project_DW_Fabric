"""Motor de qualidade (nb_qualidade + config/qualidade.yml) e monitoramento (nb_monitoramento)."""
import datetime as dt
import json

import pytest
import yaml
from conftest import RAIZ_PROJETO, UTC, ts

C0 = ts("2026-10-20T10:00:00")
COB = dict(PK_Colaborador="X" * 32, NomeColaborador="A", PK_ColaboradorAusente="Y" * 32, NomeColaboradorAusente="B",
           CodPosto=100, DataInicio=C0, DataFim=C0, SistemaOrigem="NEXTI")


def test_qualidade_yml_valido():
    cfg = yaml.safe_load((RAIZ_PROJETO / "config" / "qualidade.yml").read_text(encoding="utf-8"))
    tipos = {"chave_unica", "nao_nulo", "sem_linhas", "valores_permitidos", "intervalo", "minimo_linhas",
             "sem_timestamp_sem_fuso"}
    for r in cfg["regras"]:
        assert {"nome", "grupo", "tabela", "tipo", "severidade"} <= set(r), r
        assert r["tipo"] in tipos and r["severidade"] in ("erro", "alerta"), r
        assert r["tabela"].split(".")[0] in ("Bronze", "Silver", "Gold"), r


@pytest.fixture(scope="module")
def coberturas_com_problemas(lake):
    """Gold de coberturas com uma chave repetida, uma autocobertura e uma data fora de faixa."""
    lake.gravar("Gold", "gold_fato_coberturas", [
        dict(IdCobertura=1, DataCalendario=dt.date(2026, 10, 1), CodColaborador=10, CodColaboradorAusente=11, **COB),
        dict(IdCobertura=1, DataCalendario=dt.date(2026, 10, 1), CodColaborador=10, CodColaboradorAusente=11, **COB),
        dict(IdCobertura=2, DataCalendario=dt.date(2026, 10, 2), CodColaborador=12, CodColaboradorAusente=12, **COB),
        dict(IdCobertura=3, DataCalendario=dt.date(25, 5, 7), CodColaborador=13, CodColaboradorAusente=14, **COB)])
    lake.gravar("Silver", "silver_fato_coberturas_nexti", [dict(id=1, _dt_carga=C0), dict(id=2, _dt_carga=C0)])
    saida = lake.rodar_json("nb_qualidade", {"grupo": "COBERTURAS", "lote": "Q1"})
    return lake, saida, lake.ler("Bronze", "ctl_qualidade").query("lote == 'Q1'").set_index("regra")


def test_qualidade_aponta_cada_regra_quebrada(coberturas_com_problemas):
    _, saida, res = coberturas_com_problemas
    assert res.loc["chave única da Gold de coberturas", "status"] == "FALHA"
    assert res.loc["colaborador cobrindo a si mesmo", "linhas_problema"] == 1
    assert res.loc["datas de cobertura fora de faixa", "severidade"] == "alerta"
    assert res.loc["chave única da Silver de coberturas", "status"] == "OK"
    assert "datas de cobertura fora de faixa" in saida["falhas_alerta"]


def test_qualidade_roda_so_o_grupo_pedido_e_o_geral(coberturas_com_problemas):
    _, _, res = coberturas_com_problemas
    assert set(res["grupo"]) <= {"COBERTURAS", "GERAL"}


def test_regra_que_nao_consegue_rodar_vira_erro_de_execucao(coberturas_com_problemas):
    _, _, res = coberturas_com_problemas
    # a Gold de ausências não existe neste lake: a regra GERAL não derruba as outras
    assert res.loc["Gold de ausências não vazia", "status"] == "ERRO_EXECUCAO"


def test_monitoramento_gera_alertas_e_nao_repete(coberturas_com_problemas):
    lake, _, _ = coberturas_com_problemas
    # falha registrada no log e rotina sem executar há 2 horas
    lake.gravar("Bronze", "ctl_log", [dict(lote="X", ciclo="ROTINA_COBERTURAS", tabela_id="ROTINA.COBERTURAS",
                                           etapa="ROTINA", linhas=0, status="FALHA", mensagem="HTTP 500",
                                           dt_registro=dt.datetime.now(UTC))], modo="append")
    escopo = {}
    import build_notebooks
    exec(build_notebooks.codigo_comum(), escopo)
    escopo["gravar_watermark_gold"]("ROTINA_COBERTURAS", "execucao", dt.datetime.now(UTC) - dt.timedelta(hours=2))

    saida = lake.rodar_json("nb_monitoramento")
    alertas = lake.ler("Bronze", "ctl_alertas")
    tipos = set(alertas["tipo"])
    assert "FALHA_ROTINA" in tipos and "QUALIDADE" in tipos
    agora_brt = dt.datetime.now(dt.timezone(dt.timedelta(hours=-3))).strftime("%H:%M")
    if "06:20" <= agora_brt <= "20:00":   # dentro da janela, a rotina parada vira alerta
        assert "ROTINA_ATRASADA" in tipos
    assert saida["novos"] == len(alertas)

    # segunda verificação logo em seguida: nada novo (falhas já vistas, alertas silenciados)
    assert lake.rodar_json("nb_monitoramento")["novos"] == 0
