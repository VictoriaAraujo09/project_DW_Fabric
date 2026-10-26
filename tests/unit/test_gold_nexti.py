"""Regras de negócio das Golds da Nexti (mesmas do DW do Airflow)."""
import datetime as dt

import pyarrow as pa
import pytest
from conftest import UTC, ts

C0, C1 = ts("2026-10-20T10:00:00"), ts("2026-10-21T10:00:00")
HOJE = dt.datetime.now(dt.timezone(dt.timedelta(hours=-3))).date()


def d(texto):
    return ts(texto + "T00:00:00")


def _aus(i, pessoa, ini, fim, nota, usuario, removida, carga):
    return dict(id=i, personId=pessoa, startDateTime=d(ini), finishDateTime=d(fim) if fim else None, note=nota,
                userRegisterId=usuario, removed=removida, _dt_carga=carga, startMinute=None, finishMinute=None,
                medicalDoctorId=None, medicalDoctorName=None, medicalDoctorCrm=None, cidId=None, cidCode=None,
                cidDescription=None, absenceSituationId=15932)


@pytest.fixture(scope="module")
def nexti(lake):
    lake.gravar("Silver", "silver_dim_colaboradores_nexti", [
        dict(id=10, companyId=1, demissionDate=None, _dt_carga=C0),
        dict(id=11, companyId=1, demissionDate=d("2026-10-05"), _dt_carga=C0)])
    lake.gravar("Silver", "silver_dim_empresas_nexti", [
        dict(id=1, companyNumber="123", companyName="GS", active=True, _dt_carga=C0)])
    lake.gravar("Silver", "silver_dim_postos_nexti", [
        dict(id=100, clientId=1000, companyId=1, _dt_carga=C0), dict(id=200, clientId=2000, companyId=1, _dt_carga=C0)])
    lake.gravar("Silver", "silver_dim_usuarios_nexti", [dict(id=1, name="Sistema", active=True, _dt_carga=C0)])
    lake.gravar("Silver", "silver_fato_transferencias_posto_nexti", [
        dict(id=1, personId=10, workplaceId=100, transferDateTime=d("2026-10-01"), lastUpdate=C0, removed=False, _dt_carga=C0),
        dict(id=2, personId=10, workplaceId=200, transferDateTime=d("2026-10-10"), lastUpdate=C0, removed=False, _dt_carga=C0),
        dict(id=3, personId=11, workplaceId=100, transferDateTime=d("2026-10-01"), lastUpdate=C0, removed=False, _dt_carga=C0)])
    lake.gravar("Silver", "silver_fato_ausencias_nexti", [
        _aus(500, 10, "2026-10-08", "2026-10-11", "Marcação não identificada", 1, False, C0),
        _aus(501, 11, "2026-10-03", None, "x", 2, True, C0)])
    return lake


# ---------------------------------------------------------------- movimentação (posto do dia)
def test_movimentacao_vigencia_por_transferencia(nexti):
    r = nexti.rodar_json("nb_gold_fato_movimentacao_colaborador")
    mov = nexti.ler("Gold", "gold_fato_movimentacao_colaborador").set_index(["CodColaborador", "DataReferencia"])
    assert r["completo"]
    assert mov.loc[(10, dt.date(2026, 10, 9)), "CodPosto"] == 100       # véspera da próxima transferência
    assert mov.loc[(10, dt.date(2026, 10, 10)), "CodCliente"] == 2000   # nova transferência vale no dia
    assert (10, HOJE) in mov.index                                        # estende até hoje
    assert len(mov.loc[(10, dt.date(2026, 10, 1)), "PK_Colaborador"]) == 32


def test_movimentacao_limitada_pela_demissao(nexti):
    mov = nexti.ler("Gold", "gold_fato_movimentacao_colaborador").set_index(["CodColaborador", "DataReferencia"])
    assert mov.xs(11).index.max() == dt.date(2026, 10, 5)


def test_movimentacao_sem_mudanca_nao_regrava(nexti):
    assert nexti.rodar_json("nb_gold_fato_movimentacao_colaborador")["colaboradores"] == 0


# ---------------------------------------------------------------- ausências
def test_ausencias_um_dia_por_linha_com_posto_do_dia(nexti):
    nexti.rodar("nb_gold_fato_ausencias")
    g = nexti.ler("Gold", "gold_fato_ausencias").set_index(["IdAusencia", "DataCalendario"])
    assert len(g.xs(500)) == 4
    assert g.loc[(500, dt.date(2026, 10, 9)), "CodPosto"] == 100 and g.loc[(500, dt.date(2026, 10, 11)), "CodPosto"] == 200


def test_ausencia_automatica_exige_usuario_sistema_e_justificativa(nexti):
    g = nexti.ler("Gold", "gold_fato_ausencias").set_index(["IdAusencia", "DataCalendario"])
    assert g.xs(500)["FlagAusenciaAutomatica"].all()
    assert not g.xs(501)["FlagAusenciaAutomatica"].any()


def test_ausencia_removida_fica_inativa(nexti):
    g = nexti.ler("Gold", "gold_fato_ausencias").set_index(["IdAusencia", "DataCalendario"])
    assert not g.loc[(501, dt.date(2026, 10, 3)), "Ativo"]


def test_ausencias_sem_mudanca_nao_regrava(nexti):
    assert nexti.rodar_json("nb_gold_fato_ausencias")["ausencias"] == 0


def test_ausencia_encurtada_inativa_os_dias_que_sairam(nexti):
    nexti.gravar("Silver", "silver_fato_ausencias_nexti", [
        _aus(500, 10, "2026-10-09", "2026-10-10", "Marcação não identificada", 1, False, C1),
        _aus(501, 11, "2026-10-03", None, "x", 2, True, C0)])
    r = nexti.rodar_json("nb_gold_fato_ausencias")
    g = nexti.ler("Gold", "gold_fato_ausencias").set_index(["IdAusencia", "DataCalendario"])
    assert r["ausencias"] == 1
    assert sorted(g.xs(500).query("Ativo").index) == [dt.date(2026, 10, 9), dt.date(2026, 10, 10)]
    assert not g.loc[(500, dt.date(2026, 10, 8)), "Ativo"] and g.loc[(500, dt.date(2026, 10, 8)), "DataInativacao"] is not None
    assert len(g) == 5


# ---------------------------------------------------------------- coberturas (nb_gold + .sql)
COB = dict(personName="A", absenteeName="B", replacementTypeId=1, note=None, registerDate=C0, replacementReasonId=0,
           replacementReasonName=None, replacementResourceId=None, replacementResourceName=None, shiftId=5, removed=False)


def test_coberturas_sem_autocobertura_e_com_cnpj_do_posto(nexti):
    nexti.gravar("Silver", "silver_fato_coberturas_nexti", [
        dict(id=900, personId=10, absenteeId=11, workplaceId=100, startDateTime=d("2026-10-01"),
             finishDateTime=d("2026-10-03"), lastUpdate=C0, _dt_carga=C0, **COB),
        dict(id=901, personId=10, absenteeId=10, workplaceId=100, startDateTime=d("2026-10-01"),
             finishDateTime=None, lastUpdate=C0, _dt_carga=C0, **COB)])
    r = nexti.rodar_json("nb_gold", {"gold": "gold_fato_coberturas"})
    g = nexti.ler("Gold", "gold_fato_coberturas")
    assert r["completo"] and len(g) == 3 and set(g["IdCobertura"]) == {900}
    assert g["CodMotivoSubstituicao"].isna().all() and g["CNPJEmpresaOrigem"].eq("123").all()   # 0 -> NULL


def test_cobertura_encurtada_nao_deixa_dia_orfao(nexti):
    nexti.gravar("Silver", "silver_fato_coberturas_nexti", [
        dict(id=900, personId=10, absenteeId=11, workplaceId=200, startDateTime=d("2026-10-02"),
             finishDateTime=d("2026-10-02"), lastUpdate=C1, _dt_carga=C1, **COB)])
    r = nexti.rodar_json("nb_gold", {"gold": "gold_fato_coberturas"})
    g = nexti.ler("Gold", "gold_fato_coberturas")
    assert not r["completo"] and len(g) == 1 and g.iloc[0]["CodPosto"] == 200


# ---------------------------------------------------------------- marcação (nb_gold + .sql)
def _mk(i, tipo, removida=False, posto=100):
    return dict(id=i, personId=10, externalPersonId="E10", workplaceId=posto, clockingTypeId=tipo,
                clockingDate=ts("2026-10-20T08:01:00"), removed=removida, latitude=-12.9, longitude=-38.5,
                lastUpdate=C0, _dt_carga=C0)


def test_marcacao_so_tipos_1_5_6_nao_removidas_e_de_posto_cadastrado(nexti):
    nexti.gravar("Silver", "silver_fato_marcacoes_ponto_nexti",
                 [_mk(1, 1), _mk(2, 3), _mk(3, 5, removida=True), _mk(4, 6, posto=999)])
    nexti.rodar("nb_gold", {"gold": "gold_fato_marcacao_ponto"})
    g = nexti.ler("Gold", "gold_fato_marcacao_ponto")
    assert list(g["IdMarcacaoPonto"]) == [1]
    assert g.iloc[0]["DataReferencia"] == dt.date(2026, 10, 20) and g.iloc[0]["CNPJEmpresaOrigem"] == "123"


# ---------------------------------------------------------------- compatibilidade com o SQL endpoint
@pytest.mark.parametrize("tabela", ["gold_fato_ausencias", "gold_fato_coberturas",
                                    "gold_fato_movimentacao_colaborador", "gold_fato_marcacao_ponto"])
def test_gold_sem_timestamp_sem_fuso(nexti, tabela):
    for campo in nexti.ler_arrow("Gold", tabela).schema:
        assert not (pa.types.is_timestamp(campo.type) and campo.type.tz is None), campo
