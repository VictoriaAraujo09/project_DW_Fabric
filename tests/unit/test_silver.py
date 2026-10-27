"""Bronze → Silver pelo nb_orquestrador: padronização, incremental, retenção e colunas novas."""
import datetime as dt
import json

from conftest import UTC, ts


def _lan(codigo, valor, hist, obs, alteracao, lote, carga, foto=None):
    return dict(Codigo=codigo, EMP_Codigo=1, VALOR=valor, HIST=hist, OBS=obs, FOTO=foto,
                DataUltimaAlteracao=ts(alteracao), _origem="GS", _lote=lote, _dt_carga=ts(carga))


def _ciclo_inicial(lake):
    lake.gravar("Bronze", "bronze_fato_lancamentos_gs", [
        _lan(1, 10.1234567, "abc   ", "", "2026-10-21T10:00:00", "L1", "2026-10-21T12:00:00", foto=b"\x00"),
        _lan(2, float("nan"), "def", "x", "2026-10-21T11:00:00", "L1", "2026-10-21T12:00:00")], modo="append")
    hoje = dt.datetime.now(UTC).replace(microsecond=0)
    lake.gravar("Bronze", "bronze_dim_empresas_gs", [
        dict(Codigo=1, NOME="Antiga", _origem="GS", _lote="L0", _dt_carga=hoje - dt.timedelta(days=10))], modo="append")
    lake.gravar("Bronze", "bronze_dim_empresas_gs", [
        dict(Codigo=1, NOME="Matriz", _origem="GS", _lote="L1", _dt_carga=hoje)], modo="append")
    lake.gravar("Bronze", "bronze_dim_empresas_4s", [
        dict(CODIGO="0001", NOME="Matriz 4S   ", _origem="4S", _lote="L1", _dt_carga=hoje)], modo="append")
    lake.rodar("nb_orquestrador", {"lote": "L1", "ciclo": "COMPLETO", "resultados": json.dumps([
        "GS.DBO.LAN|2026-10-21T11:00:00.000|2", "GS.DBO.EMP|2026-10-21T12:00:00.000|1",
        "4S.EMP|2026-10-21 12:00:00|1"])})


def test_padronizacao_da_silver(lake):
    _ciclo_inicial(lake)
    sil = lake.ler_arrow("Silver", "silver_fato_lancamentos_gs")
    assert str(sil.schema.field("VALOR").type) == "decimal128(18, 6)"
    assert "FOTO" not in sil.column_names                                   # binário sai
    p = sil.to_pandas().set_index("Codigo")
    assert p.loc[1, "HIST"] == "abc" and p["OBS"].isna()[1]                 # rtrim e vazio -> NULL
    assert str(p.loc[1, "VALOR"]) == "10.123457" and p["VALOR"].isna()[2]   # decimal e NaN -> NULL
    msg = lake.log("L1").query("etapa == 'SILVER' and tabela_id == 'GS.DBO.LAN'").iloc[0]["mensagem"]
    assert msg == "NaN/infinito -> NULL: VALOR=1"


def test_4s_colunas_maiusculas_e_char_com_espacos(lake):
    e4s = lake.ler("Silver", "silver_dim_empresas_4s")
    assert len(e4s) == 1 and e4s.loc[0, "NOME"] == "Matriz 4S"


def test_retencao_da_bronze_no_ciclo_completo(lake):
    assert list(lake.ler("Bronze", "bronze_dim_empresas_gs")["_lote"]) == ["L1"]


def test_incremental_merge_mantem_a_versao_mais_recente(lake):
    lake.gravar("Bronze", "bronze_fato_lancamentos_gs", [
        _lan(2, 25.0, "d", None, "2026-10-21T13:00:00", "L2", "2026-10-21T14:00:00"),
        _lan(2, 22.0, "d", None, "2026-10-21T12:30:00", "L2", "2026-10-21T14:00:00"),
        _lan(3, 30.0, "g  ", "", "2026-10-21T13:00:00", "L2", "2026-10-21T14:00:00")], modo="append")
    lake.rodar("nb_orquestrador", {"lote": "L2", "ciclo": "FATOS",
                                   "resultados": json.dumps(["GS.DBO.LAN|2026-10-21T13:00:00.000|3"])})
    sil = lake.ler("Silver", "silver_fato_lancamentos_gs").set_index("Codigo")
    assert sorted(sil.index) == [1, 2, 3] and float(sil.loc[2, "VALOR"]) == 25.0
    assert lake.watermark("GS.DBO.LAN") == "2026-10-21T13:00:00.000"


def test_coluna_nova_na_origem_entra_sem_recriar_a_tabela(lake):
    escopo = {}
    import build_notebooks
    exec(build_notebooks.codigo_comum(), escopo)
    criar = escopo["criar"]
    escopo["gravar"]("Bronze", "bronze_teste_evolucao", criar([dict(id=1, a="x")], "id bigint, a string"),
                     modo="overwrite")
    escopo["gravar"]("Bronze", "bronze_teste_evolucao", criar([dict(id=2, a="y", cid="M54")],
                                                               "id bigint, a string, cid string"))
    t = lake.ler("Bronze", "bronze_teste_evolucao").set_index("id")
    assert t.loc[2, "cid"] == "M54" and t["cid"].isna()[1]
