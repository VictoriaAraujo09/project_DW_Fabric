"""Cadastro de controle e planejamento do ciclo (nb_setup_controle, nb_preparar_ciclo)."""
import pytest


def test_setup_e_idempotente(lake):
    antes = len(lake.ler("Bronze", "ctl_tabelas"))
    lake.rodar("nb_setup_controle")
    assert len(lake.ler("Bronze", "ctl_tabelas")) == antes


def test_credenciais_e_binarios_fora_das_copias(lake):
    saida = lake.rodar_json("nb_preparar_ciclo", {"ciclo": "COMPLETO", "lote": "L1"})
    copias = {t["id"]: t["query_copy"] for lista in saida["origens"].values() for t in lista}
    for tid in ("GS.DBO.CLI", "GS.DBO.EST", "GS.DBO.EMP", "GS.DBO.NFS", "4S.EST"):
        q = copias[tid]
        assert "SELECT *" not in q and "SENHA" not in q.upper() and "TOKEN" not in q.upper(), tid
        assert "Logotipo" not in q, tid
    assert "ImpNFEConteudoArquivoA1" not in copias["GS.DBO.EST"] and "CNPJCPF" in copias["GS.DBO.EST"]
    # colunas que não existem no 4S ficam fora só dele
    assert "MESSUBSEQUENTEFAT" in copias["GS.DBO.CLI"] and "MESSUBSEQUENTEFAT" not in copias["4S.CLI"]
    assert "MESSUBSEQUENTEFAT" not in copias["VIGON.CLI"] and ", id," not in copias["VIGON.EMP"] + ","


def test_todas_as_origens_ativas_por_padrao(lake):
    saida = lake.rodar_json("nb_preparar_ciclo", {"ciclo": "COMPLETO", "lote": "L1"})
    assert saida["selecionadas"] == "4S,GS,NEXTI,PROTHEUS,VIGON"
    assert set(saida["ids"]) == {t["id"] for lista in saida["origens"].values() for t in lista}


def test_filtro_de_uma_origem(lake):
    saida = lake.rodar_json("nb_preparar_ciclo", {"ciclo": "FATOS", "lote": "L0", "origens": "gs"})
    assert saida["selecionadas"] == "GS" and saida["origens"]["GS"] and not saida["origens"]["4S"]


@pytest.mark.parametrize("filtro", ["G5", "GS,SAP"])
def test_filtro_invalido_falha_em_vez_de_rodar_vazio(lake, filtro):
    with pytest.raises(ValueError, match="inexistentes ou inativas"):
        lake.rodar("nb_preparar_ciclo", {"ciclo": "FATOS", "lote": "L0", "origens": filtro})


def test_ciclo_fatos_nao_traz_dimensoes(lake):
    saida = lake.rodar_json("nb_preparar_ciclo", {"ciclo": "FATOS", "lote": "L0", "origens": "GS"})
    assert all(t["tabela_destino"].startswith("bronze_fato_") for t in saida["origens"]["GS"])


def test_protheus_sem_senhas_nem_binarios(lake):
    saida = lake.rodar_json("nb_preparar_ciclo", {"ciclo": "COMPLETO", "lote": "L1", "origens": "PROTHEUS"})
    copias = {t["id"]: t["query_copy"] for t in saida["origens"]["PROTHEUS"]}
    assert len(copias) == 14
    empresas = copias["PROTHEUS.DBO.SYS_COMPANY"]
    assert "M0_CODIGO" in empresas and not any(c in empresas for c in ("M0_PSW", "M0_CTPSW", "M0_LICENSA"))
    assert "A6_CFGPIX" not in copias["PROTHEUS.DBO.SA6010"] and "A6_COD" in copias["PROTHEUS.DBO.SA6010"]
    assert copias["PROTHEUS.DBO.SE2010"].startswith("SELECT *")      # sem binário: todas as colunas


def test_vigon_tem_as_mesmas_tabelas_do_fortes(lake):
    saida = lake.rodar_json("nb_preparar_ciclo", {"ciclo": "COMPLETO", "lote": "L1", "origens": "GS,4S,VIGON"})
    siglas = {o: {t["id"].split(".")[-1] for t in saida["origens"][o]} for o in ("GS", "4S", "VIGON")}
    assert siglas["VIGON"] == siglas["4S"] == siglas["GS"]
    vigon = {t["id"]: t for t in saida["origens"]["VIGON"]}
    assert vigon["VIGON.LAN"]["tabela_destino"] == "bronze_fato_lancamentos_vigon"
    assert vigon["VIGON.CLI"]["query_copy"].startswith("SELECT EMP_Codigo")   # lista de colunas permitidas


def test_primeira_carga_incremental_traz_linhas_sem_data_de_alteracao(lake):
    saida = lake.rodar_json("nb_preparar_ciclo", {"ciclo": "FATOS", "lote": "L1", "origens": "PROTHEUS"})
    se1 = next(t for t in saida["origens"]["PROTHEUS"] if t["id"] == "PROTHEUS.DBO.SE1010")
    assert "(S_T_A_M_P_ <= '#NOVO#' OR S_T_A_M_P_ IS NULL)" in se1["query_copy"]
