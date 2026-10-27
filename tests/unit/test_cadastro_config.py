"""Cadastro de origens e tabelas lido de config/ (tabelas.yml, colunas_permitidas.yml, nexti.yml)."""
import shutil

import pytest
import yaml

from conftest import RAIZ_PROJETO, Lake

CONFIG = RAIZ_PROJETO / "config"
TIPOS_CAMPO = {"inteiro", "numero", "texto", "booleano", "data_hora", "json"}


def carregar(nome):
    return yaml.safe_load((CONFIG / nome).read_text(encoding="utf-8"))


def test_cadastro_completo(lake):
    ctl = lake.ler("Bronze", "ctl_tabelas")
    origens = lake.ler("Bronze", "ctl_origens")
    cadastro = carregar("tabelas.yml")
    esperado = sum(len(b["tabelas"]) * len(b["origens"]) for b in cadastro["bases"].values())
    assert len(ctl) == esperado
    assert set(origens["origem"]) == {str(o) for o in cadastro["origens"]}
    # colunas permitidas: lista explícita, sem as ausentes da origem
    cli_4s = ctl.query("id == '4S.CLI'").iloc[0]
    assert cli_4s["colunas"] != "*" and "MESSUBSEQUENTEFAT" not in cli_4s["colunas"]
    assert "MESSUBSEQUENTEFAT" in ctl.query("id == 'GS.DBO.CLI'").iloc[0]["colunas"]
    assert ctl.query("id == 'GS.DBO.LAN'").iloc[0]["colunas"] == "*"
    # margem padrão de 10 min e a específica do previsto x trabalhado
    assert ctl.query("id == 'GS.DBO.LAN'").iloc[0]["janela_seguranca_min"] == 10
    assert ctl.query("id == 'NEXTI.TIMETRACKINGS'").iloc[0]["janela_seguranca_min"] == 35 * 24 * 60


def test_nexti_yml_coerente():
    endpoints = carregar("nexti.yml")["endpoints"]
    tabelas_nexti = {t["tabela"] for t in carregar("tabelas.yml")["bases"]["NEXTI"]["tabelas"]}
    assert tabelas_nexti <= set(endpoints)
    for chave, ep in endpoints.items():
        assert ep["caminho"] and ep["itens"] in ("content", "value"), chave
        assert set(ep["campos"].values()) <= TIPOS_CAMPO, chave
        if ep.get("rotina") or "{ini}" in ep["caminho"] or "{ini}" in str(ep.get("params")):
            assert ep.get("janela_dias"), f"{chave}: endpoint por janela sem janela_dias"


@pytest.fixture
def config_copia(tmp_path):
    destino = tmp_path / "config"
    shutil.copytree(CONFIG, destino)
    return destino


def editar(pasta, nome, funcao):
    arq = pasta / nome
    dados = yaml.safe_load(arq.read_text(encoding="utf-8"))
    funcao(dados)
    arq.write_text(yaml.safe_dump(dados, allow_unicode=True, sort_keys=False), encoding="utf-8")


def test_tabela_nova_so_pelo_yml_preserva_watermark(tmp_path, config_copia):
    lk = Lake(tmp_path / "dw", config_copia)
    lk.rodar("nb_setup_controle")
    from deltalake import DeltaTable
    DeltaTable(f"{lk.raiz}/Bronze/ctl_tabelas").update(predicate="id = 'GS.DBO.LAN'",
                                                       updates={"ultimo_watermark": "'2026-10-01T00:00:00'"})

    editar(config_copia, "tabelas.yml", lambda d: d["bases"]["FORTES"]["tabelas"].append(
        {"tabela": "PRD", "tipo": "DIM", "assunto": "produtos", "chave": "EMP_Codigo, Codigo"}))
    lk.rodar("nb_setup_controle")

    ctl = lk.ler("Bronze", "ctl_tabelas")
    assert {"GS.DBO.PRD", "4S.PRD"} <= set(ctl["id"])
    nova = ctl.query("id == 'GS.DBO.PRD'").iloc[0]
    assert nova["tabela_destino"] == "dim_produtos_gs" and nova["modo_carga"] == "FULL"
    assert lk.watermark("GS.DBO.LAN") == "2026-10-01T00:00:00"


@pytest.mark.parametrize("alteracao, erro", [
    (lambda d: d["bases"]["NEXTI"]["tabelas"].append(
        {"tabela": "INEXISTENTE", "tipo": "DIM", "assunto": "x", "chave": "id"}), "nexti.yml"),
    (lambda d: d["bases"]["FORTES"]["tabelas"].append(
        {"tabela": "PRD", "tipo": "DIM", "assunto": "x", "chave": "id", "watermak": "Data"}), "campos desconhecidos"),
    (lambda d: d["bases"]["FORTES"]["tabelas"].append(
        {"tabela": "PRD", "tipo": "DIMENSAO", "assunto": "x", "chave": "id"}), "DIM ou FATO"),
    (lambda d: d["bases"]["FORTES"]["tabelas"].append(dict(d["bases"]["FORTES"]["tabelas"][0])), "repetidas"),
])
def test_cadastro_invalido_e_recusado(tmp_path, config_copia, alteracao, erro):
    editar(config_copia, "tabelas.yml", alteracao)
    with pytest.raises(ValueError, match=erro):
        Lake(tmp_path / "dw", config_copia).rodar("nb_setup_controle")
