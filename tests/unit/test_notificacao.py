"""Aviso no Teams por execução (comum.py): comportamento e a cadência das rotinas da Nexti.

Localmente (_LOCAL) o envio não vai para o Teams: enviar_teams devolve "local". O que se verifica aqui
é a decisão de quando avisar (comportamento NORMAL/ATENÇÃO/ALERTA e o agrupamento 1 a cada N)."""
import build_notebooks


def comum(lake):
    """Funções do bloco comum ligadas a este lake (DW_LOCAL_ROOT/DW_CONFIG já apontam para ele)."""
    escopo = {}
    exec(build_notebooks.codigo_comum(), escopo)
    return escopo


def test_comportamento_por_status_e_duracao(lake):
    comp = comum(lake)["comportamento_execucao"]
    assert comp("SUCESSO", 10, 300) == "NORMAL"
    assert comp("SUCESSO", 400, 300) == "ATENÇÃO"      # concluiu, mas acima do limite
    assert comp("SUCESSO", 400, None) == "NORMAL"      # sem limite configurado, duração não pesa
    assert comp("PARCIAL", 10, 300) == "ALERTA"
    assert comp("FALHA", 10, 300) == "ALERTA"


def test_rotina_nexti_avisa_uma_a_cada_tres(lake):
    avisar = comum(lake)["avisar_execucao"]
    # nexti_cada = 3 em config/qualidade.yml: sucesso NORMAL avisa nas execuções 1 e 4
    enviados = [avisar("NEXTI", "Rotina AUSENCIAS", "SUCESSO", 10, chave="T_AUS") for _ in range(6)]
    assert enviados == ["local", "agrupado", "agrupado", "local", "agrupado", "agrupado"]


def test_falha_e_atencao_da_nexti_avisam_sempre(lake):
    avisar = comum(lake)["avisar_execucao"]
    assert avisar("NEXTI", "Rotina COBERTURAS", "FALHA", 10, erro="HTTP 500", chave="T_COB") == "local"
    # ATENÇÃO (acima de duracao_atencao_s.ROTINA = 300) não é agrupada
    assert avisar("NEXTI", "Rotina COBERTURAS", "SUCESSO", 400, chave="T_COB") == "local"


def test_ciclo_de_outras_bases_avisa_toda_execucao(lake):
    avisar = comum(lake)["avisar_execucao"]
    # base diferente de NEXTI não agrupa: todo sucesso avisa
    assert [avisar("GS,4S", "Ciclo FATOS", "SUCESSO", 10, chave="T_CICLO") for _ in range(3)] == ["local"] * 3
