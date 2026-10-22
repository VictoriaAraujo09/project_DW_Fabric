# %% [markdown]
# # nb_setup_controle
# Cria e atualiza as tabelas de controle do DW em `lh_bronze`:
# - **ctl_origens**: origens e o dialeto SQL de cada uma
# - **ctl_tabelas**: o que carregar, como e quando (lida pelo pipeline mestre a cada ciclo)
# - **ctl_log**: registro de cada ciclo
#
# Pode rodar quantas vezes quiser: faz *upsert* por `id` e **nunca apaga o `ultimo_watermark`** já gravado.
# Para incluir ou alterar tabelas, edite a lista `TABELAS` e rode de novo.

# %%
#@@COMUM@@

# %% [markdown]
# ## 1. Origens
# `ativo = False` deixa a origem fora dos ciclos (Protheus, 4S e VIGON entram na Fase 3).

# %%
ORIGENS = [
    {"origem": "GS",       "dialeto": "SQLSERVER", "ativo": True},
    {"origem": "PROTHEUS", "dialeto": "SQLSERVER", "ativo": False},
    {"origem": "4S",       "dialeto": "FIREBIRD",  "ativo": False},
    {"origem": "VIGON",    "dialeto": "FIREBIRD",  "ativo": False},
]

# %% [markdown]
# ## 2. Catálogo de tabelas (mesmo padrão para GS, 4S e VIGON)
# Padrão de nomes gerado automaticamente: `<camada>_<tipo>_<assunto>_<origem>`.
# Por exemplo, CRE do GS vira `bronze_fato_contas_receber_gs` e `silver_fato_contas_receber_gs`.
#
# Cada sigla é cadastrada para todas as origens de `ORIGENS_ERP`.
# Para ligar uma tabela, preencha `chave` e, nas fatos, `watermark` em `DETALHES`
# com o resultado das consultas `descoberta_sqlserver.sql` e `descoberta_firebird.sql`.
# Fato sem watermark ou tabela sem chave ficam cadastradas, porém desligadas (`ativo = False`).

# %%
CATALOGO = [  # (sigla, assunto, tipo)
    ("LAN", "lancamentos",                           "FATO"),
    ("CPG", "contas_pagar",                          "FATO"),
    ("NFS", "notas_fiscais",                         "FATO"),
    ("ORL", "origem_lancamentos",                    "FATO"),
    ("CRE", "contas_receber",                        "FATO"),
    ("VDR", "vencimentos_contas_receber",            "FATO"),
    ("VCP", "vencimentos_contas_pagar",              "FATO"),
    ("BVP", "baixas_contas_pagar",                   "FATO"),
    ("SCR", "rateio_servicos",                       "FATO"),
    ("BVD", "baixas_contas_receber",                 "FATO"),
    ("RDL", "rateio_receitas_despesas_lancamentos",  "FATO"),
    ("TCF", "transferencias_contas",                 "FATO"),
    ("DCP", "rateio_despesas",                       "FATO"),
    ("RRR", "rateio_receitas",                       "FATO"),
    ("LAF", "linhas_apuracao_financeira",            "FATO"),
    ("VSE", "vinculos_servicos_apf",                 "FATO"),
    ("VRD", "vinculos_natureza_centro_resultado_apf","FATO"),
    ("CLI", "clientes",                              "DIM"),
    ("FRN", "fornecedores",                          "DIM"),
    ("CRS", "centro_resultado",                      "DIM"),
    ("CRD", "natureza_financeira",                   "DIM"),
    ("EST", "estabelecimentos",                      "DIM"),
    ("CON", "contas_bancarias",                      "DIM"),
    ("SER", "servicos",                              "DIM"),
    ("BAN", "bancos",                                "DIM"),
    ("GFR", "grupo_fornecedores",                    "DIM"),
    ("APF", "apuracao_financeira",                   "DIM"),
    ("GCL", "grupo_clientes",                        "DIM"),
    ("EMP", "empresas",                              "DIM"),
]

# Origens que seguem o catálogo acima e o prefixo da tabela em cada uma
ORIGENS_ERP = {"GS": "dbo.", "4S": "", "VIGON": ""}

# Preencher após a descoberta. Exemplo:
#   "CRE": dict(chave="COD_CRE, COD_EMP", watermark="DT_ALTERACAO", tipo_wm="DATA", colunas="*"),
#   "LAN": dict(chave="NUM_LAN", watermark="NUM_LAN", tipo_wm="ID"),
# Para usar valores diferentes numa origem, use a chave "SIGLA@ORIGEM" (ex.: "CRE@4S").
DETALHES = {
}

def montar_tabelas():
    tabelas = []
    for origem, prefixo in ORIGENS_ERP.items():
        for sigla, assunto, tipo in CATALOGO:
            d = {**DETALHES.get(sigla, {}), **DETALHES.get(f"{sigla}@{origem}", {})}
            chave, wm = d.get("chave"), d.get("watermark")
            if tipo == "DIM":
                modo = "FULL"
            elif d.get("janela_dias"):
                modo = "JANELA"
            else:
                modo = "INCREMENTAL"
            pronto = bool(chave) and (modo == "FULL" or bool(wm))
            tabelas.append(dict(
                origem=origem, tabela_origem=f"{prefixo}{sigla}", assunto=assunto, tipo=tipo,
                modo_carga=modo, colunas=d.get("colunas", "*"), coluna_watermark=wm,
                tipo_watermark=d.get("tipo_wm", "DATA") if wm else None, chave_negocio=chave,
                janela_seguranca_min=d.get("janela_min", 10) if modo == "INCREMENTAL" else None,
                janela_dias=d.get("janela_dias"), filtro_adicional=d.get("filtro"),
                ativo=d.get("ativo", pronto)))
    return tabelas

TABELAS = montar_tabelas()
pendentes = sorted({t["tabela_origem"].split(".")[-1] for t in TABELAS if not t["ativo"]})
print(f"{len(TABELAS)} cadastros | ativos: {sum(t['ativo'] for t in TABELAS)} | "
      f"siglas pendentes de chave/watermark: {', '.join(pendentes) or 'nenhuma'}")

# %% [markdown]
# ## 3. Gravação (não alterar)

# %%
SCHEMA_ORIGENS = pa.schema([("origem", pa.string()), ("dialeto", pa.string()), ("ativo", pa.bool_())])

SCHEMA_TABELAS = pa.schema([
    ("id", pa.string()), ("origem", pa.string()), ("tabela_origem", pa.string()),
    ("assunto", pa.string()), ("tabela_bronze", pa.string()), ("tabela_silver", pa.string()), ("tipo", pa.string()), ("modo_carga", pa.string()),
    ("colunas", pa.string()), ("coluna_watermark", pa.string()), ("tipo_watermark", pa.string()),
    ("ultimo_watermark", pa.string()), ("chave_negocio", pa.string()),
    ("janela_seguranca_min", pa.int32()), ("janela_dias", pa.int32()),
    ("filtro_adicional", pa.string()), ("ativo", pa.bool_()),
    ("dt_atualizacao", pa.timestamp("us", tz="UTC")),
])

SCHEMA_LOG = pa.schema([
    ("lote", pa.string()), ("ciclo", pa.string()), ("tabela_id", pa.string()),
    ("etapa", pa.string()), ("linhas", pa.int64()), ("status", pa.string()),
    ("mensagem", pa.string()), ("dt_registro", pa.timestamp("us", tz="UTC")),
])

import re

def nome_tabela(camada, t):
    """Padrão: <camada>_<tipo>_<assunto>_<origem>  ex.: bronze_fato_contas_receber_gs"""
    tipo = "fato" if t["tipo"] == "FATO" else "dim"
    return f"{camada}_{tipo}_{t['assunto']}_{t['origem'].lower()}"

def validar(t):
    erros = []
    obrigatorios = ("origem", "tabela_origem", "assunto", "tipo", "modo_carga")
    if t.get("ativo"):
        obrigatorios += ("chave_negocio",)
    for campo in obrigatorios:
        if not t.get(campo):
            erros.append(f"{campo} vazio")
    if t.get("tipo") not in ("DIM", "FATO"):
        erros.append("tipo deve ser DIM ou FATO")
    if t.get("modo_carga") not in ("FULL", "INCREMENTAL", "JANELA"):
        erros.append("modo_carga deve ser FULL, INCREMENTAL ou JANELA")
    if t.get("ativo") and t.get("modo_carga") == "INCREMENTAL" and (not t.get("coluna_watermark") or t.get("tipo_watermark") not in ("DATA", "ID")):
        erros.append("INCREMENTAL exige coluna_watermark e tipo_watermark DATA ou ID")
    if t.get("modo_carga") == "JANELA" and (not t.get("coluna_watermark") or not t.get("janela_dias")):
        erros.append("JANELA exige coluna_watermark (data do documento) e janela_dias")
    if not re.fullmatch(r"[a-z][a-z0-9_]*", t.get("assunto") or ""):
        erros.append("assunto deve ser minúsculo, sem acento e com _ (ex.: contas_receber)")
    if erros:
        raise ValueError(f"{t.get('tabela_origem')}: " + "; ".join(erros))

# origens: sempre reescrita a partir da lista acima
gravar(LH_BRONZE, "ctl_origens", pa.Table.from_pylist(ORIGENS, schema=SCHEMA_ORIGENS), modo="overwrite")

# tabelas: upsert preservando ultimo_watermark
agora = agora_utc()
linhas = []
for t in TABELAS:
    validar(t)
    linhas.append({**{c: None for c in SCHEMA_TABELAS.names}, **t,
                   "id": f"{t['origem']}.{t['tabela_origem']}".upper(),
                   "tabela_bronze": nome_tabela("bronze", t), "tabela_silver": nome_tabela("silver", t),
                   "ultimo_watermark": None, "dt_atualizacao": agora})
ids = [l["id"] for l in linhas]
if len(ids) != len(set(ids)):
    raise ValueError("Há tabelas repetidas na lista TABELAS")
nomes = [l["tabela_bronze"] for l in linhas]
if len(nomes) != len(set(nomes)):
    raise ValueError("Dois cadastros geram o mesmo nome de tabela: revise o campo assunto")
novos = pa.Table.from_pylist(linhas, schema=SCHEMA_TABELAS)

if not existe(LH_BRONZE, "ctl_tabelas"):
    gravar(LH_BRONZE, "ctl_tabelas", novos, modo="overwrite")
else:
    atualizar = {c: f"s.{c}" for c in SCHEMA_TABELAS.names if c not in ("id", "ultimo_watermark")}
    (tabela_delta(LH_BRONZE, "ctl_tabelas")
        .merge(novos, predicate="t.id = s.id", source_alias="s", target_alias="t")
        .when_matched_update(updates=atualizar)
        .when_not_matched_insert_all()
        .execute())

if not existe(LH_BRONZE, "ctl_log"):
    gravar(LH_BRONZE, "ctl_log", SCHEMA_LOG.empty_table(), modo="overwrite")

print(f"ctl_origens: {len(ORIGENS)} | ctl_tabelas: {ler(LH_BRONZE, 'ctl_tabelas').num_rows} linhas")

# %% [markdown]
# ## 4. Conferência

# %%
ctl =ler(LH_BRONZE, "ctl_tabelas")
print(duckdb.sql("SELECT id, tabela_bronze, tabela_silver, modo_carga, coluna_watermark, ultimo_watermark, ativo FROM ctl ORDER BY id"))
