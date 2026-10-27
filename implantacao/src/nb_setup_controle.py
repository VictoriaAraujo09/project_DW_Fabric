# %% [markdown]
# # nb_setup_controle
# Cria e atualiza as tabelas de controle do DW no lakehouse `Bronze`:
# - **ctl_origens**: origens e o dialeto SQL de cada uma
# - **ctl_tabelas**: o que carregar, como e quando (lida pelo pipeline mestre a cada ciclo)
# - **ctl_log**: registro de cada ciclo
#
# Pode rodar quantas vezes quiser: faz *upsert* por `id` e **nunca apaga o `ultimo_watermark`** já gravado.
# Para incluir ou alterar tabelas, edite `config/tabelas.yml`, implante e rode de novo (sem mexer aqui).

# %%
#@@COMUM@@

# %% [markdown]
# ## 1. Origens e tabelas
# O cadastro fica em arquivos de configuração, fora deste notebook (no Fabric: lakehouse Gold →
# Files/config, copiados da pasta `config/` pela implantação):
# - `tabelas.yml`: origens e, por base, as tabelas (tipo, assunto, chave, coluna de alteração);
# - `colunas_permitidas.yml`: lista explícita de colunas das tabelas com credenciais ou binários;
# - `nexti.yml`: endpoints e campos da API Nexti (usado aqui só para conferir o cadastro).
#
# Nome no DW: {fato|dim}_{assunto}_{sufixo}; a Bronze recebe "bronze_" e a Silver "silver_"
# (ex.: bronze_fato_contas_receber_gs, silver_dim_clientes_4s).

# %%
CADASTRO = ler_config("tabelas.yml")
PERMITIDAS = ler_config("colunas_permitidas.yml")
ENDPOINTS_API = ler_config("nexti.yml")["endpoints"]

ORIGENS = [{"origem": str(o), "dialeto": v["dialeto"], "ativo": bool(v["ativo"])}
           for o, v in CADASTRO["origens"].items()]
DIALETO = {o["origem"]: o["dialeto"] for o in ORIGENS}

CAMPOS_TABELA = {"tabela", "tipo", "assunto", "chave", "watermark", "margem_min", "ativo", "modo_carga",
                 "janela_dias", "filtro_adicional"}

def colunas_de(base, origem, tabela):
    lista = (PERMITIDAS.get(base) or {}).get(tabela)
    if not lista:
        return "*"
    fora = {c.upper() for c in ((PERMITIDAS.get("ausentes") or {}).get(origem) or {}).get(tabela, [])}
    return ", ".join(c.strip() for c in lista.split(",") if c.strip() and c.strip().upper() not in fora)

TABELAS = []
for base, grupo in CADASTRO["bases"].items():
    for origem, local in grupo["origens"].items():
        origem = str(origem)
        if origem not in DIALETO:
            raise ValueError(f"tabelas.yml, base {base}: origem {origem} não está em 'origens'")
        for t in grupo["tabelas"]:
            sobra = set(t) - CAMPOS_TABELA
            if sobra:
                raise ValueError(f"tabelas.yml, {base}.{t.get('tabela')}: campos desconhecidos {sorted(sobra)}")
            if DIALETO[origem] == "API" and t["tabela"] not in ENDPOINTS_API:
                raise ValueError(f"tabelas.yml, {base}.{t['tabela']}: endpoint não definido em nexti.yml")
            wm = t.get("watermark")
            TABELAS.append(dict(
                origem=origem, tabela_origem=f"{local.get('prefixo') or ''}{t['tabela']}",
                tabela_destino=f"{str(t['tipo']).lower()}_{t['assunto']}_{local['sufixo']}", tipo=t["tipo"],
                modo_carga=t.get("modo_carga") or ("INCREMENTAL" if wm else "FULL"), coluna_watermark=wm,
                colunas=colunas_de(base, origem, t["tabela"]),
                tipo_watermark="DATA" if wm else None, chave_negocio=t["chave"],
                janela_seguranca_min=t.get("margem_min", 10) if wm else None, janela_dias=t.get("janela_dias"),
                filtro_adicional=t.get("filtro_adicional"), ativo=t.get("ativo", True)))

# %% [markdown]
# ## 2. Gravação (não alterar)

# %%
SCHEMA_ORIGENS = pa.schema([("origem", pa.string()), ("dialeto", pa.string()), ("ativo", pa.bool_())])

SCHEMA_TABELAS = pa.schema([
    ("id", pa.string()), ("origem", pa.string()), ("tabela_origem", pa.string()),
    ("tabela_destino", pa.string()), ("tipo", pa.string()), ("modo_carga", pa.string()),
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

def validar(t):
    erros = []
    for campo in ("origem", "tabela_origem", "tabela_destino", "tipo", "modo_carga", "chave_negocio"):
        if not t.get(campo):
            erros.append(f"{campo} vazio")
    if t.get("tipo") not in ("DIM", "FATO"):
        erros.append("tipo deve ser DIM ou FATO")
    if t.get("modo_carga") not in ("FULL", "INCREMENTAL", "JANELA"):
        erros.append("modo_carga deve ser FULL, INCREMENTAL ou JANELA")
    if t.get("modo_carga") == "INCREMENTAL" and (not t.get("coluna_watermark") or t.get("tipo_watermark") not in ("DATA", "ID")):
        erros.append("INCREMENTAL exige coluna_watermark e tipo_watermark DATA ou ID")
    if t.get("modo_carga") == "JANELA" and (not t.get("coluna_watermark") or not t.get("janela_dias")):
        erros.append("JANELA exige coluna_watermark (data do documento) e janela_dias")
    if t.get("tabela_destino") != (t.get("tabela_destino") or "").lower():
        erros.append("tabela_destino deve ser minúscula (ex.: gs_lan)")
    if erros:
        raise ValueError(f"{t.get('tabela_origem')}: " + "; ".join(erros))

# origens: sempre reescrita a partir de tabelas.yml
gravar(LH_BRONZE, "ctl_origens", pa.Table.from_pylist(ORIGENS, schema=SCHEMA_ORIGENS), modo="overwrite")

# tabelas: upsert preservando ultimo_watermark
agora = agora_utc()
linhas = []
for t in TABELAS:
    validar(t)
    linhas.append({**{c: None for c in SCHEMA_TABELAS.names}, **t,
                   "id": f"{t['origem']}.{t['tabela_origem']}".upper(),
                   "ultimo_watermark": None, "dt_atualizacao": agora})
ids = [l["id"] for l in linhas]
if len(ids) != len(set(ids)):
    raise ValueError("Há tabelas repetidas em tabelas.yml")
novos = pa.Table.from_pylist(linhas, schema=SCHEMA_TABELAS)

if not existe(LH_BRONZE, "ctl_tabelas"):
    gravar(LH_BRONZE, "ctl_tabelas", novos, modo="overwrite")
else:
    # dt_atualizacao também fica: marca o último avanço do watermark (usada pelo nb_monitoramento)
    atualizar = {c: f"s.{c}" for c in SCHEMA_TABELAS.names if c not in ("id", "ultimo_watermark", "dt_atualizacao")}
    (tabela_delta(LH_BRONZE, "ctl_tabelas")
        .merge(novos, predicate="t.id = s.id", source_alias="s", target_alias="t")
        .when_matched_update(updates=atualizar)
        .when_not_matched_insert_all()
        .execute())

if not existe(LH_BRONZE, "ctl_log"):
    gravar(LH_BRONZE, "ctl_log", SCHEMA_LOG.empty_table(), modo="overwrite")

print(f"ctl_origens: {len(ORIGENS)} | ctl_tabelas: {ler(LH_BRONZE, 'ctl_tabelas').num_rows} linhas")

# %% [markdown]
# ## 3. Conferência

# %%
ctl = ler(LH_BRONZE, "ctl_tabelas")
print(duckdb.sql("SELECT id, tipo, modo_carga, coluna_watermark, ultimo_watermark, ativo FROM ctl ORDER BY id"))
