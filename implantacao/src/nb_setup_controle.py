# %% [markdown]
# # nb_setup_controle
# Cria e atualiza as tabelas de controle do DW no lakehouse `Bronze`:
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
# `ativo = False` deixa a origem fora dos ciclos (Protheus e VIGON entram na Fase 3).

# %%
ORIGENS = [
    {"origem": "GS",       "dialeto": "SQLSERVER", "ativo": True},
    {"origem": "PROTHEUS", "dialeto": "SQLSERVER", "ativo": False},
    {"origem": "4S",       "dialeto": "FIREBIRD",  "ativo": True},
    {"origem": "VIGON",    "dialeto": "FIREBIRD",  "ativo": False},
    {"origem": "NEXTI",    "dialeto": "API",       "ativo": True},   # lida pelo nb_ingestao_api
]

# %% [markdown]
# ## 2. Tabelas
# | campo | valores | uso |
# |---|---|---|
# | tipo | DIM / FATO | DIM roda só no ciclo completo (05:00); FATO em todos |
# | modo_carga | FULL / INCREMENTAL / JANELA | FULL = cópia completa; INCREMENTAL = por watermark; JANELA = últimos N dias |
# | tipo_watermark | DATA / ID | DATA = data/hora de alteração; ID = número crescente |
# | colunas | lista separada por vírgula | evite `*` (não traz colunas LOB inúteis) |
# | chave_negocio | colunas separadas por vírgula | usada no MERGE da Silver |
#
# GS AG (SQL Server) e 4S AG (Firebird) são o mesmo sistema Fortes, com as mesmas tabelas:
# a lista `FORTES` vale para as duas origens. Chaves = chave primária do GS AG.
# Com `coluna_watermark` a carga é INCREMENTAL por data de alteração; sem ela, FULL.

# %%
# Nome no DW: {fato|dim}_{assunto}_{origem}; a Bronze recebe "bronze_" e a Silver "silver_"
# (ex.: bronze_fato_contas_receber_gs, silver_dim_clientes_4s).

# %%
# sigla, tipo, assunto, chave_negocio, coluna_watermark
FORTES = [
    ("LAN", "FATO", "lancamentos", "EMP_Codigo, Codigo", "DataUltimaAlteracao"),
    ("CPG", "FATO", "contas_pagar", "EMP_Codigo, Codigo", "DataUltimaAlteracao"),
    ("NFS", "FATO", "notas_fiscais", "EMP_Codigo, Codigo", None),
    ("ORL", "FATO", "origem_lancamentos", "EMP_Codigo, LAN_Codigo", None),
    ("CRE", "FATO", "contas_receber", "EMP_Codigo, Codigo", "DataUltimaAlteracao"),
    ("VDR", "FATO", "vencimentos_contas_receber", "EMP_Codigo, CRE_Codigo, Sequencial", None),
    ("VCP", "FATO", "vencimentos_contas_pagar", "EMP_Codigo, CPG_Codigo, Data, Sequencial", None),
    ("BVP", "FATO", "baixas_contas_pagar",
     "EMP_Codigo, VCP_CPG_Codigo, VCP_Data, VCP_Sequencial, Sequencial", "DataUltimaAlteracao"),
    ("SCR", "FATO", "rateio_servicos", "EMP_Codigo, CRE_Codigo, Sequencial", None),
    ("BVD", "FATO", "baixas_contas_receber",
     "EMP_Codigo, VDR_CRE_Codigo, VDR_Sequencial, Sequencial", "DataUltimaAlteracao"),
    ("RDL", "FATO", "rateio_receitas_despesas_lancamentos", "EMP_Codigo, LAN_Codigo, Sequencial", None),
    ("TCF", "FATO", "transferencias_contas", "EMP_Codigo, Data, Sequencial", "DataUltimaAlteracao"),
    ("DCP", "FATO", "rateio_despesas", "EMP_Codigo, CPG_Codigo, CRD_Codigo, EST_Codigo, CRS_Codigo", None),
    ("RRR", "FATO", "rateio_receitas", "EMP_Codigo, CRE_Codigo, CRD_Codigo, EST_Codigo, CRS_Codigo", None),
    ("LAF", "FATO", "linhas_apuracao_financeira", "EMP_Codigo, APF_Sequencial, Codigo", None),
    ("VSE", "FATO", "vinculos_servicos_apf",
     "EMP_Codigo, LAF_APF_Sequencial, LAF_Codigo, SER_Codigo, Faturado", None),
    ("VRD", "FATO", "vinculos_natureza_centro_resultado_apf",
     "EMP_Codigo, LAF_APF_Sequencial, LAF_Codigo, CODIGO", None),
    ("CLI", "DIM", "clientes", "EMP_Codigo, Codigo", None),
    ("FRN", "DIM", "fornecedores", "EMP_Codigo, Codigo", None),
    ("CRS", "DIM", "centro_resultado", "EMP_Codigo, Codigo", None),
    ("CRD", "DIM", "natureza_financeira", "EMP_Codigo, Codigo", None),
    ("EST", "DIM", "estabelecimentos", "EMP_Codigo, Codigo", None),
    ("CON", "DIM", "contas_bancarias", "EMP_Codigo, Codigo", None),
    ("SER", "DIM", "servicos", "EMP_Codigo, Codigo", None),
    ("BAN", "DIM", "bancos", "EMP_Codigo, Codigo", None),
    ("GFR", "DIM", "grupo_fornecedores", "EMP_Codigo, Codigo", None),
    ("APF", "DIM", "apuracao_financeira", "EMP_Codigo, Sequencial", None),
    ("GCL", "DIM", "grupo_clientes", "EMP_Codigo, Codigo", None),
    ("EMP", "DIM", "empresas", "Codigo", None),
]

# %% [markdown]
# ### Colunas permitidas
# Tabelas com credenciais (senhas, tokens, chaves de API, certificado) ou binários não usam `*`:
# carregam só a lista abaixo. Coluna nova na origem dessas tabelas não entra até ser incluída aqui.

# %%
# sigla: colunas carregadas (lista explícita, sem credenciais e binários)
COLUNAS_PERMITIDAS = {
    "CLI": (
        "EMP_Codigo, Codigo, Nome, NomeFantasia, CNPJCPF, ENDCEP, EndLogradouro, EndNumero, "
        "EndComplemento, ENDBairro, MUN_UFD_Sigla, MUN_Codigo, DDD, Celular, Contato, OBS, "
        "GCL_Codigo, VRP_Codigo, COB_Codigo, CRD_Codigo, Vencimento, Site, Boleto, Recibo, "
        "Etiqueta, GeraNFAuto, DebAutomatico, BAN_Codigo, Agencia, CC, AtualObs, ContaContabil, "
        "ISS, HorasATECompradas, HorasATEMensais, HorasATEUsadas, HorasATETreinamento, "
        "CLI_Pagador, HCA, HUA, Exterior, CentralizaHrCLIPag, NrDiasLiberacao, ObsSuporte, "
        "DtNascimento, Sexo, IRRF, RG, IE, IM, REFERENCIA, NaoCobrarAtendimento, DtUltPesquisa, "
        "TPA_Codigo, AliqISS, Fone, TRA_Codigo, RetemINSS, IDENTIFICADOR, LimiteCredito, "
        "CLC_Codigo, DtAtualizacao, PISDiferenciado, PIS, COFINS, CSLL, IsentoIE, BloquearATE, "
        "VencEnesimoDiaUtil, CRS_Codigo, BloqueiaLicenca, UtilizaRG, IRRFMinDiferenciado, "
        "VLRMINIRRF, PAI_Codigo, CobrancaImpressa, CampoExtra1, CampoExtra2, CampoExtra3, "
        "CampoExtra4, CampoExtra5, CampoExtra6, CampoExtra7, CampoExtra8, CampoExtra9, "
        "CampoExtra10, CONTACONTABILACL, Fax, DDD_CELULAR, DDD_FAX, idEstrangeiro, "
        "IndicadorIEDestinatario, GEC_Codigo, TVI_Codigo, Email, CLI_Pae, ISUF, OrgaoPublico, "
        "INCENTIVOSFISCAISZFM, OptanteSimples, CONSUMIDORFINAL, BloquearFIN, GFC_CODIGO, IDWS, "
        "CodParticipante, juro, multa, EMP_Contabil, EST_Contabil, CRS_Contabil, "
        "ContaContabilDespesa, ContaContabilFornecedor, BloquearCRE, BloquearPEV, BloquearNFS, "
        "PkSysPdv, DV_Agencia, DV_CC, EhCLIPagador, ID_VendasMobile, NOE_CODIGO, "
        "NaoInformaVrIBPTFederal, NaoInformaVrIBPTEstadual, NaoInformaVrIBPTMunicipal, "
        "BloqueioLiberadoAte, DEDUCAOISS, PERCENTUALDEDUCAOISS, DataUltimaAlteracao, "
        "COMANDOENVIOBANCO, IDCLIENTEBANCO, IDPASTABOLETOCONECTA, IDPASTANOTAFISCALCONECTA, "
        "CRIARPASTABOLETOSNOTASCONECTA, CSRFMinDiferenciado, VLRMINCSRF, Desconto, "
        "RP_HABAPPCOLABORADOR, DDD_WHATSAPP, WHATSAPP, BNF_CODIGO, NPNIFSisCoServ, "
        "TipoVinculacaoSisCoServ, ID_CLIENTEFWF, CNAE, Inativo, IAG_CODIGO, "
        "SomenteDestacarPISCOFINSCSLL, SomenteDestacarISS, CNPJINTERMEDIADOR, "
        "IDENTIFICADORINTERMEDIADOR, POSSUIDUASOUMAISIM, DtLimiteGEC, INSTITUICAOFINPGTO, "
        "INTERMEDIADORTRANSACAO, CTC_Codigo, HIS_Codigo, somentedestacarIRRF, TranspFrete, "
        "ID_ASAAS, TIPODIAFAT, DTREGISTRO, DESCONSIDERARVALORMINIMO, TIPOASSINANTENFCOM, "
        "ENTEGOVERNAMENTAL, TIPOENTEGOVERNAMENTAL, somentedestacarINSS, MESSUBSEQUENTEFAT "
    ),
    "EMP": (
        "Codigo, Nome, Apelido, Fortes, INSS, IM, AliquotasEspeciais, ULTIMADTENVIOEMAILPEDIDO, "
        "ULTIMADTENVIOEMAILVCPVENCIDO, UltimaDTAtualizacaoVAS, UltimaDTEnvioEmailVDRVencendo, "
        "ULTIMADATAENVIOEMAILBOLETOS, ULTIMADATAVALIDACAOVIC, IntegradoPolibrasNet, "
        "ULTIMADTPREVISAOFATURAMENTO, dtprocbloqcli, statusprocbloqcli, DTLIBERACAOMANIFESTO, "
        "ULTIMADATAENVIOWHATSAPPBOLETOS, id "
    ),
    "EST": (
        "EMP_Codigo, Codigo, CNPJCPF, EndLogradouro, EndNumero, EndComplemento, Bairro, CEP, "
        "MUN_UFD_Sigla, MUN_Codigo, DDD, ESTContabil, ISS, Mnemonico, RetemIRRF, IM, Nome, IE, "
        "OptanteSIMPLES, RegimeEspecial, CNAE, SeriePadrao, RegimeCOFINS, STB_Codigo, "
        "STBSN_Codigo, ImagemEmail, LinkImagemEmail, CorCabecalhoEmail, CorCorpoEmail, "
        "CorFonteEmail, CorpoReciboDuplicataEmail, CorpoBoletoEmail, NomeFantasiaObsCREBoleto, "
        "NomeFantasiaRecDuplicata, EmailRemetente, NomeApresentarRemetente, ESTFiscal, "
        "RegimeEspecialICMSSubst, AssEnvEmaBol, AssEnvEmaRecDup, AssEnvEmaCartaCob, "
        "AssEnvEmaCartaQuita, CorpoCartaCobEmail, CorpoCartaQuitaEmail, ReceptorConfRecebBol, "
        "AssEnvEmaColetaPreco, CorpoColetaPrecoEmail, TipoCertificado, AmbienteEnvioNF, "
        "INCENTIVOFISCAL, NFS_ESPECIE, TSApagarNumeroSerieNFSe, TSValidarIMNFSe, "
        "TSValidarNumeroEndNFSe, TSTentaviasProcessamentoNFSe, TSTentaviasProcessamentoNFe, "
        "TSLocalCertificado, TSTipoConsultaAutorizacaoNFSe, TSTipoConsultaImpressaoNFSe, "
        "TSDescricaoServicoCustomizada, TSDescricaoServicoNFSe, TSDescricaoServicoRPSNFSe, "
        "TSUnicaDescricaoServico, TSUnicaDescricaoServicoRPS, TSDescontoPrevFaturaDuplicata, "
        "SeriePadraoNFSe, NFe_Especie, AmbienteEnvioNFSe, VERSAONFE, MODALIDADEENVIO, "
        "TSCOMPDESCRICAOSERVICONFSE, TSCOMPDESCRICAORPSNFSE, TSUTILIZARCOMPDESCPROD, "
        "VLDRPRNGERAL, VLDRSITTRIBISS, VLDRSITTRIBCOFINS, VLDRSITTRIBPIS, VLDRUF, VLDRMUN, "
        "VLDRNATUREZATRIBISS, VLDREXIGIBILIDADEISS, VLDROPERASERVPRESTADO, VLDRITEMLISTASERV, "
        "VLDRCODTRIBMUN, DTEMISSAONUMERAR, NOMENOTAFISCAL, EMPContabil, UTILIZAQUEBRALINHANFSE, "
        "TSUTILIZARDATAENVIO, TSOMITIRIMTOMADORFORA, TSCPROCODESTFISCAL, OMITIRMSGLEITRANSP, "
        "COLETAPRECORESPONDERPARAEMAIL, ContribuinteIPI, FONE, FAX, ORIENTACAODANFE, "
        "OPERACAOPRESENCACOMPRADOR, EXIBIRRETENCOES, IMPRIMIRDADOSFATURA, VARIOSSERVICOSNFSE, "
        "CNPJESCRITCONTABIL, IntegraAtlas, SEMPRERENUMERALOTE, integragnre, IntegradoACContabil, "
        "integranfce, AmbienteEnvioNFCe, CRS_Codigo, HabilitaDifal, LASTSYNCPEDIDOMOBILE, "
        "UTILIZARCOMPETENCIANFSE, EmailContador, SERIEPADRAOMDFE, AmbienteEnvioMDFe, IntegraMDFE, "
        "MODALIDADEENVIONFCE, UTILIZADODADOSLOGINNFSE, OMITIRCNPJCLIEXTERIOR, "
        "ENVIARBOLETOEMAILNFSE, DtInicioAtividade, HabTipoReceitaTribCRE, LASTSYNCCLIENTEMOBILE, "
        "OMITIRDADOSCRENFE, ENVIARCODTRIBUTACAONOCNAE, VISUALIZANFSEXML, "
        "UltimoIDClienteVendasMobile, OMITIRVALORDEDUCOES, SeriePadraoNFCe, IMPRIMIRUNIDADETRIB, "
        "TAF, NroRegEstadual, FORMATOIMPRESSAOCTEOS, AMBIENTEEMISSAOCTE, SERIEPADRAOCTEOS, "
        "NAOENVIARPARAVEDASMOBILE, ASSEMAILCARCOBRANCA, EXIBIRIMPOSTOSFEDCTEOS, TIPOEMITENTEMDF, "
        "MODOEMISSAONFSE, IMPUNDTRIBCFOPEXPORT, AssEnvEmaInfoCli, CorpoInfoCli, "
        "TipoAmbienteGerencianet, RNTC, UtilizarIntegracaoGerencianet, SUGERIRUFMUNCLI, "
        "IDENTPRESTADOR, EMPFiscal, CONTACONTABILMAPEAMENTOAUTO, ENVIARCJNOTABOLETOEMAIL, "
        "TSUTILIZARDATAENVIONFE, TSUTILIZARDATAENVIONFCE, ContaContabilFRN, ContaContabilAFR, "
        "ContaContabilCLI, ContaContabilACL, ContaContabilCON, UTILIZALOGINOAUTH2NFSE, "
        "NUMEROAEDFNFSE, OMITIROUTRASINFORMACOES, GNRE_CODIGO_PRODUTO, GNRE_CODIGO_RECEITA, "
        "RETZEROESQTSCPROCOD, MANTERNUMERORPSSISTEMA, UtilizarIntegracaoBBBoletoWeb, "
        "TipoAmbienteBBBoletoWeb, COB_CODIGO, IntegradoPolibrasNet, "
        "OPERACAOPRESENCACOMPRADORNFCe, CONTAVIRTUALPJBANK, TIPOAMBIENTEPJBANK, AGENCIAPJBANK, "
        "CONTACORRENTEPJBANK, BAN_CODIGOPJBANK, TIPOTRANSPORTADORAMDFE, EMAILPJBANK, "
        "ULTRAPASSOUSSN, ULTIMADTATUALIZACAOSSN, UtilizarIntegracaoPJBank, APAGARNUMEROSERIENFE, "
        "APAGARNUMEROSERIENFCE, ASSUNTOENVEMABOLCJNOTA, DATAEMISSAONFCOMPLEMENTAR, "
        "USARFUSOHORARIOCONFIG, FUSOHORARIOCONFIG, INTEGRAPAGAMENTOBB, TIPOAMBIENTEBBINTEGRAPAG, "
        "BB_REQUESTID, ImpNFETipoCertificado, ImpNFENomeCertificadoA3, "
        "UTILIZARINTEGRACAOBBCOBRANCA, tipoambientebbcobranca, TIPOAMBIENTEBBPIX, "
        "UTILIZARINTEGRACAOPIXBB, ASSENVEMABOLPIX, CORPOBOLETOPIXEMAIL, ASSEMAILBOLETOBANCA, "
        "ASSENVEMANF, CORPOEMAILNF, APURACAOPISCOFINSREGIMEMISTO, TSUSARLOTCOMPDESCPROD, "
        "TSUTILIZARREDBASECALC, MAPEARCCONTABILSCL, OPERACAOINTERMEDIADOR, TSUSARDADOSIMPOSTOS, "
        "FORCARDESTAQUEISS, ENVIARPDFBOLETOFORTES, ESCREVINFRESPTEC, "
        "LOCALCERTIFICADOBBINTEGRAPAG, LOCALCERTPRIVATEKEYBBINTEGRAPAG, EXTENSAOARQUIVO, "
        "NOMEFANTASIA, INTEGRAEXTRATOBB, TIPOAMBIENTEBBINTEGRAEXT, LOCALCERTIFICADOBBINTEGRAEXT, "
        "LOCALCERTPRIVATEKEYBBINTEGRAEXT, DTEMISSAONUMERARMDF, ASSENVEMATERMINOCTT, "
        "CORPOEMAILTERMINOCTT, GerarNotaPorServico, SMTPServerEmpresa, SMTPPortEmpresa, "
        "AutenticacaoSMTPEmpresa, RequerSSLTLSEmpresa, AutoTLSEmpresa, LOCALXMLMDFE, "
        "LOCALPDFMDFE, INTEGRAAPIASAAS, AmbienteEnvioNFCom, SeriePadraoNFCom, "
        "NumeroSerieCertificado, NaoFormatarNumeroRPS, CompDescricaoServico, AmbienteNacional, "
        "REGIMEAPURACAOTRIBUTARIA, SITUACAOSIMPLES, VERSAOAPIBBINTEGRAEXT, "
        "UTILIZARPERCVALORAPROXTRIB, DESTINATARIOMESMOTOMADOR, EnviarECONFNFe, EnviarECONFNFCe "
    ),
    "NFS": (
        "EMP_Codigo, Codigo, CRE_Codigo, EST_Codigo, CLI_Codigo, Formulario, Numero, Serie, "
        "SubSerie, AMIDF, DtEmissao, DtSaida, CFOP_Codigo, Cancelada, ValorFrete, ValorSeguro, "
        "ValorOutrasDespesas, ValorIPI, BaseICMS, ValorICMS, ValorProdutos, ValorIRRF, BaseISS, "
        "ValorISS, ValorServicos, ValorTotal, Observacao, ObsCorpoNF, ValorPIS, ValorCOFINS, "
        "ValorCSL, ValorPISCOFINSCSL, ICMSSobreIPI, SeloNumero, SeloSerie, IAG_Codigo, Especie, "
        "BaseICMSSubst, ValorICMSSubst, Peso_Bruto, Peso_Liquido, DtCancel, MotivoCancel, "
        "TranspNome, TranspFrete, TranspPlaca, TranspUFPlaca, TranspCNPJCPF, TranspEndereco, "
        "TranspMUN_Codigo, TranspUF, TranspIE, TranspQuantidade, TranspEspecie, TRA_Codigo, "
        "ValorINSS, DeducaoBaseINSS, NFe_DtGeracao, DeducaoBaseISS, MUN_UFD_Sigla, MUN_Codigo, "
        "ServicoNFe, BaseICMSDiferenciada, ExportaACFiscal, Desconto, FreteBaseIcmsSubst, "
        "RazaoNota, HIS_Codigo, NFe_Numero, NFe_CodVeri, StatusEnvio, MensagemStatus, "
        "RetornoEnvio, INSSManual, NOE_Codigo, TranspRNTC, ISSRetido, IPIDesconto, ICMSDesconto, "
        "AIDF, TranspUFDest, TranspMUN_CodigoDest, TEN_Codigo, ECL_Sequencial, NaoUsarEndPadrao, "
        "ISSDesconto, FreteNaoCompoeICMS, NFPropria, NFSubstituida, UFD_Sigla_Emb, LocalEmbarque, "
        "ValorBruto, DescontoIncondicional, TranspVolume, ART, NFEstornada, VALORPRODUTOICMS, "
        "FreteNaoCompoeIPI, CHAVECANCELAMENTO, NUMLOTE, DataHoraCont, JustificativaCont, "
        "Contingencia, LOGXMLERRO, OPERACAOCOMCONSUMIDORFINAL, OPERACAOPRESENCACOMPRADOR, "
        "VALOROUTRASRETENCOES, ULTIMORETORNONOTAFISCAL, VERSAONFE, MODALIDADEENVIO, "
        "TRANSPNATUREZAMERCADORIA, TRANSPESPECIEACONDICIONAMENTO, IDWS, VALORII, horasaida, "
        "MSNENVIOGNRE, STATUSGNRE, PercDescIncondicional, BLOQUEIOGNRE, NFConsumidor, "
        "TIPOEMITENTE, CNPJCPF, DATACOMPETENCIA, ICMSCompoeTotalNota, FINALIDADENFE, "
        "IPIOUTRASDESP, NUM_PREVENDA_SYSPDV, PREVENDA_SYSPDV_LIDA, UFD_SIGLADESTINOCTE, "
        "MUN_CODIGODESTINOCTE, EXTERIORDESTINOCTE, UFD_SIGLAORIGEMCTE, MUN_CODIGOORIGEMCTE, "
        "EXTERIORORIGEMCTE, TIPOCTE, TIPOSERVICOCTE, PRC_Codigo, BASEICMSUFDEST, "
        "ALIQICMSCOMBPOBREZAUFDEST, ALIQICMSUFDEST, ALIQICMSINTEREST, ALIQICMSPROVISORIOINTEREST, "
        "VALORICMSCOMBPOBREZAUFDEST, VALORICMSINTERESTUFDEST, VALORICMSINTERESTUFREMET, "
        "VEI_CODIGO, OutrasDespesasCompoeICMS, CHAVECTEORIGINAL, Marca, Transpnumero, "
        "ICMSSobreIPI_ICMS, ICMSSobreIPI_ICMSSubs, EMITIR, PROCESSAMENTO, DTHORAPROCESSAMENTO, "
        "QTDCONSULTAS, IPIDIFAL, TIPOFRETAMENTO, USU_CODIGO, JAEXPACTXT, Protocolo, "
        "OPERACAOINTERMEDIADOR, CNPJINTERMEDIADOR, IDENTIFICADORINTERMEDIADOR, "
        "PERMITIR_EDITAR_TOTAL_ITEM, NUMEROFATURA, VALORORIGINALFATURA, VALORDESCONTOFATURA, "
        "CHAVECTEANULACAO, CLI_INTERMEDIADORTRANSACAO, CHAVEGTVE, TIPOCOMPGTVE, VLRCOMPGTVE, "
        "NOMECOMPGTVE, CODIGOCANCELAMENTO, BaseICMSSTManual, NFDEVOLVIDA, LOCALDESPACHO, "
        "TipoFaturamento, TotValorIBS, TotValorCBS, TIPODEBCRED, UFD_SIGLACONSUMOIBSCBS, "
        "MUN_CODCONSUMOIBSCBS, TIPOOPERACAOGOV, CodObra, EndLogradouroObra, EndNumeroObra, "
        "EndComplementoObra, BairroObra, CEPObra, MUN_UFD_Sigla_Obra, MUN_Codigo_Obra, "
        "NUMERONFSE, POSSUIPAGANTECIPADO, LocacaoBemMovel, BASEPISCOFINSCSL "
    ),
}

# %%
# Colunas de COLUNAS_PERMITIDAS que não existem na versão do Fortes de cada origem
# (conferido no catálogo RDB$RELATION_FIELDS do 4S em 22/10/2026).
COLUNAS_AUSENTES = {"4S": {"CLI": ["MESSUBSEQUENTEFAT"], "EMP": ["id"]}}

def colunas_de(origem, sigla):
    if sigla not in COLUNAS_PERMITIDAS:
        return "*"
    fora = {c.upper() for c in COLUNAS_AUSENTES.get(origem, {}).get(sigla, [])}
    return ", ".join(c.strip() for c in COLUNAS_PERMITIDAS[sigla].split(",")
                     if c.strip() and c.strip().upper() not in fora)

# origem: (prefixo da tabela na origem, sufixo do nome no DW)
ORIGENS_FORTES = {"GS": ("dbo.", "gs"), "4S": ("", "4s")}

TABELAS = [
    dict(origem=origem, tabela_origem=f"{esquema}{sigla}",
         tabela_destino=f"{tipo.lower()}_{assunto}_{sufixo}", tipo=tipo,
         modo_carga="INCREMENTAL" if wm else "FULL", coluna_watermark=wm,
         colunas=colunas_de(origem, sigla),
         tipo_watermark="DATA" if wm else None, chave_negocio=chave,
         janela_seguranca_min=10 if wm else None, janela_dias=None, filtro_adicional=None, ativo=True)
    for origem, (esquema, sufixo) in ORIGENS_FORTES.items()
    for sigla, tipo, assunto, chave, wm in FORTES
]

# %% [markdown]
# ### Nexti (API)
# `tabela_origem` é a chave do endpoint em `ENDPOINTS_NEXTI` (nb_ingestao_api); os campos gravados
# ficam em `COLUNAS_NEXTI`. Incrementais: marcações e ausências pela data da última alteração (margem de
# 10 min); previsto x trabalhado pela data de referência, reprocessando os últimos 35 dias (apuração do
# mês ainda aberta).

# %%
# chave do endpoint, tipo, assunto, chave_negocio, coluna_watermark, margem de reprocessamento (min)
NEXTI = [
    ("CLOCKINGS", "FATO", "marcacoes_ponto", "id", "lastUpdate", 10),
    ("TIMETRACKINGS", "FATO", "previsto_trabalhado", "personId, referenceDate", "referenceDate", 35 * 24 * 60),
    ("ABSENCES", "FATO", "ausencias", "id", "lastUpdate", 10),
    ("PERSONS", "DIM", "colaboradores", "id", None, None),
    ("WORKPLACES", "DIM", "postos", "id", None, None),
    ("SCHEDULES", "DIM", "escalas", "id", None, None),
    ("SHIFTS", "DIM", "horarios", "id", None, None),
    ("CLIENTS", "DIM", "clientes", "id", None, None),
    ("COMPANIES", "DIM", "empresas", "id", None, None),
    ("BUSINESSUNITS", "DIM", "unidades_negocio", "id", None, None),
    ("CAREERS", "DIM", "cargos", "id", None, None),
    ("ABSENCESITUATIONS", "DIM", "situacoes_ausencia", "id", None, None),
]

TABELAS += [
    dict(origem="NEXTI", tabela_origem=endpoint, tabela_destino=f"{tipo.lower()}_{assunto}_nexti", tipo=tipo,
         modo_carga="INCREMENTAL" if wm else "FULL", coluna_watermark=wm, colunas="*",
         tipo_watermark="DATA" if wm else None, chave_negocio=chave, janela_seguranca_min=margem,
         janela_dias=None, filtro_adicional=None, ativo=True)
    for endpoint, tipo, assunto, chave, wm, margem in NEXTI
]

# %% [markdown]
# ## 3. Gravação (não alterar)

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

# origens: sempre reescrita a partir da lista acima
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
    raise ValueError("Há tabelas repetidas na lista TABELAS")
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
print(duckdb.sql("SELECT id, tipo, modo_carga, coluna_watermark, ultimo_watermark, ativo FROM ctl ORDER BY id"))
