# Levantamento para padronização dos schemas da Silver

**Data:** 22/10/2026 · **Base:** as 29 tabelas `silver_*_gs` do lakehouse Silver, depois do primeiro ciclo completo do GS AG

Inventário completo, coluna a coluna: [`inventario_colunas_silver.csv`](inventario_colunas_silver.csv) (separador `;`, abre no Excel). Ele tem o tipo atual, a categoria e a proposta de cada uma das 1.410 colunas.

---

## 1. Situação encontrada (antes da padronização)

> As regras da seção 3 foram implementadas e a Silver foi recriada em 22/10/2026. Esta seção mostra o retrato **anterior**, que serviu de base para as decisões.

Antes da padronização, a Silver tinha **os mesmos tipos da origem**. A cópia do pipeline traduz os tipos do SQL Server para o Delta, e a Silver só elimina linhas duplicadas pela chave.

| Tipo atual | Colunas | Exemplo | Avaliação |
|---|---:|---|---|
| Texto | 717 | `EMP_Codigo = '0001'`, `Nome` | Ok. Códigos com zeros à esquerda **precisam** continuar como texto |
| Inteiro | 407 | `ExportaAC`, `Vencimento` | Ok. Inclui as flags 0/1 do Fortes |
| Float (`double`) | 192 | `Valor`, `ISS`, `AliqISS` | **Ajustar.** Tipo aproximado: gera centavos "fantasmas" em somas |
| Data/hora | 92 | `DtEmissao`, `DataUltimaAlteracao` | Ok. Horário de Brasília, como no sistema |
| Binário | 2 | `Logotipo` | **Remover.** Sem uso analítico |
| + técnicas | 5 por tabela | `_origem`, `_lote`, `_dt_carga`, `_ativo`, `_dt_atualizacao` | Controle do DW |

### Por tabela

| Tabela | Colunas | Texto | Inteiro | Float | Data/hora | Binário |
|---|---:|---:|---:|---:|---:|---:|
| `silver_dim_apuracao_financeira_gs` | 3 | 2 | 1 | 0 | 0 | 0 |
| `silver_dim_bancos_gs` | 8 | 8 | 0 | 0 | 0 | 0 |
| `silver_dim_centro_resultado_gs` | 12 | 8 | 2 | 1 | 1 | 0 |
| `silver_dim_clientes_gs` | 172 | 89 | 63 | 12 | 8 | 0 |
| `silver_dim_contas_bancarias_gs` | 15 | 10 | 3 | 1 | 1 | 0 |
| `silver_dim_empresas_gs` | 22 | 5 | 6 | 0 | 10 | 1 |
| `silver_dim_estabelecimentos_gs` | 285 | 154 | 124 | 1 | 5 | 1 |
| `silver_dim_fornecedores_gs` | 76 | 56 | 11 | 5 | 4 | 0 |
| `silver_dim_grupo_clientes_gs` | 5 | 3 | 2 | 0 | 0 | 0 |
| `silver_dim_grupo_fornecedores_gs` | 3 | 3 | 0 | 0 | 0 | 0 |
| `silver_dim_natureza_financeira_gs` | 25 | 14 | 10 | 0 | 1 | 0 |
| `silver_dim_servicos_gs` | 59 | 24 | 23 | 11 | 1 | 0 |
| `silver_fato_baixas_contas_pagar_gs` | 23 | 6 | 8 | 4 | 5 | 0 |
| `silver_fato_baixas_contas_receber_gs` | 29 | 9 | 8 | 6 | 6 | 0 |
| `silver_fato_contas_pagar_gs` | 94 | 37 | 8 | 42 | 7 | 0 |
| `silver_fato_contas_receber_gs` | 86 | 39 | 19 | 23 | 5 | 0 |
| `silver_fato_lancamentos_gs` | 22 | 14 | 5 | 1 | 2 | 0 |
| `silver_fato_linhas_apuracao_financeira_gs` | 34 | 10 | 23 | 1 | 0 | 0 |
| `silver_fato_notas_fiscais_gs` | 198 | 100 | 44 | 45 | 9 | 0 |
| `silver_fato_origem_lancamentos_gs` | 17 | 6 | 8 | 0 | 3 | 0 |
| `silver_fato_rateio_despesas_gs` | 10 | 9 | 0 | 1 | 0 | 0 |
| `silver_fato_rateio_receitas_despesas_lancamentos_gs` | 10 | 8 | 1 | 1 | 0 | 0 |
| `silver_fato_rateio_receitas_gs` | 7 | 6 | 0 | 1 | 0 | 0 |
| `silver_fato_rateio_servicos_gs` | 32 | 15 | 7 | 10 | 0 | 0 |
| `silver_fato_transferencias_contas_gs` | 20 | 12 | 5 | 1 | 2 | 0 |
| `silver_fato_vencimentos_contas_pagar_gs` | 65 | 35 | 9 | 13 | 8 | 0 |
| `silver_fato_vencimentos_contas_receber_gs` | 67 | 27 | 14 | 12 | 14 | 0 |
| `silver_fato_vinculos_natureza_centro_resultado_apf_gs` | 6 | 5 | 1 | 0 | 0 | 0 |
| `silver_fato_vinculos_servicos_apf_gs` | 5 | 3 | 2 | 0 | 0 | 0 |
| **Total** | **1.410** | **717** | **407** | **192** | **92** | **2** |

### Amostra de qualidade (contas a receber, 328.970 linhas)

| Verificação | Resultado |
|---|---|
| Espaços no início ou no fim dos códigos (`EMP_Codigo`, `Codigo`, `CLI_Codigo`) | 0 |
| Textos vazios (`''`) em `OBS` e `CampoExtra1` | 0 (vêm como nulo) |
| `Valor` com mais de 2 casas decimais | 25 linhas: resíduo do tipo float |
| Maior `Valor` | 4.620.000,00 |
| Datas de emissão | de 2002 a 2030, com títulos futuros |

---

## 2. ⚠️ Achado de segurança: senhas e tokens no DW (corrigido em 22/10/2026)

Com `colunas = "*"`, a primeira carga trouxe credenciais do Fortes para a Bronze e a Silver. Uma revisão coluna a coluna encontrou **49 colunas de credenciais e 2 binárias**, num total de 51 excluídas:

| Tabela | Colunas excluídas |
|---|---|
| clientes (5) | `Senha`, `SenhaPortal`, `RP_SENHAAPPCOLABORADOR`, `RP_USUAPPCOLABORADOR`, `TOKENCARTAOASAAS` |
| empresas (2) | `TOKENLICENCIADOFORTESDOC`, `Logotipo` |
| estabelecimentos (43) | **Certificado digital:** `CERTIFICADO`, `ImpNFEConteudoArquivoA1` (conteúdo do arquivo A1), `ImpNFESenhaA1`, `TSSenhaCertificado`. **Banco do Brasil:** `ACCESSTOKENBB`, `DTACCESSTOKENBB`, `CLIENTID…`/`CLIENTSECRET…`/`GWAPPDEVKEY…` de BBWEB, INTEGRAPAG, COBRANCA, PIX e INTEGRAEXT, `IDCLIENTBBINTEGRAPAG`. **PJBank:** `CREDENCIALPJBANK`, `CHAVEPJBANK`. **NFS-e / fiscais:** `LOGINNFSE`, `SENHANFSE`, `SENHAOAUTHNFSE`, `CLIENTIDNFSE`, `CLIENTSECRETNFSE`, `TOKENIBPT`, `TokenGnre`, `TokenNFCe`, `IDTOKENNFCE`, `TokenMDFe`, `TOKENASSINANTEFORTESDOC`. **Outros:** `ClientIDGerencianet`, `ClientSecretGerencianet`, `CHAVEJBBWEB`, `TokenPedidoMobile`, `TokenAuth2EMP`, `TOKENASAAS`, `SMTPUsuarioEmpresa`, `SMTPSenhaEmpresa`, `Logotipo` |
| notas fiscais (1) | `TOKENGNRE` |

Colunas com nome parecido que **foram mantidas**, por serem dado de negócio: chaves de documento fiscal (`CHAVECANCELAMENTO`, `CHAVECTEORIGINAL`, `CHAVECTEANULACAO`, `CHAVEGTVE`), `CHAVEPIX` e `CHAVETRANSFBRADESCO` dos vencimentos a pagar, caminhos de arquivo e número de série de certificados.

**O que foi feito:**
1. ✅ As 4 tabelas passaram a carregar uma lista explícita de colunas (`COLUNAS_PERMITIDAS` no `nb_setup_controle`), no GS e no 4S. Uma coluna nova no Fortes, nessas tabelas, só entra quando for incluída na lista.
2. ✅ As 4 tabelas foram apagadas da Bronze e todas as tabelas da Silver foram apagadas e recriadas. Apagar a pasta da tabela remove também o histórico Delta, então não sobra versão antiga com os dados.
3. ⏳ **Pendente, com a TI:** avaliar a troca das credenciais que passaram pela cópia, principalmente o certificado A1 e sua senha, os tokens do Banco do Brasil e a credencial do PJBank, conforme a política da empresa.

---

## 3. Regras propostas para a Silver

Regras **automáticas por tipo**, aplicadas pelo `nb_orquestrador` ao gravar a Silver. Valem para qualquer tabela e qualquer origem, sem configuração coluna a coluna.

| # | Regra | Antes | Depois | Por quê |
|---|---|---|---|---|
| R1 | Float → decimal exato | `double` | `DECIMAL(18,6)` | Soma exata de valores; 6 casas cobrem alíquotas e percentuais |
| R2 | Remover espaços do fim dos textos | `'0001   '` | `'0001'` | Campos `CHAR` do Firebird (4S) vêm completados; sem isso, os relacionamentos falham |
| R3 | Texto vazio → nulo | `''` | `NULL` | Um único jeito de representar "sem informação" |
| R4 | Data/hora sem conversão de fuso | — | — | O valor é a hora de Brasília registrada no sistema. Nenhuma camada converte |
| R5 | Remover binários e credenciais | `Logotipo`, `Senha…` | — | Sem uso analítico e com risco de segurança (seção 2) |
| R6 | Manter nomes de colunas da origem | `EMP_Codigo` | `EMP_Codigo` | Rastreabilidade até o sistema. Nomes amigáveis ficam na Gold |
| R7 | Códigos continuam texto | `'000010'` | `'000010'` | Converter para número perde os zeros à esquerda e quebra o vínculo com o sistema |
| R8 | Flags 0/1 continuam inteiro | `0`/`1` | `0`/`1` | Não há como distinguir flag de número de forma automática; a conversão para Sim/Não fica na Gold |

Classificação das colunas float pelo nome (apoio para a revisão, no CSV):

| Categoria | Colunas | Exemplos |
|---|---:|---|
| Valor monetário | 139 | `Valor`, `ValorBruto`, `ISS`, `IRRF`, `ValorICMS` |
| Percentual / taxa | 51 | `AliqISS`, `AliqICMS`, `PerAcrescimo`, `Juros`, `Multa` |
| Quantidade | 2 | `QtdIndice` |

---

### Qualidade da origem: valores suspeitos

Na conversão para decimal, todos os valores couberam no `DECIMAL(18,6)`, que vai até 999 bilhões. Alguns valores parecem erro de digitação no Fortes e devem ser confirmados com a área:

| Tabela.coluna | Maior valor absoluto |
|---|---:|
| `silver_fato_baixas_contas_receber_gs.Juros` | 60.000.000.000,00 |
| `silver_fato_vencimentos_contas_receber_gs.ValorOriginal` | 401.100.000,00 |
| `silver_fato_baixas_contas_receber_gs.Valor` | 23.648.369,00 |
| `silver_fato_contas_receber_gs.PercDescIncondicional` | 12.377.700 (%) |

## 4. Decisões (aprovadas em 22/10/2026)

| # | Decisão | Opções | Aprovado |
|---|---|---|---|
| D1 | Precisão dos decimais | `DECIMAL(18,6)` para tudo · `(18,2)` para valores e `(18,6)` para taxas | **(18,6) para tudo.** Uma regra só, sem depender do nome da coluna. O arredondamento para 2 casas fica na apresentação |
| D2 | Nomes de colunas na Silver | Manter da origem · padronizar em `snake_case` | **Manter.** A padronização entra na Gold, onde as origens são unificadas |
| D3 | Unificar GS e 4S | Tabelas separadas por origem · uma tabela com coluna `_origem` | Separadas na Silver (como hoje); **unificadas na Gold** |
| D4 | Linhas excluídas na origem | Ignorar · reconciliação semanal marcando `_ativo = false` | Reconciliação semanal, depois do piloto |
| D5 | Tratamento das credenciais (seção 2) | Excluir na Bronze · mascarar | **Excluir na Bronze.** Dado que não entra não vaza |

---

## 5. Implementação

| # | Passo | Situação |
|---|---|---|
| 1 | **Bronze:** lista explícita de `colunas` em clientes, empresas, estabelecimentos e notas fiscais | ✅ |
| 2 | **Silver:** função `padronizar()` no `nb_orquestrador`, aplicada antes do MERGE (R1, R2, R3 e a parte de binários de R5) | ✅ |
| 3 | **Recriar a Silver:** tabelas `silver_*_gs` apagadas, `ultimo_watermark` do GS zerado e ciclo `COMPLETO` | ✅ |
| 4 | **Limpar credenciais da Bronze:** as 4 tabelas afetadas foram apagadas e recriadas | ✅ |
| 5 | Teste local com float, texto com espaços, texto vazio, binário e retenção | ✅ |
| 6 | **Validar:** soma de `Valor` por empresa e mês, Bronze (float) × Silver (decimal), com diferença menor que R$ 0,01 por grupo | ✅ Lançamentos: 1.624 grupos, total R$ 5.302.623.849,59 igual nas duas camadas. Contas a receber: 1.870 grupos, total R$ 3.020.765.364,07 igual. Nenhum grupo com diferença de 1 centavo ou mais |
