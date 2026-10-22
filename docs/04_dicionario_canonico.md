# Dicionário canônico: nomes padrão dos campos do DW

Cada sistema chama a mesma informação de um jeito. A data de emissão de um título é `DtEmissao` no Fortes, `E1_EMISSAO` no Protheus e pode ser `EMISSAO` ou `DT_EMIS` em outro sistema. O **dicionário canônico** define **um nome único para cada informação de negócio**. O **de-para** registra, para cada sistema, qual coluna corresponde a qual nome padrão.

| Arquivo | Conteúdo |
|---|---|
| [`implantacao/mapeamento/dicionario_canonico.csv`](../implantacao/mapeamento/dicionario_canonico.csv) | Entidades e campos padrão, com tipo e descrição |
| [`implantacao/mapeamento/de_para.csv`](../implantacao/mapeamento/de_para.csv) | Coluna de cada origem → campo padrão, com a transformação necessária e o grau de confiança |
| [`implantacao/mapeamento/gerar_mapeamento.py`](../implantacao/mapeamento/gerar_mapeamento.py) | Fonte dos dois CSVs. Edite aqui e rode o script, que valida e gera os arquivos |

**Onde é usado:** a Bronze e a Silver mantêm os nomes da origem (decisão D2), o que garante a rastreabilidade. O de-para é aplicado na **Gold**, que junta as origens numa única tabela por entidade, por exemplo uma `gold_fato_parcela_receber` com GS, 4S e Protheus e uma coluna `origem`. A Gold ainda não foi construída (Fase 4). O de-para de cada origem é feito na inclusão dela, para que a Gold já nasça com todas as origens.

---

## Regras de nomes

| Prefixo | Uso | Exemplos |
|---|---|---|
| `codigo_` | Identificador ou chave, **sempre texto** | `codigo_cliente`, `codigo_titulo` |
| `numero_` | Número de documento que aparece para o usuário, texto | `numero_nota`, `numero_parcela` |
| `nome_` | Descrição ou nome | `nome_cliente`, `nome_natureza` |
| `data_` | Data (sem hora) | `data_emissao`, `data_vencimento` |
| `data_hora_` / `data_ultima_alteracao` | Data com hora | `data_ultima_alteracao` |
| `valor_` | Valor monetário, `DECIMAL(18,6)` | `valor_bruto`, `valor_juros` |
| `percentual_` | Percentual ou alíquota | `percentual_desconto` |
| `quantidade_` | Quantidade | `quantidade_itens` |
| sem prefixo, adjetivo | Sim/não (booleano) | `quitada`, `cancelada`, `inativo` |

Outras regras:
- Minúsculas, sem acento, palavras separadas por `_`, em português.
- O nome diz **o que é**, não de onde vem: `data_vencimento`, e não `dt_venc_e1`.
- Se o mesmo conceito tiver duas versões, o nome diferencia: `data_vencimento` × `data_vencimento_original`, `valor_bruto` × `valor_liquido`.
- `codigo_empresa` está em todas as entidades, porque todas as origens são multiempresa.

## Transformações previstas no de-para

| Situação na origem | Transformação | Exemplo |
|---|---|---|
| Data em texto `AAAAMMDD` | Converter para data; vazio ou `'        '` vira nulo | Protheus `E1_EMISSAO = '20261022'` |
| Chave composta | Concatenar com separador | Protheus `A1_COD + '-' + A1_LOJA` → `codigo_cliente` |
| Flag `S`/`N`, `1`/`0`, `T`/`F` | Converter para booleano | `Quitado = 1` → `quitada = true` |
| Código de situação | Tabela de equivalência na Gold | `E1_SITUACA = '0'` → "em carteira" |
| CNPJ/CPF com máscara | Manter só os dígitos | `12.345.678/0001-90` → `12345678000190` |
| Valor com sinal invertido | Multiplicar por −1 | Saídas negativas num sistema e positivas no outro |
| Exclusão lógica | Filtrar ou marcar `_ativo = false` | Protheus `D_E_L_E_T_ = '*'` |

## Grau de confiança

| Confiança | Significado | O que fazer |
|---|---|---|
| `alta` | Nome, tipo e conteúdo batem com a descrição do campo | Nada |
| `média` | Bate pelo nome e tipo, mas o significado exato precisa ser confirmado | Confirmar com quem usa o sistema |
| `baixa` | Suposição a partir do nome | Validar com dados antes de usar na Gold |

---

## Entidades

Situação em 22/10/2026: **21 entidades e 162 campos**, todos com de-para para o Fortes (GS e 4S).

| Entidade | Tipo | Fortes | Campos padrão |
|---|---|---|---|
| `empresa` | DIM | EMP | codigo_empresa, nome_empresa |
| `estabelecimento` | DIM | EST | codigo_estabelecimento, nome_estabelecimento, nome_fantasia, cnpj, uf, codigo_municipio |
| `cliente` | DIM | CLI | codigo_cliente, nome_cliente, nome_fantasia, cnpj_cpf, uf, codigo_municipio, codigo_grupo_cliente, inativo |
| `grupo_cliente` | DIM | GCL | codigo_grupo_cliente, nome_grupo_cliente |
| `fornecedor` | DIM | FRN | codigo_fornecedor, nome_fornecedor, nome_fantasia, cnpj_cpf, uf, codigo_grupo_fornecedor |
| `grupo_fornecedor` | DIM | GFR | codigo_grupo_fornecedor, nome_grupo_fornecedor |
| `natureza_financeira` | DIM | CRD | codigo_natureza, nome_natureza, tipo_natureza, codigo_natureza_pai, inativo |
| `centro_resultado` | DIM | CRS | codigo_centro_resultado, nome_centro_resultado, codigo_centro_resultado_pai, inativo |
| `banco` | DIM | BAN | codigo_banco, numero_banco, nome_banco |
| `conta_bancaria` | DIM | CON | codigo_conta_bancaria, nome_conta_bancaria, codigo_banco, agencia, numero_conta, inativo |
| `titulo_receber` | FATO | CRE | codigo_titulo, codigo_estabelecimento, codigo_cliente, numero_documento, data_emissao, valor_bruto, valor_liquido, codigo_natureza, codigo_centro_resultado, data_ultima_alteracao |
| `parcela_receber` | FATO | VDR | codigo_titulo, numero_parcela, data_vencimento, data_vencimento_original, valor_parcela, valor_original, valor_desconto, quitada, data_cancelamento |
| `baixa_receber` | FATO | BVD | codigo_titulo, numero_parcela, sequencial_baixa, data_baixa, data_pagamento, valor_baixa, valor_desconto, valor_juros, valor_multa, data_cancelamento |
| `titulo_pagar` | FATO | CPG | codigo_titulo, codigo_estabelecimento, codigo_fornecedor, numero_documento, data_emissao, data_entrada, valor_bruto, valor_liquido, codigo_natureza, codigo_centro_resultado, data_ultima_alteracao |
| `parcela_pagar` | FATO | VCP | codigo_titulo, numero_parcela, data_vencimento, data_vencimento_original, valor_parcela, valor_desconto, valor_juros, valor_multa, quitada, data_cancelamento |
| `baixa_pagar` | FATO | BVP | codigo_titulo, numero_parcela, sequencial_baixa, data_baixa, valor_baixa, valor_desconto, valor_juros, valor_multa, data_cancelamento |
| `lancamento_financeiro` | FATO | LAN | codigo_lancamento, codigo_estabelecimento, codigo_conta_bancaria, data_lancamento, valor, tipo_movimento, origem_lancamento, historico, codigo_natureza, codigo_centro_resultado |
| `transferencia_contas` | FATO | TCF | data_transferencia, sequencial, codigo_conta_origem, codigo_conta_destino, valor, historico |
| `nota_fiscal` | FATO | NFS | codigo_nota, codigo_estabelecimento, codigo_cliente, numero_nota, serie, data_emissao, cfop, valor_total, valor_servicos, valor_produtos, cancelada, data_cancelamento, codigo_titulo_receber |
| `rateio_receita` | FATO | RRR | codigo_titulo, codigo_natureza, codigo_centro_resultado, codigo_estabelecimento, valor |
| `rateio_despesa` | FATO | DCP | codigo_titulo, codigo_natureza, codigo_centro_resultado, codigo_estabelecimento, valor |

O `codigo_empresa` foi omitido da coluna de campos, mas está em todas as entidades.

**Tabelas do Fortes ainda sem entidade:** ORL (origem de lançamentos), SCR (rateio de serviços), RDL (rateio de lançamentos), LAF/APF/VSE/VRD (apuração financeira) e SER (serviços). Elas entram no dicionário quando a Gold precisar delas.

## Como uma origem nova entra no dicionário

1. Cada tabela da origem é associada a uma entidade existente, se houver. A descrição de negócio informada na planilha é o que permite essa associação.
2. Cada coluna é associada a um campo padrão, a partir do nome, do tipo, da descrição no dicionário do sistema (quando existir) e de uma amostra dos dados.
3. Uma informação que não existe no dicionário vira **campo novo**, seguindo as regras de nomes acima. Uma tabela sem entidade equivalente vira **entidade nova**.
4. O de-para entra em `gerar_mapeamento.py` com o grau de confiança. Os itens `média` e `baixa` vão para a lista de dúvidas da origem.
