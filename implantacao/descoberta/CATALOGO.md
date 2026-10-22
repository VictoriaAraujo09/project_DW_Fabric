# Catálogo de tabelas do DW (GS, 4S e VIGON)

Padrão de nomes: `<camada>_<tipo>_<assunto>_<origem>`, por exemplo `bronze_fato_contas_receber_gs`. Na Gold, a tabela que existe em mais de uma origem deve perder o sufixo de origem e ganhar a coluna `empresa` (a confirmar).

Dimensões têm carga completa 1x/dia. Fatos têm carga incremental por watermark.

| Sigla | Assunto | Tipo | Bronze (GS) | Chave | Watermark |
|---|---|---|---|---|---|
| LAN | lancamentos | FATO | `bronze_fato_lancamentos_gs` | pendente | pendente |
| CPG | contas_pagar | FATO | `bronze_fato_contas_pagar_gs` | pendente | pendente |
| NFS | notas_fiscais | FATO | `bronze_fato_notas_fiscais_gs` | pendente | pendente |
| ORL | origem_lancamentos | FATO | `bronze_fato_origem_lancamentos_gs` | pendente | pendente |
| CRE | contas_receber | FATO | `bronze_fato_contas_receber_gs` | pendente | pendente |
| VDR | vencimentos_contas_receber | FATO | `bronze_fato_vencimentos_contas_receber_gs` | pendente | pendente |
| VCP | vencimentos_contas_pagar | FATO | `bronze_fato_vencimentos_contas_pagar_gs` | pendente | pendente |
| BVP | baixas_contas_pagar | FATO | `bronze_fato_baixas_contas_pagar_gs` | pendente | pendente |
| SCR | rateio_servicos | FATO | `bronze_fato_rateio_servicos_gs` | pendente | pendente |
| BVD | baixas_contas_receber | FATO | `bronze_fato_baixas_contas_receber_gs` | pendente | pendente |
| RDL | rateio_receitas_despesas_lancamentos | FATO | `bronze_fato_rateio_receitas_despesas_lancamentos_gs` | pendente | pendente |
| TCF | transferencias_contas | FATO | `bronze_fato_transferencias_contas_gs` | pendente | pendente |
| DCP | rateio_despesas | FATO | `bronze_fato_rateio_despesas_gs` | pendente | pendente |
| RRR | rateio_receitas | FATO | `bronze_fato_rateio_receitas_gs` | pendente | pendente |
| LAF | linhas_apuracao_financeira | FATO | `bronze_fato_linhas_apuracao_financeira_gs` | pendente | pendente |
| VSE | vinculos_servicos_apf | FATO | `bronze_fato_vinculos_servicos_apf_gs` | pendente | pendente |
| VRD | vinculos_natureza_centro_resultado_apf | FATO | `bronze_fato_vinculos_natureza_centro_resultado_apf_gs` | pendente | pendente |
| CLI | clientes | DIM | `bronze_dim_clientes_gs` | pendente | n/a (FULL) |
| FRN | fornecedores | DIM | `bronze_dim_fornecedores_gs` | pendente | n/a (FULL) |
| CRS | centro_resultado | DIM | `bronze_dim_centro_resultado_gs` | pendente | n/a (FULL) |
| CRD | natureza_financeira | DIM | `bronze_dim_natureza_financeira_gs` | pendente | n/a (FULL) |
| EST | estabelecimentos | DIM | `bronze_dim_estabelecimentos_gs` | pendente | n/a (FULL) |
| CON | contas_bancarias | DIM | `bronze_dim_contas_bancarias_gs` | pendente | n/a (FULL) |
| SER | servicos | DIM | `bronze_dim_servicos_gs` | pendente | n/a (FULL) |
| BAN | bancos | DIM | `bronze_dim_bancos_gs` | pendente | n/a (FULL) |
| GFR | grupo_fornecedores | DIM | `bronze_dim_grupo_fornecedores_gs` | pendente | n/a (FULL) |
| APF | apuracao_financeira | DIM | `bronze_dim_apuracao_financeira_gs` | pendente | n/a (FULL) |
| GCL | grupo_clientes | DIM | `bronze_dim_grupo_clientes_gs` | pendente | n/a (FULL) |
| EMP | empresas | DIM | `bronze_dim_empresas_gs` | pendente | n/a (FULL) |

Pendente: rodar `descoberta/descoberta_sqlserver.sql` (GS) e `descoberta/descoberta_firebird.sql` (4S e VIGON) para preencher a chave e o watermark em `DETALHES` no `nb_setup_controle`. Protheus segue outra estrutura e terá catálogo próprio.
