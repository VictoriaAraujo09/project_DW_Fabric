# project_DW_Fabric

Data Warehouse da GS no Microsoft Fabric. Usa a arquitetura Medallion (Bronze, Silver e Gold), com carga incremental orientada a metadados.

| Pasta | Conteúdo |
|---|---|
| `implantacao/` | Script de implantação via API REST, fonte dos notebooks, gerador do pipeline mestre e testes |
| `implantacao/descoberta/` | Consultas de descoberta das tabelas (SQL Server e Firebird) e o catálogo |
| `fabric/DW_DEV/` | Itens do workspace sincronizados pela integração Git do Fabric |

O passo a passo está em `implantacao/LEIA-ME.md`.

Padrão de nomes das tabelas: `<camada>_<tipo>_<assunto>_<origem>`, por exemplo `bronze_fato_contas_receber_gs`.
