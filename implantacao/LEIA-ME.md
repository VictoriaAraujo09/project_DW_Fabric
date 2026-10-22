# Fase 1 – Fundação do DW no Microsoft Fabric

Este pacote cria a estrutura do DW em poucos minutos: workspaces, lakehouses, tabelas de controle, notebooks e o pipeline mestre. O processo é orientado a metadados: para incluir uma tabela, basta acrescentar uma linha na tabela de controle, sem criar pipeline novo.

## Conteúdo

| Arquivo | O que é |
|---|---|
| `implantar_fundacao.py` | Script que cria tudo no Fabric pela API REST. Pode ser rodado de novo sem duplicar nada |
| `config_fundacao.json` | Nome da capacidade e IDs das conexões do gateway |
| `pipeline_mestre.py` | Gera a definição do pipeline `pl_mestre_dw` (usado pelo script) |
| `notebooks/nb_setup_controle.ipynb` | Cria `ctl_origens`, `ctl_tabelas` e `ctl_log` em `lh_bronze` |
| `notebooks/nb_preparar_ciclo.ipynb` | Lê o controle e monta as consultas incrementais de cada tabela |
| `notebooks/nb_orquestrador.ipynb` | Faz o MERGE na Silver, avança os watermarks e grava o log |
| `src/`, `build_notebooks.py`, `teste_local.py` | Fonte dos notebooks e teste local, para versionar no GitHub |

## Padrão de nomes

Formato: `<camada>_<tipo>_<assunto>_<origem>`, tudo minúsculo e sem acento. Na tabela de controle você informa só o `assunto`, e os nomes de cada camada são gerados automaticamente.

| Origem | Tabela | Assunto | Bronze | Silver |
|---|---|---|---|---|
| GS | CRE | contas_receber | `bronze_fato_contas_receber_gs` | `silver_fato_contas_receber_gs` |
| GS | FILIAL | filial | `bronze_dim_filial_gs` | `silver_dim_filial_gs` |

Tipo: `fato` ou `dim`. Origem: `gs`, `protheus`, `4s` ou `vigon`.

## Passo a passo

**1. Preparar o computador** (uma vez só)
```
pip install azure-identity requests
```

**2. Descobrir o ID da conexão do GS**
```
python implantar_fundacao.py --listar-conexoes
```
O login abre no navegador. Na lista, copie o ID da conexão do SQL Server do GS e cole em `connection_id` da origem GS no arquivo `config_fundacao.json`. Preencha também `capacidade` com o nome da F4. As outras origens podem ficar vazias por enquanto.

**3. Implantar**
```
python implantar_fundacao.py
```
O script cria DW_DEV e DW_PROD na F4. Os itens (lakehouses, notebooks e pipeline) são criados só no DW_DEV. O DW_PROD recebe os itens pelo deployment pipeline (passo 7).

**4. Cadastrar as tabelas do GS**
No DW_DEV, abra `nb_setup_controle`, substitua os dois exemplos da lista `TABELAS` pelas 28 tabelas do GS e execute o notebook. Ele valida cada linha antes de gravar.

**5. Primeiro ciclo**
Execute `pl_mestre_dw` manualmente com o parâmetro `ciclo = COMPLETO`. Depois confira o resultado no SQL endpoint do `lh_bronze`:
```sql
SELECT * FROM ctl_log ORDER BY dt_registro DESC;
SELECT id, ultimo_watermark FROM ctl_tabelas;
```

**6. Conectar o DW_DEV ao GitHub** (pela interface)
Em Configurações do workspace → Integração com Git → GitHub, selecione o repositório e a branch. Recomendo versionar também a pasta `src/` deste pacote no mesmo repositório.

**7. Criar o deployment pipeline DEV → PROD** (pela interface)
Crie o deployment pipeline com os estágios DW_DEV e DW_PROD. A promoção leva os itens; os dados não são copiados. No DW_PROD, rode o `nb_setup_controle` uma vez. Os agendamentos ficam só no PROD: 05:00, 12:00 e 17:00, com o parâmetro `ciclo = AUTO`, que decide sozinho entre ciclo completo e de fatos pelo horário.

**8. Limpeza**
Remova os itens de teste: Dw Teste, testeSintetico, pipeline_teste_service, StagingLakehouseForDataflows_* e DataflowsStagingWarehouse. Mantenha os agendamentos e relatórios atuais até o novo fluxo assumir cada assunto.

## Como o ciclo funciona

1. `Preparar_Ciclo` lê `ctl_tabelas` e monta as consultas de cada tabela ativa.
2. Para cada origem, um `ForEach` com paralelismo 4 executa, por tabela:
   - um **Lookup** que lê o novo watermark na origem (`MAX` da coluna de alteração);
   - se o watermark mudou, uma **Copy** só das linhas da janela, gravadas na Bronze por append;
   - se não mudou, a cópia é pulada, o que economiza cerca de 360 CU-s.
3. `Orquestrador` faz o MERGE na Silver por chave de negócio e avança o watermark. O watermark só avança se todas as tabelas derem certo.
4. Se qualquer etapa falhar, a falha é gravada no `ctl_log`, o pipeline fica como falho e o próximo ciclo reprocessa a mesma janela sem duplicar dados.

## Pontos a conferir no primeiro teste

- **Kernel dos notebooks:** os notebooks precisam abrir como Python notebook, não PySpark. Se abrirem como PySpark, troque o idioma para Python no canto superior do notebook.
- **Conexão do pipeline:** abra o `pl_mestre_dw` e confira se o Lookup e a Copy do `ForEach_GS` mostram a conexão do gateway. Se a conexão exigir banco de dados, preencha `database` no config e rode o script de novo.
- **Firebird (4S e VIGON):** o caminho por ODBC já está no gerador do pipeline, mas só será testado na Fase 3.
- **Exclusões na origem:** ainda não são tratadas. A reconciliação semanal entra depois do piloto.
