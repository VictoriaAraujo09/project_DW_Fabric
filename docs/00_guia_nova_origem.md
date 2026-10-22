# Guia padrão: incluir um sistema novo no DW

Para quem vai trazer um sistema novo (ERP, folha, CRM, planilha em banco) para o DW pela primeira vez. Siga as etapas em ordem. Cada uma diz **quem faz**, **o que entregar** e **como saber que terminou**.

O guia vale para qualquer sistema. Os quadros **Exemplo Protheus** mostram como fica num caso real e servem para o primeiro teste.

> **Quem é quem neste guia**
> - **Você:** responsável pela inclusão. Preenche a planilha, roda as consultas e valida os números.
> - **TI / DBA:** cria o acesso ao banco e o gateway.
> - **Claude (Claude Code no VS Code, aberto na pasta do projeto):** monta o cadastro, faz o de-para dos campos, implanta e roda as cargas. Ele lê os arquivos que você salvar em `docs/origens/<sistema>/`.

---

## Visão geral

| # | Etapa | Quem | Entrega |
|---|---|---|---|
| 1 | Pedir o acesso | Você → TI | Gateway, conexão e usuário somente leitura |
| 2 | Pegar o ID da conexão | Você | ID da conexão |
| 3 | Listar as tabelas e descrever cada uma | Você | `tabelas.csv` |
| 4 | Extrair a estrutura do banco | Você | `estrutura.csv` e, se houver, `dicionario_sistema.csv` |
| 5 | Pedir a inclusão ao Claude | Você → Claude | Mensagem padrão (etapa 5) |
| 6 | Revisar a proposta | Você | Aprovação ou correções |
| 7 | Implantar e carregar no DW Dev | Claude | Bronze e Silver carregadas |
| 8 | Validar os números | Você | Totais conferidos com o sistema |

Tempo típico: 1 a 2 dias, dependendo do acesso da TI e do número de tabelas.

---

## Etapa 1: pedir o acesso à TI

Envie à TI este pedido, adaptado:

> Preciso de acesso de **leitura** ao banco do sistema **\<nome do sistema\>** para o Data Warehouse no Microsoft Fabric:
> 1. Usuário de banco **somente leitura** (`SELECT`) nas tabelas da lista anexa e nas tabelas de dicionário do sistema, se existirem.
> 2. **Gateway on-premises** do Fabric instalado num servidor que enxergue o banco (pode ser um gateway já existente).
> 3. **Conexão** nesse gateway, criada com o usuário do item 1, com acesso liberado para a minha conta.
> 4. Informações: tipo e versão do banco, servidor, porta, nome do banco e janela de horário em que as consultas são permitidas.

**Terminou quando:** a conexão aparece em *Fabric → Configurações → Gerenciar conexões e gateways → Conexões*, com status ativo.

**Bancos suportados hoje:** SQL Server e Firebird. Para Oracle, PostgreSQL, MySQL ou outro banco, avise o Claude logo no início: é preciso desenvolver o conector antes, o que leva cerca de meio dia a mais.

---

## Etapa 2: pegar o ID da conexão

No terminal do VS Code, na pasta do projeto:

```powershell
.\.venv\Scripts\python.exe implantacao\implantar_fundacao.py --listar-conexoes
```

Na lista, ache a conexão pelo nome e copie o **ID** (formato `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) e o caminho (servidor;banco).

> **Exemplo Protheus**
> `c4b4f96f-b6a6-40f4-8dbb-2351c2e6dd83  Protheus PROD  OnPremisesGateway  SQL: 45.6.153.9,1819;C7FTHA_136941_PR_PD`

---

## Etapa 3: listar as tabelas e descrever cada uma

1. Crie a pasta `docs/origens/<sistema>/`, em minúsculas e sem espaço (ex.: `docs/origens/protheus/`).
2. Copie o modelo [`docs/modelos/tabelas_origem_modelo.csv`](modelos/tabelas_origem_modelo.csv) para essa pasta com o nome `tabelas.csv`.
3. Abra no Excel e preencha uma linha por tabela. Apague as linhas de exemplo.

| Coluna | Obrigatória | O que colocar |
|---|---|---|
| `tabela_origem` | **Sim** | Nome físico da tabela no banco, com esquema: `dbo.SE1010` |
| `descricao_negocio` | **Sim** | **O que a tabela guarda, em linguagem de negócio**, e o que é uma linha. Ex.: *"Títulos a receber: uma linha por parcela de cada título"*. É a informação mais importante: é com ela que o Claude relaciona a tabela às que já existem no DW |
| `tipo_sugerido` | Não | `FATO` (movimento: títulos, baixas, notas, lançamentos) ou `DIM` (cadastro: clientes, naturezas, contas) |
| `entidade_equivalente` | Não | Se souber, a entidade do [dicionário canônico](04_dicionario_canonico.md#entidades) que a tabela representa (`parcela_receber`, `cliente`…). Se não souber, deixe em branco |
| `chave` | Não | Colunas que identificam uma linha. A consulta da etapa 4 traz isso |
| `coluna_alteracao` | Não | Coluna com a data/hora da última alteração, se existir |
| `volume_linhas` | Não | Aproximado. A consulta da etapa 4 traz isso |
| `observacoes` | Não | Particularidades: filtros, exclusão lógica, empresas que interessam, campos que não podem vir |

**Dicas para a descrição:**
- Diga o que é **uma linha**: "uma linha por parcela", "uma linha por item da nota", "uma linha por baixa".
- Diga se a tabela tem **cabeçalho e itens** separados: "cabeçalho da nota fiscal de saída; os itens estão na SD2".
- Cite o **equivalente no Fortes**, se souber: "equivale à VDR + CRE do Fortes".

> **Exemplo Protheus: tabelas do módulo financeiro, sugestão inicial a confirmar com a TI**
>
> | tabela_origem | descricao_negocio | entidade_equivalente |
> |---|---|---|
> | `dbo.SE1010` | Títulos a receber, uma linha por parcela | `parcela_receber` e `titulo_receber` |
> | `dbo.SE2010` | Títulos a pagar, uma linha por parcela | `parcela_pagar` e `titulo_pagar` |
> | `dbo.SE5010` | Movimentação bancária e baixas (a receber e a pagar) | `lancamento_financeiro`, `baixa_receber`, `baixa_pagar` |
> | `dbo.FK1010` / `dbo.FK2010` | Baixas a receber / a pagar (versões 12.1.x) | `baixa_receber` / `baixa_pagar` |
> | `dbo.SEV010` / `dbo.SEZ010` | Rateio do título por natureza / por centro de custo | `rateio_receita` / `rateio_despesa` |
> | `dbo.SA1010` | Clientes (chave código + loja) | `cliente` |
> | `dbo.SA2010` | Fornecedores (chave código + loja) | `fornecedor` |
> | `dbo.SED010` | Naturezas financeiras | `natureza_financeira` |
> | `dbo.CTT010` | Centros de custo | `centro_resultado` |
> | `dbo.SA6010` | Bancos, agências e contas (tabela única) | `conta_bancaria` e `banco` |
> | `dbo.SF2010` / `dbo.SD2010` | Nota fiscal de saída: cabeçalho / itens | `nota_fiscal` |
>
> O sufixo `010` é o código da empresa 01. Se houver outras empresas (`020`, `030`…), liste as tabelas de cada uma. A tabela de empresas e filiais (SM0) fica fora do banco nas versões antigas; pergunte à TI onde ela está.

**Terminou quando:** `tabelas.csv` está salvo, com tabela e descrição preenchidas em todas as linhas.

---

## Etapa 4: extrair a estrutura do banco

Rode as consultas abaixo no banco da origem com o usuário de leitura, pelo SSMS, DBeaver ou outra ferramenta. Exporte cada resultado como **CSV** para a pasta `docs/origens/<sistema>/`.

### 4.1 Estrutura das tabelas → `estrutura.csv` (obrigatório)

Uma linha por coluna, com tipo, tamanho, se faz parte da chave primária, número de linhas e descrição, quando o banco tiver.

**SQL Server**
```sql
SELECT s.name AS esquema, t.name AS tabela, c.column_id AS ordem, c.name AS coluna,
       ty.name AS tipo, c.max_length AS tamanho, c.precision AS precisao, c.scale AS escala,
       c.is_nullable AS aceita_nulo,
       CASE WHEN ic.column_id IS NOT NULL THEN 1 ELSE 0 END AS chave_primaria,
       (SELECT SUM(p.rows) FROM sys.partitions p
         WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)) AS linhas_tabela,
       CAST(ep.value AS nvarchar(500)) AS descricao
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.columns c ON c.object_id = t.object_id
JOIN sys.types ty ON ty.user_type_id = c.user_type_id
LEFT JOIN sys.indexes i ON i.object_id = t.object_id AND i.is_primary_key = 1
LEFT JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                              AND ic.column_id = c.column_id
LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id AND ep.minor_id = c.column_id
                                    AND ep.name = 'MS_Description'
WHERE t.name IN ('<TABELA1>', '<TABELA2>')          -- as tabelas do tabelas.csv, sem esquema
ORDER BY t.name, c.column_id;
```

**Firebird**
```sql
SELECT TRIM(rf.RDB$RELATION_NAME) AS tabela, rf.RDB$FIELD_POSITION AS ordem,
       TRIM(rf.RDB$FIELD_NAME) AS coluna, f.RDB$FIELD_TYPE AS tipo, f.RDB$FIELD_LENGTH AS tamanho,
       f.RDB$FIELD_SCALE AS escala, rf.RDB$NULL_FLAG AS nao_nulo,
       CAST(rf.RDB$DESCRIPTION AS VARCHAR(500)) AS descricao
FROM RDB$RELATION_FIELDS rf
JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = rf.RDB$FIELD_SOURCE
WHERE rf.RDB$RELATION_NAME IN ('<TABELA1>', '<TABELA2>')   -- em MAIÚSCULAS
ORDER BY 1, 2;
```

**Oracle / PostgreSQL / MySQL:** use `ALL_TAB_COLUMNS` + `ALL_COL_COMMENTS` (Oracle) ou `information_schema.columns` (PostgreSQL e MySQL). Se tiver dúvida, peça a consulta pronta ao Claude, informando o banco.

### 4.2 Dicionário do próprio sistema → `dicionario_sistema.csv` (se existir)

Muitos ERPs guardam o nome amigável de cada campo em tabelas próprias. Esse é o melhor insumo para o de-para: diz que `E1_VENCREA` é "Vencimento real". Pergunte à TI se o sistema tem esse dicionário e onde ele fica.

> **Exemplo Protheus** (versões com dicionário no banco, tabelas `SX2` e `SX3` + código da empresa):
> ```sql
> SELECT X3_ARQUIVO AS tabela, X3_ORDEM AS ordem, X3_CAMPO AS coluna, X3_TIPO AS tipo,
>        X3_TAMANHO AS tamanho, X3_DECIMAL AS decimais, X3_TITULO AS titulo, X3_DESCRIC AS descricao
> FROM SX3010
> WHERE X3_ARQUIVO IN ('SE1', 'SE2', 'SE5', 'SA1', 'SA2', 'SED', 'CTT', 'SA6', 'SF2', 'SD2')
>   AND D_E_L_E_T_ = ' '
> ORDER BY X3_ARQUIVO, X3_ORDEM;
> ```

### 4.3 Amostra de dados → `amostra_<tabela>.csv` (recomendado para fatos)

Exporte **50 linhas** de cada tabela de fato. A amostra mostra o formato real dos dados, por exemplo se a data é texto `20261022` ou um tipo data, e ajuda a confirmar o significado de campos ambíguos.

```sql
SELECT TOP 50 * FROM dbo.<TABELA> ORDER BY <coluna de data> DESC;   -- SQL Server
SELECT FIRST 50 * FROM <TABELA> ORDER BY <coluna de data> DESC;     -- Firebird
```

> ⚠️ **Não exporte amostra de tabelas de cadastro** com dados pessoais ou credenciais, como clientes, usuários, empresa ou parâmetros. Para essas tabelas, a estrutura da 4.1 basta. Os CSVs de amostra ficam só na sua pasta e **não devem ir para o GitHub**: a pasta `docs/origens/*/amostra_*` está no `.gitignore`.

**Terminou quando:** a pasta `docs/origens/<sistema>/` tem `tabelas.csv`, `estrutura.csv` e, se houver, `dicionario_sistema.csv` e as amostras.

---

## Etapa 5: pedir a inclusão ao Claude

No Claude Code, aberto na pasta do projeto, envie esta mensagem, trocando o que está entre `< >`:

```text
Incluir nova origem no DW seguindo docs/00_guia_nova_origem.md.

Sistema: <nome do sistema e versão>
Nome curto da origem: <EX.: PROTHEUS>   Sufixo das tabelas no DW: <ex.: pth>
Banco: <SQL Server / Firebird / outro> — servidor/banco: <servidor;banco>
Conexão no Fabric: <nome da conexão> — ID: <id da conexão>
Arquivos: docs/origens/<sistema>/ (tabelas.csv, estrutura.csv, dicionario_sistema.csv, amostras)
Empresas/filiais que interessam: <todas / lista>
Particularidades: <exclusão lógica, filtros, horários, o que mais souber>

Quero a proposta para revisão antes de implantar.
```

---

## Etapa 6: revisar a proposta do Claude

O Claude devolve uma proposta em `docs/origens/<sistema>/proposta.md`, com seis partes:

| Parte | O que é | O que você confere |
|---|---|---|
| **1. Tabelas e carga** | Para cada tabela: tipo, nome no DW (`bronze_fato_parcelas_receber_pth`), chave e forma de carga (incremental, completa ou últimos N dias) | O nome faz sentido para o negócio? As tabelas grandes estão em carga incremental? |
| **2. Equivalência com o DW** | Para cada tabela, a entidade do dicionário canônico (ex.: SE1 → `parcela_receber`), ou "entidade nova" | A tabela representa mesmo aquilo? |
| **3. De-para de campos** | Para cada coluna útil: o campo padrão, a transformação e a confiança. Ex.: `E1_EMISSAO` → `data_emissao` (texto AAAAMMDD → data, alta); `E1_VENCREA` → `data_vencimento` (alta) | Os itens de confiança `média` e `baixa` |
| **4. Campos e entidades novos** | Informações que não existiam no DW, com o nome padrão proposto | Os nomes seguem as [regras](04_dicionario_canonico.md#regras-de-nomes)? |
| **5. Colunas excluídas** | Senhas, tokens, certificados, binários e dados pessoais que não devem entrar | Falta alguma? |
| **6. Dúvidas** | O que não deu para decidir só com a estrutura | Responder cada uma |

Responda no próprio Claude Code: "aprovado" ou a lista de correções. As dúvidas da parte 6 precisam estar respondidas antes da implantação.

> **Exemplo Protheus: dúvidas típicas**
> - A exclusão lógica (`D_E_L_E_T_ = '*'`) entra filtrada ou marcada como excluída?
> - As colunas `S_T_A_M_P_` / `I_N_S_D_T_` estão habilitadas? Sem elas, a SE1 e a SE2 ficam em carga dos últimos N dias.
> - O título a receber do DW é por parcela (como a SE1) ou por título (prefixo + número)?
> - Quais tipos de título (`E1_TIPO`) entram nas análises? `NF`, `DP`, `BOL`? Os de abatimento (`AB-`, `IR-`…) ficam fora?

---

## Etapa 7: implantar e carregar no DW Dev (Claude)

Depois da aprovação, o Claude:
1. Registra a conexão no `config_fundacao.json` e as tabelas no `nb_setup_controle`.
2. Registra o de-para em `implantacao/mapeamento/gerar_mapeamento.py` e gera os CSVs.
3. Roda o teste local.
4. Implanta no DW Dev só com a origem nova e roda o **ciclo completo**. As outras origens não são tocadas.
5. Confere que **Bronze = Silver** em todas as tabelas, que não há chave duplicada e que os tipos estão padronizados.
6. Roda um **ciclo de fatos** e confere que as tabelas incrementais trazem só as alterações.
7. Reimplanta o pipeline com todas as origens.
8. Atualiza a documentação ([03](03_documentacao_projeto.md) e [04](04_dicionario_canonico.md)).

Se algo falhar (coluna inexistente, tipo inválido, nome diferente), o Claude corrige e roda de novo. A falha fica registrada no `ctl_log`, e o ponto de parada só avança quando o ciclo inteiro dá certo.

**Terminou quando:** o Claude informa os dois ciclos com sucesso e o número de linhas por tabela.

---

## Etapa 8: validar os números (você)

A validação técnica garante que os dados chegaram. A validação de negócio garante que eles **batem com o sistema**. Escolha 2 ou 3 relatórios que a área já usa e compare com o DW:

| Verificação | No sistema de origem | No DW (SQL endpoint do lakehouse Silver) | Tolerância |
|---|---|---|---|
| Total emitido por mês | Relatório de títulos emitidos | `SUM(valor)` por mês de `data_emissao` | R$ 0,01 |
| Saldo em aberto numa data | Relatório de posição de títulos | Parcelas não quitadas e não canceladas | R$ 0,01 |
| Quantidade de cadastros ativos | Tela de clientes | `COUNT(*)` dos clientes ativos | Exata |

O Claude monta as consultas do lado do DW a partir do relatório que você indicar. Registre o resultado em `docs/origens/<sistema>/validacao.md`. Se um número não bater, a diferença normalmente vem de filtro (tipo de título, exclusão lógica, empresa) e se resolve na proposta.

**Terminou quando:** os totais batem e a validação está registrada.

---

## Checklist final

- [ ] Conexão ativa com usuário somente leitura
- [ ] `docs/origens/<sistema>/` com `tabelas.csv` e `estrutura.csv`
- [ ] Proposta revisada e dúvidas respondidas
- [ ] Colunas sensíveis fora da carga
- [ ] Ciclo completo e ciclo de fatos com sucesso no DW Dev
- [ ] De-para registrado e dicionário canônico atualizado
- [ ] Números de negócio conferidos (`validacao.md`)
- [ ] Documentação do projeto atualizada

---

## Referências

- [01 – Detalhes técnicos da Bronze](01_nova_origem_bronze.md): formas de carga, particularidades por sistema e consultas de levantamento.
- [02 – Padronização da Silver](02_levantamento_silver.md): regras de tipos e colunas sensíveis.
- [03 – Documentação do projeto](03_documentacao_projeto.md): ambiente, notebooks e pipeline.
- [04 – Dicionário canônico](04_dicionario_canonico.md): nomes padrão dos campos e de-para.
