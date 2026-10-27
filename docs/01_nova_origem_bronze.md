# Como incluir uma nova origem na camada Bronze: detalhes técnicos

> **Novo no processo?** Comece pelo [guia padrão](00_guia_nova_origem.md). Este documento é a referência técnica que ele usa: formas de carga, particularidades por sistema e os passos de implantação.

Referência técnica para trazer um sistema novo para o DW (exemplo: **Protheus**). Ela lista o que precisa existir no Fabric, as informações que devem ser levantadas antes e os passos técnicos para colocar as tabelas no ciclo.

> Não é preciso criar pipeline novo. O DW é orientado a metadados: uma origem nova é uma entrada no `config_fundacao.json` e uma lista de tabelas no `nb_setup_controle`. O script de implantação gera o resto.

---

## 1. Pré-requisitos no Fabric

| # | Item | Quem faz | Como conferir |
|---|---|---|---|
| 1 | **Gateway on-premises** instalado num servidor que enxerga o banco | Infra / TI | *Configurações → Gerenciar conexões e gateways → Gateways* |
| 2 | **Conexão** criada nesse gateway, apontando para o banco | Admin do Fabric | Mesma tela, aba *Conexões* |
| 3 | **Usuário de banco somente leitura**, usado pela conexão | DBA | `SELECT` nas tabelas, sem permissão de escrita |
| 4 | O usuário que implanta tem acesso à conexão (*Usuários* da conexão) | Admin do Fabric | A conexão aparece em `--listar-conexoes` |

Conectores suportados hoje:

| Banco | `conector` (config) | `dialeto` (setup) | Situação |
|---|---|---|---|
| SQL Server | `SQLSERVER` | `SQLSERVER` | Em produção (GS AG) |
| Firebird | `ODBC` | `FIREBIRD` | Pronto, ainda não testado contra o banco real (4S AG) |
| Oracle, MySQL, PostgreSQL etc. | — | — | Precisa de desenvolvimento: um dialeto novo em `nb_preparar_ciclo` e um conector novo em `pipeline_mestre.py` |

---

## 2. Formulário da origem

Preencha antes de começar. Sem os itens marcados com **\***, não dá para implantar.

### 2.1 Identificação

| Campo | Exemplo (Protheus) | Para que serve |
|---|---|---|
| **\*** Nome curto da origem | `PROTHEUS` | Fica no `id` das tabelas de controle (`PROTHEUS.DBO.SE1010`). Use maiúsculas, sem espaço |
| **\*** Sufixo no DW | `pth` | Fim do nome das tabelas: `bronze_fato_titulos_receber_pth`. Use minúsculas e não comece com número |
| **\*** Sistema e versão | TOTVS Protheus 12.1.2310 | Regras próprias do sistema (ver seção 5) |
| **\*** Banco e versão | SQL Server 2019 | Define o conector e o dialeto SQL |
| **\*** Servidor e banco | `45.6.153.9,1819` / `C7FTHA_136941_PR_PD` | Conferência da conexão |
| Empresas e filiais | Empresa 01, filiais 0101 e 0102 | Filtros e chaves |
| Fuso horário dos dados | Brasília | Datas e horas não são convertidas, mas o fuso precisa ser conhecido |
| Janela permitida para consultas | Sem restrição / fora das 08–18h | Pode mudar os horários do ciclo |
| Responsável técnico na origem | Nome e e-mail | Dúvidas sobre tabelas e acesso |

### 2.2 Acesso

| Campo | Exemplo | Como obter |
|---|---|---|
| **\*** Nome do gateway | o gateway que enxerga o servidor do Protheus | Tela de gateways |
| **\*** Nome da conexão | `Protheus PROD` | Tela de conexões |
| **\*** ID da conexão | `c4b4f96f-b6a6-40f4-8dbb-2351c2e6dd83` | `python implantacao\implantar_fundacao.py --listar-conexoes` |
| **\*** Nome do banco (SQL Server) | `C7FTHA_136941_PR_PD` | Aparece depois do `;` no caminho da conexão |

### 2.3 Tabelas

Uma linha por tabela. Esta planilha alimenta diretamente a lista de tabelas do `nb_setup_controle`.

| Campo | Exemplo | Regra |
|---|---|---|
| **\*** Tabela na origem (com esquema) | `dbo.SE1010` | No Firebird, sem esquema: `LAN` |
| **\*** Assunto | `titulos_receber` | Minúsculas, sem acento, com `_`. Vira `bronze_fato_titulos_receber_pth` |
| **\*** Tipo | `FATO` ou `DIM` | `DIM` só entra no ciclo completo (05:00); `FATO` entra em todos os ciclos |
| **\*** Chave | `R_E_C_N_O_` | Colunas que identificam a linha de forma única. É a chave do MERGE na Silver |
| **\*** Coluna de data de alteração | `S_T_A_M_P_` ou vazio | Se existir e for confiável, a carga é **incremental** |
| **\*** Volume aproximado | 1.200.000 linhas | Decide a forma de carga (seção 3) |
| Colunas a excluir | senhas, tokens, imagens | **Obrigatório revisar.** Ver seção 4 |
| Filtro fixo | `D_E_L_E_T_ = ' '` | Vai em `filtro_adicional` |
| Dias de janela | 90 | Só para a carga do tipo `JANELA` |

---

## 3. Como escolher a forma de carga

```
A tabela tem coluna de data de alteração confiável (sem nulos)?
├── Sim → INCREMENTAL (tipo_watermark = DATA). Traz só o que mudou desde o último ciclo.
└── Não
    ├── Tem ID crescente e as linhas nunca são alteradas (só inseridas)?
    │   └── Sim → INCREMENTAL (tipo_watermark = ID)
    ├── É pequena (até ~200 mil linhas) ou é dimensão?
    │   └── Sim → FULL (cópia completa)
    └── É grande e só muda nos documentos recentes?
        └── Sim → JANELA (recarrega os últimos N dias pela data do documento)
```

Para confirmar que a coluna de alteração é confiável, conte os nulos. O valor tem que ser zero:

```sql
SELECT COUNT(*) total,
       SUM(CASE WHEN <coluna_alteracao> IS NULL THEN 1 ELSE 0 END) sem_data
FROM <tabela>;
```

> Uma tabela FULL grande pesa em todos os ciclos. No GS AG, as 11 fatos FULL somam cerca de 2,8 milhões de linhas por ciclo. Prefira INCREMENTAL ou JANELA sempre que possível.

---

## 4. Colunas sensíveis: revisão obrigatória

Por padrão, a carga traz **todas as colunas** (`colunas = "*"`). No GS AG, isso trouxe 49 colunas de credenciais para o DW, entre elas o conteúdo do certificado digital A1 e tokens do Banco do Brasil (ver o [levantamento da Silver](02_levantamento_silver.md)). Para cada origem nova:

1. Procure colunas com `SENHA`, `PASSWORD`, `TOKEN`, `SECRET`, `CLIENTID`, `APIKEY`/`DEVKEY`, `CREDENCIAL`, `CERTIFICADO`, `LOGIN` e `USUARIO` no nome.
2. **Leia a lista de colunas das tabelas de cadastro** (empresa, filial, parâmetros, integrações). No Fortes, o conteúdo do certificado estava numa coluna chamada `ImpNFEConteudoArquivoA1`, que nenhum filtro por nome pega.
3. Procure colunas binárias (imagens, anexos, XML grande).
4. Nas tabelas que tiverem esse tipo de coluna, coloque a lista explícita do que **deve** ser carregado em [`config/colunas_permitidas.yml`](../config/colunas_permitidas.yml), em vez de `*`.

```sql
-- SQL Server: colunas suspeitas
SELECT t.name tabela, c.name coluna, ty.name tipo
FROM sys.columns c
JOIN sys.tables t ON t.object_id = c.object_id
JOIN sys.types ty ON ty.user_type_id = c.user_type_id
WHERE t.name IN (<tabelas>)
  AND (c.name LIKE '%SENHA%' OR c.name LIKE '%TOKEN%' OR c.name LIKE '%PASSWORD%'
       OR c.name LIKE '%SECRET%' OR ty.name IN ('image', 'varbinary', 'binary'));
```

---

## 5. Particularidades conhecidas por sistema

### Protheus (TOTVS)
- **Nome das tabelas** = alias + código da empresa: `SE1010` é a SE1 da empresa 01. Cada empresa é uma tabela separada.
- **Exclusão lógica:** a linha apagada continua no banco com `D_E_L_E_T_ = '*'`. Use `filtro_adicional = "D_E_L_E_T_ = ' '"`, ou traga as linhas excluídas e trate na Silver. A segunda opção é melhor para detectar exclusões.
- **Chave:** `R_E_C_N_O_` é único por tabela e serve como chave. A chave de negócio (por exemplo `E1_FILIAL, E1_PREFIXO, E1_NUM, E1_PARCELA, E1_TIPO`) pode ser registrada como referência.
- **Data de alteração:** só existe se as colunas `S_T_A_M_P_` / `I_N_S_D_T_` estiverem habilitadas no DBAccess. Confirme com a TI. Sem elas, use `R_E_C_N_O_` como watermark `ID`, que só pega inserções, ou carga `JANELA`.
- **Campos de data** costumam ser texto `AAAAMMDD` (`E1_EMISSAO`). A conversão para data é feita na Silver.

### Fortes (GS / 4S)
- Chaves compostas, começando por `EMP_Codigo`. Códigos são texto com zeros à esquerda: manter como texto.
- `DataUltimaAlteracao` existe nas principais fatos e não tem nulos no GS AG.
- No Firebird (4S), campos `CHAR` vêm completados com espaços. A remoção dos espaços é feita na Silver.

---

## 6. Levantamento técnico das tabelas

Rode na origem para preencher a planilha da seção 2.3 de uma vez.

**SQL Server (2017 ou mais novo):** chave primária e colunas de data de cada tabela.

```sql
SELECT SCHEMA_NAME(t.schema_id) AS esquema, t.name AS tabela,
  (SELECT SUM(p.rows) FROM sys.partitions p
    WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)) AS linhas,
  (SELECT STRING_AGG(c.name, ', ') WITHIN GROUP (ORDER BY ic.key_ordinal)
     FROM sys.indexes i
     JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
     JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
    WHERE i.object_id = t.object_id AND i.is_primary_key = 1) AS chave_primaria,
  (SELECT STRING_AGG(c.name, ', ')
     FROM sys.columns c JOIN sys.types ty ON ty.user_type_id = c.user_type_id
    WHERE c.object_id = t.object_id
      AND ty.name IN ('date','datetime','datetime2','smalldatetime')) AS colunas_data
FROM sys.tables t
WHERE t.name IN (<lista de tabelas>)
ORDER BY t.name;
```

**Firebird:** chave primária de cada tabela.

```sql
SELECT TRIM(rc.RDB$RELATION_NAME) AS tabela,
       LIST(TRIM(s.RDB$FIELD_NAME), ', ') AS chave_primaria
FROM RDB$RELATION_CONSTRAINTS rc
JOIN RDB$INDEX_SEGMENTS s ON s.RDB$INDEX_NAME = rc.RDB$INDEX_NAME
WHERE rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'
  AND rc.RDB$RELATION_NAME IN (<lista de tabelas em MAIÚSCULAS>)
GROUP BY rc.RDB$RELATION_NAME;
```

---

## 7. Passo a passo técnico

**1. Registrar a conexão** em [`implantacao/config_fundacao.json`](../implantacao/config_fundacao.json), na lista `origens`:

```json
{"origem": "PROTHEUS", "conector": "SQLSERVER",
 "connection_id": "c4b4f96f-b6a6-40f4-8dbb-2351c2e6dd83", "database": "C7FTHA_136941_PR_PD"}
```

**2. Ativar a origem** em [`config/tabelas.yml`](../config/tabelas.yml), na seção `origens`:

```yaml
  PROTHEUS: {dialeto: SQLSERVER, ativo: true}
```

**3. Cadastrar as tabelas** no mesmo arquivo, em `bases`: um grupo da base, com as origens que a usam (prefixo e sufixo do nome) e uma linha por tabela, com tabela, tipo, assunto, chave e coluna de alteração. Nenhum notebook é alterado: o `nb_setup_controle` lê o arquivo, valida cada linha e recusa um cadastro incompleto, repetido ou com campo desconhecido.

```yaml
  PROTHEUS:
    origens:
      PROTHEUS: {prefixo: dbo., sufixo: protheus}
    tabelas:
      - {tabela: SE1010, tipo: FATO, assunto: titulos_receber, chave: R_E_C_N_O_, watermark: S_T_A_M_P_}
```

Origem por API (como a Nexti): o endpoint e os campos gravados ficam em [`config/nexti.yml`](../config/nexti.yml), e a linha em `tabelas.yml` usa a chave do endpoint em `tabela`.

**4. Gerar os notebooks e testar localmente:**

```powershell
cd implantacao
$env:PYTHONUTF8='1'; ..\.venv\Scripts\python.exe build_notebooks.py
..\.venv\Scripts\python.exe -m pytest   # na raiz do projeto: testes unitários e de qualidade
cd ..
```

**5. Implantar no DW Dev e rodar um ciclo completo só da origem nova:**

```powershell
.\.venv\Scripts\python.exe implantacao\implantar_fundacao.py --somente-itens --origens PROTHEUS --executar-setup --executar-ciclo COMPLETO
```

**6. Conferir no SQL endpoint do lakehouse Bronze:**

```sql
SELECT tabela_id, etapa, linhas, status, mensagem
FROM ctl_log WHERE lote = (SELECT TOP 1 lote FROM ctl_log ORDER BY dt_registro DESC);
```

Em cada tabela, o número de linhas da etapa `SILVER` deve ser igual ao da etapa `BRONZE` na primeira carga. Se for menor, a chave não é única e precisa ser revista.

**7. Reimplantar com todas as origens**, sem o `--origens`, para que o pipeline final tenha um `ForEach` para cada origem:

```powershell
.\.venv\Scripts\python.exe implantacao\implantar_fundacao.py --somente-itens
```

---

## 8. Checklist de entrega

- [ ] Gateway e conexão funcionando, com usuário somente leitura
- [ ] ID da conexão e banco no `config_fundacao.json`
- [ ] Origem ativa em `config/tabelas.yml`, com o dialeto certo
- [ ] Planilha de tabelas preenchida: chave, tipo, assunto e forma de carga
- [ ] Colunas sensíveis e binárias excluídas (`colunas` explícito)
- [ ] Nulos na coluna de alteração conferidos (zero)
- [ ] Teste local rodado sem erro
- [ ] Primeiro ciclo completo no DW Dev: Bronze = Silver em todas as tabelas
- [ ] Ciclo de fatos rodado: incrementais trazendo só as alterações
- [ ] Documentação do projeto atualizada ([03_documentacao_projeto.md](03_documentacao_projeto.md))
