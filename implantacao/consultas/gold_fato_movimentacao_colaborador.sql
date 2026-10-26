-- Gold: posto e cliente de cada colaborador em cada dia (Nexti).
-- Executada pelo nb_gold_fato_movimentacao_colaborador, que escolhe os colaboradores (tabela `alterados`)
-- e grava o resultado. Fontes registradas pelo notebook: transf (Silver transferências de posto),
-- src_colab, src_emp, src_postos (Silver cadastros) e a variável `hoje` (data de Brasília).
-- Regra do gold_fato_movimentacao_colaborador do Airflow: a transferência vale do dia dela até a véspera
-- da próxima, limitada a hoje e à demissão; no mesmo dia vale a alterada por último.

WITH col AS (
    SELECT id AS CodColaborador, companyId AS CodEmpresa, CAST(demissionDate AS DATE) AS DataDemissao
    FROM src_colab QUALIFY row_number() OVER (PARTITION BY id ORDER BY _dt_carga DESC) = 1),
emp AS (SELECT id AS CodEmpresa, companyNumber AS CNPJ FROM src_emp WHERE coalesce(active, TRUE)),
pos AS (SELECT id AS CodPosto, clientId AS CodCliente FROM src_postos),
eventos AS (
    SELECT t.personId AS CodColaborador, t.workplaceId AS CodPosto, pos.CodCliente,
           CAST(t.transferDateTime AS DATE) AS DataTransferencia, t.id AS IdMovimentacao,
           emp.CNPJ, col.DataDemissao,
           row_number() OVER (PARTITION BY t.personId, CAST(t.transferDateTime AS DATE)
                              ORDER BY coalesce(t.lastUpdate, t._dt_carga) DESC, t.id DESC) AS rn_dia
    FROM transf t
    JOIN alterados a ON a.CodColaborador = t.personId
    JOIN pos ON pos.CodPosto = t.workplaceId
    JOIN col ON col.CodColaborador = t.personId
    JOIN emp ON emp.CodEmpresa = col.CodEmpresa
    WHERE NOT coalesce(t.removed, FALSE) AND t.transferDateTime IS NOT NULL),
vigencia AS (
    SELECT *, lead(DataTransferencia) OVER (PARTITION BY CodColaborador
                                            ORDER BY DataTransferencia, IdMovimentacao) - 1 AS FimProxima
    FROM eventos WHERE rn_dia = 1),
ajustada AS (
    SELECT *, CASE WHEN DataDemissao IS NOT NULL AND DataDemissao < FimNatural THEN DataDemissao
                   ELSE FimNatural END AS FimVigencia
    FROM (SELECT *, least(coalesce(FimProxima, getvariable('hoje')), getvariable('hoje')) AS FimNatural
          FROM vigencia)
    WHERE FimNatural >= DataTransferencia),
dias AS (
    SELECT *, unnest(generate_series(DataTransferencia::TIMESTAMP, FimVigencia::TIMESTAMP,
                                     INTERVAL 1 DAY))::DATE AS DataReferencia
    FROM ajustada WHERE FimVigencia >= DataTransferencia)
SELECT upper(md5(coalesce(CNPJ, '') || '|NEXTI|' || CAST(CodColaborador AS VARCHAR))) AS PK_Colaborador,
       upper(md5(coalesce(CNPJ, '') || '|NEXTI|' || coalesce(CAST(CodCliente AS VARCHAR), '')))
           AS PK_Condominio,
       CodColaborador, CodPosto, CodCliente, DataTransferencia, DataReferencia, IdMovimentacao,
       'NEXTI' AS SistemaOrigem, now() AS DataAtualizacao
FROM dias
QUALIFY row_number() OVER (PARTITION BY CodColaborador, DataReferencia
                           ORDER BY DataTransferencia DESC, IdMovimentacao DESC) = 1
