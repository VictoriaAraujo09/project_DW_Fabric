-- Gold: coberturas da Nexti, uma linha por cobertura e por dia.
-- Mesma regra do gold_fato_coberturas do Airflow:
--   - fica fora a cobertura em que o colaborador cobre a si mesmo (personId = absenteeId);
--   - só postos de empresas cadastradas (CNPJ da empresa do posto);
--   - o período vai de startDateTime até finishDateTime (ou só o dia de início, se vier vazio);
--   - na alteração, a cobertura inteira é apagada e reinserida: dias que saíram do período não ficam órfãos.
--
-- Configuração lida pelo nb_gold:
-- @destino gold_fato_coberturas
-- @chave IdCobertura
-- @incremental fato.id
-- @fontes fato=Silver.silver_fato_coberturas_nexti; postos=Silver.silver_dim_postos_nexti; emp=Silver.silver_dim_empresas_nexti

WITH coberturas AS (
    SELECT f.*, e.companyNumber AS CNPJ,
           CAST(f.startDateTime AS DATE) AS ini,
           CAST(coalesce(f.finishDateTime, f.startDateTime) AS DATE) AS fim
    FROM fato f
    JOIN postos p ON p.id = f.workplaceId
    JOIN emp e ON e.id = p.companyId
    WHERE f.id IS NOT NULL
      AND f.startDateTime IS NOT NULL
      AND (nullif(f.personId, 0) IS NULL OR nullif(f.absenteeId, 0) IS NULL OR f.personId <> f.absenteeId)
),
dias AS (
    SELECT *, unnest(generate_series(ini::TIMESTAMP, fim::TIMESTAMP, INTERVAL 1 DAY))::DATE AS DataCalendario
    FROM coberturas
    WHERE fim >= ini
)
SELECT
    id AS IdCobertura,
    DataCalendario,
    upper(md5(coalesce(CNPJ, '') || '|NEXTI|' || coalesce(CAST(nullif(personId, 0) AS VARCHAR), ''))) AS PK_Colaborador,
    nullif(personId, 0) AS CodColaborador,
    personName AS NomeColaborador,
    upper(md5(coalesce(CNPJ, '') || '|NEXTI|' || coalesce(CAST(nullif(absenteeId, 0) AS VARCHAR), '')))
        AS PK_ColaboradorAusente,
    nullif(absenteeId, 0) AS CodColaboradorAusente,
    absenteeName AS NomeColaboradorAusente,
    nullif(workplaceId, 0) AS CodPosto,
    ini::TIMESTAMPTZ AS DataInicio,
    fim::TIMESTAMPTZ AS DataFim,
    nullif(replacementTypeId, 0) AS CodTipoSubstituicao,
    note AS Justificativa,
    registerDate AS DataAtualizacaoCobertura,
    nullif(replacementReasonId, 0) AS CodMotivoSubstituicao,
    replacementReasonName AS MotivoSubstituicao,
    nullif(replacementResourceId, 0) AS CodRecursoSubstituicao,
    replacementResourceName AS RecursoSubstituicao,
    nullif(shiftId, 0) AS CodTurno,
    CAST(lastUpdate AS DATE)::TIMESTAMPTZ AS DataUltimaAtualizacaoCobertura,
    removed AS Removido,
    'NEXTI' AS SistemaOrigem,
    now() AS DataIngestao,
    now() AS DataModificacao,
    CNPJ AS CNPJEmpresaOrigem
FROM dias
QUALIFY row_number() OVER (PARTITION BY id, DataCalendario ORDER BY lastUpdate DESC NULLS LAST) = 1
