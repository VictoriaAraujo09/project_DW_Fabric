-- Gold: ausências da Nexti, uma linha por ausência e por dia, com posto e cliente do dia.
-- Executada pelo nb_gold_fato_ausencias, que escolhe as ausências (tabela `alteradas`), marca como
-- inativos os dias que sumiram e grava. Fontes registradas pelo notebook: aus (Silver ausências), mov
-- (Gold movimentação), src_colab, src_emp, src_usu (Silver cadastros) e `automaticas` (justificativas).
-- Regra do gold_fato_ausencias do Airflow: FlagAusenciaAutomatica = usuário "Sistema" + justificativa
-- automática; FlagDiaPrevistoTrabalho = 0 até a rotina do previsto; removida na Nexti = Ativo 0.

WITH col AS (
    SELECT id AS CodColaborador, companyId AS CodEmpresa,
           row_number() OVER (PARTITION BY id ORDER BY _dt_carga DESC) AS _rn
    FROM src_colab
    |> WHERE _rn = 1),
emp AS (SELECT id AS CodEmpresa, companyNumber AS CNPJ, companyName AS Nome
        FROM src_emp WHERE coalesce(active, TRUE)),
sistema AS (SELECT DISTINCT id FROM src_usu WHERE trim(name) = 'Sistema'),
base AS (
    SELECT a.*, col.CodEmpresa, emp.CNPJ, emp.Nome,
           CAST(a.startDateTime AS DATE) AS ini,
           CAST(coalesce(a.finishDateTime, a.startDateTime) AS DATE) AS fim
    FROM aus a
    JOIN alteradas x ON x.id = a.id
    JOIN col ON col.CodColaborador = a.personId
    JOIN emp ON emp.CodEmpresa = col.CodEmpresa
    WHERE a.id IS NOT NULL AND a.startDateTime IS NOT NULL),
dias AS (
    SELECT *, explode(sequence(ini, fim, INTERVAL 1 DAY)) AS DataCalendario
    FROM base WHERE fim >= ini)
SELECT upper(md5(coalesce(d.CNPJ, '') || '|NEXTI|' || CAST(d.personId AS STRING))) AS PK_Colaborador,
       d.id AS IdAusencia,
       CAST(d.personId AS STRING) AS CodColaborador,
       CAST(d.CodEmpresa AS STRING) AS CodEmpresa,
       m.CodCliente, m.CodPosto,
       CAST(d.absenceSituationId AS STRING) AS CodSituacao,
       d.DataCalendario,
       CAST(d.ini AS TIMESTAMP) AS DataInicio, CAST(d.fim AS TIMESTAMP) AS DataFim,
       d.startMinute AS InicioMinuto, d.finishMinute AS FimMinuto,
       d.note AS Justificativa,
       CAST(d.medicalDoctorId AS STRING) AS CodMedico, d.medicalDoctorName AS Medico,
       d.medicalDoctorCrm AS CRM, CAST(d.cidId AS STRING) AS CidId, d.cidCode AS CodCid,
       d.cidDescription AS CidDescricao, d.userRegisterId AS CodUsuarioRegistro,
       (s.id IS NOT NULL AND j.justificativa IS NOT NULL) AS FlagAusenciaAutomatica,
       FALSE AS FlagDiaPrevistoTrabalho,
       d.Nome AS NomeEmpresa, d.CNPJ AS CNPJEmpresaOrigem, 'NEXTI' AS SistemaOrigem,
       current_timestamp() AS DataIngestao,
       NOT coalesce(d.removed, FALSE) AS Ativo,
       current_timestamp() AS DataAtualizacao,
       CASE WHEN coalesce(d.removed, FALSE) THEN current_timestamp() END AS DataInativacao,
       row_number() OVER (PARTITION BY d.id, d.DataCalendario ORDER BY NOT coalesce(d.removed, FALSE) DESC) AS _rn
FROM dias d
LEFT JOIN mov m ON m.CodColaborador = d.personId AND m.DataReferencia = d.DataCalendario
LEFT JOIN sistema s ON s.id = d.userRegisterId
LEFT JOIN (SELECT DISTINCT justificativa FROM automaticas) j ON j.justificativa = trim(d.note)
|> WHERE _rn = 1
|> DROP _rn
