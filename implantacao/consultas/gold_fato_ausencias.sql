-- Gold: ausências da Nexti, uma linha por ausência e por dia, com posto e cliente do dia.
-- Executada pelo nb_gold_fato_ausencias, que escolhe as ausências (tabela `alteradas`), marca como
-- inativos os dias que sumiram e grava. Fontes registradas pelo notebook: aus (Silver ausências), mov
-- (Gold movimentação), src_colab, src_emp, src_usu (Silver cadastros) e a variável `automaticas`.
-- Regra do gold_fato_ausencias do Airflow: FlagAusenciaAutomatica = usuário "Sistema" + justificativa
-- automática; FlagDiaPrevistoTrabalho = 0 até a rotina do previsto; removida na Nexti = Ativo 0.

WITH col AS (
    SELECT id AS CodColaborador, companyId AS CodEmpresa
    FROM src_colab QUALIFY row_number() OVER (PARTITION BY id ORDER BY _dt_carga DESC) = 1),
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
    SELECT *, unnest(generate_series(ini::TIMESTAMP, fim::TIMESTAMP, INTERVAL 1 DAY))::DATE AS DataCalendario
    FROM base WHERE fim >= ini)
SELECT upper(md5(coalesce(d.CNPJ, '') || '|NEXTI|' || CAST(d.personId AS VARCHAR))) AS PK_Colaborador,
       d.id AS IdAusencia,
       CAST(d.personId AS VARCHAR) AS CodColaborador,
       CAST(d.CodEmpresa AS VARCHAR) AS CodEmpresa,
       m.CodCliente, m.CodPosto,
       CAST(d.absenceSituationId AS VARCHAR) AS CodSituacao,
       d.DataCalendario,
       d.ini::TIMESTAMPTZ AS DataInicio, d.fim::TIMESTAMPTZ AS DataFim,
       d.startMinute AS InicioMinuto, d.finishMinute AS FimMinuto,
       d.note AS Justificativa,
       CAST(d.medicalDoctorId AS VARCHAR) AS CodMedico, d.medicalDoctorName AS Medico,
       d.medicalDoctorCrm AS CRM, CAST(d.cidId AS VARCHAR) AS CidId, d.cidCode AS CodCid,
       d.cidDescription AS CidDescricao, d.userRegisterId AS CodUsuarioRegistro,
       (d.userRegisterId IN (SELECT id FROM sistema)
        AND trim(d.note) IN (SELECT unnest(getvariable('automaticas')))) AS FlagAusenciaAutomatica,
       FALSE AS FlagDiaPrevistoTrabalho,
       d.Nome AS NomeEmpresa, d.CNPJ AS CNPJEmpresaOrigem, 'NEXTI' AS SistemaOrigem,
       now() AS DataIngestao,
       NOT coalesce(d.removed, FALSE) AS Ativo,
       now() AS DataAtualizacao,
       CASE WHEN coalesce(d.removed, FALSE) THEN now() END AS DataInativacao
FROM dias d
LEFT JOIN mov m ON m.CodColaborador = d.personId AND m.DataReferencia = d.DataCalendario
QUALIFY row_number() OVER (PARTITION BY d.id, d.DataCalendario ORDER BY Ativo DESC) = 1
