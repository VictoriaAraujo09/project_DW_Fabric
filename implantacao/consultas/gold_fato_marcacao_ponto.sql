-- Gold: marcações de ponto da Nexti, uma linha por marcação.
-- Mesma regra do gold_fato_marcacao_ponto do Airflow:
--   - só entradas e saídas (tipos 1, 5 e 6) não removidas;
--   - colaborador e empresa atuais do cadastro (PK_Colaborador com o CNPJ dessa empresa);
--   - CNPJEmpresaOrigem é o da empresa do posto da marcação (só postos de empresas cadastradas);
--   - DataReferencia é a data da própria marcação (horário de Brasília), não o referenceDate da API.
-- Diferença deliberada: marcação removida ou que mudou de tipo sai da Gold (o Airflow a mantinha com os
-- valores antigos), porque o nb_gold apaga e reinsere cada marcação alterada.
--
-- Configuração lida pelo nb_gold:
-- @destino gold_fato_marcacao_ponto
-- @chave IdMarcacaoPonto
-- @incremental fato.id
-- @fontes fato=Silver.silver_fato_marcacoes_ponto_nexti; colab=Silver.silver_dim_colaboradores_nexti; emp=Silver.silver_dim_empresas_nexti; postos=Silver.silver_dim_postos_nexti

WITH col AS (
    SELECT id AS CodColaborador, companyId AS CodEmpresa,
           row_number() OVER (PARTITION BY id ORDER BY _dt_carga DESC) AS _rn
    FROM colab
    |> WHERE _rn = 1
)
SELECT
    upper(md5(coalesce(e.companyNumber, '') || '|NEXTI|' || CAST(col.CodColaborador AS STRING))) AS PK_Colaborador,
    m.id AS IdMarcacaoPonto,
    m.clockingDate AS DataHoraMarcacao,
    CAST(m.clockingDate AS DATE) AS DataReferencia,
    m.personId AS CodColaborador,
    m.externalPersonId AS IdExternoColaborador,
    m.workplaceId AS CodPosto,
    ep.companyNumber AS CNPJEmpresaOrigem,
    m.latitude AS Latitude,
    m.longitude AS Longitude,
    'NEXTI' AS SistemaOrigem,
    m._dt_carga AS DataIngestao,
    m.lastUpdate AS DataModificacao,
    row_number() OVER (PARTITION BY m.id ORDER BY m.lastUpdate DESC NULLS LAST) AS _rn
FROM fato m
JOIN col ON col.CodColaborador = m.personId
JOIN emp e ON e.id = col.CodEmpresa
JOIN postos p ON p.id = m.workplaceId
JOIN emp ep ON ep.id = p.companyId
WHERE m.clockingTypeId IN (1, 5, 6)
  AND NOT coalesce(m.removed, FALSE)
|> WHERE _rn = 1
|> DROP _rn
