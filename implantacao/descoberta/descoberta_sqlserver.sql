/* Descoberta das 29 tabelas do catálogo no SQL Server (GS).
   Rodar no SSMS, no banco do GS, com usuário de leitura. Devolve uma linha por tabela:
   - linhas e tamanho_mb ......... volume (para a estimativa de consumo da F4)
   - chave_pk .................... colunas da chave primária -> vira "chave" no DETALHES
   - colunas_data ................ candidatas a watermark (data/hora de alteração)
   - datas_indexadas ............. colunas de data que são 1ª coluna de algum índice (melhor watermark)
   - identity_rowversion ......... IDs crescentes ou rowversion (alternativa de watermark)
   Exporte o resultado (botão direito > Salvar resultados como... CSV) e me envie. */

WITH alvo AS (
    SELECT v FROM (VALUES
        ('LAN'),('CPG'),('NFS'),('ORL'),('CRE'),('VDR'),('VCP'),('BVP'),('SCR'),('BVD'),
        ('RDL'),('CLI'),('TCF'),('FRN'),('DCP'),('RRR'),('LAF'),('CRS'),('CRD'),('EST'),
        ('CON'),('SER'),('VSE'),('VRD'),('BAN'),('GFR'),('APF'),('GCL'),('EMP')
    ) t(v)
)
SELECT
    s.name + '.' + t.name AS tabela,
    (SELECT SUM(p.rows) FROM sys.partitions p
      WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)) AS linhas,
    (SELECT CAST(SUM(a.total_pages) * 8 / 1024.0 AS decimal(12, 1))
       FROM sys.partitions p JOIN sys.allocation_units a ON a.container_id = p.partition_id
      WHERE p.object_id = t.object_id) AS tamanho_mb,
    STUFF((SELECT ', ' + c.name
             FROM sys.indexes i
             JOIN sys.index_columns ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
             JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE i.object_id = t.object_id AND i.is_primary_key = 1
            ORDER BY ic.key_ordinal FOR XML PATH('')), 1, 2, '') AS chave_pk,
    STUFF((SELECT ', ' + c.name
             FROM sys.columns c
            WHERE c.object_id = t.object_id
              AND TYPE_NAME(c.system_type_id) IN ('datetime', 'datetime2', 'smalldatetime', 'date', 'datetimeoffset')
            ORDER BY c.column_id FOR XML PATH('')), 1, 2, '') AS colunas_data,
    STUFF((SELECT DISTINCT ', ' + c.name
             FROM sys.index_columns ic
             JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE ic.object_id = t.object_id AND ic.key_ordinal = 1
              AND TYPE_NAME(c.system_type_id) IN ('datetime', 'datetime2', 'smalldatetime', 'date', 'datetimeoffset')
              FOR XML PATH('')), 1, 2, '') AS datas_indexadas,
    STUFF((SELECT ', ' + c.name
             FROM sys.columns c
            WHERE c.object_id = t.object_id
              AND (c.is_identity = 1 OR TYPE_NAME(c.system_type_id) = 'timestamp')
              FOR XML PATH('')), 1, 2, '') AS identity_rowversion,
    (SELECT COUNT(*) FROM sys.columns c WHERE c.object_id = t.object_id) AS qtd_colunas
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN alvo ON alvo.v = t.name
ORDER BY tamanho_mb DESC;
