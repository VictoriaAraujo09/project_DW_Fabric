/* Descoberta das 29 tabelas do catálogo no Firebird (4S e VIGON).
   Rodar em cada base (IBExpert, FlameRobin, DBeaver ou isql) e me enviar o resultado.
   O Firebird não informa linhas e tamanho; o volume sai da 1ª carga na Bronze (ver Resumo do projeto). */

/* 1) Versão do Firebird: a partir da 3.0 dá para usar RDB$RECORD_VERSION como watermark */
SELECT RDB$GET_CONTEXT('SYSTEM', 'ENGINE_VERSION') AS versao FROM RDB$DATABASE;

/* 2) Chave primária, colunas de data e datas indexadas por tabela */
SELECT
    TRIM(r.RDB$RELATION_NAME) AS tabela,
    (SELECT LIST(TRIM(s.RDB$FIELD_NAME), ', ')
       FROM RDB$RELATION_CONSTRAINTS rc
       JOIN RDB$INDEX_SEGMENTS s ON s.RDB$INDEX_NAME = rc.RDB$INDEX_NAME
      WHERE rc.RDB$RELATION_NAME = r.RDB$RELATION_NAME
        AND rc.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY') AS chave_pk,
    (SELECT LIST(TRIM(rf.RDB$FIELD_NAME), ', ')
       FROM RDB$RELATION_FIELDS rf
       JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = rf.RDB$FIELD_SOURCE
      WHERE rf.RDB$RELATION_NAME = r.RDB$RELATION_NAME
        AND f.RDB$FIELD_TYPE IN (12, 35)) AS colunas_data,          /* 12 = DATE, 35 = TIMESTAMP */
    (SELECT LIST(TRIM(s.RDB$FIELD_NAME), ', ')
       FROM RDB$INDICES i
       JOIN RDB$INDEX_SEGMENTS s ON s.RDB$INDEX_NAME = i.RDB$INDEX_NAME
       JOIN RDB$RELATION_FIELDS rf ON rf.RDB$RELATION_NAME = i.RDB$RELATION_NAME
                                  AND rf.RDB$FIELD_NAME = s.RDB$FIELD_NAME
       JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = rf.RDB$FIELD_SOURCE
      WHERE i.RDB$RELATION_NAME = r.RDB$RELATION_NAME
        AND s.RDB$FIELD_POSITION = 0
        AND f.RDB$FIELD_TYPE IN (12, 35)) AS datas_indexadas,
    (SELECT COUNT(*) FROM RDB$RELATION_FIELDS rf
      WHERE rf.RDB$RELATION_NAME = r.RDB$RELATION_NAME) AS qtd_colunas
FROM RDB$RELATIONS r
WHERE COALESCE(r.RDB$SYSTEM_FLAG, 0) = 0
  AND r.RDB$VIEW_BLR IS NULL
  AND TRIM(r.RDB$RELATION_NAME) IN (
      'LAN','CPG','NFS','ORL','CRE','VDR','VCP','BVP','SCR','BVD',
      'RDL','CLI','TCF','FRN','DCP','RRR','LAF','CRS','CRD','EST',
      'CON','SER','VSE','VRD','BAN','GFR','APF','GCL','EMP')
ORDER BY 1;
