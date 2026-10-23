"""Gera a definição (pipeline-content.json) do pipeline mestre pl_mestre_dw.

Um ForEach por origem, cada um com a conexão do gateway fixa. Motivo: parametrizar a conexão
on-premises por expressão ainda não é suportado de forma documentada no Fabric. Incluir uma
origem nova = acrescentar uma entrada em ORIGENS no config e reimplantar.
"""
import json

def _expr(v):
    return {"value": v, "type": "Expression"}

def _politica(timeout="0.02:00:00", retry=0):
    return {"timeout": timeout, "retry": retry, "retryIntervalInSeconds": 60,
            "secureOutput": False, "secureInput": False}

def _notebook(nome, nb_id, ws_id, params, depende=None):
    return {
        "name": nome, "type": "TridentNotebook", "dependsOn": depende or [],
        "policy": _politica(),
        "typeProperties": {
            "notebookId": nb_id, "workspaceId": ws_id,
            "parameters": {k: {"value": _expr(v) if v.startswith("@") else v, "type": "string"}
                           for k, v in params.items()},
        },
    }

def _dataset_origem(o):
    if o["conector"] == "SQLSERVER":
        ds = {"annotations": [], "type": "SqlServerTable", "schema": [], "typeProperties": {},
              "externalReferences": {"connection": o["connection_id"]}}
        if o.get("database"):
            ds["typeProperties"]["database"] = o["database"]
        return ds
    return {"annotations": [], "type": "OdbcTable", "schema": [], "typeProperties": {},
            "externalReferences": {"connection": o["connection_id"]}}

def _fonte(o, consulta):
    if o["conector"] == "SQLSERVER":
        return {"type": "SqlServerSource", "sqlReaderQuery": _expr(consulta),
                "queryTimeout": "02:00:00", "partitionOption": "None"}
    return {"type": "OdbcSource", "query": _expr(consulta), "queryTimeout": "02:00:00"}

def _foreach(o, ws_id, lh_bronze_id, paralelismo):
    n = o["origem"]
    lookup, cond, copia = f"Lookup_Max_{n}", f"Se_Mudou_{n}", f"Copiar_{n}"
    novo = f"coalesce(activity('{lookup}').output.firstRow.novo, '')"

    atv_lookup = {
        "name": lookup, "type": "Lookup", "dependsOn": [], "policy": _politica("0.00:30:00", 1),
        "typeProperties": {"source": _fonte(o, "@item().query_max"),
                           "datasetSettings": _dataset_origem(o), "firstRowOnly": True},
    }
    atv_copia = {
        "name": copia, "type": "Copy", "dependsOn": [], "policy": _politica("0.02:00:00", 1),
        "typeProperties": {
            "source": {**_fonte(o, f"@replace(item().query_copy, '#NOVO#', {novo})"),
                       "datasetSettings": _dataset_origem(o)},
            "sink": {
                "type": "LakehouseTableSink", "tableActionOption": "Append",
                "datasetSettings": {
                    "annotations": [],
                    "linkedService": {"name": "Bronze", "properties": {
                        "annotations": [], "type": "Lakehouse",
                        "typeProperties": {"workspaceId": ws_id, "artifactId": lh_bronze_id,
                                           "rootFolder": "Tables"}}},
                    "type": "LakehouseTable", "schema": [],
                    "typeProperties": {"table": _expr("@item().tabela_destino")},
                },
            },
            "enableStaging": False,
            "translator": {"type": "TabularTranslator", "typeConversion": True,
                           "typeConversionSettings": {"allowDataTruncation": True,
                                                      "treatBooleanAsNumber": False}},
        },
    }
    atv_registro = {
        "name": f"Registrar_{n}", "type": "AppendVariable",
        "dependsOn": [{"activity": copia, "dependencyConditions": ["Succeeded"]}],
        "typeProperties": {"variableName": "resultados", "value": _expr(
            f"@concat(item().id, '|', {novo}, '|', string(activity('{copia}').output.rowsCopied))")},
    }
    atv_if = {
        "name": cond, "type": "IfCondition",
        "dependsOn": [{"activity": lookup, "dependencyConditions": ["Succeeded"]}],
        "typeProperties": {
            # copia se: não é incremental, ou o watermark da origem mudou
            "expression": _expr(
                f"@or(not(equals(item().modo_carga, 'INCREMENTAL')), "
                f"and(not(equals({novo}, '')), not(equals({novo}, item().wm_atual))))"),
            "ifTrueActivities": [atv_copia, atv_registro],
            "ifFalseActivities": [],
        },
    }
    return {
        "name": f"ForEach_{n}", "type": "ForEach",
        "dependsOn": [{"activity": "Preparar_Ciclo", "dependencyConditions": ["Succeeded"]}],
        "typeProperties": {
            "items": _expr(f"@json(activity('Preparar_Ciclo').output.result.exitValue)['origens']['{n}']"),
            "isSequential": False, "batchCount": paralelismo,
            "activities": [atv_lookup, atv_if],
        },
    }

def gerar(ws_id, lh_bronze_id, nb_preparar_id, nb_orquestrador_id, origens, paralelismo=4, nb_api_id=None):
    ativos = [o for o in origens if o.get("connection_id") and o["conector"] != "API"]
    apis = [o for o in origens if o["conector"] == "API" and o.get("key_vault")]
    if not ativos and not apis:
        raise ValueError("Informe connection_id (banco) ou key_vault (API) de ao menos uma origem")
    if apis and not nb_api_id:
        raise ValueError("Origem API sem o notebook nb_ingestao_api")
    saida_prep = "json(activity('Preparar_Ciclo').output.result.exitValue)"
    atividades = [_notebook("Preparar_Ciclo", nb_preparar_id, ws_id,
                            {"ciclo": "@pipeline().parameters.ciclo", "lote": "@pipeline().RunId",
                             "origens": "@pipeline().parameters.origens"})]
    atividades += [_foreach(o, ws_id, lh_bronze_id, paralelismo) for o in ativos]
    depende = [{"activity": f"ForEach_{o['origem']}", "dependencyConditions": ["Succeeded"]} for o in ativos]
    params_orq = {"lote": "@pipeline().RunId", "ciclo": f"@{saida_prep}['ciclo']",
                  "resultados": "@string(variables('resultados'))", "status_ciclo": "SUCESSO"}
    if apis:
        # origens API: um notebook lê as APIs e devolve a lista id|watermark|linhas ao orquestrador
        config_api = json.dumps({o["origem"]: {"key_vault": o["key_vault"]} for o in apis})
        atividades.append(_notebook(
            "Ingestao_API", nb_api_id, ws_id,
            {"lote": "@pipeline().RunId", "ciclo": f"@{saida_prep}['ciclo']",
             "origens": "@pipeline().parameters.origens", "configuracao_api": config_api},
            [{"activity": "Preparar_Ciclo", "dependencyConditions": ["Succeeded"]}]))
        params_orq["resultados_api"] = "@activity('Ingestao_API').output.result.exitValue"
        depende.append({"activity": "Ingestao_API", "dependencyConditions": ["Succeeded"]})
    atividades.append(_notebook("Orquestrador", nb_orquestrador_id, ws_id, params_orq, depende))
    # Orquestrador "Skipped" = alguma etapa anterior falhou: registra no log e marca o pipeline como falho
    atividades.append(_notebook(
        "Registrar_Falha", nb_orquestrador_id, ws_id,
        {"lote": "@pipeline().RunId", "ciclo": "@pipeline().parameters.ciclo", "resultados": "[]",
         "status_ciclo": "FALHA",
         "mensagem": "@concat('Falha antes do orquestrador. Ver execução ', pipeline().RunId)"},
        [{"activity": "Orquestrador", "dependencyConditions": ["Skipped"]}]))
    atividades.append({
        "name": "Falhar_Pipeline", "type": "Fail",
        "dependsOn": [{"activity": "Registrar_Falha", "dependencyConditions": ["Completed"]}],
        "typeProperties": {"message": "Ciclo do DW falhou. Detalhes em Bronze.ctl_log.",
                           "errorCode": "DW_CICLO_FALHA"},
    })
    return {"properties": {
        "description": "Pipeline mestre do DW: controle -> Bronze (incremental) -> Silver/Gold -> log",
        "activities": atividades,
        "parameters": {"ciclo": {"type": "string", "defaultValue": "AUTO"},
                       "origens": {"type": "string", "defaultValue": ""}},
        "variables": {"resultados": {"type": "Array", "defaultValue": []}},
    }}

if __name__ == "__main__":
    exemplo = gerar("<workspace-id>", "<lh_bronze-id>", "<nb_preparar-id>", "<nb_orquestrador-id>",
                    [{"origem": "GS", "conector": "SQLSERVER", "connection_id": "<conexao-gs-id>"}])
    print(json.dumps(exemplo, indent=2, ensure_ascii=False))
