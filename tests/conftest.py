"""Base dos testes: executa os notebooks localmente, sem Fabric, sobre lakehouses temporários.

Cada módulo de teste recebe o seu lakehouse (fixture `lake`), então um teste não depende do estado
deixado por outro módulo. Os notebooks rodam pelo mesmo código que vai para o Fabric
(build_notebooks.script_local), com o bloco comum em modo local (DW_LOCAL_ROOT), sobre uma sessão
PySpark local com Delta. Rodar pelo contêiner de tests/docker (Java e Spark nas versões do Fabric).

As tabelas de entrada são gravadas e conferidas com o deltalake, independente do Spark dos notebooks.
"""
import datetime as dt
import json
import os
import pathlib
import sys

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

os.environ["TZ"] = "UTC"
import time  # noqa: E402
time.tzset()

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

# sessão única para todos os testes; os notebooks a reaproveitam (SparkSession.builder.getOrCreate)
SPARK = configure_spark_with_delta_pip(
    SparkSession.builder.master("local[2]").appName("dw-fabric-testes")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .config("spark.databricks.delta.snapshotPartitions", "2")).getOrCreate()
SPARK.sparkContext.setLogLevel("ERROR")

RAIZ_PROJETO = pathlib.Path(__file__).resolve().parents[1]
IMPLANTACAO = RAIZ_PROJETO / "implantacao"
sys.path.insert(0, str(IMPLANTACAO))
import build_notebooks  # noqa: E402

UTC = dt.timezone.utc


def ts(texto):
    """Data/hora ISO como timestamp rotulado UTC (convenção da Bronze)."""
    return dt.datetime.fromisoformat(texto).replace(tzinfo=UTC)


class Lake:
    def __init__(self, raiz, config=RAIZ_PROJETO / "config"):
        self.raiz = raiz
        self.config = config            # pasta dos YAMLs (um teste pode usar uma cópia alterada)

    def rodar(self, notebook, params=None):
        """Executa o notebook e devolve o valor de saída (sair)."""
        os.environ["DW_LOCAL_ROOT"] = str(self.raiz)
        os.environ["DW_CONSULTAS"] = str(IMPLANTACAO / "consultas")
        os.environ["DW_CONFIG"] = str(self.config)
        escopo = {}
        exec(build_notebooks.script_local(notebook, params), escopo)
        return escopo.get("_EXIT_VALUE")

    def rodar_json(self, notebook, params=None):
        return json.loads(self.rodar(notebook, params))

    def gravar(self, camada, tabela, linhas, modo="overwrite"):
        dados = pa.Table.from_pylist(linhas)
        # coluna só com None (tipo null) não existe no Delta: vira texto vazio
        dados = dados.cast(pa.schema([pa.field(f.name, pa.string()) if pa.types.is_null(f.type) else f
                                      for f in dados.schema]))
        kw = {"schema_mode": "overwrite"} if modo == "overwrite" else {"schema_mode": "merge"}
        write_deltalake(f"{self.raiz}/{camada}/{tabela}", dados, mode=modo, **kw)

    def ler(self, camada, tabela):
        return DeltaTable(f"{self.raiz}/{camada}/{tabela}").to_pandas()

    def ler_arrow(self, camada, tabela):
        return DeltaTable(f"{self.raiz}/{camada}/{tabela}").to_pyarrow_table()

    def watermark(self, tabela_id):
        ctl = self.ler_arrow("Bronze", "ctl_tabelas").to_pylist()
        return next(r["ultimo_watermark"] for r in ctl if r["id"] == tabela_id)   # None se vazio

    def log(self, lote):
        return self.ler("Bronze", "ctl_log").query("lote == @lote")


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    """Lakehouse temporário com o cadastro de controle (nb_setup_controle) já executado."""
    lk = Lake(tmp_path_factory.mktemp("dw"))
    lk.rodar("nb_setup_controle")
    return lk
