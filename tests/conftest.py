"""Base dos testes: executa os notebooks localmente, sem Fabric, sobre lakehouses temporários.

Cada módulo de teste recebe o seu lakehouse (fixture `lake`), então um teste não depende do estado
deixado por outro módulo. Os notebooks rodam pelo mesmo código que vai para o Fabric
(build_notebooks.script_local), com o bloco comum em modo local (DW_LOCAL_ROOT).
"""
import datetime as dt
import json
import os
import pathlib
import sys

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

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
        kw = {"schema_mode": "overwrite"} if modo == "overwrite" else {}
        write_deltalake(f"{self.raiz}/{camada}/{tabela}", pa.Table.from_pylist(linhas), mode=modo, **kw)

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
