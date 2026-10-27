"""Converte src/*.py (células '# %%') em notebooks .ipynb no formato de notebook PySpark do Fabric."""
import json, re, pathlib

RAIZ = pathlib.Path(__file__).parent
META_NB = {
    "kernel_info": {"name": "synapse_pyspark"},
    "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"},
    "language_info": {"name": "python"},
    "microsoft": {"language": "python", "language_group": "synapse_pyspark"},
}
META_CELULA = {"microsoft": {"language": "python", "language_group": "synapse_pyspark"}}

def celulas(texto):
    blocos = re.split(r"^# %%(.*)$", texto, flags=re.M)
    out = []
    for cab, corpo in zip(blocos[1::2], blocos[2::2]):
        corpo = corpo.strip("\n")
        if "[markdown]" in cab:
            out.append(("md", "\n".join(l[2:] if l.startswith("# ") else l.lstrip("#") for l in corpo.splitlines()), []))
        else:
            tags = re.findall(r'"(\w+)"', cab) if "tags" in cab else []
            out.append(("code", corpo, tags))
    return out

def codigo_comum():
    return "\n\n".join(c for t, c, _ in celulas((RAIZ / "src/comum.py").read_text(encoding="utf-8")) if t == "code")

def montar(nome):
    cells = []
    for tipo, corpo, tags in celulas((RAIZ / f"src/{nome}.py").read_text(encoding="utf-8")):
        if corpo.strip() == "#@@COMUM@@":
            corpo = codigo_comum()
        fonte = [l + "\n" for l in corpo.splitlines()]
        if fonte:
            fonte[-1] = fonte[-1].rstrip("\n")
        if tipo == "md":
            cells.append({"cell_type": "markdown", "metadata": {}, "source": fonte})
        else:
            meta = dict(META_CELULA)
            if tags:
                meta["tags"] = tags
            cells.append({"cell_type": "code", "metadata": meta, "source": fonte,
                          "outputs": [], "execution_count": None})
    for i, c in enumerate(cells):
        c["id"] = f"{nome[:20]}-{i:03d}".replace("_", "-")
    nb = {"nbformat": 4, "nbformat_minor": 5, "metadata": META_NB, "cells": cells}
    destino = RAIZ / "notebooks" / f"{nome}.ipynb"
    destino.write_text(json.dumps(nb, ensure_ascii=False, indent=1), encoding="utf-8")
    return destino

def script_local(nome, overrides=None):
    """Código executável (para testes locais), com parâmetros sobrescritos."""
    partes = []
    for tipo, corpo, tags in celulas((RAIZ / f"src/{nome}.py").read_text(encoding="utf-8")):
        if tipo != "code":
            continue
        partes.append(codigo_comum() if corpo.strip() == "#@@COMUM@@" else corpo)
        if "parameters" in tags and overrides:
            partes.append("\n".join(f"{k} = {v!r}" for k, v in overrides.items()))
    return "\n\n".join(partes)

if __name__ == "__main__":
    for n in ("nb_setup_controle", "nb_preparar_ciclo", "nb_orquestrador", "nb_ingestao_api",
              "nb_rotina_nexti", "nb_gold_fato_movimentacao_colaborador", "nb_gold_fato_ausencias", "nb_gold",
              "nb_qualidade", "nb_monitoramento"):
        print("gerado:", montar(n))
