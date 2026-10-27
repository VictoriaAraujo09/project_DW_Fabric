# Ambiente dos testes locais, com as versões do Fabric Runtime 2.0 (Spark 4.1, Delta 4.2, Java 21, Python 3.13).
# Os notebooks rodam com PySpark local sobre lakehouses temporários, sem Fabric.
#
#   docker build -t dw-fabric-testes tests/docker
#   docker run --rm -v "${PWD}:/projeto" dw-fabric-testes            # todos os testes
#   docker run --rm -v "${PWD}:/projeto" dw-fabric-testes -k silver  # filtro do pytest
#
# Rede com inspeção TLS: coloque os certificados raiz (.crt, PEM) em tests/docker/certificados antes do
# build. A pasta fica fora do Git.
FROM python:3.13-slim-trixie

COPY certificados/ /usr/local/share/ca-certificates/extra/
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-21-jre-headless ca-certificates ca-certificates-java \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt \
    PIP_CERT=/etc/ssl/certs/ca-certificates.crt \
    PYTHONUTF8=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN pip install --no-cache-dir "pyspark==4.1.1" "delta-spark==4.2.0" pyarrow pandas pyyaml requests pytest

# baixa os jars do Delta uma vez, na imagem (a sessão dos testes não precisa de rede)
RUN python -c "from delta import configure_spark_with_delta_pip as c; from pyspark.sql import SparkSession as S; \
s = c(S.builder.master('local[1]')).getOrCreate(); print(s.version); s.stop()"

# deltalake só nos testes: monta as tabelas de entrada e confere as gravadas pelo Spark
RUN pip install --no-cache-dir deltalake

WORKDIR /projeto
ENTRYPOINT ["python", "-m", "pytest"]
