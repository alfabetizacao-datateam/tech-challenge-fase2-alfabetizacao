"""
Testes — Etapa 6: Integração de Microdados de Alunos

Cobre:
  1. Filtro de presença (apenas alunos que fizeram a prova)
  2. Cálculo ponderado da taxa de alfabetização (peso_aluno)
  3. Cálculo da proficiência média ponderada
  4. Contagem de escolas distintas
  5. JOIN com Silver e geração do delta de validação
  6. Fallback: municípios sem microdados ficam com NULL (não distorce o JOIN)
"""

import sys
import os
import pytest

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

IS_WINDOWS = sys.platform == "win32"

if IS_WINDOWS:
    pytestmark = pytest.mark.skipif(
        IS_WINDOWS,
        reason="PySpark tests not supported on Windows. Run in Docker: "
               "docker build -t tech-challenge . && docker run tech-challenge pytest tests/ -v",
    )

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType, IntegerType, LongType
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


ALUNOS_SCHEMA = StructType([
    StructField("ano", IntegerType()),
    StructField("id_municipio", StringType()),
    StructField("id_escola", StringType()),
    StructField("id_aluno", StringType()),
    StructField("rede", StringType()),
    StructField("presenca", StringType()),
    StructField("alfabetizado", StringType()),
    StructField("proficiencia", DoubleType()),
    StructField("peso_aluno", DoubleType()),
]) if PYSPARK_AVAILABLE else None

SILVER_MINI_SCHEMA = StructType([
    StructField("ano", IntegerType()),
    StructField("id_municipio", StringType()),
    StructField("rede", StringType()),
    StructField("taxa_alfabetizacao", DoubleType()),
    StructField("nome_municipio", StringType()),
]) if PYSPARK_AVAILABLE else None


@pytest.fixture(scope="module")
def spark():
    if IS_WINDOWS or not PYSPARK_AVAILABLE:
        pytest.skip("PySpark não disponível")

    session = SparkSession.builder \
        .appName("TestAlunosIntegration") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.driver.memory", "1g") \
        .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def alunos_data(spark):
    """
    Dados de alunos de dois municípios:
    - 3550308 (Municipal): 3 alunos presentes (2 alfabetizados), 1 ausente
    - 2304400 (Municipal): 2 alunos presentes (ambos alfabetizados)
    - Duas escolas diferentes em 3550308
    """
    data = [
        # (ano, id_mun, id_escola, id_aluno, rede, presenca, alfabetizado, proficiencia, peso)
        (2024, "3550308", "E001", "A001", "Municipal", "Presente", "Sim", 762.0, 1.0),
        (2024, "3550308", "E001", "A002", "Municipal", "Presente", "Não", 680.0, 1.0),
        (2024, "3550308", "E002", "A003", "Municipal", "Presente", "Sim", 800.0, 2.0),  # peso 2
        (2024, "3550308", "E001", "A004", "Municipal", "Ausente",  "Não",   0.0, 1.0),  # ausente — ignorar
        (2024, "2304400", "E010", "A010", "Municipal", "Presente", "Sim", 750.0, 1.0),
        (2024, "2304400", "E010", "A011", "Municipal", "Presente", "Sim", 790.0, 1.0),
    ]
    return spark.createDataFrame(data, schema=ALUNOS_SCHEMA)


@pytest.fixture
def silver_data(spark):
    data = [
        (2024, "3550308", "Municipal", 76.0, "São Paulo"),
        (2024, "2304400", "Municipal", 98.0, "Fortaleza"),
        (2024, "9999999", "Municipal", 90.0, "Município Sem Microdados"),
    ]
    return spark.createDataFrame(data, schema=SILVER_MINI_SCHEMA)


# ---------------------------------------------------------------------------
# Testes de filtro de presença
# ---------------------------------------------------------------------------

def test_filtro_presentes_exclui_ausentes(spark, alunos_data):
    """Alunos ausentes não devem entrar no cálculo."""
    from pyspark.sql.functions import col

    df_presentes = alunos_data.filter(col("presenca") == "Presente")
    total_ausentes = df_presentes.filter(col("presenca") == "Ausente").count()

    assert total_ausentes == 0, "Alunos ausentes não devem estar na base filtrada"


def test_filtro_presentes_conta_correto(spark, alunos_data):
    """Deve haver 5 alunos presentes (4 de 3550308 menos 1 ausente = 3, mais 2 de 2304400 = 5)."""
    from pyspark.sql.functions import col

    n_presentes = alunos_data.filter(col("presenca") == "Presente").count()
    assert n_presentes == 5


# ---------------------------------------------------------------------------
# Testes de agregação por município/rede/ano
# ---------------------------------------------------------------------------

def test_taxa_alfabetizacao_ponderada(spark, alunos_data):
    """
    Município 3550308, Municipal, 2024:
    - A001: Sim, peso=1 → contribui 1.0
    - A002: Não, peso=1 → contribui 0.0
    - A003: Sim, peso=2 → contribui 2.0
    - Total peso: 4, Total alfa: 3 → taxa = 3/4 * 100 = 75.0%
    """
    from pyspark.sql.functions import col, sum as spark_sum, when, round as spark_round

    df_p = alunos_data.filter(col("presenca") == "Presente")
    df_agg = df_p.groupBy("id_municipio", "ano", "rede").agg(
        spark_round(
            spark_sum(when(col("alfabetizado") == "Sim", col("peso_aluno")).otherwise(0.0))
            / spark_sum("peso_aluno") * 100,
            2
        ).alias("taxa_micro")
    )

    row = df_agg.filter(col("id_municipio") == "3550308").collect()
    assert len(row) == 1
    assert abs(row[0]["taxa_micro"] - 75.0) < 0.01, f"Esperado 75.0, obtido {row[0]['taxa_micro']}"


def test_taxa_100_pct_dois_alfabetizados(spark, alunos_data):
    """Município 2304400: ambos os alunos são alfabetizados → taxa = 100%."""
    from pyspark.sql.functions import col, sum as spark_sum, when, round as spark_round

    df_p = alunos_data.filter(col("presenca") == "Presente")
    df_agg = df_p.groupBy("id_municipio", "ano", "rede").agg(
        spark_round(
            spark_sum(when(col("alfabetizado") == "Sim", col("peso_aluno")).otherwise(0.0))
            / spark_sum("peso_aluno") * 100,
            2
        ).alias("taxa_micro")
    )

    row = df_agg.filter(col("id_municipio") == "2304400").collect()
    assert len(row) == 1
    assert abs(row[0]["taxa_micro"] - 100.0) < 0.01


def test_proficiencia_media_ponderada(spark, alunos_data):
    """
    Município 3550308:
    - A001: prof=762, peso=1 → 762*1=762
    - A002: prof=680, peso=1 → 680*1=680
    - A003: prof=800, peso=2 → 800*2=1600
    - Total peso=4, total=3042 → média=3042/4=760.5
    """
    from pyspark.sql.functions import col, sum as spark_sum, round as spark_round

    df_p = alunos_data.filter(col("presenca") == "Presente")
    df_agg = df_p.groupBy("id_municipio").agg(
        spark_round(
            spark_sum(col("proficiencia") * col("peso_aluno")) / spark_sum("peso_aluno"),
            2
        ).alias("prof_media")
    )

    row = df_agg.filter(col("id_municipio") == "3550308").collect()
    assert len(row) == 1
    assert abs(row[0]["prof_media"] - 760.5) < 0.01, f"Esperado 760.5, obtido {row[0]['prof_media']}"


def test_qtd_escolas_distintas(spark, alunos_data):
    """Município 3550308 tem 2 escolas distintas (E001 e E002)."""
    from pyspark.sql.functions import col, countDistinct

    df_p = alunos_data.filter(col("presenca") == "Presente")
    df_agg = df_p.groupBy("id_municipio").agg(
        countDistinct("id_escola").alias("qtd_escolas")
    )

    row = df_agg.filter(col("id_municipio") == "3550308").collect()
    assert len(row) == 1
    assert row[0]["qtd_escolas"] == 2


# ---------------------------------------------------------------------------
# Testes de JOIN com Silver
# ---------------------------------------------------------------------------

def test_join_left_preserva_municipio_sem_microdados(spark, alunos_data, silver_data):
    """
    Município 9999999 existe no Silver mas NÃO tem microdados.
    O LEFT JOIN deve preservar esse município com NULL nas colunas de microdados.
    """
    from pyspark.sql.functions import col, sum as spark_sum, when, round as spark_round, countDistinct

    df_p = alunos_data.filter(col("presenca") == "Presente")
    df_agg = df_p.groupBy("id_municipio", "ano", "rede").agg(
        spark_round(
            spark_sum(when(col("alfabetizado") == "Sim", col("peso_aluno")).otherwise(0.0))
            / spark_sum("peso_aluno") * 100, 2
        ).alias("taxa_alunos_alfabetizados_microdados"),
        countDistinct("id_escola").alias("qtd_escolas_avaliadas"),
    )

    df_joined = silver_data.join(df_agg, on=["id_municipio", "ano", "rede"], how="left")

    sem_micro = df_joined.filter(col("id_municipio") == "9999999").collect()
    assert len(sem_micro) == 1
    assert sem_micro[0]["taxa_alunos_alfabetizados_microdados"] is None


def test_delta_calculo_correto(spark, alunos_data, silver_data):
    """
    Município 3550308: taxa_micro=75.0, taxa_inep=76.0 → delta=-1.0
    O delta representa a diferença entre o calculado e o publicado.
    """
    from pyspark.sql.functions import col, sum as spark_sum, when, round as spark_round, countDistinct

    df_p = alunos_data.filter(col("presenca") == "Presente")
    df_agg = df_p.groupBy("id_municipio", "ano", "rede").agg(
        spark_round(
            spark_sum(when(col("alfabetizado") == "Sim", col("peso_aluno")).otherwise(0.0))
            / spark_sum("peso_aluno") * 100, 2
        ).alias("taxa_alunos_alfabetizados_microdados"),
    )

    df_joined = silver_data.join(df_agg, on=["id_municipio", "ano", "rede"], how="left") \
        .withColumn(
            "delta_taxa_micro_vs_inep",
            spark_round(col("taxa_alunos_alfabetizados_microdados") - col("taxa_alfabetizacao"), 2)
        )

    row = df_joined.filter(col("id_municipio") == "3550308").collect()
    assert len(row) == 1
    # taxa_micro=75.0, taxa_inep=76.0 → delta=-1.0
    assert abs(row[0]["delta_taxa_micro_vs_inep"] - (-1.0)) < 0.01


def test_silver_preserva_todas_colunas_originais(spark, alunos_data, silver_data):
    """Após o JOIN, o Silver deve manter todas as suas colunas originais."""
    from pyspark.sql.functions import col, sum as spark_sum, when, round as spark_round

    df_p = alunos_data.filter(col("presenca") == "Presente")
    df_agg = df_p.groupBy("id_municipio", "ano", "rede").agg(
        spark_round(
            spark_sum(when(col("alfabetizado") == "Sim", col("peso_aluno")).otherwise(0.0))
            / spark_sum("peso_aluno") * 100, 2
        ).alias("taxa_alunos_alfabetizados_microdados"),
    )

    df_joined = silver_data.join(df_agg, on=["id_municipio", "ano", "rede"], how="left")

    colunas_silver_originais = silver_data.columns
    for c in colunas_silver_originais:
        assert c in df_joined.columns, f"Coluna original '{c}' ausente após o JOIN"


def test_contagem_registros_silver_preservada(spark, alunos_data, silver_data):
    """O LEFT JOIN não deve criar ou perder linhas do Silver."""
    from pyspark.sql.functions import col, sum as spark_sum, when, round as spark_round

    df_p = alunos_data.filter(col("presenca") == "Presente")
    df_agg = df_p.groupBy("id_municipio", "ano", "rede").agg(
        spark_round(
            spark_sum(when(col("alfabetizado") == "Sim", col("peso_aluno")).otherwise(0.0))
            / spark_sum("peso_aluno") * 100, 2
        ).alias("taxa_alunos_alfabetizados_microdados"),
    )

    n_silver_original = silver_data.count()
    df_joined = silver_data.join(df_agg, on=["id_municipio", "ano", "rede"], how="left")
    n_joined = df_joined.count()

    assert n_joined == n_silver_original, (
        f"JOIN modificou o número de registros: {n_silver_original} → {n_joined}"
    )
