import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

hadoop_home = os.path.join(project_root, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + os.path.join(hadoop_home, "bin")

python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

# ⚠️ PySpark on Windows has known JVM↔Python serialization issues
# https://issues.apache.org/jira/browse/SPARK-15328
# Tests MUST run in Docker or cloud environments
IS_WINDOWS = sys.platform == "win32"

if IS_WINDOWS:
    pytestmark = pytest.mark.skipif(
        IS_WINDOWS,
        reason="PySpark tests not supported on Windows due to JVM serialization issues. "
               "Run tests in Docker: docker build -t tech-challenge . && "
               "docker run tech-challenge pytest tests/ -v"
    )


@pytest.fixture(scope="session")
def spark():
    if IS_WINDOWS:
        pytest.skip("PySpark tests require Docker or Linux. See STRATEGY_C_PRAGMATIC.md")

    spark = SparkSession.builder \
        .appName("TestSession") \
        .master("local[4]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.python.worker.memory", "512m") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


MUNICIPIO_SCHEMA = StructType([
    StructField("ano", IntegerType()),
    StructField("id_municipio", StringType()),
    StructField("serie", IntegerType()),
    StructField("rede", IntegerType()),
    StructField("taxa_alfabetizacao", DoubleType()),
    StructField("media_portugues", DoubleType()),
    StructField("proporcao_aluno_nivel_0", DoubleType()),
    StructField("proporcao_aluno_nivel_1", DoubleType()),
    StructField("proporcao_aluno_nivel_2", DoubleType()),
    StructField("proporcao_aluno_nivel_3", DoubleType()),
    StructField("proporcao_aluno_nivel_4", DoubleType()),
    StructField("proporcao_aluno_nivel_5", DoubleType()),
    StructField("proporcao_aluno_nivel_6", DoubleType()),
    StructField("proporcao_aluno_nivel_7", DoubleType()),
    StructField("proporcao_aluno_nivel_8", DoubleType()),
])

META_MUNICIPIO_SCHEMA = StructType([
    StructField("ano", IntegerType()),
    StructField("id_municipio", StringType()),
    StructField("rede", StringType()),
    StructField("taxa_alfabetizacao", DoubleType()),
    StructField("meta_alfabetizacao_2024", DoubleType()),
    StructField("meta_alfabetizacao_2025", DoubleType()),
    StructField("meta_alfabetizacao_2026", DoubleType()),
    StructField("meta_alfabetizacao_2027", DoubleType()),
    StructField("meta_alfabetizacao_2028", DoubleType()),
    StructField("meta_alfabetizacao_2029", DoubleType()),
    StructField("meta_alfabetizacao_2030", DoubleType()),
    StructField("nivel_alfabetizacao", StringType()),
    StructField("percentual_participacao", StringType()),
])

GOLD_SCHEMA = StructType([
    StructField("ano", IntegerType()),
    StructField("sigla_uf", StringType()),
    StructField("taxa_alfabetizacao_media", DoubleType()),
    StructField("qtd_municipios_analisados", IntegerType()),
])

SILVER_MART_SCHEMA = StructType([
    StructField("ano", IntegerType()),
    StructField("id_municipio", StringType()),
    StructField("nome_municipio", StringType()),
    StructField("sigla_uf", StringType()),
    StructField("rede", StringType()),
    StructField("taxa_alfabetizacao", DoubleType()),
    StructField("media_portugues", DoubleType()),
    StructField("populacao_total", DoubleType()),
    StructField("meta_alfabetizacao_2024", DoubleType()),
    StructField("deficit_absoluto_proxy", DoubleType()),
])


@pytest.fixture
def sample_municipio_data(spark):
    data = [
        (2023, "3550308", 2, 3, 85.5, 750.0, 0.0, 0.0, 5.0, 10.0, 20.0, 30.0, 15.0, 10.0, 10.0),
        (2023, "3550308", 2, 5, 92.0, 780.0, 0.0, 0.0, 2.0, 8.0, 15.0, 25.0, 25.0, 15.0, 10.0),
        (2024, "3550308", 2, 3, 87.2, 760.0, 0.0, 0.0, 4.0, 9.0, 18.0, 28.0, 18.0, 12.0, 11.0),
        (2023, "2304400", 2, 3, 95.99, 794.0, 0.0, 0.0, 4.01, 0.0, 0.0, 19.44, 38.89, 15.12, 22.53),
        (2023, "1302603", 2, 3, 98.0, None, None, None, None, None, None, None, None, None, None),
        (2023, "9999999", 2, 0, 99.0, 800.0, 0.0, 0.0, 1.0, 0.0, 0.0, 10.0, 30.0, 30.0, 29.0),
    ]
    return spark.createDataFrame(data, schema=MUNICIPIO_SCHEMA)


@pytest.fixture
def sample_meta_municipio_data(spark):
    data = [
        (2023, "3550308", "Municipal", None, 80.0, 82.0, 85.0, 88.0, 90.0, 93.0, 95.0, None, None),
        (2024, "3550308", "Municipal", None, 80.0, 82.0, 85.0, 88.0, 90.0, 93.0, 95.0, None, None),
        (2023, "2304400", "Municipal", None, 90.0, 91.0, 92.0, 93.0, 94.0, 95.0, 96.0, None, None),
        (2024, "2304400", "Municipal", None, 90.0, 91.0, 92.0, 93.0, 94.0, 95.0, 96.0, None, None),
        (2023, "1302603", "Municipal", None, 95.0, 95.5, 96.0, 96.5, 97.0, 97.5, 98.0, None, None),
        (2024, "1302603", "Municipal", None, 95.0, 95.5, 96.0, 96.5, 97.0, 97.5, 98.0, None, None),
    ]
    return spark.createDataFrame(data, schema=META_MUNICIPIO_SCHEMA)


@pytest.fixture
def sample_gold_data(spark):
    data = [
        (2023, "SP", 88.75, 2),
        (2023, "CE", 95.99, 1),
        (2023, "AM", 98.00, 1),
        (2024, "SP", 87.20, 1),
    ]
    return spark.createDataFrame(data, schema=GOLD_SCHEMA)


@pytest.fixture
def sample_mart_data(spark):
    data = [
        (2023, "3550308", "Sao Paulo", "SP", "Municipal", 85.5, 750.0, 11450983.0, 90.0, -37000.0),
        (2023, "3550308", "Sao Paulo", "SP", "Estadual", 92.0, 780.0, 11450983.0, None, -37000.0),
        (2024, "3550308", "Sao Paulo", "SP", "Municipal", 87.2, 760.0, 11450983.0, 90.0, -37000.0),
        (2023, "2304400", "Fortaleza", "CE", "Municipal", 95.99, 794.0, 2686612.0, 90.0, -12000.0),
        (2023, "2304400", "Fortaleza", "CE", "Privada", 98.5, 850.0, 2686612.0, None, -12000.0),
        (2023, "1302603", "Manaus", "AM", "Municipal", 98.0, None, 2193737.0, 95.0, -5000.0),
        (2023, "1302603", "Manaus", "AM", "Estadual", 97.0, None, 2193737.0, None, -5000.0),
        (2024, "1302603", "Manaus", "AM", "Municipal", 98.5, None, 2193737.0, 95.0, -5000.0),
        (2023, "9999999", "Test City", "TT", "Municipal", 99.0, 800.0, 50000.0, None, None),
    ]
    return spark.createDataFrame(data, schema=SILVER_MART_SCHEMA)
