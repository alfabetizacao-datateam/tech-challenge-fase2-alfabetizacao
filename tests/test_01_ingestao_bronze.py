import os
import glob
import tempfile
import gzip
from pyspark.sql import SparkSession


def get_spark_session(app_name="TestBronze"):
    return SparkSession.builder.appName(app_name).getOrCreate()


def ingest_local_files_to_bronze(spark, source_dir, bronze_dir):
    csv_files = glob.glob(os.path.join(source_dir, "*.csv"))
    gz_files = glob.glob(os.path.join(source_dir, "*.csv.gz"))
    all_files = csv_files + gz_files

    if not all_files:
        print("Nenhum arquivo encontrado na pasta de dados!")
        return

    for file_path in all_files:
        filename = os.path.basename(file_path)
        table_name = filename.replace(".csv", "").replace(".gz", "")

        df_raw = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(file_path)

        output_path = os.path.join(bronze_dir, table_name)
        df_raw.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(output_path)

        df_raw.count()


def test_get_spark_session():
    s = get_spark_session("TestBronze")
    assert s is not None
    assert s.sparkContext.appName == "TestBronze"
    s.stop()


def test_ingest_local_files_to_bronze(spark, tmp_path):
    source = tmp_path / "source"
    bronze = tmp_path / "bronze"
    source.mkdir()
    bronze.mkdir()

    csv_path = source / "test_data.csv"
    csv_path.write_text("id,nome,valor\n1,foo,10.5\n2,bar,20.3\n", encoding="utf-8")

    ingest_local_files_to_bronze(spark, str(source), str(bronze))

    output_dir = str(bronze / "test_data")
    assert os.path.isdir(output_dir)

    df = spark.read.parquet(output_dir)
    assert df.count() == 2
    assert df.columns == ["id", "nome", "valor"]


def test_ingest_gz_file(spark, tmp_path):
    source = tmp_path / "source_gz"
    bronze = tmp_path / "bronze_gz"
    source.mkdir()
    bronze.mkdir()

    gz_path = source / "data.csv.gz"
    with gzip.open(str(gz_path), "wt", encoding="utf-8") as f:
        f.write("x,y\n1,2\n3,4\n")

    ingest_local_files_to_bronze(spark, str(source), str(bronze))

    df = spark.read.parquet(str(bronze / "data"))
    assert df.count() == 2


def test_ingest_empty_dir(spark, tmp_path, capsys):
    empty = tmp_path / "empty"
    bronze = tmp_path / "bronze_empty"
    empty.mkdir()
    bronze.mkdir()

    ingest_local_files_to_bronze(spark, str(empty), str(bronze))
    captured = capsys.readouterr()
    assert "Nenhum arquivo" in captured.out


def test_idempotent_overwrite(spark, tmp_path):
    source = tmp_path / "src_idem"
    bronze = tmp_path / "br_idem"
    source.mkdir()
    bronze.mkdir()

    csv_path = source / "dup.csv"
    csv_path.write_text("k,v\n1,a\n2,b\n", encoding="utf-8")

    ingest_local_files_to_bronze(spark, str(source), str(bronze))
    ingest_local_files_to_bronze(spark, str(source), str(bronze))

    df = spark.read.parquet(str(bronze / "dup"))
    assert df.count() == 2