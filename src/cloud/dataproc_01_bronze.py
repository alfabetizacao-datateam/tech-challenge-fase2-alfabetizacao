"""
Bronze ingestion for Dataproc/GCS.
Usage: pyspark --bucket gs://bucket-name
"""
import argparse
from pyspark.sql import SparkSession


def get_spark():
    return SparkSession.builder.appName("BronzeIngestion-GCS").getOrCreate()


def list_gcs_files(spark, gcs_dir):
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    fs = Path(gcs_dir).getFileSystem(sc._jsc.hadoopConfiguration())
    statuses = fs.listStatus(Path(gcs_dir))
    return [s.getPath().toString() for s in statuses]


def ingest_to_bronze(spark, input_dir, bronze_dir):
    files = list_gcs_files(spark, input_dir)
    csv_files = [f for f in files if f.endswith(".csv") or f.endswith(".csv.gz")]

    if not csv_files:
        print(f"AVISO: Nenhum CSV encontrado em {input_dir}")
        return

    print(f"Encontrados {len(csv_files)} arquivos para ingesto Bronze.")

    for file_path in csv_files:
        filename = file_path.split("/")[-1]
        table_name = filename.replace(".csv.gz", "").replace(".csv", "")

        print(f"Lendo: {filename}")
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(file_path)

        output_path = f"{bronze_dir}/{table_name}"
        print(f"Salvando Bronze: {output_path}")
        df.write.format("parquet").mode("overwrite").save(output_path)
        print(f"  -> {table_name}: {df.count()} registros")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, help="gs://bucket-name")
    args = parser.parse_args()

    bucket = args.bucket.rstrip("/")
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    ingest_to_bronze(spark, f"{bucket}/input", f"{bucket}/bronze")

    spark.stop()
    print("Bronze finalizado.")
