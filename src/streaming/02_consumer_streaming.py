import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("StreamingConsumer")

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))

hadoop_home = os.path.join(project_root, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + os.path.join(hadoop_home, "bin")
if "JAVA_HOME" in os.environ:
    java_bin = os.path.join(os.environ["JAVA_HOME"], "bin")
    if java_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")

STREAMING_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("sigla_uf", StringType(), True),
    StructField("id_municipio", StringType(), True),
    StructField("nome_municipio", StringType(), True),
    StructField("nova_medicao_saeb", DoubleType(), True),
    StructField("status_meta", StringType(), True),
    StructField("meta_atingida", BooleanType(), True),
])


def get_spark_session(app_name="StreamingConsumer"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.streaming.schemaInference", "false") \
        .getOrCreate()


def start_streaming_consumer(landing_zone_path: str, output_path: str, checkpoint_path: str):
    logger.info("=" * 60)
    logger.info(f"Consumer iniciado")
    logger.info(f"  Landing zone: {landing_zone_path}")
    logger.info(f"  Output:       {output_path}")
    logger.info(f"  Checkpoint:   {checkpoint_path}")
    logger.info("=" * 60)

    if not os.path.isdir(landing_zone_path):
        logger.warning(f"Landing zone nao existe: {landing_zone_path}")
        logger.warning("Criando diretorio vazio. Produza eventos primeiro.")
        os.makedirs(landing_zone_path, exist_ok=True)

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    df_stream = spark \
        .readStream \
        .format("json") \
        .schema(STREAMING_SCHEMA) \
        .option("cleanSource", "archive") \
        .option("sourceArchiveDir", os.path.join(os.path.dirname(checkpoint_path), "archived")) \
        .load(landing_zone_path)

    logger.info(f"Schema detectado: {df_stream.schema.simpleString()}")

    query = df_stream \
        .writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="10 seconds") \
        .start()

    logger.info("Streaming query iniciada. Aguardando dados...")
    query.awaitTermination()


if __name__ == "__main__":
    env = os.environ.get("ENV", "dev")

    if env == "prod":
        landing_dir = os.path.join(project_root, "datalake", "raw", "streaming_landing")
        output_dir = os.path.join(project_root, "datalake", "bronze", "streaming_eventos")
        chkpt_dir = os.path.join(project_root, "datalake", "checkpoints", "streaming_eventos")
    else:
        landing_dir = os.path.join(project_root, "datalake_sample", "raw", "streaming_landing")
        output_dir = os.path.join(project_root, "datalake_sample", "bronze", "streaming_eventos")
        chkpt_dir = os.path.join(project_root, "datalake_sample", "checkpoints", "streaming_eventos")

    os.makedirs(landing_dir, exist_ok=True)
    start_streaming_consumer(landing_dir, output_dir, chkpt_dir)