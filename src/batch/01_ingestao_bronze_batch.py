import os
import glob
from pyspark.sql import SparkSession
import logging

# Configuração robusta de caminhos baseada na localização do script
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))

# Resolvendo a dependência do Hadoop no Windows
hadoop_home = os.path.join(project_root, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + os.path.join(hadoop_home, "bin")

# Configuração de Logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_spark_session(app_name="BronzeIngestion"):
    """
    Inicializa a Spark Session.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def ingest_local_files_to_bronze(spark: SparkSession, source_dir: str, bronze_dir: str):
    """
    Lê todos os arquivos CSV (e CSV.GZ) da pasta de dados local e os 
    ingere na camada Bronze no formato Parquet, mantendo a fidelidade bruta.
    """
    logger.info(f"Procurando arquivos em: {source_dir}")
    
    # Busca por arquivos .csv e .csv.gz
    csv_files = glob.glob(os.path.join(source_dir, "*.csv"))
    gz_files = glob.glob(os.path.join(source_dir, "*.csv.gz"))
    all_files = csv_files + gz_files
    
    if not all_files:
        logger.warning("Nenhum arquivo encontrado na pasta de dados!")
        return

    logger.info(f"Encontrados {len(all_files)} arquivos para ingestão.")

    for file_path in all_files:
        # Extrai o nome do arquivo para usar como nome da tabela
        # Ex: "Alunos.csv" -> "Alunos", "br_inep...uf.csv.gz" -> "br_inep...uf"
        filename = os.path.basename(file_path)
        table_name = filename.replace(".csv", "").replace(".gz", "")
        
        logger.info(f"Lendo arquivo bruto: {filename}...")
        
        # Lê o CSV. O Spark lida automaticamente com descompressão de .gz
        df_raw = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(file_path)
            
        output_path = os.path.join(bronze_dir, table_name)
        
        logger.info(f"Salvando dados na camada Bronze (Parquet): {output_path}")
        
        # Escreve em formato Parquet na camada Bronze (append/overwrite)
        # Usamos overwrite para garantir idempotência em execuções locais de teste
        df_raw.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(output_path)
            
        logger.info(f"Tabela '{table_name}' ingerida com sucesso! Total de registros: {df_raw.count()}")

if __name__ == "__main__":
    spark = get_spark_session()
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
    
    # Configuração de Arquitetura Híbrida (Nuvem vs Local)
    ENV = os.environ.get("ENV", "dev")
    
    if ENV == "prod":
        SOURCE_DATA_DIR = os.path.join(project_root, "dados")
        LAKE_BRONZE_DIR = os.path.join(project_root, "datalake", "bronze")
        logger.info(f" Executando em modo PROD (Local - Dados Completos)")
    else:
        SOURCE_DATA_DIR = os.path.join(project_root, "dados_sample")
        LAKE_BRONZE_DIR = os.path.join(project_root, "datalake_sample", "bronze")
        logger.info(f" Executando em modo DEV (Local - Amostra)")
        
    ingest_local_files_to_bronze(spark, SOURCE_DATA_DIR, LAKE_BRONZE_DIR)
    
    spark.stop()
    logger.info("Processo da Camada Bronze finalizado.")
