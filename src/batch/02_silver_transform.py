import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, when, isnan, substring
from pyspark.sql.types import StringType, DoubleType

# Configuração de HADOOP_HOME e winutils
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
hadoop_home = os.path.join(project_root, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + os.path.join(hadoop_home, "bin")

# Configuração de JAVA_HOME para garantir que o java.exe seja encontrado pelo PySpark
if "JAVA_HOME" in os.environ:
    java_bin = os.path.join(os.environ["JAVA_HOME"], "bin")
    if java_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")

import sys
def get_spark_session(app_name="SilverTransform"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def run_silver_transformation(spark, bronze_dir, silver_dir):
    print("="*80)
    print("[SILVER] INICIANDO TRANSFORMACOES DA CAMADA SILVER (OBT)")
    print("="*80)
    
    path_municipio = os.path.join(bronze_dir, "br_inep_avaliacao_alfabetizacao_municipio")
    path_meta_municipio = os.path.join(bronze_dir, "br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio")
    
    if not os.path.exists(path_municipio) or not os.path.exists(path_meta_municipio):
        print("ERRO: Tabelas da Bronze não encontradas. Rode a Ingestão primeiro.")
        return
        
    print("1. Lendo Tabelas Brutas (Bronze)...")
    df_muni = spark.read.parquet(path_municipio)
    df_meta = spark.read.parquet(path_meta_municipio)
    
    print("2. Tipagem de Dados (Schema Enforcement)...")
    # Garante que IDs sejam Strings para não perder zeros à esquerda
    df_muni = df_muni.withColumn("id_municipio", col("id_municipio").cast(StringType()))
    df_meta = df_meta.withColumn("id_municipio", col("id_municipio").cast(StringType()))
    
    # Garante que as taxas sejam Decimais (Double)
    df_muni = df_muni.withColumn("taxa_alfabetizacao", col("taxa_alfabetizacao").cast(DoubleType()))
    
    # Garante que rede seja String e Mapeia os códigos do INEP para cruzar com as Metas
    # Códigos reais do dataset: 0=Federal, 2=Estadual, 3=Municipal, 5=Privada
    df_muni = df_muni.withColumn("rede", 
        when(col("rede") == 0, "Federal")
        .when(col("rede") == 2, "Estadual")
        .when(col("rede") == 3, "Municipal")
        .when(col("rede") == 5, "Privada")
        .otherwise(col("rede").cast(StringType()))
    )
    df_meta = df_meta.withColumn("rede", col("rede").cast(StringType()))
    
    # As metas podem vir com strings se tiverem valores como "Não se aplica", então vamos castar
    for ano_meta in range(2024, 2031):
        col_meta = f"meta_alfabetizacao_{ano_meta}"
        if col_meta in df_meta.columns:
            df_meta = df_meta.withColumn(col_meta, col(col_meta).cast(DoubleType()))

    print("3. Limpeza de Dados (Nulls e Padronização)...")
    # A pedido de negócio (decisão estratégica), manteremos os valores nulos nas proporções de nível.
    # Preencher com 0.0 distorceria as médias analíticas na Camada Gold (agregando falsos zeros).
    # Nenhuma transformação destrutiva será aplicada aos Nulos numéricos.
    
    print("4. Integração das Tabelas (One Big Table)...")
    # A tabela de metas tem 2 linhas por municipio (ano_referencia=2023 e 2024) com os mesmos targets.
    # Precisamos deduplicar para evitar multiplicação de linhas no LEFT JOIN.
    # Renomeamos taxa_alfabetizacao da meta se existir para evitar conflito
    if "taxa_alfabetizacao" in df_meta.columns:
        df_meta = df_meta.withColumnRenamed("taxa_alfabetizacao", "taxa_alfabetizacao_base_meta")

    # Remove duplicatas de meta: 1 row por (id_municipio, rede) — descarta a coluna ano
    meta_cols_to_drop = ["ano"]
    df_meta_dedup = df_meta.drop(*[c for c in meta_cols_to_drop if c in df_meta.columns]) \
                           .dropDuplicates(subset=["id_municipio", "rede"])

    # Condição do Join: apenas (id_municipio, rede) — sem ano, pois metas são plurianuais
    join_cond = [
        df_muni.id_municipio == df_meta_dedup.id_municipio,
        df_muni.rede == df_meta_dedup.rede
    ]
    
    # OBT = One Big Table
    df_obt = df_muni.join(df_meta_dedup, join_cond, "left") \
                     .drop(df_meta_dedup.id_municipio) \
                     .drop(df_meta_dedup.rede)
    
    # Mapeamento do código do estado (primeiros 2 dígitos do id_municipio do IBGE) para sigla_uf
    uf_mapping = {
        "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
        "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
        "28": "SE", "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP", "41": "PR",
        "42": "SC", "43": "RS", "50": "MS", "51": "MT", "52": "GO", "53": "DF"
    }
    
    state_code = substring(col("id_municipio"), 1, 2)
    mapping_expr = when(state_code == "11", "RO")
    for code, uf in uf_mapping.items():
        if code != "11":
            mapping_expr = mapping_expr.when(state_code == code, uf)
    mapping_expr = mapping_expr.otherwise("Unknown")
    
    df_obt = df_obt.withColumn("sigla_uf", mapping_expr)
    
    print("4.5 Enriquecimento de Dados (Buscando Nomes dos Municípios via API do IBGE)...")
    import urllib.request
    import json
    import gzip
    import io
    try:
        url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            if response.info().get('Content-Encoding') == 'gzip':
                f = gzip.GzipFile(fileobj=io.BytesIO(response.read()))
                raw_json = f.read().decode('utf-8')
            else:
                raw_json = response.read().decode('utf-8')
        
        # Salvar JSON no disco para que o Spark leia pela JVM (evita crash do Python Worker no Python 3.13+)
        temp_json_path = os.path.join(bronze_dir, "ibge_municipios_temp.json")
        with open(temp_json_path, "w", encoding="utf-8") as f:
            f.write(raw_json)
            
        # Lê via Spark JVM
        df_ibge_raw = spark.read.json(temp_json_path)
        
        # Formata o DataFrame
        df_nomes = df_ibge_raw.select(
            col("id").cast("string").alias("id_municipio_ibge"),
            col("nome").alias("nome_municipio")
        )
        
        # Faz o LEFT JOIN com a OBT
        df_obt = df_obt.join(df_nomes, df_obt.id_municipio == df_nomes.id_municipio_ibge, "left") \
                       .drop("id_municipio_ibge")
        print("    -> Nomes dos municípios adicionados com sucesso!")
        
        print("4.6 Enriquecimento de Dados (Buscando População via API do IBGE SIDRA)...")
        url_pop = "https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2021/variaveis/9324?localidades=N6[all]"
        req_pop = urllib.request.Request(url_pop, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req_pop) as response:
            if response.info().get('Content-Encoding') == 'gzip':
                f = gzip.GzipFile(fileobj=io.BytesIO(response.read()))
                pop_json = f.read().decode('utf-8')
            else:
                pop_json = response.read().decode('utf-8')
                
        # Parseia a estrutura complexa do SIDRA para extrair apenas ID e População
        pop_data = json.loads(pop_json)
        series = pop_data[0]['resultados'][0]['series']
        
        temp_pop_path = os.path.join(bronze_dir, "ibge_populacao_temp.jsonl")
        with open(temp_pop_path, "w", encoding="utf-8") as f:
            for s in series:
                # Trata casos em que a população não foi informada ('-' ou nulo)
                val = s['serie']['2021']
                pop = int(val) if val.isnumeric() else None
                f.write(json.dumps({"id_municipio_ibge": s['localidade']['id'], "populacao_total": pop}) + "\n")
                
        # Lê o arquivo JSONL com a JVM do Spark
        df_pop = spark.read.json(temp_pop_path)
        
        # Junta a população na OBT
        df_obt = df_obt.join(df_pop, df_obt.id_municipio == df_pop.id_municipio_ibge, "left") \
                       .drop("id_municipio_ibge")
        print("    -> População dos municípios adicionada com sucesso!")
        
        # Cria a Feature de Negócio: Deficit Absoluto (Proxy)
        # Quantas pessoas, em volume absoluto, representam a parcela não alfabetizada?
        from pyspark.sql.functions import round as spark_round
        df_obt = df_obt.withColumn("deficit_absoluto_proxy", 
                                   spark_round(((100 - col("taxa_alfabetizacao")) / 100) * col("populacao_total"), 0))
        
    except Exception as e:
        print(f"    -> Aviso: Falha ao enriquecer dados externos do IBGE. Motivo: {e}")

    print("5. Salvando Dados Transformados na Camada Silver (Parquet)...")
    # Vamos particionar por ano e rede para eficiência no BI
    output_path = os.path.join(silver_dir, "alfabetizacao_municipios_obt")
    df_obt.write \
        .format("parquet") \
        .mode("overwrite") \
        .partitionBy("ano", "rede") \
        .save(output_path)
        
    print(f"Camada Silver gerada com sucesso em: {output_path}")
    print(f" Total de Registros Finais na OBT: {df_obt.count()}")

if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    env = os.environ.get("ENV", "dev")
    if env == "dev":
        LAKE_BRONZE_DIR = os.path.abspath(os.path.join(project_root, "datalake_sample", "bronze"))
        LAKE_SILVER_DIR = os.path.abspath(os.path.join(project_root, "datalake_sample", "silver"))
    else:
        LAKE_BRONZE_DIR = os.path.abspath(os.path.join(project_root, "datalake", "bronze"))
        LAKE_SILVER_DIR = os.path.abspath(os.path.join(project_root, "datalake", "silver"))
    
    run_silver_transformation(spark, LAKE_BRONZE_DIR, LAKE_SILVER_DIR)
    
    spark.stop()
