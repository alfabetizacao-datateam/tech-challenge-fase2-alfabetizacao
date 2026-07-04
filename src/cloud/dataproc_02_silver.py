"""
Silver transformation for Dataproc/GCS.
Usage: pyspark --bucket gs://bucket-name
"""
import argparse
import json
import gzip
import io
import urllib.request
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, substring
from pyspark.sql.types import StringType, DoubleType


def get_spark():
    return SparkSession.builder.appName("SilverTransform-GCS").getOrCreate()


UF_MAP = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
    "28": "SE", "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP", "41": "PR",
    "42": "SC", "43": "RS", "50": "MS", "51": "MT", "52": "GO", "53": "DF"
}


def run_silver(spark, bronze_dir, silver_dir):
    print("=" * 70)
    print("[SILVER] TRANSFORMACOES DA CAMADA SILVER (OBT)")
    print("=" * 70)

    path_municipio = f"{bronze_dir}/br_inep_avaliacao_alfabetizacao_municipio"
    path_meta_municipio = f"{bronze_dir}/br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio"

    print("1. Lendo Bronze...")
    df_muni = spark.read.parquet(path_municipio)
    df_meta = spark.read.parquet(path_meta_municipio)

    print("2. Tipagem e mapeamento de rede...")
    df_muni = df_muni.withColumn("id_municipio", col("id_municipio").cast(StringType()))
    df_meta = df_meta.withColumn("id_municipio", col("id_municipio").cast(StringType()))
    df_muni = df_muni.withColumn("taxa_alfabetizacao", col("taxa_alfabetizacao").cast(DoubleType()))

    df_muni = df_muni.withColumn("rede",
        when(col("rede") == 0, "Federal")
        .when(col("rede") == 2, "Estadual")
        .when(col("rede") == 3, "Municipal")
        .when(col("rede") == 5, "Privada")
        .otherwise(col("rede").cast(StringType()))
    )
    df_meta = df_meta.withColumn("rede", col("rede").cast(StringType()))

    for ano_meta in range(2024, 2031):
        col_meta = f"meta_alfabetizacao_{ano_meta}"
        if col_meta in df_meta.columns:
            df_meta = df_meta.withColumn(col_meta, col(col_meta).cast(DoubleType()))

    print("3. Deduplicando metas e fazendo OBT join...")
    if "taxa_alfabetizacao" in df_meta.columns:
        df_meta = df_meta.withColumnRenamed("taxa_alfabetizacao", "taxa_alfabetizacao_base_meta")

    df_meta_dedup = df_meta.drop(*[c for c in ["ano"] if c in df_meta.columns]) \
                           .dropDuplicates(subset=["id_municipio", "rede"])

    join_cond = [
        df_muni.id_municipio == df_meta_dedup.id_municipio,
        df_muni.rede == df_meta_dedup.rede
    ]
    df_obt = df_muni.join(df_meta_dedup, join_cond, "left") \
                    .drop(df_meta_dedup.id_municipio) \
                    .drop(df_meta_dedup.rede)

    print("4. Adicionando sigla_uf...")
    state_code = substring(col("id_municipio"), 1, 2)
    mapping_expr = when(state_code == "11", "RO")
    for code, uf in UF_MAP.items():
        if code != "11":
            mapping_expr = mapping_expr.when(state_code == code, uf)
    mapping_expr = mapping_expr.otherwise("Unknown")
    df_obt = df_obt.withColumn("sigla_uf", mapping_expr)

    print("4.5 Enriquecendo com IBGE (nomes municípios)...")
    try:
        url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=30) as response:
            if response.info().get("Content-Encoding") == "gzip":
                raw_json = gzip.GzipFile(fileobj=io.BytesIO(response.read())).read().decode("utf-8")
            else:
                raw_json = response.read().decode("utf-8")

        ibge_data = json.loads(raw_json)
        pdf_ibge = pd.DataFrame([
            {"id_municipio_ibge": str(m["id"]), "nome_municipio": m["nome"]}
            for m in ibge_data
        ])
        df_ibge = spark.createDataFrame(pdf_ibge)
        df_obt = df_obt.join(df_ibge, df_obt.id_municipio == df_ibge.id_municipio_ibge, "left") \
                       .drop("id_municipio_ibge")
        print("  -> Nomes de municípios adicionados.")

        print("4.6 Enriquecendo com IBGE SIDRA (população)...")
        url_pop = "https://servicodados.ibge.gov.br/api/v3/agregados/6579/periodos/2021/variaveis/9324?localidades=N6[all]"
        req_pop = urllib.request.Request(url_pop, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req_pop, timeout=60) as response:
            if response.info().get("Content-Encoding") == "gzip":
                pop_json = gzip.GzipFile(fileobj=io.BytesIO(response.read())).read().decode("utf-8")
            else:
                pop_json = response.read().decode("utf-8")

        pop_data = json.loads(pop_json)
        series = pop_data[0]["resultados"][0]["series"]
        records = []
        for s in series:
            val = s["serie"]["2021"]
            pop = int(val) if val.isnumeric() else None
            records.append({"id_municipio_ibge": s["localidade"]["id"], "populacao_total": pop})
        df_pop = spark.createDataFrame(pd.DataFrame(records))
        df_obt = df_obt.join(df_pop, df_obt.id_municipio == df_pop.id_municipio_ibge, "left") \
                       .drop("id_municipio_ibge")

        from pyspark.sql.functions import round as spark_round
        df_obt = df_obt.withColumn("deficit_absoluto_proxy",
                                   spark_round(((100 - col("taxa_alfabetizacao")) / 100) * col("populacao_total"), 0))
        print("  -> População adicionada.")
    except Exception as e:
        print(f"  -> AVISO: Falha enriquecimento IBGE: {e}")

    print("5. Salvando Silver (Parquet particionado por ano/rede)...")
    output_path = f"{silver_dir}/alfabetizacao_municipios_obt"
    df_obt.write.format("parquet").mode("overwrite") \
        .partitionBy("ano", "rede").save(output_path)

    print(f"Silver OK: {output_path}")
    print(f"Total registros OBT: {df_obt.count()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, help="gs://bucket-name")
    args = parser.parse_args()

    bucket = args.bucket.rstrip("/")
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    run_silver(spark, f"{bucket}/bronze", f"{bucket}/silver")

    spark.stop()
    print("Silver finalizado.")
