import os
import sys
import logging
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as spark_sum, count, max as spark_max, coalesce, when, first
from pyspark.sql.window import Window
from sklearn.neighbors import KNeighborsRegressor
from sklearn.preprocessing import MinMaxScaler

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MetaImputator")

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))

hadoop_home = os.path.join(project_root, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + os.path.join(hadoop_home, "bin")
if "JAVA_HOME" in os.environ:
    java_bin = os.path.join(os.environ["JAVA_HOME"], "bin")
    if java_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")

TARGET_COL = "meta_alfabetizacao_2024"
FEATURES = ["populacao_total_normalizada", "taxa_alfabetizacao_normalizada", "deficit_normalizada"]
K_NEIGHBORS = 5
RANDOM_STATE = 42


def get_spark_session():
    return SparkSession.builder.appName("MetaImputator").getOrCreate()


def resolve_paths():
    env = os.environ.get("ENV", "dev")
    if env == "prod":
        return (
            os.path.join(project_root, "datalake", "silver", "alfabetizacao_municipios_obt"),
            os.path.join(project_root, "datalake", "silver", "alfabetizacao_municipios_obt_com_metas_imputadas"),
        )
    return (
        os.path.join(project_root, "datalake_sample", "silver", "alfabetizacao_municipios_obt"),
        os.path.join(project_root, "datalake_sample", "silver", "alfabetizacao_municipios_obt_com_metas_imputadas"),
    )


def etapa1_propagar_meta_por_municipio(spark, silver_path, output_path):
    logger.info("=" * 60)
    logger.info("ETAPA 1: Propagacao de meta existente dentro do mesmo municipio")
    logger.info("=" * 60)

    df = spark.read.parquet(silver_path)
    total = df.count()
    logger.info(f"  Silver carregada: {total} registros, {len(df.columns)} colunas")

    tem_meta_antes = df.filter(col(TARGET_COL).isNotNull()).count()
    logger.info(f"  Com meta original: {tem_meta_antes} ({tem_meta_antes/total*100:.1f}%)")

    janela = Window.partitionBy("id_municipio")
    df_com_propagacao = df.withColumn(
        "meta_por_municipio",
        first(TARGET_COL, ignorenulls=True).over(janela)
    )
    df_com_propagacao = df_com_propagacao.withColumn(
        "meta_alfabetizacao_2024_imputada",
        coalesce(col(TARGET_COL), col("meta_por_municipio"))
    )

    apos_propag = df_com_propagacao.filter(col("meta_alfabetizacao_2024_imputada").isNotNull()).count()
    logger.info(f"  Apos propagacao: {apos_propag} ({apos_propag/total*100:.1f}%)")

    ainda_sem_meta = df_com_propagacao.filter(col("meta_alfabetizacao_2024_imputada").isNull()).count()
    logger.info(f"  Ainda sem meta (entram no KNN): {ainda_sem_meta} ({ainda_sem_meta/total*100:.1f}%)")

    return df_com_propagacao


def etapa2_knn_imputacao(spark, df):
    logger.info("=" * 60)
    logger.info("ETAPA 2: Imputacao KNN ponderada para municipios sem meta")
    logger.info("=" * 60)

    if df.filter(col("meta_alfabetizacao_2024_imputada").isNull()).count() == 0:
        logger.info("  Nenhum municipio sem meta — KNN nao necessario.")
        return df.drop("meta_por_municipio")

    logger.info("  Agregando municipios-alvo (sem meta)...")
    df.createOrReplaceTempView("vw_full")
    df_target = spark.sql("""
        SELECT id_municipio, nome_municipio, sigla_uf,
               ROUND(AVG(taxa_alfabetizacao), 2) as taxa_media,
               ROUND(AVG(populacao_total), 0) as populacao,
               ROUND(SUM(deficit_absoluto_proxy), 0) as deficit_total
        FROM vw_full
        WHERE meta_alfabetizacao_2024_imputada IS NULL
        GROUP BY id_municipio, nome_municipio, sigla_uf
    """)
    pdf_target = df_target.toPandas()
    logger.info(f"  Municipios para imputar KNN: {len(pdf_target)}")

    logger.info("  Agregando municipios de referencia (com meta)...")
    df_ref = spark.sql("""
        SELECT id_municipio, sigla_uf,
               ROUND(AVG(taxa_alfabetizacao), 2) as taxa_media,
               ROUND(AVG(populacao_total), 0) as populacao,
               ROUND(SUM(deficit_absoluto_proxy), 0) as deficit_total,
               ROUND(AVG(meta_alfabetizacao_2024_imputada), 2) as meta
        FROM vw_full
        WHERE meta_alfabetizacao_2024_imputada IS NOT NULL
        GROUP BY id_municipio, nome_municipio, sigla_uf
    """)
    pdf_ref = df_ref.toPandas()
    logger.info(f"  Municipios de referencia: {len(pdf_ref)}")

    def knn_predict_por_uf(row, referencia, k=K_NEIGHBORS):
        uf = row["sigla_uf"]
        ref_uf = referencia[referencia["sigla_uf"] == uf]
        if len(ref_uf) < 3:
            ref_uf = referencia
        if len(ref_uf) == 0:
            return np.nan

        X_ref = ref_uf[["taxa_media", "populacao", "deficit_total"]].values
        y_ref = ref_uf["meta"].values
        X_target = np.array([[row["taxa_media"], row["populacao"], row["deficit_total"]]])

        scaler = MinMaxScaler()
        X_ref_scaled = scaler.fit_transform(X_ref)
        X_target_scaled = scaler.transform(X_target)

        knn = KNeighborsRegressor(n_neighbors=min(k, len(ref_uf)), weights="distance")
        knn.fit(X_ref_scaled, y_ref)
        return round(knn.predict(X_target_scaled)[0], 2)

    pdf_target["meta_knn"] = pdf_target.apply(
        lambda r: knn_predict_por_uf(r, pdf_ref), axis=1
    )

    knn_ok = pdf_target["meta_knn"].notna().sum()
    logger.info(f"  KNN imputou {knn_ok}/{len(pdf_target)} municipios")

    logger.info(f"\n  Estatisticas das metas imputadas via KNN:")
    logger.info(f"    Media: {pdf_target['meta_knn'].mean():.1f}%")
    logger.info(f"    Mediana: {pdf_target['meta_knn'].median():.1f}%")
    logger.info(f"    Min: {pdf_target['meta_knn'].min():.1f}%")
    logger.info(f"    Max: {pdf_target['meta_knn'].max():.1f}%")
    logger.info(f"    Desvio padrao: {pdf_target['meta_knn'].std():.1f}%")

    logger.info("  Aplicando imputacao via join Spark...")
    knn_json_dir = os.path.join(project_root, "datalake_sample", "temp", "knn_predictions")
    os.makedirs(knn_json_dir, exist_ok=True)
    pdf_knn = pdf_target[pdf_target["meta_knn"].notna()][["id_municipio", "meta_knn"]]
    pdf_knn.to_json(os.path.join(knn_json_dir, "data.json"), orient="records", lines=True, force_ascii=False)

    df_knn = spark.read.schema("id_municipio string, meta_knn double").json(knn_json_dir)
    df_knn.cache().count()

    import shutil
    shutil.rmtree(knn_json_dir, ignore_errors=True)

    df_final = df.drop("meta_por_municipio").join(
        df_knn, on="id_municipio", how="left"
    ).withColumn(
        "meta_alfabetizacao_2024_imputada",
        when(
            col("meta_alfabetizacao_2024_imputada").isNull(),
            col("meta_knn")
        ).otherwise(col("meta_alfabetizacao_2024_imputada"))
    ).drop("meta_knn")

    return df_final


def run_imputacao():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    env = os.environ.get("ENV", "dev")
    logger.info(f"Ambiente: ENV={env}")

    silver_path, output_path = resolve_paths()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df_propagado = etapa1_propagar_meta_por_municipio(spark, silver_path, output_path)
    df_final = etapa2_knn_imputacao(spark, df_propagado)

    total = df_final.count()
    com_meta = df_final.filter(col("meta_alfabetizacao_2024_imputada").isNotNull()).count()
    sem_meta = df_final.filter(col("meta_alfabetizacao_2024_imputada").isNull()).count()

    logger.info("\n" + "=" * 60)
    logger.info("RESUMO DA IMPUTACAO")
    logger.info("=" * 60)
    logger.info(f"  Total registros:         {total}")
    logger.info(f"  Com meta (original):     {df_final.filter(col(TARGET_COL).isNotNull()).count()}")
    logger.info(f"  Com meta (imputada):     {com_meta}")
    logger.info(f"  Ainda sem meta:          {sem_meta}")
    logger.info(f"  Cobertura final:         {com_meta/total*100:.1f}%")
    logger.info(f"\n  Salvando em: {output_path}")

    df_final.write.format("parquet").mode("overwrite").partitionBy("ano", "rede").save(output_path)

    logger.info("  OK — Imputacao concluida.")
    spark.stop()


if __name__ == "__main__":
    run_imputacao()
