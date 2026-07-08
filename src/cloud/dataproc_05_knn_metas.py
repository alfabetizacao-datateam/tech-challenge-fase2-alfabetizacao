"""
Imputacao KNN de metas para Dataproc/GCS.
Usage: pyspark --bucket gs://bucket-name [--k 5] [--holdout-frac 0.2]

Reads:
  gs://{bucket}/silver/alfabetizacao_municipios_obt
Writes:
  gs://{bucket}/silver/alfabetizacao_municipios_obt_com_metas_imputadas  (particionado ano/rede)
  gs://{bucket}/silver/metrics_knn_imputacao.json  (validacao holdout — ver ADR-004/ADR-015)

Fluxo (identico a src/features/02_imputar_metas_knn.py, adaptado para GCS):
  1. Etapa 1: propaga meta ja existente dentro do mesmo municipio entre redes
     (first(meta, ignorenulls=True) OVER (PARTITION BY id_municipio))
  2. Etapa 2: KNN (K=5, por UF) para os municipios que ainda ficaram sem meta
  3. Validacao holdout: esconde a meta de uma fracao dos municipios COM meta
     conhecida, mede MAE/RMSE do KNN contra o valor real (achado da auditoria
     2026-07-07: script nunca teve validacao estatistica real)

So a rede Municipal tem meta oficial do PDE (ADR-004) — sem esta imputacao,
Estadual/Federal/Privada ficam com meta_alfabetizacao_2024 NULL, o que hoje
zera gap_meta/status_risco pra ~56% dos registros em agg_municipio_ranking e
agg_top10_uf.
"""
import argparse
import json
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, first, when
from pyspark.sql.window import Window

TARGET_COL = "meta_alfabetizacao_2024"
K_NEIGHBORS = 5
RANDOM_STATE = 42


def get_spark():
    return SparkSession.builder.appName("KnnMetasImputacao-GCS").getOrCreate()


def gcs_write_text(spark, gcs_path: str, content: str) -> None:
    """Escreve conteudo texto em um arquivo no GCS (identico ao helper de dataproc_04_siconfi.py)."""
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    fs = Path(gcs_path).getFileSystem(sc._jsc.hadoopConfiguration())
    stream = fs.create(Path(gcs_path), True)
    writer = sc._jvm.java.io.OutputStreamWriter(stream, "UTF-8")
    writer.write(content)
    writer.close()
    stream.close()


def etapa1_propagar_meta_por_municipio(df):
    print("1. Propagando meta existente dentro do mesmo municipio (entre redes)...")
    total = df.count()
    tem_meta_antes = df.filter(col(TARGET_COL).isNotNull()).count()
    print(f"   Com meta original: {tem_meta_antes}/{total} ({tem_meta_antes/total*100:.1f}%)")

    janela = Window.partitionBy("id_municipio")
    df_prop = df.withColumn("meta_por_municipio", first(TARGET_COL, ignorenulls=True).over(janela))
    df_prop = df_prop.withColumn(
        "meta_alfabetizacao_2024_imputada",
        coalesce(col(TARGET_COL), col("meta_por_municipio"))
    )

    apos = df_prop.filter(col("meta_alfabetizacao_2024_imputada").isNotNull()).count()
    print(f"   Apos propagacao: {apos}/{total} ({apos/total*100:.1f}%)")
    return df_prop


def _knn_predict_por_uf(row, referencia, k=K_NEIGHBORS):
    from sklearn.neighbors import KNeighborsRegressor
    from sklearn.preprocessing import MinMaxScaler

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
    return round(float(knn.predict(X_target_scaled)[0]), 2)


def validar_knn_holdout(pdf_ref, k=K_NEIGHBORS, frac_holdout=0.2, seed=RANDOM_STATE):
    """Esconde a meta de uma fracao dos municipios COM meta conhecida, prediz
    via o mesmo KNN por UF, mede o erro contra o valor real. Ver ADR-015."""
    rng = np.random.RandomState(seed)
    idx_holdout = pdf_ref.sample(frac=frac_holdout, random_state=rng).index
    pdf_holdout = pdf_ref.loc[idx_holdout].copy()
    pdf_referencia_reduzida = pdf_ref.drop(index=idx_holdout)

    pdf_holdout["meta_prevista"] = pdf_holdout.apply(
        lambda r: _knn_predict_por_uf(r, pdf_referencia_reduzida, k), axis=1
    )
    validos = pdf_holdout.dropna(subset=["meta_prevista"])
    erro_abs = (validos["meta"] - validos["meta_prevista"]).abs()
    mae = float(erro_abs.mean())
    rmse = float(np.sqrt((erro_abs ** 2).mean()))

    print("=" * 70)
    print("VALIDACAO KNN (holdout — municipios com meta conhecida)")
    print("=" * 70)
    print(f"  Holdout: {len(validos)}/{len(pdf_holdout)} municipios avaliados")
    print(f"  MAE:  {mae:.2f} pontos percentuais")
    print(f"  RMSE: {rmse:.2f} pontos percentuais")
    if mae > 10:
        print("  AVISO: MAE > 10pp — imputacao pouco confiavel, revisar features/k antes de confiar na cobertura.")
    return {"mae": round(mae, 2), "rmse": round(rmse, 2), "n_holdout": int(len(validos))}


def etapa2_knn_imputacao(spark, df, k=K_NEIGHBORS, holdout_frac=0.2):
    print("2. Imputacao KNN ponderada para municipios sem meta...")

    if df.filter(col("meta_alfabetizacao_2024_imputada").isNull()).count() == 0:
        print("   Nenhum municipio sem meta — KNN nao necessario.")
        return df.drop("meta_por_municipio"), None

    # deficit_total: AVG entre linhas (ano x rede), nao SUM — cada linha usa a
    # populacao TOTAL do municipio, somar entre redes/anos distorceria a
    # feature do KNN (ver ADR-015).
    df.createOrReplaceTempView("vw_full")
    df_target = spark.sql("""
        SELECT id_municipio, nome_municipio, sigla_uf,
               ROUND(AVG(taxa_alfabetizacao), 2) as taxa_media,
               ROUND(AVG(populacao_total), 0) as populacao,
               ROUND(AVG(deficit_absoluto_proxy), 0) as deficit_total
        FROM vw_full
        WHERE meta_alfabetizacao_2024_imputada IS NULL
        GROUP BY id_municipio, nome_municipio, sigla_uf
    """)
    pdf_target = df_target.toPandas()
    print(f"   Municipios para imputar KNN: {len(pdf_target)}")

    df_ref = spark.sql("""
        SELECT id_municipio, sigla_uf,
               ROUND(AVG(taxa_alfabetizacao), 2) as taxa_media,
               ROUND(AVG(populacao_total), 0) as populacao,
               ROUND(AVG(deficit_absoluto_proxy), 0) as deficit_total,
               ROUND(AVG(meta_alfabetizacao_2024_imputada), 2) as meta
        FROM vw_full
        WHERE meta_alfabetizacao_2024_imputada IS NOT NULL
        GROUP BY id_municipio, nome_municipio, sigla_uf
    """)
    pdf_ref = df_ref.toPandas()
    print(f"   Municipios de referencia: {len(pdf_ref)}")

    metricas_validacao = validar_knn_holdout(pdf_ref, k=k, frac_holdout=holdout_frac)

    pdf_target["meta_knn"] = pdf_target.apply(
        lambda r: _knn_predict_por_uf(r, pdf_ref, k), axis=1
    )
    knn_ok = pdf_target["meta_knn"].notna().sum()
    print(f"   KNN imputou {knn_ok}/{len(pdf_target)} municipios")
    print(f"   Media: {pdf_target['meta_knn'].mean():.1f}% | Mediana: {pdf_target['meta_knn'].median():.1f}%")

    pdf_knn = pdf_target[pdf_target["meta_knn"].notna()][["id_municipio", "meta_knn"]]
    df_knn = spark.createDataFrame(pdf_knn)

    df_final = df.drop("meta_por_municipio").join(
        df_knn, on="id_municipio", how="left"
    ).withColumn(
        "meta_alfabetizacao_2024_imputada",
        when(col("meta_alfabetizacao_2024_imputada").isNull(), col("meta_knn"))
        .otherwise(col("meta_alfabetizacao_2024_imputada"))
    ).drop("meta_knn")

    return df_final, metricas_validacao


def run_knn_imputacao(spark, bucket, k=K_NEIGHBORS, holdout_frac=0.2):
    print("=" * 70)
    print("[KNN] IMPUTACAO DE METAS")
    print("=" * 70)

    silver_obt_path = f"{bucket}/silver/alfabetizacao_municipios_obt"
    output_path = f"{bucket}/silver/alfabetizacao_municipios_obt_com_metas_imputadas"
    metrics_path = f"{bucket}/silver/metrics_knn_imputacao.json"

    print(f"Lendo Silver OBT: {silver_obt_path}")
    df = spark.read.parquet(silver_obt_path)
    print(f"Registros: {df.count()}")

    df_propagado = etapa1_propagar_meta_por_municipio(df)
    df_final, metricas_validacao = etapa2_knn_imputacao(spark, df_propagado, k=k, holdout_frac=holdout_frac)

    if metricas_validacao is not None:
        gcs_write_text(spark, metrics_path, json.dumps(metricas_validacao, ensure_ascii=False, indent=2))
        print(f"Metricas de validacao salvas em: {metrics_path}")

    total = df_final.count()
    com_meta = df_final.filter(col("meta_alfabetizacao_2024_imputada").isNotNull()).count()

    print("=" * 70)
    print("[KNN] RESUMO")
    print(f"  Total registros:     {total}")
    print(f"  Com meta (original): {df_final.filter(col(TARGET_COL).isNotNull()).count()}")
    print(f"  Com meta (imputada): {com_meta}")
    print(f"  Cobertura final:     {com_meta/total*100:.1f}%")
    if metricas_validacao is not None:
        print(f"  MAE holdout:         {metricas_validacao['mae']} pontos percentuais")
    print(f"  Salvando em: {output_path}")
    print("=" * 70)

    df_final.write.format("parquet").mode("overwrite").partitionBy("ano", "rede").save(output_path)
    print("[KNN] Imputacao finalizada.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Imputacao KNN de metas para pipeline cloud (Dataproc)")
    parser.add_argument("--bucket", required=True, help="gs://bucket-name (sem trailing slash)")
    parser.add_argument("--k", type=int, default=K_NEIGHBORS, help="Numero de vizinhos do KNN (default: 5)")
    parser.add_argument("--holdout-frac", type=float, default=0.2, help="Fracao de holdout para validacao (default: 0.2)")
    args = parser.parse_args()

    bucket = args.bucket.rstrip("/")
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    run_knn_imputacao(spark, bucket, k=args.k, holdout_frac=args.holdout_frac)

    spark.stop()
    print("KNN finalizado.")
