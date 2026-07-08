import os, sys, json, warnings, logging
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, functions as F

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MLClustering")

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
hadoop_home = os.path.join(project_root, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + os.path.join(hadoop_home, "bin")
if "JAVA_HOME" in os.environ:
    java_bin = os.path.join(os.environ["JAVA_HOME"], "bin")
    if java_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")


def get_spark_session():
    return SparkSession.builder.appName("MLClustering").getOrCreate()


def resolve_paths():
    env = os.environ.get("ENV", "dev")
    base = "datalake_sample" if env == "dev" else "datalake"
    gold_dir = os.path.join(project_root, base, "gold")
    silver_dir = os.path.join(project_root, base, "silver")
    return silver_dir, gold_dir


def load_unified_data(spark, silver_dir):
    path_imputado = os.path.join(silver_dir, "alfabetizacao_municipios_obt_com_metas_imputadas")
    path_enriched = os.path.join(silver_dir, "alfabetizacao_municipios_obt_enriquecido")

    df = spark.read.parquet(path_imputado)
    if "meta_alfabetizacao_2024_imputada" in df.columns:
        df = df.drop("meta_alfabetizacao_2024").withColumnRenamed(
            "meta_alfabetizacao_2024_imputada", "meta_alfabetizacao_2024"
        )

    if os.path.isdir(path_enriched) and any(
        f.endswith(".parquet") for _, _, fs in os.walk(path_enriched) for f in fs
    ):
        df_enr = spark.read.parquet(path_enriched)
        cols_fin = ["despesa_educacao", "gasto_por_habitante_educacao", "custo_por_ponto_alfabetizacao"]
        cols_exist = [c for c in cols_fin if c in df_enr.columns]
        df_base = df_enr.select("id_municipio", "ano", "rede", *cols_exist)
        df = df.join(df_base, on=["id_municipio", "ano", "rede"], how="left")

    logger.info(f"Dataset unificado: {df.count()} linhas, {len(df.columns)} colunas")
    return df


def build_features(df):
    # deficit_absoluto_proxy: AVG entre linhas (ano x rede), nao SUM — cada
    # linha usa a populacao TOTAL do municipio (ver 02_silver_transform.py),
    # somar entre redes/anos contaria essa populacao varias vezes (ate 3x por
    # rede reportada). Ver ADR-015.
    pdf = df.groupBy("id_municipio", "sigla_uf").agg(
        F.max("nome_municipio").alias("nome_municipio"),
        F.round(F.avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"),
        F.round(F.avg("populacao_total"), 0).alias("populacao_total"),
        F.round(F.avg("deficit_absoluto_proxy"), 0).alias("deficit_absoluto_proxy"),
        F.round(F.avg("gasto_por_habitante_educacao"), 2).alias("gasto_per_capita_medio"),
        F.round(F.avg("custo_por_ponto_alfabetizacao"), 2).alias("custo_por_ponto_alfabetizacao_medio"),
    ).toPandas()

    # deficit_per_capita (nao deficit_log) evita dupla contagem de escala: deficit_absoluto_proxy
    # ja correlaciona 0.955 com populacao_total, entao usar log(deficit) + log(populacao) como
    # features separadas conta o tamanho do municipio duas vezes na distancia do K-Means.
    # Identico a dataproc_03_gold.py::build_mart_vulnerabilidade_ml (deficit_per_capita + log_populacao).
    pdf["deficit_per_capita"] = np.where(
        pdf["populacao_total"] > 0,
        pdf["deficit_absoluto_proxy"].fillna(0) / pdf["populacao_total"],
        0.0,
    )
    pdf["populacao_log"] = np.log1p(pdf["populacao_total"].fillna(1))
    pdf["gasto_per_capita_medio"] = pdf["gasto_per_capita_medio"].fillna(pdf["gasto_per_capita_medio"].median())

    feature_cols = ["taxa_alfabetizacao_media", "gasto_per_capita_medio", "deficit_per_capita", "populacao_log"]
    logger.info(f"Features para clustering: {feature_cols}")
    logger.info(f"Total municipios: {len(pdf)}")
    return pdf, feature_cols


def run_clustering(pdf, feature_cols, k=4):
    from sklearn.preprocessing import StandardScaler
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score

    X = pdf[feature_cols].values
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = kmeans.fit_predict(X_scaled)
    sil = silhouette_score(X_scaled, labels)

    pdf["cluster"] = labels
    logger.info(f"K-Means(K={k}): silhouette={sil:.4f}")

    centers = scaler.inverse_transform(kmeans.cluster_centers_)
    for i in range(k):
        logger.info(f"  Cluster {i}: centroide [{feature_cols[0]}={centers[i][0]:.1f}, "
                    f"{feature_cols[1]}={centers[i][1]:.0f}, "
                    f"deficit_per_capita={centers[i][2]:.4f}, pop_log={centers[i][3]:.1f}]")

    return pdf, labels, kmeans, scaler, sil


def profile_clusters(pdf):
    cluster_profile = pdf.groupby("cluster").agg(
        qtd=("id_municipio", "count"),
        taxa_media=("taxa_alfabetizacao_media", "mean"),
        gasto_medio=("gasto_per_capita_medio", "mean"),
        deficit_medio=("deficit_absoluto_proxy", "mean"),
        deficit_pc_medio=("deficit_per_capita", "mean"),
        populacao_media=("populacao_total", "mean"),
    ).round(4).sort_values("cluster")

    logger.info("\nPerfil dos clusters:")
    logger.info(cluster_profile.to_string())

    cluster_names = {}
    for _, row in cluster_profile.iterrows():
        c = int(row.name)
        alta_taxa = row["taxa_media"] >= pdf["taxa_alfabetizacao_media"].median()
        alto_gasto = row["gasto_medio"] >= pdf["gasto_per_capita_medio"].median()
        grande_porte = row["populacao_media"] >= pdf["populacao_total"].median()
        # deficit per capita (nao absoluto) — mesma base usada como feature do K-Means
        alto_deficit = row["deficit_pc_medio"] >= pdf["deficit_per_capita"].median()

        if not alta_taxa and alto_gasto and not alto_deficit:
            nome = f"{c} - Ineficiente (Gasto alto, resultado baixo)"
        elif not alta_taxa and not alto_gasto and alto_deficit:
            nome = f"{c} - Vulneravel (Baixa taxa, alto deficit)"
        elif not alta_taxa and not alto_gasto and not alto_deficit:
            nome = f"{c} - Subinvestido (Baixa taxa, baixo gasto)"
        elif alta_taxa and not alto_gasto:
            nome = f"{c} - Eficiente (Alta taxa, gasto controlado)"
        elif alta_taxa and alto_gasto:
            nome = f"{c} - Alto Gasto (Alta taxa, alto gasto)"
        elif alta_taxa and grande_porte and alto_deficit:
            nome = f"{c} - Metropole (Alta taxa, alto volume)"
        elif not alta_taxa and grande_porte and alto_deficit:
            nome = f"{c} - Critico (Baixa taxa, grande porte)"
        else:
            nome = f"{c} - Perfil Misto"

        cluster_names[c] = nome

    pdf["nome_cluster"] = pdf["cluster"].map(cluster_names)
    return pdf, cluster_profile, cluster_names


def save_clusters(pdf, gold_dir):
    pdf.to_parquet(os.path.join(gold_dir, "agg_clusters_municipios", "dados.parquet"), index=False)
    logger.info(f"Salvo em: {gold_dir}/agg_clusters_municipios")


def main():
    logger.info("=" * 60)
    logger.info("ML - CLUSTERIZACAO DE MUNICIPIOS (K-MEANS)")
    logger.info("=" * 60)

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    silver_dir, gold_dir = resolve_paths()
    os.makedirs(os.path.join(gold_dir, "agg_clusters_municipios"), exist_ok=True)

    df = load_unified_data(spark, silver_dir)
    pdf, feature_cols = build_features(df)
    spark.stop()

    pdf, labels, kmeans, scaler, sil = run_clustering(pdf, feature_cols, k=4)
    pdf, cluster_profile, cluster_names = profile_clusters(pdf)
    save_clusters(pdf, gold_dir)

    logger.info(f"\nSilhouette Score: {sil:.4f}")
    logger.info(f"Clusters gerados: {len(set(labels))}")
    logger.info("Done.")
    return pdf, cluster_names, kmeans, scaler


if __name__ == "__main__":
    main()
