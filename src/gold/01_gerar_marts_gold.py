import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum as spark_sum, min, max, mean, when, isnan, isnull,
    round as spark_round, lit, countDistinct, rand, row_number, percentile_approx
)
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("GoldMartGenerator")

# ---------------------------------------------------------------------------
# Parametros economicos do modelo de custo (ver ADR-012 e ADR-013)
# Mantidos identicos aos de src/cloud/dataproc_03_gold.py para que o script
# local e o pipeline cloud produzam o mesmo numero — ver docs/NUMEROS_RECALCULADOS.md
# ---------------------------------------------------------------------------
CUSTO_PONTO_PER_CAPITA_DEFAULT = 20.0  # R$/habitante/ponto percentual (ADR-012)
FRACAO_POPULACAO_ALFABETIZAVEL = 0.013  # coorte de idade unica ~7 anos (ADR-013)

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))

hadoop_home = os.path.join(project_root, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + os.path.join(hadoop_home, "bin")
if "JAVA_HOME" in os.environ:
    java_bin = os.path.join(os.environ["JAVA_HOME"], "bin")
    if java_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")


def get_spark_session(app_name="GoldMartGenerator"):
    return SparkSession.builder.appName(app_name).getOrCreate()


def resolve_paths():
    env = os.environ.get("ENV", "dev")
    if env == "prod":
        base_silver = os.path.join(project_root, "datalake", "silver")
        gold_dir = os.path.join(project_root, "datalake", "gold")
    else:
        base_silver = os.path.join(project_root, "datalake_sample", "silver")
        gold_dir = os.path.join(project_root, "datalake_sample", "gold")

    silver_obt = os.path.join(base_silver, "alfabetizacao_municipios_obt")
    silver_imputado = os.path.join(base_silver, "alfabetizacao_municipios_obt_com_metas_imputadas")
    silver_enriched = os.path.join(base_silver, "alfabetizacao_municipios_obt_enriquecido")

    return silver_obt, silver_imputado, silver_enriched, gold_dir


def _tem_parquet_valido(path):
    if not os.path.isdir(path):
        return False
    for root, dirs, files in os.walk(path):
        for f in files:
            if f.endswith(".parquet"):
                return True
    return False


def load_silver(spark, silver_obt_path, silver_imputado_path, silver_enriched_path):
    tem_imputado = _tem_parquet_valido(silver_imputado_path)
    tem_siconfi = _tem_parquet_valido(silver_enriched_path)

    if tem_imputado and tem_siconfi:
        logger.info("Combinando metas imputadas (KNN) + dados financeiros (SICONFI)...")
        df_imp = spark.read.parquet(silver_imputado_path)
        df_enr = spark.read.parquet(silver_enriched_path)
        if "meta_alfabetizacao_2024_imputada" in df_imp.columns:
            df_imp = df_imp.drop("meta_alfabetizacao_2024").withColumnRenamed(
                "meta_alfabetizacao_2024_imputada", "meta_alfabetizacao_2024"
            )
        cols_financeiras = ["despesa_educacao", "gasto_por_habitante_educacao",
                            "custo_por_ponto_alfabetizacao"]
        cols_existentes = [c for c in cols_financeiras if c in df_enr.columns]
        df_base = df_enr.select("id_municipio", "ano", "rede", *cols_existentes)
        df = df_imp.join(df_base, on=["id_municipio", "ano", "rede"], how="left")
        logger.info(f"  Merge OK: {df.count()} registros | {len(df.columns)} colunas")
    elif tem_imputado:
        logger.info(f"Metas imputadas via KNN encontradas! Carregando: {silver_imputado_path}")
        df = spark.read.parquet(silver_imputado_path)
        if "meta_alfabetizacao_2024_imputada" in df.columns:
            df = df.drop("meta_alfabetizacao_2024").withColumnRenamed(
                "meta_alfabetizacao_2024_imputada", "meta_alfabetizacao_2024"
            )
        logger.info(f"  Registros: {df.count()} | Colunas: {len(df.columns)}")
    elif tem_siconfi:
        logger.info(f"SICONFI encontrado (sem imputacao). Carregando: {silver_enriched_path}")
        df = spark.read.parquet(silver_enriched_path)
        logger.info(f"  Registros: {df.count()} | Colunas: {len(df.columns)}")
    else:
        logger.info(f"Carregando Silver OBT (original): {silver_obt_path}")
        df = spark.read.parquet(silver_obt_path)
        logger.info(f"  Registros: {df.count()} | Colunas: {len(df.columns)}")
        logger.info("Dica: python src/siconfi/01_ingestao_siconfi.py + src/features/02_imputar_metas_knn.py")

    logger.info(f"  Colunas SICONFI disponiveis: {[c for c in ['despesa_educacao','gasto_por_habitante_educacao','custo_por_ponto_alfabetizacao'] if c in df.columns]}")
    return df, tem_siconfi


def build_mart_uf_indicadores(df):
    logger.info("=" * 60)
    logger.info("MART 1: agg_uf_indicadores — Visao Executiva por UF/ano")
    logger.info("=" * 60)
    logger.info("  Proposito: Relatorio executivo para o ministro. Qual UF")
    logger.info("  esta melhor/pior? Onde o gap entre taxa e meta e maior?")

    cols_meta = [c for c in df.columns if c.startswith("meta_alfabetizacao_")]

    mart = df.groupBy("ano", "sigla_uf").agg(
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"),
        spark_round(percentile_approx("taxa_alfabetizacao", 0.5), 2).alias("taxa_alfabetizacao_mediana"),
        spark_round(min("taxa_alfabetizacao"), 2).alias("taxa_min"),
        spark_round(max("taxa_alfabetizacao"), 2).alias("taxa_max"),
        count("id_municipio").alias("qtd_municipios_analisados"),
        spark_round(spark_sum("deficit_absoluto_proxy"), 0).alias("deficit_total_estimado"),
        spark_round(avg("media_portugues"), 2).alias("media_portugues_media"),
    )

    if "meta_alfabetizacao_2024" in df.columns:
        mart_alvo = df.filter(col("meta_alfabetizacao_2024").isNotNull()) \
            .groupBy("ano", "sigla_uf").agg(
                spark_round(avg("meta_alfabetizacao_2024"), 2).alias("meta_2024_media"),
                countDistinct(when(col("meta_alfabetizacao_2024").isNotNull(), col("id_municipio")))
                    .alias("qtd_municipios_com_meta"),
                spark_round(
                    (count(when(col("taxa_alfabetizacao") >= col("meta_alfabetizacao_2024"), 1))
                     / count("*") * 100), 1
                ).alias("pct_municipios_acima_da_meta")
            )
        mart = mart.join(mart_alvo, ["ano", "sigla_uf"], "left")
        mart = mart.withColumn(
            "gap_meta",
            spark_round(col("taxa_alfabetizacao_media") - col("meta_2024_media"), 2)
        )

    mart = mart.orderBy("ano", "sigla_uf")
    logger.info(f"  Gerado: {mart.count()} linhas")
    return mart


def build_mart_municipio_ranking(df):
    logger.info("=" * 60)
    logger.info("MART 2: agg_municipio_ranking — Ranking de Priorizacao")
    logger.info("=" * 60)
    logger.info("  Proposito: Lista priorizada de municipios para alocacao")
    logger.info("  de recursos. Quanto maior o score, maior a urgencia.")

    df_base = df.select(
        "ano", "id_municipio", "nome_municipio", "sigla_uf",
        "taxa_alfabetizacao", "meta_alfabetizacao_2024", "populacao_total",
        "deficit_absoluto_proxy", "rede"
    ).dropDuplicates(["id_municipio", "ano", "rede"])

    mart = df_base.groupBy("ano", "id_municipio", "sigla_uf").agg(
        max("nome_municipio").alias("nome_municipio"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao"),
        spark_round(avg("meta_alfabetizacao_2024"), 2).alias("meta_alfabetizacao_2024"),
        spark_round(spark_sum("deficit_absoluto_proxy"), 0).alias("deficit_absoluto_proxy"),
        spark_round(avg("populacao_total"), 0).alias("populacao_total")
    )

    mart = mart.withColumn(
        "gap_meta",
        spark_round(col("taxa_alfabetizacao") - col("meta_alfabetizacao_2024"), 2)
    )
    mart = mart.withColumn(
        "status_risco",
        when(col("gap_meta") >= 0, "1 - Meta Atingida (Excelencia)")
        .when(col("gap_meta") >= -10, "2 - Risco Leve (Atencao)")
        .when(col("gap_meta") >= -25, "3 - Risco Moderado (Acao Necessaria)")
        .otherwise("4 - Risco Critico (Abaixo de 75%)")
    )

    # score baseado em severidade por habitante (gap relativo + distancia da meta)
    # Sem peso direto de populacao_total — cidades grandes tem capacidade fiscal
    # propria e nao devem dominar o ranking apenas por escala populacional.
    # deficit_absoluto_proxy correlaciona 0.955 com populacao_total (e' o mesmo
    # proxy de escala) — inclui-lo aqui junto de um peso direto de populacao
    # duplicava o peso de tamanho no score (~60% efetivo, nao os 20% nominais).
    # Formula identica a src/cloud/dataproc_03_gold.py (mesmo mart).
    mart = mart.withColumn(
        "score_prioridade",
        spark_round(
            when(col("gap_meta") < 0, (-col("gap_meta")) / 100.0).otherwise(lit(0)) * 0.6 +
            when(lit(80.0) - col("taxa_alfabetizacao") > 0,
                 (lit(80.0) - col("taxa_alfabetizacao")) / 80.0).otherwise(lit(0)) * 0.4
            , 4
        )
    )

    window_nacional = Window.partitionBy("ano").orderBy(col("score_prioridade").desc())
    window_estadual = Window.partitionBy("ano", "sigla_uf").orderBy(col("score_prioridade").desc())
    mart = mart.withColumn("ranking_nacional", row_number().over(window_nacional))
    mart = mart.withColumn("ranking_uf", row_number().over(window_estadual))

    mart = mart.orderBy("ano", col("score_prioridade").desc())
    logger.info(f"  Gerado: {mart.count()} linhas")
    return mart


def build_mart_rede_indicadores(df):
    logger.info("=" * 60)
    logger.info("MART 3: agg_rede_indicadores — Comparacao por Rede de Ensino")
    logger.info("=" * 60)
    logger.info("  Proposito: Entender diferencas entre rede Municipal,")
    logger.info("  Estadual, Federal e Privada por UF.")

    mart = df.groupBy("ano", "sigla_uf", "rede").agg(
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"),
        count("id_municipio").alias("qtd_registros"),
        spark_round(avg("media_portugues"), 2).alias("media_portugues_media"),
        spark_round(avg("deficit_absoluto_proxy"), 0).alias("deficit_medio")
    )

    mart_total = mart.groupBy("ano", "sigla_uf").agg(
        spark_sum("qtd_registros").alias("total_registros_uf")
    )
    mart = mart.join(mart_total, ["ano", "sigla_uf"], "left")
    mart = mart.withColumn(
        "pct_da_rede",
        spark_round((col("qtd_registros") / col("total_registros_uf")) * 100, 1)
    )

    if "meta_alfabetizacao_2024" in df.columns:
        mart_meta_rede = df.filter(col("meta_alfabetizacao_2024").isNotNull()) \
            .groupBy("ano", "sigla_uf", "rede").agg(
                spark_round(avg("meta_alfabetizacao_2024"), 2).alias("meta_2024_media")
            )
        mart = mart.join(mart_meta_rede, ["ano", "sigla_uf", "rede"], "left")

    mart = mart.orderBy("ano", "sigla_uf", "rede")
    logger.info(f"  Gerado: {mart.count()} linhas")
    return mart


def build_mart_priorizacao(df):
    logger.info("=" * 60)
    logger.info("MART 4: agg_priorizacao — Matriz Equidade vs Eficiencia")
    logger.info("=" * 60)
    logger.info("  Proposito: Classificar municipios em 4 quadrantes para")
    logger.info("  direcionar estrategia de investimento:")
    logger.info("    - Maxima: alta urgencia + alto volume")
    logger.info("    - Equidade: alta urgencia + baixo volume")
    logger.info("    - Eficiencia: baixa urgencia + alto volume")
    logger.info("    - Monitoramento: baixa urgencia + baixo volume")

    df_agg = df.groupBy("id_municipio", "sigla_uf").agg(
        max("nome_municipio").alias("nome_municipio"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"),
        spark_round(spark_sum("deficit_absoluto_proxy"), 0).alias("deficit_absoluto_proxy"),
        spark_round(avg("populacao_total"), 0).alias("populacao_total"),
        countDistinct("ano").alias("anos_com_dado")
    )

    # Porte do municipio — contexto de capacidade fiscal propria
    df_agg = df_agg.withColumn(
        "porte_municipio",
        when(col("populacao_total") >= 500000, "1-Metropole")
        .when(col("populacao_total") >= 100000, "2-Grande")
        .when(col("populacao_total") >= 20000, "3-Medio")
        .otherwise("4-Pequeno")
    )
    # Deficit por habitante — elimina vies de escala populacional (deficit_absoluto_proxy
    # correlaciona 0.955 com populacao_total; usar o valor bruto no quadrante/ranking
    # fazia metropoles dominarem so por tamanho, mesmo com taxa mediana).
    df_agg = df_agg.withColumn(
        "deficit_per_capita",
        spark_round(
            when(col("populacao_total") > 0, col("deficit_absoluto_proxy") / col("populacao_total"))
            .otherwise(lit(0)), 4)
    )

    mediana_deficit_pc = df_agg.approxQuantile("deficit_per_capita", [0.5], 0.01)[0] or 0.5
    mediana_taxa = df_agg.approxQuantile("taxa_alfabetizacao_media", [0.5], 0.01)[0] or 50

    logger.info(f"  Mediana deficit per capita (corte Eficiencia): {mediana_deficit_pc:.4f}")
    logger.info(f"  Mediana taxa (corte Equidade): {mediana_taxa:.1f}%")

    # Quadrantes baseados em taxa e deficit per capita (nao absoluto)
    df_agg = df_agg.withColumn(
        "quadrante",
        when(
            (col("taxa_alfabetizacao_media") < mediana_taxa) &
            (col("deficit_per_capita") >= mediana_deficit_pc),
            "1 - Maxima (Equidade + Eficiencia)"
        )
        .when(
            (col("taxa_alfabetizacao_media") < mediana_taxa) &
            (col("deficit_per_capita") < mediana_deficit_pc),
            "2 - Equidade (Alta Severidade)"
        )
        .when(
            (col("taxa_alfabetizacao_media") >= mediana_taxa) &
            (col("deficit_per_capita") >= mediana_deficit_pc),
            "3 - Eficiencia (Alto Volume)"
        )
        .otherwise("4 - Monitoramento")
    )

    # Ranking por vulnerabilidade: dentro do quadrante, menor taxa = maior urgencia.
    # Metropoles sao penalizadas pois tem capacidade fiscal pra autofinanciar
    # (ISS, transferencias de ICMS) sem depender de repasse federal.
    df_agg = df_agg.withColumn(
        "peso_vulnerabilidade",
        when(col("porte_municipio") == "1-Metropole", lit(0.6))
        .when(col("porte_municipio") == "2-Grande", lit(0.8))
        .otherwise(lit(1.0))
    )
    df_agg = df_agg.withColumn(
        "score_vulnerabilidade",
        spark_round((lit(100.0) - col("taxa_alfabetizacao_media")) / 100.0 * col("peso_vulnerabilidade"), 4)
    )

    window_ranking = Window.orderBy(
        when(col("quadrante").startswith("1"), 1)
        .when(col("quadrante").startswith("2"), 2)
        .when(col("quadrante").startswith("3"), 3)
        .otherwise(4).asc(),
        col("score_vulnerabilidade").desc()
    )
    df_agg = df_agg.withColumn("ranking_prioridade", row_number().over(window_ranking))

    df_agg = df_agg.drop("peso_vulnerabilidade").orderBy("ranking_prioridade")
    logger.info(f"  Gerado: {df_agg.count()} linhas")
    return df_agg


def build_mart_siconfi_uf(df):
    logger.info("=" * 60)
    logger.info("MART 5: agg_siconfi_uf — Gasto vs Resultado por UF")
    logger.info("=" * 60)
    logger.info("  Proposito: Responder a pergunta central do PROBLEM.md:")
    logger.info("  'O municipio que recebe mais repasse por aluno possui")
    logger.info("  a melhor taxa de alfabetizacao?'")

    cols_disponiveis = set(df.columns)
    cols_siconfi = [c for c in ["despesa_educacao", "gasto_por_habitante_educacao",
                                 "custo_por_ponto_alfabetizacao"] if c in cols_disponiveis]

    if not cols_siconfi:
        logger.warning("  Colunas SICONFI nao encontradas. Mart vazio.")
        return None

    agg_exprs = []
    for c in cols_siconfi:
        agg_exprs.append(spark_round(avg(c), 2).alias(f"{c}_medio"))
        agg_exprs.append(spark_sum(c).alias(f"{c}_total"))

    agg_exprs.append(count(when(col("despesa_educacao").isNotNull(), 1))
                     .alias("qtd_municipios_com_dado_fiscal"))
    agg_exprs.append(spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"))

    mart = df.groupBy("ano", "sigla_uf").agg(*agg_exprs)

    if "gasto_por_habitante_educacao" in cols_disponiveis:
        mart = mart.withColumn(
            "eficiencia_gasto",
            spark_round(
                col("taxa_alfabetizacao_media") / col("gasto_por_habitante_educacao_medio"),
                4
            )
        )
    else:
        mart = mart.withColumn("eficiencia_gasto", lit(None))

    mart = mart.orderBy("ano", "sigla_uf")
    logger.info(f"  Gerado: {mart.count()} linhas")
    return mart


def build_mart_eficiencia_financeira(df):
    logger.info("=" * 60)
    logger.info("MART 7: agg_eficiencia_financeira — Eficiencia do Gasto por Municipio")
    logger.info("=" * 60)
    logger.info("  Proposito: Responder 'O municipio que gasta mais tem")
    logger.info("  a melhor taxa?' — ranking de eficiencia do gasto.")

    cols = set(df.columns)
    if not {"gasto_por_habitante_educacao", "custo_por_ponto_alfabetizacao"}.intersection(cols):
        logger.warning("  Colunas SICONFI nao encontradas. Mart vazio.")
        return None

    df_agg = df.groupBy("id_municipio", "sigla_uf").agg(
        max("nome_municipio").alias("nome_municipio"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"),
        spark_round(avg("populacao_total"), 0).alias("populacao_total"),
        spark_round(avg("gasto_por_habitante_educacao"), 2).alias("gasto_per_capita_medio"),
        spark_round(avg("custo_por_ponto_alfabetizacao"), 2).alias("custo_por_ponto_alfabetizacao_medio"),
        spark_round(spark_sum("despesa_educacao"), 2).alias("despesa_educacao_total"),
    )

    med_gasto = df_agg.approxQuantile("gasto_per_capita_medio", [0.5], 0.01)[0] or 1000
    med_taxa = df_agg.approxQuantile("taxa_alfabetizacao_media", [0.5], 0.01)[0] or 50
    logger.info(f"  Mediana gasto per capita: R$ {med_gasto:.2f}")
    logger.info(f"  Mediana taxa: {med_taxa:.1f}%")

    df_agg = df_agg.withColumn(
        "classificacao_eficiencia",
        when((col("taxa_alfabetizacao_media") >= med_taxa) & (col("gasto_per_capita_medio") <= med_gasto), "1 - Eficiente (Alta taxa, Baixo gasto)")
        .when((col("taxa_alfabetizacao_media") >= med_taxa) & (col("gasto_per_capita_medio") > med_gasto), "2 - Alto Gasto (Alta taxa, Alto gasto)")
        .when((col("taxa_alfabetizacao_media") < med_taxa) & (col("gasto_per_capita_medio") <= med_gasto), "3 - Subinvestido (Baixa taxa, Baixo gasto)")
        .otherwise("4 - Ineficiente (Baixa taxa, Alto gasto)")
    )

    window_nac = Window.orderBy(col("custo_por_ponto_alfabetizacao_medio").asc())
    window_uf = Window.partitionBy("sigla_uf").orderBy(col("custo_por_ponto_alfabetizacao_medio").asc())
    df_agg = df_agg.withColumn("rank_eficiencia_nacional", row_number().over(window_nac))
    df_agg = df_agg.withColumn("rank_eficiencia_uf", row_number().over(window_uf))

    df_agg = df_agg.orderBy("rank_eficiencia_nacional")
    logger.info(f"  Gerado: {df_agg.count()} linhas")
    return df_agg


def build_mart_top10_uf(df):
    logger.info("=" * 60)
    logger.info("MART 6: agg_top10_uf — Top 10 Municipios Prioritarios por UF")
    logger.info("=" * 60)
    logger.info("  Proposito: Para cada UF, os 10 municipios com maior")
    logger.info("  score de prioridade. Acao imediata por estado.")

    df_base = df.select(
        "ano", "id_municipio", "nome_municipio", "sigla_uf",
        "taxa_alfabetizacao", "meta_alfabetizacao_2024", "populacao_total",
        "deficit_absoluto_proxy"
    ).dropDuplicates(["id_municipio", "ano"])

    mart = df_base.groupBy("ano", "id_municipio", "sigla_uf").agg(
        max("nome_municipio").alias("nome_municipio"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao"),
        spark_round(avg("meta_alfabetizacao_2024"), 2).alias("meta_alfabetizacao_2024"),
        spark_round(spark_sum("deficit_absoluto_proxy"), 0).alias("deficit_absoluto_proxy"),
        spark_round(avg("populacao_total"), 0).alias("populacao_total")
    )

    mart = mart.withColumn(
        "gap_meta",
        spark_round(col("taxa_alfabetizacao") - col("meta_alfabetizacao_2024"), 2)
    )
    mart = mart.withColumn(
        "status_risco",
        when(col("gap_meta") >= 0, "1 - Meta Atingida (Excelencia)")
        .when(col("gap_meta") >= -10, "2 - Risco Leve (Atencao)")
        .when(col("gap_meta") >= -25, "3 - Risco Moderado (Acao Necessaria)")
        .otherwise("4 - Risco Critico (Abaixo de 75%)")
    )

    # score consistente com agg_municipio_ranking — sem peso direto de populacao
    # (deficit_absoluto_proxy correlaciona 0.955 com populacao_total, dupla
    # contagem). Formula identica a src/cloud/dataproc_03_gold.py.
    mart = mart.withColumn(
        "score_prioridade",
        spark_round(
            when(col("gap_meta") < 0, (-col("gap_meta")) / 100.0).otherwise(lit(0)) * 0.6 +
            when(lit(80.0) - col("taxa_alfabetizacao") > 0,
                 (lit(80.0) - col("taxa_alfabetizacao")) / 80.0).otherwise(lit(0)) * 0.4,
            4
        )
    )

    window_uf_ano = Window.partitionBy("ano", "sigla_uf").orderBy(col("score_prioridade").desc())
    mart = mart.withColumn("rank_uf", row_number().over(window_uf_ano))

    mart = mart.filter(col("rank_uf") <= 10)
    mart = mart.orderBy("ano", "sigla_uf", "rank_uf")

    logger.info(f"  Gerado: {mart.count()} linhas (10 por UF)")
    return mart


def build_mart_custo_ineficiencia(df):
    logger.info("=" * 60)
    logger.info("MART 8: agg_custo_ineficiencia — Custo do Gasto Ineficiente (R$)")
    logger.info("=" * 60)
    logger.info("  Proposito: Colocar valor em R$ na ineficiencia. Quanto")
    logger.info("  cada municipio gasta ACIMA do benchmark eficiente?")

    cols = set(df.columns)
    if "gasto_por_habitante_educacao" not in cols:
        logger.warning("  SICONFI nao disponivel. Mart vazio.")
        return None

    df_agg = df.groupBy("id_municipio", "sigla_uf").agg(
        max("nome_municipio").alias("nome_municipio"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"),
        spark_round(avg("populacao_total"), 0).alias("populacao_total"),
        spark_round(avg("gasto_por_habitante_educacao"), 2).alias("gasto_per_capita_medio"),
        spark_round(spark_sum("despesa_educacao"), 2).alias("despesa_educacao_total"),
    )

    mediana_gasto = df_agg.approxQuantile("gasto_per_capita_medio", [0.5], 0.01)[0] or 1000
    mediana_taxa = df_agg.approxQuantile("taxa_alfabetizacao_media", [0.5], 0.01)[0] or 50

    df_agg = df_agg.withColumn(
        "classificacao",
        when((col("taxa_alfabetizacao_media") >= mediana_taxa) & (col("gasto_per_capita_medio") <= mediana_gasto), "Eficiente")
        .when((col("taxa_alfabetizacao_media") >= mediana_taxa) & (col("gasto_per_capita_medio") > mediana_gasto), "Alto Gasto")
        .when((col("taxa_alfabetizacao_media") < mediana_taxa) & (col("gasto_per_capita_medio") <= mediana_gasto), "Subinvestido")
        .otherwise("Ineficiente")
    )

    benchmark_per_capita = df_agg.filter(col("classificacao") == "Eficiente") \
        .agg(avg("gasto_per_capita_medio")).collect()[0][0] or mediana_gasto

    df_agg = df_agg.withColumn(
        "gasto_excedente_per_capita",
        when(col("gasto_per_capita_medio") > benchmark_per_capita,
             spark_round(col("gasto_per_capita_medio") - benchmark_per_capita, 2))
        .otherwise(lit(0))
    )
    df_agg = df_agg.withColumn(
        "custo_ineficiencia_r1",
        spark_round(col("gasto_excedente_per_capita") * col("populacao_total"), 2)
    )
    # SICONFI DCA ja e anual (Demonstrativo de Contas Anuais)
    # custo_ineficiencia_r1 ja representa o custo anual — nao multiplicar por 2
    df_agg = df_agg.withColumn(
        "custo_ineficiencia_r1_anual",
        spark_round(col("custo_ineficiencia_r1"), 2)  # identico ao semestral — valor ja e anual
    )

    window_uf = Window.partitionBy("sigla_uf").orderBy(col("custo_ineficiencia_r1").desc())
    df_agg = df_agg.withColumn("rank_perda_uf", row_number().over(window_uf))

    df_agg = df_agg.filter(col("classificacao") == "Ineficiente") \
        .orderBy(col("custo_ineficiencia_r1").desc())

    total_waste = df_agg.agg(spark_sum("custo_ineficiencia_r1")).collect()[0][0] or 0
    logger.info(f"  Perda total estimada (Ineficientes): R$ {total_waste:,.2f}")
    logger.info(f"  Gerado: {df_agg.count()} municipios ineficientes")
    return df_agg


def build_mart_projecao_investimento(df, df_eficiencia=None):
    logger.info("=" * 60)
    logger.info("MART 9: agg_projecao_investimento — Custo para Atingir 80% de Alfabetizacao")
    logger.info("=" * 60)
    logger.info("  Proposito: Quanto custaria levar cada municipio a 80% de alfabetizacao?")
    logger.info("  Modelo: custo marginal per capita (ADR-012) x populacao alfabetizavel (ADR-013)")
    logger.info("  Fallback: R$20/hab/ponto (~R$2.000/aluno-ano) se nao ha benchmark SICONFI")

    cols = set(df.columns)
    if "gasto_por_habitante_educacao" not in cols:
        logger.warning("  SICONFI nao disponivel. Mart vazio.")
        return None

    df_agg = df.groupBy("id_municipio", "sigla_uf").agg(
        max("nome_municipio").alias("nome_municipio"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"),
        spark_round(avg("populacao_total"), 0).alias("populacao_total"),
        spark_round(avg("gasto_por_habitante_educacao"), 2).alias("gasto_per_capita_medio"),
        spark_round(avg("custo_por_ponto_alfabetizacao"), 2).alias("custo_por_ponto_observado"),
    )

    # -------------------------------------------------------------------------
    # Benchmark de custo marginal PER CAPITA (R$/hab/ponto percentual) — ADR-012.
    # Derivado da mediana dos municipios Eficientes (classificacao_eficiencia
    # comeca com "1") quando ha SICONFI; caso contrario usa a constante default.
    # -------------------------------------------------------------------------
    custo_ponto_pc = CUSTO_PONTO_PER_CAPITA_DEFAULT
    fonte_benchmark = f"fallback (R${CUSTO_PONTO_PER_CAPITA_DEFAULT}/hab/ponto, ~R$2.000/aluno-ano)"

    if df_eficiencia is not None:
        try:
            eficientes = df_eficiencia.filter(col("classificacao_eficiencia").startswith("1"))
            if "custo_por_ponto_alfabetizacao_medio" in df_eficiencia.columns:
                mediana = eficientes.approxQuantile("custo_por_ponto_alfabetizacao_medio", [0.5], 0.01)
                if mediana and mediana[0] and mediana[0] > 0:
                    custo_ponto_pc = round(mediana[0], 2)
                    fonte_benchmark = f"mediana SICONFI/Eficientes = R${custo_ponto_pc:,.2f}/hab/ponto"
        except Exception as e:
            logger.warning(f"  Erro ao calcular benchmark SICONFI: {e}. Usando fallback.")

    logger.info(f"  Benchmark custo marginal: {fonte_benchmark}")

    df_agg = df_agg.withColumn(
        "gap_ate_80",
        spark_round(lit(80.0) - col("taxa_alfabetizacao_media"), 2)
    )
    df_agg = df_agg.withColumn(
        "gap_ate_80",
        when(col("gap_ate_80") < 0, lit(0)).otherwise(col("gap_ate_80"))
    )
    # populacao_total e a populacao TOTAL do municipio (todos habitantes), nao
    # contagem de alunos do 2o ano — sem a fracao abaixo o custo infla ~77x
    # (ver ADR-013).
    df_agg = df_agg.withColumn(
        "populacao_alfabetizavel_estimada",
        spark_round(col("populacao_total") * lit(FRACAO_POPULACAO_ALFABETIZAVEL), 0)
    )
    df_agg = df_agg.withColumn(
        "custo_estimado_para_atingir_80",
        spark_round(col("gap_ate_80") * lit(custo_ponto_pc) * col("populacao_alfabetizavel_estimada"), 2)
    )
    df_agg = df_agg.withColumn(
        "custo_per_capta_atingir_80",
        when(col("populacao_total") > 0,
             spark_round(col("custo_estimado_para_atingir_80") / col("populacao_total"), 2))
        .otherwise(lit(0))
    )
    df_agg = df_agg.withColumn(
        "categoria_investimento",
        when(col("custo_estimado_para_atingir_80") <= 500000, "1 - Baixo (<R$500k)")
        .when(col("custo_estimado_para_atingir_80") <= 5000000, "2 - Medio (R$500k-R$5M)")
        .when(col("custo_estimado_para_atingir_80") <= 50000000, "3 - Alto (R$5M-R$50M)")
        .otherwise("4 - Muito Alto (>R$50M)")
    )
    # Coluna de rastreabilidade: mostra qual benchmark foi usado neste calculo
    df_agg = df_agg.withColumn("benchmark_custo_ponto_per_capita", lit(round(custo_ponto_pc, 2)))

    df_agg = df_agg.filter(col("gap_ate_80") > 0) \
        .orderBy(col("custo_estimado_para_atingir_80").desc())

    total_necessario = df_agg.agg(spark_sum("custo_estimado_para_atingir_80")).collect()[0][0] or 0
    logger.info(f"  Benchmark fonte: {fonte_benchmark}")
    logger.info(f"  Investimento total necessario: R$ {total_necessario:,.2f}")
    logger.info(f"  Gerado: {df_agg.count()} municipios abaixo de 80%")
    return df_agg


def build_mart_alunos_municipios(spark, silver_final_path):
    logger.info("=" * 60)
    logger.info("MART 14: agg_alunos_municipios — Microdados vs INEP")
    logger.info("=" * 60)
    logger.info("  Propósito: Validação e cobertura. Compara a taxa de")
    logger.info("  alfabetização calculada dos microdados com a publicada")
    logger.info("  pelo INEP. Revela cobertura de avaliação e consistência.")

    df = spark.read.parquet(silver_final_path)

    colunas_micro = [
        "taxa_alunos_alfabetizados_microdados",
        "proficiencia_media_microdados",
        "qtd_alunos_avaliados",
        "qtd_escolas_avaliadas",
        "delta_taxa_micro_vs_inep",
    ]
    micro_disponiveis = [c for c in colunas_micro if c in df.columns]
    if not micro_disponiveis:
        logger.warning("  Silver final sem colunas de microdados — execute 06_alunos_bronze_to_silver.py primeiro.")
        return None

    colunas_base = ["ano", "id_municipio", "sigla_uf", "nome_municipio", "rede", "taxa_alfabetizacao"]
    colunas_select = [c for c in colunas_base if c in df.columns] + micro_disponiveis

    mart = df.select(*colunas_select) \
        .filter(col("taxa_alunos_alfabetizados_microdados").isNotNull()) \
        .withColumn("abs_delta", spark_round(
            when(col("delta_taxa_micro_vs_inep") < 0, -col("delta_taxa_micro_vs_inep"))
            .otherwise(col("delta_taxa_micro_vs_inep")),
            2
        )) \
        .withColumn("status_consistencia",
            when(col("abs_delta") <= 1.0, "Consistente")
            .when(col("abs_delta") <= 3.0, "Atenção")
            .otherwise("Inconsistente")
        ) \
        .orderBy(col("abs_delta").desc())

    mart = mart.drop("abs_delta")
    logger.info(f"  Gerado: {mart.count()} registros com microdados")
    return mart


def save_mart(mart, gold_dir, mart_name, partition_col="ano"):
    output_path = os.path.join(gold_dir, mart_name)

    if not mart.columns:
        logger.warning(f"  Mart {mart_name} vazio — pulando.")
        return

    writer = mart.write.format("parquet").mode("overwrite")

    if partition_col and partition_col in mart.columns:
        writer = writer.partitionBy(partition_col)

    writer.save(output_path)

    mart_count = mart.count()
    mart_cols = len(mart.columns)
    size_mb = 0
    for root, dirs, files in os.walk(output_path):
        for f in files:
            fp = os.path.join(root, f)
            if f.endswith(".parquet"):
                size_mb += os.path.getsize(fp)

    logger.info(f"  Salvo: {output_path}")
    logger.info(f"    Registros: {mart_count} | Colunas: {mart_cols} | "
                f"Tamanho: {size_mb / 1024:.1f} KB")


def run_gold_generation():
    logger.info("=" * 70)
    logger.info("GERACAO DE MARTS DA CAMADA GOLD (GOLD MART GENERATOR)")
    logger.info("=" * 70)

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    env = os.environ.get("ENV", "dev")
    logger.info(f"Ambiente: ENV={env}")

    silver_obt_path, silver_imputado_path, silver_enriched_path, gold_dir = resolve_paths()
    os.makedirs(gold_dir, exist_ok=True)

    silver_final_path = os.path.join(os.path.dirname(silver_obt_path), "alfabetizacao_municipios_obt_final")

    df, tem_siconfi = load_silver(spark, silver_obt_path, silver_imputado_path, silver_enriched_path)

    logger.info("\n")
    marts = {}

    mart1 = build_mart_uf_indicadores(df)
    marts["agg_uf_indicadores"] = mart1

    mart2 = build_mart_municipio_ranking(df)
    marts["agg_municipio_ranking"] = mart2

    mart3 = build_mart_rede_indicadores(df)
    marts["agg_rede_indicadores"] = mart3

    mart4 = build_mart_priorizacao(df)
    marts["agg_priorizacao"] = mart4

    if tem_siconfi:
        mart5 = build_mart_siconfi_uf(df)
        if mart5 is not None:
            marts["agg_siconfi_uf"] = mart5
    else:
        logger.info("MART 5: agg_siconfi_uf — PULADO (SICONFI nao disponivel)")

    mart6 = build_mart_top10_uf(df)
    marts["agg_top10_uf"] = mart6

    if tem_siconfi:
        mart7 = build_mart_eficiencia_financeira(df)
        if mart7 is not None:
            marts["agg_eficiencia_financeira"] = mart7

    if tem_siconfi:
        mart8 = build_mart_custo_ineficiencia(df)
        if mart8 is not None:
            marts["agg_custo_ineficiencia"] = mart8

    if tem_siconfi:
        # Passa mart7 para calibrar benchmark pelo custo real dos Eficientes (SICONFI)
        df_eficiencia_para_benchmark = marts.get("agg_eficiencia_financeira", None)
        mart9 = build_mart_projecao_investimento(df, df_eficiencia=df_eficiencia_para_benchmark)
        if mart9 is not None:
            marts["agg_projecao_investimento"] = mart9

    tem_microdados = _tem_parquet_valido(silver_final_path)
    if tem_microdados:
        mart14 = build_mart_alunos_municipios(spark, silver_final_path)
        if mart14 is not None:
            marts["agg_alunos_municipios"] = mart14
    else:
        logger.info("MART 14: agg_alunos_municipios — PULADO (execute 06_alunos_bronze_to_silver.py primeiro)")

    logger.info("\n" + "=" * 70)
    logger.info("SALVANDO MARTS NO DATALAKE")
    logger.info("=" * 70)

    for mart_name, mart_df in marts.items():
        save_mart(mart_df, gold_dir, mart_name)

    logger.info("\n" + "=" * 70)
    logger.info("RESUMO DA CAMADA GOLD ENRIQUECIDA")
    logger.info("=" * 70)
    logger.info(f"  Gold em: {gold_dir}")
    for mart_name, mart_df in marts.items():
        logger.info(f"    {mart_name:35s} {mart_df.count():>6} linhas  {len(mart_df.columns):>2} colunas")
    logger.info("=" * 70)

    spark.stop()


if __name__ == "__main__":
    run_gold_generation()
