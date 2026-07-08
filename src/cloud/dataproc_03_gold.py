"""
Gold marts generation for Dataproc/GCS.
Usage: pyspark --bucket gs://bucket-name

Generates marts (16 total):
  Base (sempre): agg_uf_indicadores, agg_evolucao_temporal, agg_municipio_ranking,
                 agg_rede_indicadores, agg_priorizacao, agg_top10_uf,
                 agg_clusters_municipios, agg_vulnerabilidade_ml,
                 agg_alocacao_otima, agg_qualidade_resumo
  SICONFI (condicional): agg_eficiencia_financeira, agg_custo_ineficiencia,
                         agg_projecao_investimento, agg_correlacoes_uf,
                         agg_roi_executivo, agg_alocacao_otima_estrategias
"""
import argparse
import sys
import traceback
sys.path.insert(0, "/tmp/scripts")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, count, sum as spark_sum, min, max, when, lit,
    round as spark_round, countDistinct, row_number, percentile_approx, lag
)
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Parametros economicos do modelo de custo (ver ADR-012)
# ---------------------------------------------------------------------------
# Custo marginal de referencia para elevar a alfabetizacao: R$ por habitante
# por ponto percentual. Usado como fallback quando nao ha dados SICONFI para
# derivar o benchmark empirico dos municipios eficientes.
# Racional: metrica PER CAPITA (nao absoluta) para eliminar distorcao de escala
# populacional — municipios grandes nao devem parecer "caros" so por tamanho.
CUSTO_PONTO_PER_CAPITA_DEFAULT = 20.0  # R$/habitante/ponto percentual

# Fracao da populacao TOTAL do municipio estimada como coorte de idade unica
# (~7 anos, 2o ano do ensino fundamental) — ver ADR-013. Sem isso, o custo em
# R$ multiplica gap% x populacao_total (todos os habitantes), inflando o
# resultado em ~77x (populacao_total nao e contagem de alunos).
FRACAO_POPULACAO_ALFABETIZAVEL = 0.013

# Orcamento federal hipotetico para o cenario de alocacao otima (ADR-010).
ORCAMENTO_ALOCACAO = 500_000_000  # R$ 500 milhoes


def get_spark():
    return SparkSession.builder.appName("GoldMarts-GCS").getOrCreate()


def safe_quantile(df, col_name, quantile=0.5, default=1.0):
    """approxQuantile retorna [] quando coluna é toda null — retorna default nesse caso."""
    result = df.approxQuantile(col_name, [quantile], 0.01)
    return (result[0] if result else None) or default


def safe_build(mart_name, builder, *args, **kwargs):
    """Executa um builder de mart isoladamente.

    Uma excecao em um mart (ex: KMeans.fit falhar por nulos nas features)
    nao pode derrubar o job inteiro nem ser engolida silenciosamente pelo
    save_mart (que so checa 'is None'). Aqui o traceback completo vai pro
    log do Dataproc e o pipeline segue para os demais marts.
    """
    try:
        return builder(*args, **kwargs)
    except Exception:
        print(f"  [ERRO] {mart_name}: excecao na construcao do mart")
        traceback.print_exc()
        return None


def gcs_path_exists(spark, path):
    try:
        sc = spark.sparkContext
        Path = sc._jvm.org.apache.hadoop.fs.Path
        fs = Path(path).getFileSystem(sc._jsc.hadoopConfiguration())
        return fs.exists(Path(path))
    except Exception:
        return False


def load_silver(spark, silver_dir):
    silver_obt = f"{silver_dir}/alfabetizacao_municipios_obt"
    silver_enriched = f"{silver_dir}/alfabetizacao_municipios_obt_enriquecido"
    silver_imputado = f"{silver_dir}/alfabetizacao_municipios_obt_com_metas_imputadas"

    tem_imputado = gcs_path_exists(spark, silver_imputado)
    tem_siconfi = gcs_path_exists(spark, silver_enriched)

    if tem_imputado and tem_siconfi:
        print("Usando: metas imputadas KNN + SICONFI")
        df_imp = spark.read.parquet(silver_imputado)
        df_enr = spark.read.parquet(silver_enriched)
        if "meta_alfabetizacao_2024_imputada" in df_imp.columns:
            df_imp = df_imp.drop("meta_alfabetizacao_2024") \
                           .withColumnRenamed("meta_alfabetizacao_2024_imputada", "meta_alfabetizacao_2024")
        cols_fin = [c for c in ["despesa_educacao", "gasto_por_habitante_educacao", "custo_por_ponto_alfabetizacao"]
                    if c in df_enr.columns]
        df_base = df_enr.select("id_municipio", "ano", "rede", *cols_fin)
        df = df_imp.join(df_base, on=["id_municipio", "ano", "rede"], how="left")
    elif tem_imputado:
        print("Usando: metas imputadas KNN")
        df = spark.read.parquet(silver_imputado)
        if "meta_alfabetizacao_2024_imputada" in df.columns:
            df = df.drop("meta_alfabetizacao_2024") \
                   .withColumnRenamed("meta_alfabetizacao_2024_imputada", "meta_alfabetizacao_2024")
    elif tem_siconfi:
        print("Usando: Silver + SICONFI")
        df = spark.read.parquet(silver_enriched)
    else:
        print("Usando: Silver OBT base")
        df = spark.read.parquet(silver_obt)

    tem_siconfi_cols = "despesa_educacao" in df.columns
    print(f"Silver carregado: {df.count()} registros | SICONFI={tem_siconfi_cols}")
    return df, tem_siconfi_cols


def build_mart_uf_indicadores(df):
    print("MART 1: agg_uf_indicadores — panorama por UF e ano (taxa, meta e gap de alfabetizacao)")
    mart = df.groupBy("ano", "sigla_uf").agg(
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"),
        spark_round(percentile_approx("taxa_alfabetizacao", 0.5), 2).alias("taxa_alfabetizacao_mediana"),
        spark_round(min("taxa_alfabetizacao"), 2).alias("taxa_min"),
        spark_round(max("taxa_alfabetizacao"), 2).alias("taxa_max"),
        countDistinct("id_municipio").alias("qtd_municipios_analisados"),
        spark_round(spark_sum("deficit_absoluto_proxy"), 0).alias("deficit_total_estimado"),
        spark_round(avg("media_portugues"), 2).alias("media_portugues_media"),
    )
    if "meta_alfabetizacao_2024" in df.columns:
        mart_alvo = df.filter(col("meta_alfabetizacao_2024").isNotNull()) \
            .groupBy("ano", "sigla_uf").agg(
                spark_round(avg("meta_alfabetizacao_2024"), 2).alias("meta_2024_media"),
                countDistinct(when(col("meta_alfabetizacao_2024").isNotNull(), col("id_municipio"))).alias("qtd_municipios_com_meta"),
                spark_round((count(when(col("taxa_alfabetizacao") >= col("meta_alfabetizacao_2024"), 1)) / count("*") * 100), 1).alias("pct_municipios_acima_da_meta")
            )
        mart = mart.join(mart_alvo, ["ano", "sigla_uf"], "left")
        mart = mart.withColumn("gap_meta", spark_round(col("taxa_alfabetizacao_media") - col("meta_2024_media"), 2))
    return mart.orderBy("ano", "sigla_uf")


def build_mart_evolucao_temporal(df):
    """Evolucao temporal do indicador de alfabetizacao por UF (exemplo explicito
    do enunciado: 'Evolucao temporal do indicador'). Calcula a taxa media por
    UF/ano e a variacao ano-a-ano (pontos percentuais e %) via window lag."""
    print("MART: agg_evolucao_temporal — evolucao ano a ano da taxa de alfabetizacao por UF")
    base = df.groupBy("ano", "sigla_uf").agg(
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"),
        countDistinct("id_municipio").alias("qtd_municipios"),
        spark_round(spark_sum("deficit_absoluto_proxy"), 0).alias("deficit_total_estimado"),
    )
    w = Window.partitionBy("sigla_uf").orderBy("ano")
    base = base.withColumn("taxa_ano_anterior", lag("taxa_alfabetizacao_media").over(w))
    base = base.withColumn("variacao_pp",
        spark_round(col("taxa_alfabetizacao_media") - col("taxa_ano_anterior"), 2))
    base = base.withColumn("variacao_pct",
        when(col("taxa_ano_anterior") > 0,
             spark_round((col("taxa_alfabetizacao_media") - col("taxa_ano_anterior"))
                         / col("taxa_ano_anterior") * 100, 2))
        .otherwise(lit(None)))
    base = base.withColumn("tendencia",
        when(col("variacao_pp") > 0, "1 - Melhora")
        .when(col("variacao_pp") < 0, "2 - Piora")
        .when(col("variacao_pp") == 0, "3 - Estavel")
        .otherwise("4 - Sem base comparativa")
    )
    return base.orderBy("sigla_uf", "ano")


def build_mart_municipio_ranking(df):
    print("MART 2: agg_municipio_ranking — ranking nacional de urgencia por municipio")
    df_base = df.select("ano", "id_municipio", "nome_municipio", "sigla_uf",
                        "taxa_alfabetizacao", "meta_alfabetizacao_2024",
                        "populacao_total", "deficit_absoluto_proxy", "rede") \
                .dropDuplicates(["id_municipio", "ano", "rede"])

    mart = df_base.groupBy("ano", "id_municipio", "sigla_uf").agg(
        max("nome_municipio").alias("nome_municipio"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao"),
        spark_round(avg("meta_alfabetizacao_2024"), 2).alias("meta_alfabetizacao_2024"),
        spark_round(spark_sum("deficit_absoluto_proxy"), 0).alias("deficit_absoluto_proxy"),
        spark_round(avg("populacao_total"), 0).alias("populacao_total")
    )
    mart = mart.withColumn("gap_meta", spark_round(col("taxa_alfabetizacao") - col("meta_alfabetizacao_2024"), 2))
    mart = mart.withColumn("status_risco",
        when(col("gap_meta") >= 0, "1 - Meta Atingida (Excelencia)")
        .when(col("gap_meta") >= -10, "2 - Risco Leve (Atencao)")
        .when(col("gap_meta") >= -25, "3 - Risco Moderado (Acao Necessaria)")
        .otherwise("4 - Risco Critico (Abaixo de 75%)")
    )
    min_gap = mart.select(min("gap_meta")).collect()[0][0] or -100
    # score baseado em severidade por habitante (gap relativo + distância da meta)
    # Removido peso direto de populacao_total — cidades grandes têm capacidade fiscal própria
    # e não devem dominar o ranking apenas por escala populacional
    mart = mart.withColumn("score_prioridade",
        spark_round(
            when(col("gap_meta") < 0, (-col("gap_meta")) / 100.0).otherwise(lit(0)) * 0.6 +
            when(lit(80.0) - col("taxa_alfabetizacao") > 0,
                 (lit(80.0) - col("taxa_alfabetizacao")) / 80.0).otherwise(lit(0)) * 0.4,
            4
        )
    )
    window_nac = Window.partitionBy("ano").orderBy(col("score_prioridade").desc())
    window_uf = Window.partitionBy("ano", "sigla_uf").orderBy(col("score_prioridade").desc())
    mart = mart.withColumn("ranking_nacional", row_number().over(window_nac))
    mart = mart.withColumn("ranking_uf", row_number().over(window_uf))
    mart = mart.withColumn("bucket_qualidade",
        when(col("taxa_alfabetizacao") < 25, "1-Critico")
        .when(col("taxa_alfabetizacao") < 50, "2-Ruim")
        .when(col("taxa_alfabetizacao") < 75, "3-Razoavel")
        .otherwise("4-Excelente")
    )
    return mart.orderBy("ano", col("score_prioridade").desc())


def build_mart_rede_indicadores(df):
    print("MART 3: agg_rede_indicadores — comparacao entre redes de ensino (Municipal, Estadual, Federal, Privada)")
    mart = df.groupBy("ano", "sigla_uf", "rede").agg(
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"),
        count("id_municipio").alias("qtd_registros"),
        spark_round(avg("media_portugues"), 2).alias("media_portugues_media"),
        spark_round(avg("deficit_absoluto_proxy"), 0).alias("deficit_medio")
    )
    mart_total = mart.groupBy("ano", "sigla_uf").agg(spark_sum("qtd_registros").alias("total_registros_uf"))
    mart = mart.join(mart_total, ["ano", "sigla_uf"], "left")
    mart = mart.withColumn("pct_da_rede", spark_round((col("qtd_registros") / col("total_registros_uf")) * 100, 1))
    if "meta_alfabetizacao_2024" in df.columns:
        mart_meta = df.filter(col("meta_alfabetizacao_2024").isNotNull()) \
            .groupBy("ano", "sigla_uf", "rede").agg(spark_round(avg("meta_alfabetizacao_2024"), 2).alias("meta_2024_media"))
        mart = mart.join(mart_meta, ["ano", "sigla_uf", "rede"], "left")
    return mart.orderBy("ano", "sigla_uf", "rede")


def build_mart_priorizacao(df):
    print("MART 4: agg_priorizacao — matriz equidade x eficiencia: onde investir rende mais impacto social")
    df_agg = df.groupBy("id_municipio", "sigla_uf").agg(
        max("nome_municipio").alias("nome_municipio"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"),
        spark_round(avg("deficit_absoluto_proxy"), 0).alias("deficit_absoluto_proxy"),
        spark_round(avg("populacao_total"), 0).alias("populacao_total"),
        countDistinct("ano").alias("anos_com_dado")
    )
    # Porte do município — contexto de capacidade fiscal própria
    df_agg = df_agg.withColumn("porte_municipio",
        when(col("populacao_total") >= 500000, "1-Metropole")
        .when(col("populacao_total") >= 100000, "2-Grande")
        .when(col("populacao_total") >= 20000,  "3-Medio")
        .otherwise("4-Pequeno")
    )
    # Déficit por habitante — elimina viés de escala populacional
    df_agg = df_agg.withColumn("deficit_per_capita",
        spark_round(
            when(col("populacao_total") > 0, col("deficit_absoluto_proxy") / col("populacao_total"))
            .otherwise(lit(0)), 4)
    )
    mediana_taxa = safe_quantile(df_agg, "taxa_alfabetizacao_media", default=50.0)
    mediana_deficit_pc = safe_quantile(df_agg, "deficit_per_capita", default=0.5)
    # Quadrantes baseados em taxa e déficit per capita (não absoluto)
    df_agg = df_agg.withColumn("quadrante",
        when((col("taxa_alfabetizacao_media") < mediana_taxa) & (col("deficit_per_capita") >= mediana_deficit_pc), "1 - Maxima (Equidade + Eficiencia)")
        .when((col("taxa_alfabetizacao_media") < mediana_taxa) & (col("deficit_per_capita") < mediana_deficit_pc),  "2 - Equidade (Alta Severidade)")
        .when((col("taxa_alfabetizacao_media") >= mediana_taxa) & (col("deficit_per_capita") >= mediana_deficit_pc), "3 - Eficiencia (Alto Volume)")
        .otherwise("4 - Monitoramento")
    )
    # Ranking por vulnerabilidade: dentro do quadrante, menor taxa = maior urgência
    # Metropoles são penalizadas pois têm capacidade fiscal para autofinanciar (ISS, transferências)
    df_agg = df_agg.withColumn("peso_vulnerabilidade",
        when(col("porte_municipio") == "1-Metropole", lit(0.6))
        .when(col("porte_municipio") == "2-Grande",   lit(0.8))
        .otherwise(lit(1.0))
    )
    df_agg = df_agg.withColumn("score_vulnerabilidade",
        spark_round((lit(100.0) - col("taxa_alfabetizacao_media")) / 100.0 * col("peso_vulnerabilidade"), 4)
    )
    window_rank = Window.orderBy(
        when(col("quadrante").startswith("1"), 1).when(col("quadrante").startswith("2"), 2)
        .when(col("quadrante").startswith("3"), 3).otherwise(4).asc(),
        col("score_vulnerabilidade").desc()
    )
    df_agg = df_agg.withColumn("ranking_prioridade", row_number().over(window_rank))
    return df_agg.drop("peso_vulnerabilidade").orderBy("ranking_prioridade")


def build_mart_top10_uf(df):
    print("MART 6: agg_top10_uf — os 10 municipios mais prioritarios de cada UF")
    df_base = df.select("ano", "id_municipio", "nome_municipio", "sigla_uf",
                        "taxa_alfabetizacao", "meta_alfabetizacao_2024",
                        "populacao_total", "deficit_absoluto_proxy") \
                .dropDuplicates(["id_municipio", "ano"])
    mart = df_base.groupBy("ano", "id_municipio", "sigla_uf").agg(
        max("nome_municipio").alias("nome_municipio"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao"),
        spark_round(avg("meta_alfabetizacao_2024"), 2).alias("meta_alfabetizacao_2024"),
        spark_round(spark_sum("deficit_absoluto_proxy"), 0).alias("deficit_absoluto_proxy"),
        spark_round(avg("populacao_total"), 0).alias("populacao_total")
    )
    mart = mart.withColumn("gap_meta", spark_round(col("taxa_alfabetizacao") - col("meta_alfabetizacao_2024"), 2))
    mart = mart.withColumn("status_risco",
        when(col("gap_meta") >= 0, "1 - Meta Atingida (Excelencia)")
        .when(col("gap_meta") >= -10, "2 - Risco Leve (Atencao)")
        .when(col("gap_meta") >= -25, "3 - Risco Moderado (Acao Necessaria)")
        .otherwise("4 - Risco Critico (Abaixo de 75%)")
    )
    min_gap = mart.select(min("gap_meta")).collect()[0][0] or -100
    # score consistente com agg_municipio_ranking: urgência baseada em gap relativo
    mart = mart.withColumn("score_prioridade",
        spark_round(
            when(col("gap_meta") < 0, (-col("gap_meta")) / 100.0).otherwise(lit(0)) * 0.6 +
            when(lit(80.0) - col("taxa_alfabetizacao") > 0,
                 (lit(80.0) - col("taxa_alfabetizacao")) / 80.0).otherwise(lit(0)) * 0.4,
            4
        )
    )
    window_uf = Window.partitionBy("ano", "sigla_uf").orderBy(col("score_prioridade").desc())
    mart = mart.withColumn("rank_uf", row_number().over(window_uf))
    return mart.filter(col("rank_uf") <= 10).orderBy("ano", "sigla_uf", "rank_uf")


def build_mart_eficiencia_financeira(df):
    print("MART 7: agg_eficiencia_financeira — classificacao de cada municipio por eficiencia do gasto")
    if "gasto_por_habitante_educacao" not in df.columns:
        return None
    df_agg = df.groupBy("id_municipio", "sigla_uf").agg(
        max("nome_municipio").alias("nome_municipio"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"),
        spark_round(avg("populacao_total"), 0).alias("populacao_total"),
        spark_round(avg("gasto_por_habitante_educacao"), 2).alias("gasto_per_capita_medio"),
        spark_round(avg("custo_por_ponto_alfabetizacao"), 2).alias("custo_por_ponto_alfabetizacao_medio"),
        spark_round(spark_sum("despesa_educacao"), 2).alias("despesa_educacao_total"),
    )
    med_gasto = safe_quantile(df_agg, "gasto_per_capita_medio", default=1000.0)
    med_taxa = safe_quantile(df_agg, "taxa_alfabetizacao_media", default=50.0)
    df_agg = df_agg.withColumn("classificacao_eficiencia",
        when((col("taxa_alfabetizacao_media") >= med_taxa) & (col("gasto_per_capita_medio") <= med_gasto), "1 - Eficiente (Alta taxa, Baixo gasto)")
        .when((col("taxa_alfabetizacao_media") >= med_taxa) & (col("gasto_per_capita_medio") > med_gasto), "2 - Alto Gasto (Alta taxa, Alto gasto)")
        .when((col("taxa_alfabetizacao_media") < med_taxa) & (col("gasto_per_capita_medio") <= med_gasto), "3 - Subinvestido (Baixa taxa, Baixo gasto)")
        .otherwise("4 - Ineficiente (Baixa taxa, Alto gasto)")
    )
    window_nac = Window.orderBy(col("custo_por_ponto_alfabetizacao_medio").asc())
    window_uf = Window.partitionBy("sigla_uf").orderBy(col("custo_por_ponto_alfabetizacao_medio").asc())
    df_agg = df_agg.withColumn("rank_eficiencia_nacional", row_number().over(window_nac))
    df_agg = df_agg.withColumn("rank_eficiencia_uf", row_number().over(window_uf))
    return df_agg.orderBy("rank_eficiencia_nacional")


def build_mart_custo_ineficiencia(df):
    print("MART 8: agg_custo_ineficiencia — quanto do orcamento ja existente esta sendo desperdicado por ma gestao")
    if "gasto_por_habitante_educacao" not in df.columns:
        return None
    df_agg = df.groupBy("id_municipio", "sigla_uf").agg(
        max("nome_municipio").alias("nome_municipio"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"),
        spark_round(avg("populacao_total"), 0).alias("populacao_total"),
        spark_round(avg("gasto_por_habitante_educacao"), 2).alias("gasto_per_capita_medio"),
        spark_round(spark_sum("despesa_educacao"), 2).alias("despesa_educacao_total"),
    )
    mediana_gasto = safe_quantile(df_agg, "gasto_per_capita_medio", default=1000.0)
    mediana_taxa = safe_quantile(df_agg, "taxa_alfabetizacao_media", default=50.0)
    df_agg = df_agg.withColumn("classificacao",
        when((col("taxa_alfabetizacao_media") >= mediana_taxa) & (col("gasto_per_capita_medio") <= mediana_gasto), "Eficiente")
        .when((col("taxa_alfabetizacao_media") >= mediana_taxa) & (col("gasto_per_capita_medio") > mediana_gasto), "Alto Gasto")
        .when((col("taxa_alfabetizacao_media") < mediana_taxa) & (col("gasto_per_capita_medio") <= mediana_gasto), "Subinvestido")
        .otherwise("Ineficiente")
    )
    benchmark = df_agg.filter(col("classificacao") == "Eficiente").agg(avg("gasto_per_capita_medio")).collect()[0][0] or mediana_gasto
    df_agg = df_agg.withColumn("gasto_excedente_per_capita",
        when(col("gasto_per_capita_medio") > benchmark, spark_round(col("gasto_per_capita_medio") - benchmark, 2))
        .otherwise(lit(0))
    )
    df_agg = df_agg.withColumn("custo_ineficiencia_r1", spark_round(col("gasto_excedente_per_capita") * col("populacao_total"), 2))
    df_agg = df_agg.withColumn("custo_ineficiencia_r1_anual", spark_round(col("custo_ineficiencia_r1"), 2))
    window_uf = Window.partitionBy("sigla_uf").orderBy(col("custo_ineficiencia_r1").desc())
    df_agg = df_agg.withColumn("rank_perda_uf", row_number().over(window_uf))
    return df_agg.filter(col("classificacao") == "Ineficiente").orderBy(col("custo_ineficiencia_r1").desc())


def resolve_custo_marginal_benchmark(df_eficiencia):
    """Deriva o benchmark de custo marginal PER CAPITA (R$/hab/ponto) — ver ADR-012.

    Mediana dos municipios EFICIENTES (classificacao_eficiencia comeca com "1")
    quando ha SICONFI; caso contrario, a constante default documentada. Usado
    tanto por agg_projecao_investimento quanto por agg_alocacao_otima — as duas
    marts precisam do MESMO custo por municipio para os numeros serem
    reconciliaveis entre si (o knapsack nao pode usar um custo diferente do que
    a projecao de investimento reporta para o mesmo municipio).
    """
    custo_ponto_pc = CUSTO_PONTO_PER_CAPITA_DEFAULT
    if df_eficiencia is not None:
        try:
            eficientes = df_eficiencia.filter(col("classificacao_eficiencia").startswith("1"))
            if "custo_por_ponto_alfabetizacao_medio" in df_eficiencia.columns:
                mediana = eficientes.approxQuantile("custo_por_ponto_alfabetizacao_medio", [0.5], 0.01)
                if mediana and mediana[0] and mediana[0] > 0:
                    custo_ponto_pc = round(mediana[0], 2)
        except Exception:
            pass
    return custo_ponto_pc


def build_mart_projecao_investimento(df, df_eficiencia=None):
    print("MART 9: agg_projecao_investimento — quanto custaria levar cada municipio a 80% de alfabetizacao")
    if "gasto_por_habitante_educacao" not in df.columns:
        return None
    df_agg = df.groupBy("id_municipio", "sigla_uf").agg(
        max("nome_municipio").alias("nome_municipio"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"),
        spark_round(avg("populacao_total"), 0).alias("populacao_total"),
        spark_round(avg("gasto_por_habitante_educacao"), 2).alias("gasto_per_capita_medio"),
        spark_round(avg("custo_por_ponto_alfabetizacao"), 2).alias("custo_por_ponto_observado"),
    )
    # IMPORTANTE: metrica per capita (nao despesa total) — sem distorcao de escala.
    custo_ponto_pc = resolve_custo_marginal_benchmark(df_eficiencia)
    print(f"  Benchmark custo marginal: R${custo_ponto_pc}/hab/ponto (~R${round(custo_ponto_pc*100,0)}/aluno)")
    df_agg = df_agg.withColumn("gap_ate_80", spark_round(when(lit(80.0) - col("taxa_alfabetizacao_media") < 0, lit(0)).otherwise(lit(80.0) - col("taxa_alfabetizacao_media")), 2))
    # custo = gap(pp) x custo_marginal(R$/hab/pp) x populacao_ALFABETIZAVEL(hab) => R$
    # populacao_total e a populacao TOTAL do municipio (todos habitantes), nao
    # contagem de alunos do 2o ano — sem a fracao abaixo o custo infla ~77x
    # (ver ADR-013). Equivale a ~ (custo_ponto_pc * 100) R$ por aluno a alfabetizar.
    df_agg = df_agg.withColumn("populacao_alfabetizavel_estimada", spark_round(col("populacao_total") * lit(FRACAO_POPULACAO_ALFABETIZAVEL), 0))
    df_agg = df_agg.withColumn("custo_estimado_para_atingir_80", spark_round(col("gap_ate_80") * lit(custo_ponto_pc) * col("populacao_alfabetizavel_estimada"), 2))
    df_agg = df_agg.withColumn("custo_per_capta_atingir_80",
        when(col("populacao_total") > 0, spark_round(col("custo_estimado_para_atingir_80") / col("populacao_total"), 2)).otherwise(lit(0)))
    df_agg = df_agg.withColumn("categoria_investimento",
        when(col("custo_estimado_para_atingir_80") <= 500000, "1 - Baixo (<R$500k)")
        .when(col("custo_estimado_para_atingir_80") <= 5000000, "2 - Medio (R$500k-R$5M)")
        .when(col("custo_estimado_para_atingir_80") <= 50000000, "3 - Alto (R$5M-R$50M)")
        .otherwise("4 - Muito Alto (>R$50M)")
    )
    df_agg = df_agg.withColumn("benchmark_custo_ponto_per_capita", lit(round(custo_ponto_pc, 2)))
    df_agg = df_agg.withColumn("flag_metropole",
        when(col("populacao_total") >= 500000, True).otherwise(False)
    )
    df_filtrado = df_agg.filter(col("gap_ate_80") > 0)
    # Dois rankings: absoluto (escopo nacional) e per capita (vulnerabilidade fiscal)
    window_abs = Window.orderBy(col("custo_estimado_para_atingir_80").desc())
    window_pc  = Window.orderBy(col("custo_per_capta_atingir_80").desc())
    df_filtrado = df_filtrado.withColumn("ranking_custo_absoluto", row_number().over(window_abs))
    df_filtrado = df_filtrado.withColumn("ranking_custo_per_capita", row_number().over(window_pc))
    return df_filtrado.orderBy(col("custo_estimado_para_atingir_80").desc())


def build_mart_clusters_municipios(df):
    """Substitui o mart de clustering com segmentação baseada em regras (sem ML)."""
    print("MART: agg_clusters_municipios — segmentacao de municipios em perfis economico-educacionais (regras)")
    df_agg = df.groupBy("id_municipio", "sigla_uf").agg(
        max("nome_municipio").alias("nome_municipio"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_media"),
        spark_round(avg("populacao_total"), 0).alias("populacao_total"),
        spark_round(spark_sum("deficit_absoluto_proxy"), 0).alias("deficit_total")
    )
    mediana_taxa = safe_quantile(df_agg, "taxa_media", default=80.0)
    mediana_deficit = safe_quantile(df_agg, "deficit_total", default=1000.0)
    df_agg = df_agg.withColumn("cluster",
        when((col("taxa_media") >= mediana_taxa) & (col("deficit_total") < mediana_deficit), 0)
        .when((col("taxa_media") < mediana_taxa) & (col("deficit_total") >= mediana_deficit), 1)
        .when((col("taxa_media") < mediana_taxa) & (col("deficit_total") < mediana_deficit), 2)
        .otherwise(3)
    )
    df_agg = df_agg.withColumn("cluster_label",
        when(col("cluster") == 0, "Alta taxa, Baixo deficit")
        .when(col("cluster") == 1, "Baixa taxa, Alto deficit")
        .when(col("cluster") == 2, "Baixa taxa, Baixo deficit")
        .otherwise("Alta taxa, Alto deficit")
    )
    return df_agg.orderBy("cluster", col("deficit_total").desc())


def build_mart_vulnerabilidade_ml(df, tem_siconfi):
    """Clusters de vulnerabilidade educacional via K-Means (Spark MLlib).

    Implementa a 'Aplicacao em IA / clusters de vulnerabilidade educacional'
    citada no enunciado. Combina tres dimensoes por municipio:
      - Educacao: taxa media, deficit per capita
      - Territorio: populacao (escala log — evita metropoles dominarem)
      - Financas: gasto per capita em educacao (quando ha SICONFI)

    Features sao padronizadas (StandardScaler) antes do K-Means (k=4). Os
    clusters sao rotulados por nivel de vulnerabilidade (menor taxa media =
    mais vulneravel) e reporta-se o Silhouette do modelo."""
    print("MART: agg_vulnerabilidade_ml — segmentacao de municipios por vulnerabilidade educacional (K-Means MLlib)")
    from pyspark.ml.feature import VectorAssembler, StandardScaler
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.evaluation import ClusteringEvaluator
    from pyspark.sql.functions import log1p

    aggs = [
        max("nome_municipio").alias("nome_municipio"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_media"),
        spark_round(avg("populacao_total"), 0).alias("populacao_total"),
        spark_round(spark_sum("deficit_absoluto_proxy"), 0).alias("deficit_total"),
    ]
    if tem_siconfi:
        aggs.append(spark_round(avg("gasto_por_habitante_educacao"), 2).alias("gasto_per_capita"))
    df_agg = df.groupBy("id_municipio", "sigla_uf").agg(*aggs)

    df_agg = df_agg.withColumn("deficit_per_capita",
        spark_round(when(col("populacao_total") > 0, col("deficit_total") / col("populacao_total")).otherwise(lit(0)), 4))
    df_agg = df_agg.withColumn("log_populacao", log1p(col("populacao_total")))

    feature_cols = ["taxa_media", "deficit_per_capita", "log_populacao"]
    if tem_siconfi:
        feature_cols.append("gasto_per_capita")

    df_model = df_agg.dropna(subset=feature_cols)

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    df_vec = assembler.transform(df_model)
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
    df_scaled = scaler.fit(df_vec).transform(df_vec)

    kmeans = KMeans(k=4, seed=42, featuresCol="features", predictionCol="cluster")
    model = kmeans.fit(df_scaled)
    df_pred = model.transform(df_scaled)

    silhouette = None
    try:
        silhouette = ClusteringEvaluator(featuresCol="features", predictionCol="cluster").evaluate(df_pred)
        print(f"  Silhouette (k=4): {round(silhouette, 4)}")
    except Exception as e:
        print(f"  AVISO: Silhouette nao calculado: {e}")

    # Rotula clusters por vulnerabilidade: menor taxa media = mais vulneravel
    cluster_stats = df_pred.groupBy("cluster").agg(avg("taxa_media").alias("taxa_media_cluster"))
    w_vuln = Window.orderBy(col("taxa_media_cluster").asc())
    cluster_stats = cluster_stats.withColumn("rank_vuln", row_number().over(w_vuln))
    cluster_stats = cluster_stats.withColumn("nivel_vulnerabilidade",
        when(col("rank_vuln") == 1, "1 - Critica")
        .when(col("rank_vuln") == 2, "2 - Alta")
        .when(col("rank_vuln") == 3, "3 - Moderada")
        .otherwise("4 - Baixa"))
    cluster_stats = cluster_stats.withColumn("silhouette_modelo",
        lit(round(silhouette, 4) if silhouette is not None else None))

    df_out = df_pred.join(
        cluster_stats.select("cluster", "nivel_vulnerabilidade", "silhouette_modelo"), "cluster", "left")

    out_cols = ["id_municipio", "sigla_uf", "nome_municipio", "taxa_media",
                "deficit_per_capita", "populacao_total", "deficit_total"]
    if tem_siconfi:
        out_cols.append("gasto_per_capita")
    out_cols += ["cluster", "nivel_vulnerabilidade", "silhouette_modelo"]
    return df_out.select(*out_cols).orderBy("nivel_vulnerabilidade", col("taxa_media").asc())


def build_mart_alocacao_otima(df, df_eficiencia=None):
    """Alocacao otima sob RESTRICAO ORCAMENTARIA (Knapsack Greedy — ADR-010).

    Diferente de um simples ranking: ordena os municipios pela relacao
    custo-beneficio (alunos que saem do deficit por R$ investido), acumula o
    custo e marca quais cabem dentro de ORCAMENTO_ALOCACAO. Reproduz no Spark a
    heuristica greedy do ADR-010 (ordenar por razao e pegar o prefixo que cabe
    no orcamento), agora como mart consumivel no BigQuery."""
    print("MART: agg_alocacao_otima — alocacao otima de orcamento fixo entre municipios (Knapsack Greedy)")
    df_agg = df.groupBy("id_municipio", "sigla_uf").agg(
        max("nome_municipio").alias("nome_municipio"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_media"),
        spark_round(avg("populacao_total"), 0).alias("populacao_total"),
        spark_round(spark_sum("deficit_absoluto_proxy"), 0).alias("deficit_total")
    )
    df_agg = df_agg.withColumn("gap_ate_80",
        spark_round(when(lit(80.0) - col("taxa_media") < 0, lit(0)).otherwise(lit(80.0) - col("taxa_media")), 2))
    # populacao_total e a populacao TOTAL do municipio, nao contagem de alunos
    # do 2o ano — aplicar fracao antes de custo/beneficio em R$/alunos (ADR-013).
    df_agg = df_agg.withColumn("populacao_alfabetizavel_estimada", spark_round(col("populacao_total") * lit(FRACAO_POPULACAO_ALFABETIZAVEL), 0))
    # Custo per capita — MESMO benchmark de agg_projecao_investimento (ADR-012):
    # mediana SICONFI dos municipios eficientes quando disponivel, senao o
    # default. Antes desta correcao, esta mart usava sempre a constante default
    # mesmo com SICONFI disponivel, divergindo (~3%) do custo reportado em
    # agg_projecao_investimento para o mesmo municipio.
    custo_ponto_pc = resolve_custo_marginal_benchmark(df_eficiencia)
    df_agg = df_agg.withColumn("custo_estimado",
        spark_round(col("gap_ate_80") * lit(custo_ponto_pc) * col("populacao_alfabetizavel_estimada"), 2))
    df_agg = df_agg.filter((col("gap_ate_80") > 0) & (col("custo_estimado") > 0))
    # Beneficio ancorado na MESMA meta do custo (80%), nao nos 100% do deficit_total.
    # deficit_total (base 100%) e custo_estimado (base 80%) misturavam referencias
    # diferentes no score — inflava artificialmente municipios perto de 80% (ver
    # auditoria 2026-07-02). beneficio_alunos_ate_80 usa a mesma base de gap_ate_80
    # e a mesma populacao_alfabetizavel_estimada do custo (ADR-013).
    df_agg = df_agg.withColumn("beneficio_alunos_ate_80",
        spark_round((col("gap_ate_80") / 100.0) * col("populacao_alfabetizavel_estimada"), 0))
    # Beneficio por real investido: alunos que saem do deficit (ate 80%) por R$
    df_agg = df_agg.withColumn("score_custo_beneficio",
        spark_round(col("beneficio_alunos_ate_80") / col("custo_estimado"), 8))
    # Greedy: ordena por custo-beneficio desc (desempate: menor custo primeiro)
    window_rank = Window.orderBy(col("score_custo_beneficio").desc(), col("custo_estimado").asc())
    df_agg = df_agg.withColumn("ranking_alocacao", row_number().over(window_rank))
    # Soma acumulada do custo na ordem do ranking = restricao knapsack
    window_acum = Window.orderBy(col("ranking_alocacao")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df_agg = df_agg.withColumn("custo_acumulado", spark_round(spark_sum("custo_estimado").over(window_acum), 2))
    df_agg = df_agg.withColumn("selecionado_no_orcamento",
        when(col("custo_acumulado") <= lit(ORCAMENTO_ALOCACAO), True).otherwise(False))
    df_agg = df_agg.withColumn("orcamento_total", lit(ORCAMENTO_ALOCACAO))
    return df_agg.orderBy("ranking_alocacao")


def build_mart_qualidade_resumo(df):
    """Distribuição de municípios por bucket de qualidade por UF."""
    print("MART: agg_qualidade_resumo — distribuicao de municipios por qualidade de dados (bucket Critico/Ruim/Razoavel/Excelente)")
    df_base = df.select("ano", "id_municipio", "sigla_uf", "taxa_alfabetizacao", "deficit_absoluto_proxy") \
                .dropDuplicates(["id_municipio", "ano"])
    df_base = df_base.withColumn("bucket_qualidade",
        when(col("taxa_alfabetizacao") < 25, "1-Critico")
        .when(col("taxa_alfabetizacao") < 50, "2-Ruim")
        .when(col("taxa_alfabetizacao") < 75, "3-Razoavel")
        .otherwise("4-Excelente")
    )
    mart = df_base.groupBy("ano", "sigla_uf", "bucket_qualidade").agg(
        countDistinct("id_municipio").alias("qtd_municipios"),
        spark_round(avg("taxa_alfabetizacao"), 2).alias("taxa_media"),
        spark_round(spark_sum("deficit_absoluto_proxy"), 0).alias("deficit_total_estimado")
    )
    total_por_uf = df_base.groupBy("ano", "sigla_uf").agg(countDistinct("id_municipio").alias("total_municipios_uf"))
    mart = mart.join(total_por_uf, ["ano", "sigla_uf"], "left")
    mart = mart.withColumn("pct_municipios", spark_round((col("qtd_municipios") / col("total_municipios_uf")) * 100, 1))
    return mart.orderBy("ano", "sigla_uf", "bucket_qualidade")


def build_mart_correlacoes_uf(df):
    """Correlação Pearson gasto×taxa por UF (requer SICONFI)."""
    print("MART: agg_correlacoes_uf — forca da relacao entre gasto e taxa de alfabetizacao, por UF")
    if "gasto_por_habitante_educacao" not in df.columns:
        return None
    try:
        pearson_gasto_taxa = df.stat.corr("gasto_por_habitante_educacao", "taxa_alfabetizacao")
        pearson_deficit_gasto = df.stat.corr("deficit_absoluto_proxy", "gasto_por_habitante_educacao")
    except Exception:
        return None

    mart = df.groupBy("sigla_uf").agg(
        countDistinct("id_municipio").alias("n_municipios")
    )
    mart = mart.withColumn("pearson_gasto_taxa", lit(round(pearson_gasto_taxa, 4)))
    mart = mart.withColumn("pearson_deficit_gasto", lit(round(pearson_deficit_gasto, 4)))
    mart = mart.withColumn("interpretacao_correlacao",
        when(col("pearson_gasto_taxa") < 0.3, "Fraca (<0.3)")
        .when(col("pearson_gasto_taxa") < 0.6, "Moderada (0.3-0.6)")
        .otherwise("Forte (>0.6)")
    )
    return mart.orderBy("sigla_uf")


def build_mart_roi_executivo(df, mart_custo=None, mart_investimento=None):
    """ROI executivo por UF — custo da ineficiência vs investimento necessário."""
    print("MART: agg_roi_executivo — ROI executivo por UF: custo da ineficiencia vs investimento necessario")
    if mart_custo is None or mart_investimento is None:
        return None

    custo_agg = mart_custo.groupBy("sigla_uf").agg(
        spark_sum("custo_ineficiencia_r1").alias("custo_total"),
        count("id_municipio").alias("municipios_ineficientes")
    )
    invest_agg = mart_investimento.groupBy("sigla_uf").agg(
        spark_sum("custo_estimado_para_atingir_80").alias("investimento_total"),
        count("id_municipio").alias("municipios_necessitam")
    )
    mart = custo_agg.join(invest_agg, "sigla_uf", "outer")
    mart = mart.fillna(0)
    mart = mart.withColumn("roi_fator",
        when(col("investimento_total") > 0, spark_round(col("custo_total") / col("investimento_total"), 2))
        .otherwise(lit(0))
    )
    total_custo_win = spark_sum("custo_total").over(
        Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    mart = mart.withColumn("pct_ineficiencia_no_desperdicio",
        spark_round((col("custo_total") / total_custo_win) * 100, 1)
    )
    return mart.orderBy(col("roi_fator").desc())


def build_mart_alocacao_otima_estrategias(df, mart_projecao=None):
    """3 estratégias de alocação comparadas: Greedy, Máx Impacto, Menor Custo Per Capita."""
    print("MART: agg_alocacao_otima_estrategias — 3 estrategias de alocacao de orcamento comparadas (Greedy, Max Impacto, Menor Custo Per Capita)")
    if mart_projecao is None:
        return None

    # Nome explicito (nao "deficit_total") porque a base e 80%, diferente do
    # deficit_total (base 100%) usado em agg_alocacao_otima/agg_priorizacao —
    # mesmo nome com semantica diferente causava confusao entre marts.
    df_agg = mart_projecao.select(
        "id_municipio", "sigla_uf", "nome_municipio",
        col("taxa_alfabetizacao_media").alias("taxa_media"),
        "populacao_total", "populacao_alfabetizavel_estimada", "gap_ate_80",
        "custo_estimado_para_atingir_80",
        "custo_per_capta_atingir_80"
    ).withColumn(
        # populacao_alfabetizavel_estimada (nao populacao_total) — ver ADR-013.
        "alunos_no_deficit_ate_80",
        spark_round((col("gap_ate_80") / 100) * col("populacao_alfabetizavel_estimada"), 0)
    )

    estrategias = []

    df_greedy = df_agg.withColumn("score_rank",
        spark_round(col("alunos_no_deficit_ate_80") / col("custo_estimado_para_atingir_80"), 6))
    window_greedy = Window.orderBy(col("score_rank").desc())
    df_greedy = df_greedy.withColumn("estrategia", lit("Greedy-CustoBeneficio"))
    df_greedy = df_greedy.withColumn("ranking_estrategia", row_number().over(window_greedy))
    estrategias.append(df_greedy.select("id_municipio", "sigla_uf", "nome_municipio", "taxa_media",
                                        "populacao_total", "gap_ate_80", "custo_estimado_para_atingir_80",
                                        "alunos_no_deficit_ate_80", "custo_per_capta_atingir_80", "estrategia", "ranking_estrategia"))

    df_impacto = df_agg.withColumn("score_rank", col("alunos_no_deficit_ate_80"))
    window_impacto = Window.orderBy(col("score_rank").desc())
    df_impacto = df_impacto.withColumn("estrategia", lit("MaxImpactoAbsoluto"))
    df_impacto = df_impacto.withColumn("ranking_estrategia", row_number().over(window_impacto))
    estrategias.append(df_impacto.select("id_municipio", "sigla_uf", "nome_municipio", "taxa_media",
                                        "populacao_total", "gap_ate_80", "custo_estimado_para_atingir_80",
                                        "alunos_no_deficit_ate_80", "custo_per_capta_atingir_80", "estrategia", "ranking_estrategia"))

    df_percapita = df_agg.withColumn("score_rank", col("custo_per_capta_atingir_80"))
    window_percapita = Window.orderBy(col("score_rank").asc())
    df_percapita = df_percapita.withColumn("estrategia", lit("MenorCustoPerCapita"))
    df_percapita = df_percapita.withColumn("ranking_estrategia", row_number().over(window_percapita))
    estrategias.append(df_percapita.select("id_municipio", "sigla_uf", "nome_municipio", "taxa_media",
                                          "populacao_total", "gap_ate_80", "custo_estimado_para_atingir_80",
                                          "alunos_no_deficit_ate_80", "custo_per_capta_atingir_80", "estrategia", "ranking_estrategia"))

    mart = estrategias[0]
    for est in estrategias[1:]:
        mart = mart.union(est)

    return mart.orderBy("estrategia", "ranking_estrategia")


def save_mart(mart, gold_dir, mart_name, partition_col="ano"):
    if mart is None:
        print(f"  PULADO: {mart_name} (builder retornou None ou falhou — ver [ERRO] acima)")
        return
    try:
        output_path = f"{gold_dir}/{mart_name}"
        writer = mart.write.format("parquet").mode("overwrite")
        if partition_col and partition_col in mart.columns:
            writer = writer.partitionBy(partition_col)
        writer.save(output_path)
        print(f"  Salvo: {mart_name} ({mart.count()} linhas)")
    except Exception:
        print(f"  [ERRO] {mart_name}: excecao ao salvar — mart NAO foi gravado no GCS")
        traceback.print_exc()


def add_missing_cols(df):
    """Adiciona colunas obrigatórias como null se não existirem (ex: IBGE não disponível)."""
    nullable_cols = {
        "nome_municipio": "string",
        "populacao_total": "double",
        "deficit_absoluto_proxy": "double",
        "media_portugues": "double",
        "meta_alfabetizacao_2024": "double",
    }
    for col_name, dtype in nullable_cols.items():
        if col_name not in df.columns:
            from pyspark.sql.types import StringType, DoubleType
            spark_type = StringType() if dtype == "string" else DoubleType()
            df = df.withColumn(col_name, lit(None).cast(spark_type))
            print(f"  [INFO] Coluna '{col_name}' ausente — adicionada como null.")
    return df


def run_gold(spark, silver_dir, gold_dir):
    print("=" * 70)
    print("GERACAO DE MARTS GOLD (GCS)")
    print("=" * 70)

    df, tem_siconfi = load_silver(spark, silver_dir)
    df = add_missing_cols(df)

    marts = {}

    marts["agg_uf_indicadores"] = safe_build("agg_uf_indicadores", build_mart_uf_indicadores, df)
    marts["agg_evolucao_temporal"] = safe_build("agg_evolucao_temporal", build_mart_evolucao_temporal, df)
    marts["agg_municipio_ranking"] = safe_build("agg_municipio_ranking", build_mart_municipio_ranking, df)
    marts["agg_rede_indicadores"] = safe_build("agg_rede_indicadores", build_mart_rede_indicadores, df)
    marts["agg_priorizacao"] = safe_build("agg_priorizacao", build_mart_priorizacao, df)
    marts["agg_top10_uf"] = safe_build("agg_top10_uf", build_mart_top10_uf, df)
    marts["agg_clusters_municipios"] = safe_build("agg_clusters_municipios", build_mart_clusters_municipios, df)
    marts["agg_vulnerabilidade_ml"] = safe_build("agg_vulnerabilidade_ml", build_mart_vulnerabilidade_ml, df, tem_siconfi)

    marts["agg_qualidade_resumo"] = safe_build("agg_qualidade_resumo", build_mart_qualidade_resumo, df)

    if tem_siconfi:
        mart7 = safe_build("agg_eficiencia_financeira", build_mart_eficiencia_financeira, df)
        marts["agg_eficiencia_financeira"] = mart7
        mart8 = safe_build("agg_custo_ineficiencia", build_mart_custo_ineficiencia, df)
        marts["agg_custo_ineficiencia"] = mart8
        mart9 = safe_build("agg_projecao_investimento", build_mart_projecao_investimento, df, df_eficiencia=mart7)
        marts["agg_projecao_investimento"] = mart9

        # df_eficiencia=mart7 garante que o knapsack usa o MESMO benchmark de
        # custo marginal (calibrado via SICONFI) que agg_projecao_investimento —
        # antes desta correcao, esta mart sempre usava a constante default.
        marts["agg_alocacao_otima"] = safe_build("agg_alocacao_otima", build_mart_alocacao_otima, df, df_eficiencia=mart7)

        marts["agg_correlacoes_uf"] = safe_build("agg_correlacoes_uf", build_mart_correlacoes_uf, df)
        marts["agg_roi_executivo"] = safe_build("agg_roi_executivo", build_mart_roi_executivo, df, mart_custo=mart8, mart_investimento=mart9)
        marts["agg_alocacao_otima_estrategias"] = safe_build("agg_alocacao_otima_estrategias", build_mart_alocacao_otima_estrategias, df, mart_projecao=mart9)
    else:
        marts["agg_alocacao_otima"] = safe_build("agg_alocacao_otima", build_mart_alocacao_otima, df)
        print("SICONFI não disponível — marts financeiros e correlações pulados (usando placeholders vazios)")
        empty_schema = df.select("id_municipio", "sigla_uf").limit(0)
        for name in ["agg_eficiencia_financeira", "agg_custo_ineficiencia", "agg_projecao_investimento",
                     "agg_correlacoes_uf", "agg_roi_executivo", "agg_alocacao_otima_estrategias"]:
            marts[name] = empty_schema

    print("\nSalvando marts no GCS (sem particionamento — BigQuery externo não lê partições Hive)...")
    for name, mart_df in marts.items():
        save_mart(mart_df, gold_dir, name, partition_col=None)

    print("\n" + "=" * 70)
    print("RESUMO FINAL")
    for name, mart_df in marts.items():
        if mart_df is not None:
            print(f"  {name:40s} {mart_df.count():>6} linhas  {len(mart_df.columns):>2} colunas")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, help="gs://bucket-name")
    args = parser.parse_args()

    bucket = args.bucket.rstrip("/")
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    run_gold(spark, f"{bucket}/silver", f"{bucket}/gold")

    spark.stop()
    print("Gold finalizado.")
