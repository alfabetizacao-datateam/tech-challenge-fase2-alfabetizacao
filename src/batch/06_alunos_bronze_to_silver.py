"""
Etapa 6 do Pipeline — Integração de Microdados de Alunos na Silver

Por que este script existe:
  O Alunos.csv contém dados no nível do aluno individual (microdados).
  Os outros scripts usam apenas os agregados do INEP (taxas por município).
  Este script computa as mesmas métricas de baixo pra cima — a partir dos alunos
  — e junta ao Silver OBT, permitindo validação de consistência e novos insights.

Novas colunas adicionadas ao Silver:
  taxa_alunos_alfabetizados_microdados  → % alfabetizados (ponderado por peso_aluno)
  proficiencia_media_microdados         → média SAEB ponderada por peso_aluno
  qtd_alunos_avaliados                  → alunos que fizeram a prova (soma dos pesos)
  qtd_escolas_avaliadas                 → escolas distintas por município/ano/rede
  delta_taxa_micro_vs_inep              → microdados - taxa_inep (validação de qualidade)

Entrada:  Bronze/Alunos/ + Silver/obt_enriquecido (ou fallback para obt base)
Saída:    Silver/alfabetizacao_municipios_obt_final
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, countDistinct, when, round as spark_round, avg
)
from pyspark.sql.types import StringType

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))

hadoop_home = os.path.join(project_root, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + os.path.join(hadoop_home, "bin")
if "JAVA_HOME" in os.environ:
    java_bin = os.path.join(os.environ["JAVA_HOME"], "bin")
    if java_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def get_spark_session():
    return SparkSession.builder.appName("AlunosBronzeToSilver").getOrCreate()


def _resolve_paths():
    env = os.environ.get("ENV", "dev")
    if env == "prod":
        bronze_dir = os.path.join(project_root, "datalake", "bronze")
        silver_dir = os.path.join(project_root, "datalake", "silver")
    else:
        bronze_dir = os.path.join(project_root, "datalake_sample", "bronze")
        silver_dir = os.path.join(project_root, "datalake_sample", "silver")

    # Silver de entrada: usa a mais enriquecida disponível (fallback em cascata)
    candidates = [
        os.path.join(silver_dir, "alfabetizacao_municipios_obt_enriquecido"),
        os.path.join(silver_dir, "alfabetizacao_municipios_obt_com_metas_imputadas"),
        os.path.join(silver_dir, "alfabetizacao_municipios_obt"),
    ]
    silver_input = next(
        (p for p in candidates if _tem_parquet_valido(p)),
        None
    )

    silver_final = os.path.join(silver_dir, "alfabetizacao_municipios_obt_final")
    return bronze_dir, silver_input, silver_final


def _tem_parquet_valido(path):
    if not os.path.isdir(path):
        return False
    for _, _, files in os.walk(path):
        if any(f.endswith(".parquet") for f in files):
            return True
    return False


def agregar_microdados_alunos(spark, bronze_dir):
    """
    FASE 1-2: Lê Bronze/Alunos/ e agrega por (id_municipio, ano, rede).

    Apenas alunos PRESENTES entram no cálculo — ausentes não fizeram a prova
    e não representam desempenho real.

    O peso_aluno é o fator de expansão estatística do SAEB: cada aluno
    representa N alunos da população. Ignorar os pesos distorceria as taxas.
    """
    alunos_path = os.path.join(bronze_dir, "Alunos")
    if not _tem_parquet_valido(alunos_path):
        logger.error(f"Bronze/Alunos não encontrado: {alunos_path}")
        logger.error("Execute primeiro: python src/batch/01_ingestao_bronze_batch.py")
        return None

    logger.info(f"[FASE 1] Lendo microdados de alunos: {alunos_path}")
    df = spark.read.parquet(alunos_path)
    total_raw = df.count()
    logger.info(f"  Total de registros brutos: {total_raw:,} alunos")

    # id_municipio como STRING — crítico para o JOIN com Silver (zeros à esquerda)
    df = df.withColumn("id_municipio", col("id_municipio").cast(StringType()))

    # Apenas alunos que compareceram ao exame
    df_presentes = df.filter(col("presenca") == "Presente")
    n_presentes = df_presentes.count()
    logger.info(
        f"  Alunos avaliados (presença): {n_presentes:,} "
        f"({n_presentes / total_raw * 100:.1f}% do total)"
    )

    logger.info("[FASE 2] Agregando por (id_municipio, ano, rede)...")
    df_agg = df_presentes.groupBy("id_municipio", "ano", "rede").agg(

        # Taxa de alfabetização computada dos microdados (ponderada)
        # Numerador: peso dos alunos alfabetizados
        # Denominador: peso total de todos os presentes
        spark_round(
            spark_sum(
                when(col("alfabetizado") == "Sim", col("peso_aluno")).otherwise(0.0)
            ) / spark_sum("peso_aluno") * 100,
            2
        ).alias("taxa_alunos_alfabetizados_microdados"),

        # Proficiência média ponderada (média SAEB real dos alunos)
        spark_round(
            spark_sum(col("proficiencia") * col("peso_aluno")) / spark_sum("peso_aluno"),
            2
        ).alias("proficiencia_media_microdados"),

        # Volume efetivo de alunos avaliados (soma dos pesos = população representada)
        spark_round(spark_sum("peso_aluno"), 0).alias("qtd_alunos_avaliados"),

        # Infraestrutura: quantas escolas distintas foram avaliadas
        countDistinct("id_escola").alias("qtd_escolas_avaliadas"),
    )

    n_combinacoes = df_agg.count()
    n_municipios = df_agg.select("id_municipio").distinct().count()
    logger.info(f"  Resultado: {n_combinacoes:,} combinações (município×ano×rede) | {n_municipios:,} municípios únicos")

    return df_agg


def enriquecer_silver_com_microdados(spark, silver_input_path, df_micro_agg, silver_final_path):
    """
    FASE 3-4: JOIN dos microdados agregados com o Silver OBT existente.

    LEFT JOIN: o Silver é a tabela principal. Municípios sem microdados
    (ex: escolas federais em regiões com pouca avaliação) ficam com NULL
    nas colunas de microdados — sem distorcer os dados existentes.

    delta_taxa_micro_vs_inep é uma métrica de qualidade de dados:
    - Próximo de 0: microdados e INEP são consistentes (esperado)
    - Grande positivo: microdados mostram taxa maior que INEP
    - Grande negativo: INEP reporta taxa maior que os microdados calculam
    """
    logger.info(f"[FASE 3] Lendo Silver base: {silver_input_path}")
    df_silver = spark.read.parquet(silver_input_path)
    n_silver = df_silver.count()
    n_colunas_antes = len(df_silver.columns)
    logger.info(f"  Silver entrada: {n_silver:,} registros × {n_colunas_antes} colunas")

    # LEFT JOIN em (id_municipio, ano, rede) — as três chaves do pipeline
    df_final = df_silver.join(df_micro_agg, on=["id_municipio", "ano", "rede"], how="left")

    # Métrica de validação: diferença entre a taxa computada dos microdados e a taxa INEP
    df_final = df_final.withColumn(
        "delta_taxa_micro_vs_inep",
        spark_round(
            col("taxa_alunos_alfabetizados_microdados") - col("taxa_alfabetizacao"),
            2
        )
    )

    # Diagnóstico: cobertura e consistência
    n_com_micro = df_final.filter(
        col("taxa_alunos_alfabetizados_microdados").isNotNull()
    ).count()
    logger.info(
        f"  Cobertura de microdados: {n_com_micro:,}/{n_silver:,} registros "
        f"({n_com_micro / n_silver * 100:.1f}%)"
    )

    delta_row = df_final.filter(col("delta_taxa_micro_vs_inep").isNotNull()) \
        .selectExpr("round(avg(delta_taxa_micro_vs_inep), 3) as delta_medio") \
        .collect()
    if delta_row:
        delta_medio = delta_row[0]["delta_medio"]
        status = "OK (consistente)" if abs(delta_medio or 0) < 2 else "ATENÇÃO: discrepância alta"
        logger.info(f"  Delta médio (micro - INEP): {delta_medio} p.p. → {status}")

    logger.info(f"[FASE 4] Salvando Silver final: {silver_final_path}")
    df_final.write \
        .format("parquet") \
        .mode("overwrite") \
        .partitionBy("ano", "rede") \
        .save(silver_final_path)

    n_final = df_final.count()
    n_colunas_depois = len(df_final.columns)
    novas = n_colunas_depois - n_colunas_antes
    logger.info(f"  Silver final: {n_final:,} registros × {n_colunas_depois} colunas (+{novas} novas)")
    logger.info("  Novas colunas:")
    logger.info("    + taxa_alunos_alfabetizados_microdados")
    logger.info("    + proficiencia_media_microdados")
    logger.info("    + qtd_alunos_avaliados")
    logger.info("    + qtd_escolas_avaliadas")
    logger.info("    + delta_taxa_micro_vs_inep  ← indicador de qualidade de dados")

    return df_final


def run_alunos_integration():
    logger.info("=" * 70)
    logger.info("PIPELINE — ETAPA 6: Microdados Alunos → Silver Final")
    logger.info("=" * 70)

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    env = os.environ.get("ENV", "dev")
    logger.info(f"Ambiente: ENV={env}")

    bronze_dir, silver_input, silver_final = _resolve_paths()

    if silver_input is None:
        logger.error("Nenhuma Silver encontrada. Execute o pipeline do início.")
        spark.stop()
        return

    logger.info(f"Silver de entrada: {os.path.basename(silver_input)}")

    df_micro_agg = agregar_microdados_alunos(spark, bronze_dir)
    if df_micro_agg is None:
        spark.stop()
        return

    enriquecer_silver_com_microdados(spark, silver_input, df_micro_agg, silver_final)

    logger.info("\n" + "=" * 70)
    logger.info("ETAPA 6 CONCLUÍDA")
    logger.info(f"  Silver final: {silver_final}")
    logger.info("  Próximo passo: python src/gold/01_gerar_marts_gold.py")
    logger.info("=" * 70)

    spark.stop()


if __name__ == "__main__":
    run_alunos_integration()
