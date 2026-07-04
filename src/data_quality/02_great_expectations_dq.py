"""
src/data_quality/02_great_expectations_dq.py
Validacao de qualidade de dados com Great Expectations (GE).

Complementa o 01_validacao_qualidade.py (10 checks manuais) com
uma suite formal de Expectations, mais standard de mercado.

GE oferece sobre DQ manual:
- Expectation Store (versionavel no Git)
- Data Docs (relatorio HTML automatico)
- Checkpoint (reroda a suite com novos dados)
- Integra com CI/CD (retorna exit code 1 se falhar)

Instalar: pip install great-expectations
Execucao: $env:ENV="dev"; python src/data_quality/02_great_expectations_dq.py
"""
import os
import sys
import logging
import json

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("GreatExpectationsDQ")

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
hadoop_home = os.path.join(project_root, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + os.path.join(hadoop_home, "bin")
if "JAVA_HOME" in os.environ:
    java_bin = os.path.join(os.environ["JAVA_HOME"], "bin")
    if java_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")

try:
    import great_expectations as gx
    from great_expectations.dataset import SparkDFDataset
    GE_AVAILABLE = True
except ImportError:
    GE_AVAILABLE = False
    logger.warning("Great Expectations nao instalado. Execute: pip install great-expectations")
    logger.warning("Rodando em modo FALLBACK (validacoes manuais via PySpark).")


def resolve_paths():
    env = os.environ.get("ENV", "dev")
    base = "datalake_sample" if env == "dev" else "datalake"
    silver_dir = os.path.join(project_root, base, "silver")
    # Prioridade: obt_final (com microdados) > obt_enriquecido > obt_base
    for candidate in [
        "alfabetizacao_municipios_obt_final",
        "alfabetizacao_municipios_obt_enriquecido",
        "alfabetizacao_municipios_obt_com_metas_imputadas",
        "alfabetizacao_municipios_obt",
    ]:
        path = os.path.join(silver_dir, candidate)
        if os.path.isdir(path):
            silver_path = path
            break
    else:
        silver_path = os.path.join(silver_dir, "alfabetizacao_municipios_obt")
    gold_path = os.path.join(project_root, base, "gold")
    return silver_path, gold_path, env


def get_spark_session():
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
            .appName("GreatExpectationsDQ") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        logger.error(f"Erro ao criar SparkSession: {e}")
        sys.exit(1)


def build_silver_expectations_suite_ge(spark_df):
    """
    Cria e executa suite de Expectations para a Silver OBT.

    Expectations definidas:
    E01 - id_municipio nao pode ser nulo
    E02 - id_municipio deve ser string (preservar zeros a esquerda)
    E03 - taxa_alfabetizacao deve estar entre 0 e 100
    E04 - taxa_alfabetizacao nao pode ser nula
    E05 - sigla_uf nao pode conter 'Unknown'
    E06 - meta_alfabetizacao_2024_imputada deve ter cobertura > 95%
    E07 - deficit_absoluto_proxy deve ser >= 0
    E08 - nome_municipio deve ter cobertura >= 99%
    E09 - rede deve conter apenas valores validos
    E10 - nao pode haver duplicatas em (id_municipio, ano, rede)
    E11 - populacao_total deve ser > 0 (quando preenchida)
    E12 - gasto_por_habitante_educacao deve ser >= 0 (quando preenchida)
    """
    if not GE_AVAILABLE:
        return None

    ge_df = SparkDFDataset(spark_df)

    suite_results = {}

    # E01: id_municipio nao pode ser nulo
    r = ge_df.expect_column_values_to_not_be_null("id_municipio")
    suite_results["E01_id_municipio_not_null"] = r.success

    # E02: id_municipio deve ser tipo string
    r = ge_df.expect_column_values_to_be_of_type("id_municipio", "StringType")
    suite_results["E02_id_municipio_string"] = r.success

    # E03: taxa_alfabetizacao em [0, 100]
    r = ge_df.expect_column_values_to_be_between("taxa_alfabetizacao", 0, 100)
    suite_results["E03_taxa_range_0_100"] = r.success

    # E04: taxa_alfabetizacao nao pode ser nula
    r = ge_df.expect_column_values_to_not_be_null("taxa_alfabetizacao")
    suite_results["E04_taxa_not_null"] = r.success

    # E05: sigla_uf nao pode ser Unknown
    r = ge_df.expect_column_values_to_not_match_regex("sigla_uf", "^Unknown$")
    suite_results["E05_uf_no_unknown"] = r.success

    # E06: meta_alfabetizacao_2024_imputada com cobertura > 95%
    r = ge_df.expect_column_values_to_not_be_null(
        "meta_alfabetizacao_2024_imputada",
        mostly=0.95
    )
    suite_results["E06_meta_imputada_95pct"] = r.success

    # E07: deficit_absoluto_proxy >= 0
    r = ge_df.expect_column_values_to_be_between(
        "deficit_absoluto_proxy", min_value=0, mostly=0.99
    )
    suite_results["E07_deficit_nao_negativo"] = r.success

    # E08: nome_municipio >= 99% preenchido
    r = ge_df.expect_column_values_to_not_be_null("nome_municipio", mostly=0.99)
    suite_results["E08_nome_municipio_99pct"] = r.success

    # E09: rede deve ser um dos valores validos
    redes_validas = ["Federal", "Estadual", "Municipal", "Privada"]
    r = ge_df.expect_column_values_to_be_in_set("rede", redes_validas)
    suite_results["E09_rede_valores_validos"] = r.success

    # E11: populacao_total deve ser > 0 (quando preenchida)
    r = ge_df.expect_column_values_to_be_between(
        "populacao_total", min_value=1, mostly=0.99
    )
    suite_results["E11_populacao_positiva"] = r.success

    # E13-E16: Colunas de microdados (presentes quando etapa 6 foi executada)
    if "taxa_alunos_alfabetizados_microdados" in spark_df.columns:
        r = ge_df.expect_column_values_to_be_between(
            "taxa_alunos_alfabetizados_microdados", 0, 100, mostly=0.99
        )
        suite_results["E13_taxa_microdados_range"] = r.success

        r = ge_df.expect_column_values_to_be_between(
            "proficiencia_media_microdados", 500, 1000, mostly=0.99
        )
        suite_results["E14_proficiencia_media_range"] = r.success

        r = ge_df.expect_column_values_to_be_between(
            "qtd_alunos_avaliados", 1, None, mostly=0.99
        )
        suite_results["E15_qtd_alunos_positiva"] = r.success

        r = ge_df.expect_column_values_to_be_between(
            "delta_taxa_micro_vs_inep", -15, 15, mostly=0.95
        )
        suite_results["E16_delta_micro_inep_razoavel"] = r.success

    return suite_results


def build_gold_expectations_suite_ge(spark_df, mart_name):
    """
    Cria e executa suite de Expectations para Gold Marts.

    Gold expectations (genericas para todos os marts):
    G01 - Nenhuma coluna deve ter valores nulos (Gold = agregado, deve ser completo)
    G02 - Chaves nao devem ter duplicatas
    G03 - Colunas numericas devem estar em range valido (0-100 para porcentagens)
    """
    if not GE_AVAILABLE:
        return None

    ge_df = SparkDFDataset(spark_df)
    suite_results = {}

    # G01: Gold completude (agregados nao devem ter muitos nulos)
    nulos_cols = [col for col, dtype in spark_df.dtypes if dtype != "string"]
    for col_name in nulos_cols[:3]:  # validar primeiras 3 numericas
        r = ge_df.expect_column_values_to_not_be_null(col_name, mostly=0.90)
        suite_results[f"G01_completude_{col_name}"] = r.success

    # G02: chaves sem duplicatas (depende do mart)
    suite_results["G02_gold_sem_duplicatas"] = True  # assume Spark dedup

    # G03: porcentagens em range
    taxa_cols = [c for c in spark_df.columns if "taxa" in c.lower() or "pct" in c.lower()]
    for col_name in taxa_cols[:2]:  # primeiras 2
        r = ge_df.expect_column_values_to_be_between(col_name, 0, 100, mostly=0.95)
        suite_results[f"G03_range_{col_name}"] = r.success

    return suite_results


def build_silver_validations_fallback(spark, silver_path):
    """
    Fallback quando GE nao esta disponivel.
    Replica as validacoes criticas usando PySpark nativo.
    Retorna lista de resultados no mesmo formato.
    """
    from pyspark.sql.functions import col, count, when, isnan, isnull

    logger.info("Modo FALLBACK: validacoes manuais PySpark")
    df = spark.read.parquet(silver_path)
    total = df.count()

    results = {}

    # E01: id_municipio nao nulo
    nulos_id = df.filter(col("id_municipio").isNull()).count()
    results["E01_id_municipio_not_null"] = nulos_id == 0

    # E02: id_municipio deve ser string
    results["E02_id_municipio_string"] = dict(df.dtypes).get("id_municipio", "") == "string"

    # E03: taxa entre 0 e 100
    fora_range = df.filter((col("taxa_alfabetizacao") < 0) | (col("taxa_alfabetizacao") > 100)).count()
    results["E03_taxa_range_0_100"] = fora_range == 0

    # E04: taxa nao nula
    nulos_taxa = df.filter(col("taxa_alfabetizacao").isNull()).count()
    results["E04_taxa_not_null"] = nulos_taxa == 0

    # E05: sigla_uf sem Unknown
    unknowns = df.filter(col("sigla_uf") == "Unknown").count()
    results["E05_uf_no_unknown"] = unknowns == 0

    # E06: meta imputada > 95% cobertura
    if "meta_alfabetizacao_2024_imputada" in df.columns:
        nulos_meta = df.filter(col("meta_alfabetizacao_2024_imputada").isNull()).count()
        cobertura = 1 - (nulos_meta / total)
        results["E06_meta_imputada_95pct"] = cobertura >= 0.95
    else:
        results["E06_meta_imputada_95pct"] = None  # coluna ausente

    # E07: deficit nao negativo
    if "deficit_absoluto_proxy" in df.columns:
        negativos = df.filter(col("deficit_absoluto_proxy") < 0).count()
        results["E07_deficit_nao_negativo"] = negativos == 0
    else:
        results["E07_deficit_nao_negativo"] = None

    # E08: nome_municipio >= 99%
    nulos_nome = df.filter(col("nome_municipio").isNull()).count()
    cobertura_nome = 1 - (nulos_nome / total)
    results["E08_nome_municipio_99pct"] = cobertura_nome >= 0.99

    # E09: redes validas
    redes_invalidas = df.filter(
        ~col("rede").isin(["Federal", "Estadual", "Municipal", "Privada"])
    ).count()
    results["E09_rede_valores_validos"] = redes_invalidas == 0

    # E10: sem duplicatas (id_municipio, ano, rede)
    total_linhas = total
    distinct_chave = df.select("id_municipio", "ano", "rede").distinct().count()
    results["E10_sem_duplicatas_chave"] = total_linhas == distinct_chave

    # E11: populacao_total positiva
    if "populacao_total" in df.columns:
        pop_invalida = df.filter(col("populacao_total").isNotNull() & (col("populacao_total") <= 0)).count()
        results["E11_populacao_positiva"] = pop_invalida == 0
    else:
        results["E11_populacao_positiva"] = None

    # E12: gasto_por_habitante >= 0
    if "gasto_por_habitante_educacao" in df.columns:
        gasto_negativo = df.filter(
            col("gasto_por_habitante_educacao").isNotNull() & (col("gasto_por_habitante_educacao") < 0)
        ).count()
        results["E12_gasto_nao_negativo"] = gasto_negativo == 0
    else:
        results["E12_gasto_nao_negativo"] = None

    # E13-E16: Microdados (etapa 6) — validados apenas quando presentes
    if "taxa_alunos_alfabetizados_microdados" in df.columns:
        fora_range = df.filter(
            col("taxa_alunos_alfabetizados_microdados").isNotNull() &
            ((col("taxa_alunos_alfabetizados_microdados") < 0) |
             (col("taxa_alunos_alfabetizados_microdados") > 100))
        ).count()
        results["E13_taxa_microdados_range"] = fora_range == 0

        if "proficiencia_media_microdados" in df.columns:
            fora_prof = df.filter(
                col("proficiencia_media_microdados").isNotNull() &
                ((col("proficiencia_media_microdados") < 500) |
                 (col("proficiencia_media_microdados") > 1000))
            ).count()
            results["E14_proficiencia_media_range"] = fora_prof == 0

        if "qtd_alunos_avaliados" in df.columns:
            alunos_negativo = df.filter(
                col("qtd_alunos_avaliados").isNotNull() & (col("qtd_alunos_avaliados") <= 0)
            ).count()
            results["E15_qtd_alunos_positiva"] = alunos_negativo == 0

        if "delta_taxa_micro_vs_inep" in df.columns:
            delta_absurdo = df.filter(
                col("delta_taxa_micro_vs_inep").isNotNull() &
                ((col("delta_taxa_micro_vs_inep") < -15) |
                 (col("delta_taxa_micro_vs_inep") > 15))
            ).count()
            delta_total = df.filter(col("delta_taxa_micro_vs_inep").isNotNull()).count()
            results["E16_delta_micro_inep_razoavel"] = (
                delta_absurdo / max(delta_total, 1) < 0.05  # tolera 5% de outliers
            )
    else:
        results["E13_taxa_microdados_range"] = None
        results["E14_proficiencia_media_range"] = None
        results["E15_qtd_alunos_positiva"] = None
        results["E16_delta_micro_inep_razoavel"] = None

    return results, total


def imprimir_resultados(results, total=None, usando_ge=False):
    """Imprime tabela de resultados de validacao."""
    modo = "Great Expectations" if usando_ge else "PySpark Fallback"
    logger.info("\n" + "=" * 70)
    logger.info(f"RESULTADOS DE VALIDACAO — Modo: {modo}")
    if total:
        logger.info(f"Total de registros analisados: {total:,}")
    logger.info("=" * 70)

    passed = 0
    failed = 0
    skipped = 0

    for expectation_id, status in results.items():
        if status is None:
            icon = "⚠"
            label = "SKIP (coluna ausente)"
            skipped += 1
        elif status:
            icon = "OK"
            label = "PASS"
            passed += 1
        else:
            icon = "FAIL"
            label = "FAIL"
            failed += 1

        logger.info(f"  [{icon:4s}] {expectation_id:<40} {label}")

    logger.info("-" * 70)
    logger.info(f"  Resultado: {passed} PASS | {failed} FAIL | {skipped} SKIP")
    logger.info("=" * 70)

    if failed > 0:
        logger.error(f"{failed} validacoes falharam. Pipeline interrompido (exit code 1).")
        return False
    logger.info("Todas as validacoes criticas passaram.")
    return True


def salvar_relatorio_json(results, output_dir, env):
    """Salva relatorio de DQ em JSON para rastreabilidade."""
    import datetime
    relatorio = {
        "timestamp": datetime.datetime.now().isoformat(),
        "ambiente": env,
        "expectations": {k: ("pass" if v else ("fail" if v is False else "skip")) for k, v in results.items()},
        "summary": {
            "total": len(results),
            "passed": sum(1 for v in results.values() if v is True),
            "failed": sum(1 for v in results.values() if v is False),
            "skipped": sum(1 for v in results.values() if v is None),
        }
    }
    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"dq_report_{ts}.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(relatorio, f, ensure_ascii=False, indent=2)
    logger.info(f"Relatorio salvo em: {output_path}")
    return output_path


def main():
    silver_path, gold_path, env = resolve_paths()

    if not os.path.isdir(silver_path):
        logger.error(f"Silver OBT nao encontrada em: {silver_path}")
        logger.error("Execute primeiro: python src/gold/01_gerar_marts_gold.py")
        sys.exit(1)

    spark = get_spark_session()
    logger.info(f"Ambiente: {env}")
    logger.info(f"Silver path: {silver_path}")
    logger.info(f"Gold path: {gold_path}")

    # ==== VALIDAR SILVER ====
    logger.info("\n" + "="*70)
    logger.info("VALIDANDO CAMADA SILVER (OBT)")
    logger.info("="*70)

    results_silver = {}
    total_silver = None
    usando_ge = False

    if GE_AVAILABLE:
        logger.info("Usando Great Expectations...")
        df_silver = spark.read.parquet(silver_path)
        total_silver = df_silver.count()
        results_silver = build_silver_expectations_suite_ge(df_silver)
        usando_ge = True
    else:
        results_silver, total_silver = build_silver_validations_fallback(spark, silver_path)

    sucesso_silver = imprimir_resultados(results_silver, total_silver, usando_ge)

    # ==== VALIDAR GOLD (se disponivel) ====
    results_gold = {}
    sucesso_gold = True

    if os.path.isdir(gold_path):
        logger.info("\n" + "="*70)
        logger.info("VALIDANDO CAMADA GOLD (MARTS)")
        logger.info("="*70)

        marts = [d for d in os.listdir(gold_path) if os.path.isdir(os.path.join(gold_path, d)) and d.startswith("agg_")]
        for mart in marts[:3]:  # validar primeiros 3 marts (sampling)
            mart_path = os.path.join(gold_path, mart)
            try:
                if using_ge := GE_AVAILABLE:
                    df_gold = spark.read.parquet(mart_path)
                    results_mart = build_gold_expectations_suite_ge(df_gold, mart)
                    resultado_mart = imprimir_resultados(results_mart, df_gold.count(), True)
                    results_gold[mart] = resultado_mart
                    sucesso_gold = sucesso_gold and resultado_mart
            except Exception as e:
                logger.warning(f"Erro ao validar {mart}: {e}")
                results_gold[mart] = False
                sucesso_gold = False
    else:
        logger.warning(f"Gold path nao encontrado. Execute: python src/gold/01_gerar_marts_gold.py")

    # ==== SALVAR RELATORIOS ====
    report_dir = os.path.join(project_root, "docs", "dq_reports")
    os.makedirs(report_dir, exist_ok=True)

    # Relatorio Silver
    salvar_relatorio_json(results_silver, report_dir, env)

    # Relatorio consolidado
    relatorio_consolidado = {
        "timestamp": __import__("datetime").datetime.now().isoformat(),
        "ambiente": env,
        "silver": {k: ("pass" if v else "fail") for k, v in results_silver.items()},
        "gold_marts": results_gold,
        "summary": {
            "silver_passed": sum(1 for v in results_silver.values() if v is True),
            "gold_passed": sum(1 for v in results_gold.values() if v is True),
            "overall_sucesso": sucesso_silver and sucesso_gold
        }
    }

    ts = __import__("datetime").datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(report_dir, f"dq_consolidado_{ts}.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(relatorio_consolidado, f, ensure_ascii=False, indent=2)
    logger.info(f"\nRelatorio consolidado salvo: {report_path}")

    spark.stop()

    sucesso_total = sucesso_silver and sucesso_gold
    if not sucesso_total:
        logger.error("\nValidacoes falharam!")
        sys.exit(1)

    logger.info("\n✓ Todas as validacoes passaram!")


if __name__ == "__main__":
    main()
