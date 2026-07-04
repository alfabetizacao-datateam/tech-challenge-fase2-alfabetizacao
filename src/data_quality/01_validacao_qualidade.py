import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull, sum as spark_sum

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
    return SparkSession.builder.appName("DataQuality").getOrCreate()

def check_duplicates(df, subset, label):
    total = df.count()
    dup = df.groupBy(subset).count().filter(col("count") > 1).count()
    status = "PASSOU" if dup == 0 else "FALHOU"
    print(f"  [{status}] {label}: {dup} duplicatas em {total} registros (chave: {subset})")
    return dup == 0

def check_null_pct(df, col_name, max_pct, label):
    total = df.count()
    nulls = df.filter(col(col_name).isNull()).count()
    pct = (nulls / total * 100) if total > 0 else 0
    status = "PASSOU" if pct <= max_pct else "FALHOU"
    print(f"  [{status}] {label}: {nulls}/{total} nulos ({pct:.1f}%) — tolerância: {max_pct}%")
    return pct <= max_pct

def check_range(df, col_name, min_val, max_val, label):
    total = df.count()
    violacoes = df.filter((col(col_name) < min_val) | (col(col_name) > max_val)).count()
    status = "PASSOU" if violacoes == 0 else "FALHOU"
    print(f"  [{status}] {label}: {violacoes} violações de faixa [{min_val}, {max_val}]")
    return violacoes == 0

def check_type(df, col_name, expected_type_name, label):
    campo = df.schema[col_name]
    status = "PASSOU" if campo.dataType.typeName() == expected_type_name else "FALHOU"
    print(f"  [{status}] {label}: tipo={campo.dataType.typeName()} (esperado={expected_type_name})")
    return status == "PASSOU"

def run_quality_checks():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    env = os.environ.get("ENV", "dev")
    if env == "dev":
        silver_path = os.path.join(project_root, "datalake_sample", "silver", "alfabetizacao_municipios_obt")
        gold_path = os.path.join(project_root, "datalake_sample", "gold", "agg_alfabetizacao_uf")
    else:
        silver_path = os.path.join(project_root, "datalake", "silver", "alfabetizacao_municipios_obt")
        gold_path = os.path.join(project_root, "datalake", "gold", "agg_alfabetizacao_uf")

    erros = 0

    print("=" * 70)
    print("VALIDACAO DE QUALIDADE — CAMADA SILVER (OBT)")
    print("=" * 70)

    try:
        df_silver = spark.read.parquet(silver_path)
    except Exception as e:
        print(f"ERRO: Nao foi possivel ler a Silver. Rode o pipeline primeiro.\n  {e}")
        spark.stop()
        return

    total = df_silver.count()
    print(f"Total de registros na OBT: {total}\n")

    print("--- 1. VERIFICACAO DE CHAVES DUPLICADAS ---")
    if not check_duplicates(df_silver, ["id_municipio", "ano", "rede"], "Chave composta (id_municipio, ano, rede)"):
        erros += 1

    print("\n--- 2. TIPAGEM DE CHAVES (ZEROS A ESQUERDA) ---")
    if not check_type(df_silver, "id_municipio", "string", "id_municipio deve ser String"):
        erros += 1

    print("\n--- 3. TAXA DE ALFABETIZACAO (0 A 100) ---")
    if not check_range(df_silver, "taxa_alfabetizacao", 0.0, 100.0, "taxa_alfabetizacao [0,100]"):
        erros += 1

    print("\n--- 4. METAS EDUCACIONAIS (REDE MUNICIPAL) ---")
    df_mun = df_silver.filter(col("rede") == "Municipal")
    total_mun = df_mun.count() if df_mun.count() > 0 else 1
    if "meta_alfabetizacao_2024" in df_silver.columns:
        nulls_meta = df_mun.filter(col("meta_alfabetizacao_2024").isNull()).count()
        pct = (nulls_meta / total_mun) * 100
        if pct <= 10:
            print(f"  [PASSOU] meta_alfabetizacao_2024: {100-pct:.1f}% preenchida na rede Municipal")
        else:
            print(f"  [FALHOU] meta_alfabetizacao_2024: apenas {100-pct:.1f}% preenchida na rede Municipal")
            erros += 1

    print("\n--- 5. NULOS ESTRUTURAIS (PRESERVACAO) ---")
    null_cols_proporcao = [c for c in df_silver.columns if c.startswith("proporcao_aluno_nivel_")]
    for c in null_cols_proporcao:
        nulls = df_silver.filter(col(c).isNull()).count()
        pct = (nulls / total) * 100 if total > 0 else 0
        print(f"    {c}: {nulls}/{total} nulos ({pct:.1f}%) — PRESERVADO")

    print("\n--- 6. DEFICIT ABSOLUTO (NAO NEGATIVO) ---")
    if "deficit_absoluto_proxy" in df_silver.columns:
        if not check_range(df_silver, "deficit_absoluto_proxy", 0.0, float("inf"), "deficit >= 0"):
            erros += 1

    print("\n--- 7. SIGLA UF (SEM UNKNOWNS) ---")
    unknowns = df_silver.filter(col("sigla_uf") == "Unknown").count()
    if unknowns == 0:
        print("  [PASSOU] Nenhum municipio com UF 'Unknown'")
    else:
        print(f"  [FALHOU] {unknowns} municipios com UF 'Unknown'")
        erros += 1

    print("\n--- 8. NOME_MUNICIPIO (ENRIQUECIMENTO) ---")
    if "nome_municipio" in df_silver.columns:
        check_null_pct(df_silver, "nome_municipio", 1.0, "nome_municipio deve ser >= 99% preenchido")

    print("\n" + "=" * 70)
    print("VALIDACAO DE QUALIDADE — CAMADA GOLD")
    print("=" * 70)

    try:
        df_gold = spark.read.parquet(gold_path)
    except Exception as e:
        print(f"AVISO: Gold nao encontrada ({e})")
        spark.stop()
        return

    total_gold = df_gold.count()
    print(f"Total de registros na Gold: {total_gold}")

    if not check_range(df_gold, "taxa_alfabetizacao_media", 0.0, 100.0, "taxa_alfabetizacao_media [0,100]"):
        erros += 1

    if not check_range(df_gold, "qtd_municipios_analisados", 1, float("inf"), "qtd_municipios >= 1"):
        erros += 1

    print("\n" + "=" * 70)
    if erros == 0:
        print("RESULTADO: TODAS AS VALIDACOES PASSARAM")
    else:
        print(f"RESULTADO: {erros} VALIDACAO(OES) FALHARAM — REVISE OS PONTOS ACIMA")
    print("=" * 70)

    spark.stop()

if __name__ == "__main__":
    run_quality_checks()
