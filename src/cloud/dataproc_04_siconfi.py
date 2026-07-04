"""
SICONFI enrichment for Dataproc/GCS.
Usage: pyspark --bucket gs://bucket-name [--ano 2024]

Reads:
  gs://{bucket}/silver/alfabetizacao_municipios_obt
Writes:
  gs://{bucket}/silver/alfabetizacao_municipios_obt_enriquecido
  gs://{bucket}/siconfi/cache.json  (cache persistente entre execucoes)

Fluxo:
  1. Le Silver OBT do GCS
  2. Baixa cache SICONFI do GCS (evita re-fetch de ~3.500 municipios)
  3. Busca API Tesouro Nacional para municipios fora do cache (8 workers paralelos)
  4. Salva cache atualizado no GCS
  5. Faz JOIN Silver + SICONFI, calcula features financeiras
  6. Salva Silver enriquecido no GCS (particionado por ano/rede)
"""
import argparse
import json
import io
import concurrent.futures
import urllib.request
import urllib.error
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, when, lit
import pandas as pd

SICONFI_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/dca"
CACHE_GCS_PREFIX = "siconfi"
MAX_WORKERS = 8
REQUEST_TIMEOUT = 12
DELAY_BETWEEN_REQUESTS = 0.1


# ---------------------------------------------------------------------------
# Spark + GCS helpers
# ---------------------------------------------------------------------------

def get_spark():
    return SparkSession.builder.appName("SiconfiEnrichment-GCS").getOrCreate()


def gcs_file_exists(spark, gcs_path: str) -> bool:
    """Verifica se um arquivo existe no GCS usando Hadoop FileSystem API."""
    try:
        sc = spark.sparkContext
        Path = sc._jvm.org.apache.hadoop.fs.Path
        fs = Path(gcs_path).getFileSystem(sc._jsc.hadoopConfiguration())
        return fs.exists(Path(gcs_path))
    except Exception:
        return False


def gcs_read_text(spark, gcs_path: str) -> str:
    """Le conteudo de um arquivo texto do GCS."""
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    fs = Path(gcs_path).getFileSystem(sc._jsc.hadoopConfiguration())
    stream = fs.open(Path(gcs_path))
    reader = sc._jvm.java.io.BufferedReader(
        sc._jvm.java.io.InputStreamReader(stream, "UTF-8")
    )
    lines = []
    line = reader.readLine()
    while line is not None:
        lines.append(line)
        line = reader.readLine()
    reader.close()
    return "\n".join(lines)


def gcs_write_text(spark, gcs_path: str, content: str) -> None:
    """Escreve conteudo texto em um arquivo no GCS."""
    sc = spark.sparkContext
    Path = sc._jvm.org.apache.hadoop.fs.Path
    fs = Path(gcs_path).getFileSystem(sc._jsc.hadoopConfiguration())
    stream = fs.create(Path(gcs_path), True)  # True = overwrite
    writer = sc._jvm.java.io.OutputStreamWriter(stream, "UTF-8")
    writer.write(content)
    writer.close()
    stream.close()


# ---------------------------------------------------------------------------
# Cache SICONFI persistente no GCS
# ---------------------------------------------------------------------------

def load_cache_from_gcs(spark, bucket: str) -> dict:
    """
    Tenta carregar cache JSON do GCS.
    Cache evita re-fetch de municipios ja consultados em execucoes anteriores.
    Formato: {"id_municipio_ano": valor_float_ou_null, ...}
    """
    cache_path = f"{bucket}/{CACHE_GCS_PREFIX}/cache.json"
    if not gcs_file_exists(spark, cache_path):
        print(f"  Cache GCS nao encontrado em {cache_path} — iniciando cache vazio.")
        return {}
    try:
        raw = gcs_read_text(spark, cache_path)
        cache = json.loads(raw)
        print(f"  Cache GCS carregado: {len(cache)} entradas de {cache_path}")
        return cache
    except Exception as e:
        print(f"  AVISO: Erro ao ler cache GCS ({e}) — iniciando cache vazio.")
        return {}


def save_cache_to_gcs(spark, bucket: str, cache: dict) -> None:
    """
    Persiste cache JSON no GCS.
    Chamado a cada 200 municipios processados e no final da execucao.
    """
    cache_path = f"{bucket}/{CACHE_GCS_PREFIX}/cache.json"
    try:
        gcs_write_text(spark, cache_path, json.dumps(cache, ensure_ascii=False, indent=2))
        print(f"  Cache salvo no GCS: {len(cache)} entradas -> {cache_path}")
    except Exception as e:
        print(f"  AVISO: Nao foi possivel salvar cache no GCS: {e}")


# ---------------------------------------------------------------------------
# API SICONFI (Tesouro Nacional)
# ---------------------------------------------------------------------------

def http_fetch_siconfi(id_ente: str, ano: int) -> float | None:
    """
    Consulta a API do Tesouro Nacional (SICONFI) para um municipio+ano.

    URL: apidatalake.tesouro.gov.br/ords/siconfi/tt/dca
    Parametros:
      - an_exercicio: ano fiscal (ex: 2024)
      - id_ente: codigo IBGE do municipio (7 digitos, ex: 1100015)
      - anexo: DCA-Anexo I-E (Demonstrativo das Contas Anuais)
      - cod_conta: TotalDespesas

    Filtra pelo item "12 - Educacao" dentro das despesas totais.
    Retorna o valor em R$ (float) ou None se nao encontrar.

    Exemplo de resposta da API:
      {
        "items": [
          {"conta": "12 - Educacao e Cultura", "cod_conta": "TotalDespesas", "valor": 15234567.89}
        ]
      }
    """
    params = (
        f"?an_exercicio={ano}"
        f"&id_ente={id_ente}"
        f"&anexo=DCA-Anexo%20I-E"
        f"&cod_conta=TotalDespesas"
    )
    url = SICONFI_URL + params
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        for item in data.get("items", []):
            if "12 - Educa" in item.get("conta", "") and "TotalDespesas" in item.get("cod_conta", ""):
                raw = item.get("valor", 0)
                if isinstance(raw, str):
                    raw = raw.replace(",", ".").replace(" ", "")
                try:
                    return float(raw) if raw is not None else None
                except (ValueError, TypeError):
                    continue
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        print(f"    HTTPError {e.code} para {id_ente}/{ano}")
    except Exception as e:
        print(f"    Erro ao buscar {id_ente}/{ano}: {type(e).__name__}")
    return None


# ---------------------------------------------------------------------------
# Ingestao SICONFI com cache + paralelismo
# ---------------------------------------------------------------------------

def fetch_siconfi_for_municipios(municipios: list[str], ano: int, cache: dict, spark=None, bucket: str = None) -> dict:
    """
    Busca dados SICONFI para uma lista de id_municipio.

    Estrategia:
      1. Separa municipios em: ja em cache vs. nao em cache
      2. Faz fetch paralelo dos nao-cacheados (ThreadPoolExecutor, MAX_WORKERS=8)
      3. Salva cache no GCS a cada 200 municipios processados (flush parcial)
      4. Retorna dict {id_municipio: valor_float_ou_None}

    Por que ThreadPoolExecutor e nao Spark paralelo?
      - Requests HTTP sao IO-bound (espera de rede), nao CPU-bound
      - ThreadPoolExecutor e mais eficiente para IO: enquanto 1 thread espera,
        outras avancam. Spark usaria processos separados (overhead de serializacao)
      - 8 workers = ~8 requests simultaneos. API SICONFI suporta bem esse volume.
    """
    cache_hits = [m for m in municipios if f"{m}_{ano}" in cache]
    to_fetch = [m for m in municipios if f"{m}_{ano}" not in cache]

    print(f"  Municipios em cache: {len(cache_hits)} | A buscar na API: {len(to_fetch)}")

    resultados = {m: cache[f"{m}_{ano}"] for m in cache_hits}

    if not to_fetch:
        return resultados

    flush_counter = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(http_fetch_siconfi, id_mun, ano): id_mun
            for id_mun in to_fetch
        }
        for idx, future in enumerate(concurrent.futures.as_completed(futures)):
            id_mun = futures[future]
            try:
                valor = future.result()
            except Exception as e:
                print(f"    Excecao inesperada para {id_mun}: {e}")
                valor = None

            cache[f"{id_mun}_{ano}"] = valor
            resultados[id_mun] = valor
            flush_counter += 1

            if (idx + 1) % 200 == 0:
                print(f"    Progresso: {idx + 1}/{len(to_fetch)} municipios buscados...")
                if spark and bucket:
                    save_cache_to_gcs(spark, bucket, cache)

            time.sleep(DELAY_BETWEEN_REQUESTS)

    com_dados = sum(1 for v in resultados.values() if v is not None)
    print(f"  Resultado: {com_dados}/{len(municipios)} municipios com dados SICONFI ({com_dados/len(municipios)*100:.1f}%)")
    return resultados


# ---------------------------------------------------------------------------
# Enrichment principal
# ---------------------------------------------------------------------------

def run_siconfi_enrichment(spark, bucket: str, ano: int = 2024):
    """
    Orquestrador principal do enriquecimento SICONFI.

    Fluxo:
      Silver OBT (GCS) → fetch API SICONFI → Silver Enriquecida (GCS)
                          ↑
                     cache GCS (persistente)
    """
    print("=" * 70)
    print(f"[SICONFI] ENRIQUECIMENTO FINANCEIRO — ANO {ano}")
    print("=" * 70)

    silver_obt_path = f"{bucket}/silver/alfabetizacao_municipios_obt"
    output_path = f"{bucket}/silver/alfabetizacao_municipios_obt_enriquecido"

    print("1. Lendo Silver OBT do GCS...")
    df_silver = spark.read.parquet(silver_obt_path)
    total_registros = df_silver.count()
    print(f"   Total registros Silver: {total_registros:,}")

    print("2. Extraindo municipios unicos...")
    municipios = [
        str(row["id_municipio"]).strip()
        for row in df_silver.select("id_municipio").distinct().collect()
    ]
    print(f"   Municipios unicos: {len(municipios)}")

    print("3. Carregando cache SICONFI do GCS...")
    cache = load_cache_from_gcs(spark, bucket)

    print(f"4. Buscando dados SICONFI via API Tesouro Nacional (ano={ano})...")
    resultados = fetch_siconfi_for_municipios(municipios, ano, cache, spark=spark, bucket=bucket)

    print("5. Salvando cache atualizado no GCS...")
    save_cache_to_gcs(spark, bucket, cache)

    print("6. Construindo DataFrame SICONFI...")
    pdf_siconfi = pd.DataFrame([
        {"id_municipio": id_mun, "ano": ano, "despesa_educacao": valor}
        for id_mun, valor in resultados.items()
        if valor is not None
    ])
    print(f"   Municipios com dados financeiros: {len(pdf_siconfi)}/{len(municipios)}")

    if pdf_siconfi.empty:
        print("  AVISO: Nenhum dado SICONFI retornado pela API. Abortando enriquecimento.")
        print("  O pipeline Gold usara Silver OBT sem features financeiras.")
        return

    df_siconfi = spark.createDataFrame(pdf_siconfi)

    df_siconfi = df_siconfi.withColumn("id_municipio", col("id_municipio").cast("string"))
    df_silver = df_silver.withColumn("id_municipio", col("id_municipio").cast("string"))

    print("7. JOIN Silver + SICONFI (LEFT JOIN — preserva municipios sem dados financeiros)...")
    df_enriched = df_silver.join(df_siconfi, on=["id_municipio", "ano"], how="left")

    print("8. Calculando features financeiras derivadas...")
    if "populacao_total" in df_enriched.columns:
        df_enriched = df_enriched.withColumn(
            "gasto_por_habitante_educacao",
            spark_round(
                when(col("populacao_total") > 0, col("despesa_educacao") / col("populacao_total"))
                .otherwise(lit(None)),
                2
            )
        )
        # Custo por ponto PER CAPITA (R$/hab/ponto): usa gasto_por_habitante (nao a
        # despesa total) para evitar distorcao de escala populacional — ver ADR-012.
        # E a base do benchmark de custo marginal usado na projecao de investimento.
        df_enriched = df_enriched.withColumn(
            "custo_por_ponto_alfabetizacao",
            spark_round(
                when((col("taxa_alfabetizacao") + 1) > 0,
                     col("gasto_por_habitante_educacao") / (col("taxa_alfabetizacao") + 1))
                .otherwise(lit(None)),
                2
            )
        )
        print("   Features criadas: despesa_educacao, gasto_por_habitante_educacao, custo_por_ponto_alfabetizacao (per capita)")

    print(f"9. Salvando Silver enriquecida em: {output_path}")
    df_enriched.write.format("parquet") \
        .mode("overwrite") \
        .partitionBy("ano", "rede") \
        .save(output_path)

    print("=" * 70)
    print("[SICONFI] RESUMO")
    print(f"  Input Silver OBT     : {total_registros:,} registros")
    print(f"  Municipios SICONFI   : {len(pdf_siconfi):,}/{len(municipios)} ({len(pdf_siconfi)/len(municipios)*100:.1f}%)")
    print(f"  Municipios sem dados : {len(municipios) - len(pdf_siconfi)} (ficam com null nas cols financeiras)")
    print(f"  Output salvo em      : {output_path}")
    print(f"  Cache persistido em  : {bucket}/{CACHE_GCS_PREFIX}/cache.json")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SICONFI enrichment para pipeline cloud (Dataproc)")
    parser.add_argument("--bucket", required=True, help="gs://bucket-name (sem trailing slash)")
    parser.add_argument("--ano", type=int, default=2024, help="Ano fiscal SICONFI (default: 2024)")
    args = parser.parse_args()

    bucket = args.bucket.rstrip("/")
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    run_siconfi_enrichment(spark, bucket, args.ano)

    spark.stop()
    print("SICONFI enrichment finalizado.")
