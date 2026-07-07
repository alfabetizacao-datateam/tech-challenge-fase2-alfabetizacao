import os
import json
import time
import urllib.request
import urllib.error
import concurrent.futures
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, round as spark_round, when, lit
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
hadoop_home = os.path.join(project_root, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + os.path.join(hadoop_home, "bin")
if "JAVA_HOME" in os.environ:
    java_bin = os.path.join(os.environ["JAVA_HOME"], "bin")
    if java_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")

SICONFI_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/dca"
REQUEST_DELAY = 0.15
CACHE_FILE = "siconfi_educacao_cache.json"

class SiconfiClient:
    def __init__(self, cache_path: str):
        self.cache_path = cache_path
        self.cache = self._load_cache()

    def _load_cache(self) -> dict:
        if os.path.exists(self.cache_path):
            with open(self.cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def _save_cache(self):
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(self.cache, f, ensure_ascii=False, indent=2)

    @staticmethod
    def http_fetch_educacao(id_ente: str, ano: int = 2024) -> float | None:
        params = f"?an_exercicio={ano}&id_ente={id_ente}&anexo=DCA-Anexo%20I-E&cod_conta=TotalDespesas"
        url = SICONFI_URL + params
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
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
        except Exception as e:
            logger.warning(f"  Erro ao buscar {id_ente}/{ano}: {e}")
        return None

    def fetch_educacao(self, id_ente: str, ano: int = 2024) -> float | None:
        cache_key = f"{id_ente}_{ano}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        valor = self.http_fetch_educacao(id_ente, ano)
        self.cache[cache_key] = valor
        return valor

    def flush(self):
        self._save_cache()


def get_spark_session():
    return SparkSession.builder.appName("SiconfiIngestion").getOrCreate()


def enrich_silver_with_siconfi(
    spark: SparkSession,
    silver_obt_path: str,
    output_path: str,
    siconfi_cache_path: str,
    ano: int = 2024,
):
    logger.info("Lendo OBT da Silver...")
    df_silver = spark.read.parquet(silver_obt_path)

    pdf = df_silver.select("id_municipio").distinct().toPandas()
    total_mun = len(pdf)
    logger.info(f"Total de municipios unicos na OBT: {total_mun}")

    client = SiconfiClient(siconfi_cache_path)
    resultados = []

    id_list = [(str(row["id_municipio"]).strip(), ano) for _, row in pdf.iterrows()]
    uncached = [(id_mun, a) for id_mun, a in id_list if f"{id_mun}_{a}" not in client.cache]
    cached = [(id_mun, a) for id_mun, a in id_list if f"{id_mun}_{a}" in client.cache]
    logger.info(f"  Cache hits: {len(cached)}, to fetch: {len(uncached)}")

    for id_mun, a in cached:
        resultados.append({"id_municipio": id_mun, "ano": a, "despesa_educacao": client.cache[f"{id_mun}_{a}"]})

    fetched = 0
    MAX_WORKERS = 8
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(SiconfiClient.http_fetch_educacao, id_mun, a): (id_mun, a) for id_mun, a in uncached}
        for idx, future in enumerate(concurrent.futures.as_completed(futures)):
            id_mun, a = futures[future]
            val = future.result()
            fetched += 1
            client.cache[f"{id_mun}_{a}"] = val
            resultados.append({"id_municipio": id_mun, "ano": a, "despesa_educacao": val})
            if (idx + 1) % 100 == 0:
                logger.info(f"  Baixados {idx+1}/{len(uncached)} municipios")
                client.flush()

    client.flush()
    logger.info(f"Final: {len(resultados)} registros (cache={len(cached)}, fetched={fetched})")

    pdf_result = pd.DataFrame(resultados)
    temp_path = os.path.join(os.path.dirname(siconfi_cache_path), "siconfi_temp.parquet")
    pdf_result.to_parquet(temp_path, index=False)

    df_siconfi = spark.read.parquet(temp_path)
    df_siconfi = df_siconfi.withColumn("id_municipio", col("id_municipio").cast("string"))
    df_silver = df_silver.withColumn("id_municipio", col("id_municipio").cast("string"))

    df_enriched = df_silver.join(df_siconfi, on=["id_municipio", "ano"], how="left")

    if "populacao_total" in df_enriched.columns:
        df_enriched = df_enriched.withColumn(
            "gasto_por_habitante_educacao",
            spark_round(col("despesa_educacao") / col("populacao_total"), 2),
        )
        # Custo por ponto PER CAPITA (R$/hab/ponto): usa gasto_por_habitante (nao a
        # despesa total) para evitar distorcao de escala populacional — ver ADR-012.
        # Precisa bater com src/cloud/dataproc_04_siconfi.py (mesma formula).
        df_enriched = df_enriched.withColumn(
            "custo_por_ponto_alfabetizacao",
            spark_round(
                when(col("taxa_alfabetizacao").isNotNull(),
                     col("gasto_por_habitante_educacao") / (col("taxa_alfabetizacao") + 1))
                .otherwise(lit(None)),
                2
            ),
        )

    logger.info(f"Salvando OBT enriquecida em: {output_path}")
    df_enriched.write.format("parquet").mode("overwrite").partitionBy("ano", "rede").save(output_path)

    logger.info("SICONFI enrichment concluido!")
    return df_enriched


if __name__ == "__main__":
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    env = os.environ.get("ENV", "dev")
    if env == "dev":
        silver_obt = os.path.join(project_root, "datalake_sample", "silver", "alfabetizacao_municipios_obt")
        output = os.path.join(project_root, "datalake_sample", "silver", "alfabetizacao_municipios_obt_enriquecido")
    else:
        silver_obt = os.path.join(project_root, "datalake", "silver", "alfabetizacao_municipios_obt")
        output = os.path.join(project_root, "datalake", "silver", "alfabetizacao_municipios_obt_enriquecido")

    cache_dir = os.path.join(project_root, "datalake_sample" if env == "dev" else "datalake", "bronze")
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, CACHE_FILE)

    enrich_silver_with_siconfi(spark, silver_obt, output, cache_path)
    spark.stop()
