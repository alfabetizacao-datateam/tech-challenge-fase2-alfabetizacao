# ADR-005: `id_municipio` Como STRING (Preservar Zeros à Esquerda)

- **Status:** ACCEPTED | **Data:** 2026-05-15 | **Criticidade:** CRÍTICA | **Risco:** Data Loss

---

## 1. CONTEXTO

- **Dado:** `id_municipio` vem do INEP com formato "0120000" (7 dígitos IBGE com zeros à esquerda).

- **Problema:** Spark `inferSchema=True` (na Bronze) vê "0120000" e detecta como INT. Cast para INT = **remove zeros à esquerda**:
```
"0120000" (String) → 120000 (Int) → "120000" (6 dígitos, perda!)
```

- **Impacto Crítico:**
- LEFT JOIN com IBGE API (que retorna 7 dígitos) falha: `120000` ≠ `0120000`
- LEFT JOIN com SICONFI API falha pelo mesmo motivo
- Múltiplos estados perdem enriquecimento IBGE (nome_municipio, populacao)
- Rastreabilidade quebra: "de qual município vem este registro?"

- **Exemplo Real:**
```
INEP: Guamaré/RN → id_municipio = "0120000" (7 chars)
IBGE: Guamaré/RN → id = 120000 (5 chars)

Se cast para INT:
  "0120000" → 120000 (Spark INT)
  JOIN: WHERE id_inep = id_ibge
  120000 ≠ 120000 FALHA (comparação interna float vs int, ou 6 vs 5 dígitos)
```

---

## 2. DECISÃO

- **Escolha:** Forçar `id_municipio` como **STRING** em TODOS os layers. NUNCA fazer cast para INT.

- **Implementação:**
```python
# Em Bronze (01_ingestao_bronze_batch.py):
df = df.withColumn("id_municipio", f.col("id_municipio").cast(StringType()))

# Em Silver (02_silver_transform.py):
schema = {
    "id_municipio": StringType(), # Explícito
    ...
}
df = spark.read.schema(schema).csv("...")
```

- **Comunicação:**
- CLAUDE.md linha 12: "Nunca remover Zeros à esquerda de `id_municipio`"
- DICIONARIO_DADOS.md: "Forçar como STRING para garantir JOINs espaciais/IBGE"
- Code comment em scripts: `# id_municipio MUST stay STRING (JOIN keys depend on it)`

---

## 3. CONSEQUÊNCIAS

**Vantagens:**
- JOINs com IBGE e SICONFI funcionam 100% (correspondência exata)
- Rastreabilidade garantida (7 dígitos sempre = mesmo município)
- Portabilidade: dados são válidos em BigQuery, DuckDB, Postgres (todos suportam STRING)
- Audit trail: quem processou sabe que `"0120000"` = Guamaré/RN (não ambiguo)

**Custos:**
- Storage negligencialmente maior (STRING "0120000" vs INT 120000 = +3 bytes/linha × 23k = 70KB negligenciável)
- Performance JOIN negligencialmente menor (string comparison é < 1% do tempo total)
- Cultura: "INT é tipo natural para ID" é instinto errado (IDs DEVEM ser STRING se têm semântica)

---

## 4. ALTERNATIVAS REJEITADAS

| Opção | Problema |
|-------|----------|
| **Cast para INT** | Remove zeros; quebra JOINs; REJEITADO |
| **Cast + zfill(7)** | Workaround frágil (e se ID tem 8 dígitos em futuro?) | REJEITADO |
| **Use códigos numéricos diferentes** (ex: index 0-4342) | Perde rastreabilidade ao IBGE | REJEITADO |
| **Manter STRING desde bronze** | Correto, simples, confiável | ESCOLHIDO |

---

## 5. IMPLEMENTAÇÃO

- **Em 01_ingestao_bronze_batch.py:**
```python
def ingest_bronze(source_dir, bronze_dir, env="dev"):
    spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

    for file in glob.glob(source_dir + "/*.csv"):
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file)

        # Force id_municipio to STRING (no INT truncation)
        if "id_municipio" in df.columns:
            df = df.withColumn("id_municipio", f.col("id_municipio").cast(StringType()))

        table_name = os.path.basename(file).replace(".csv", "")
        df.write.format("parquet").mode("overwrite").save(f"{bronze_dir}/{table_name}")
```

- **Em 02_silver_transform.py:**
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("id_municipio", StringType(), False), # MUST be STRING
    StructField("ano", IntegerType(), False),
    StructField("rede", StringType(), True),
    StructField("taxa_alfabetizacao", DoubleType(), True),
    # ...
])

df = spark.read.schema(schema).parquet(f"{bronze_dir}/avaliacao")
```

- **Code Review Checklist:**
- [ ] Nenhuma chamada a `.cast(IntegerType())` em `id_municipio`
- [ ] Schema.yaml explicitamente marca `id_municipio: string`
- [ ] Comentário no código: `# id_municipio is STRING to preserve leading zeros (JOIN key)`

---

## 6. VALIDAÇÃO

- **Great Expectations Check:**
```python
# Verify no INT truncation happened
assert df['id_municipio'].dtype == 'object' # String
assert all(len(x) == 7 for x in df['id_municipio']) # All 7 digits
assert df['id_municipio'].str.startswith('0').sum() > 0 # Has leading zeros
```

- **Test:**
```python
def test_id_municipio_string_preservation():
    # Read sample INEP CSV
    original = pd.read_csv("dados/raw.csv")
    assert original['id_municipio'].iloc[0] == "0120000"

    # Read from Bronze (after Spark processing)
    bronze = spark.read.parquet("datalake/bronze/avaliacao")
    bronze_pd = bronze.toPandas()
    assert bronze_pd['id_municipio'].iloc[0] == "0120000"

    # Verify no INT happened
    assert isinstance(bronze_pd['id_municipio'].iloc[0], str)
```

---

## 7. CRITICAL INTEGRATION TESTS

**Test 1: JOIN with IBGE**
```python
# Read Silver
silver = spark.read.parquet("datalake/silver/obt")

# Simulate IBGE data
ibge = spark.createDataFrame([
    ("0120000", "Guamaré"),
    ("3500107", "São Paulo"),
])

# JOIN must work (100% match)
joined = silver.join(ibge, silver.id_municipio == ibge.id, how='left')
assert joined.filter(joined.nome_municipio == "Guamaré").count() > 0 # Shouldn't be 0
```

**Test 2: JOIN with SICONFI**
```python
# SICONFI also uses "0120000" format
siconfi = spark.createDataFrame([
    ("0120000", 45000000),
])

joined = silver.join(siconfi, silver.id_municipio == siconfi.id_ente)
assert joined.filter(joined.despesa_educacao > 0).count() > 0 # Not null
```

---

## 8. CRITERIA DE ACEITAÇÃO

- [ ] Bronze: `id_municipio` é STRING (`.cast(StringType())`)
- [ ] Silver: Schema explicitamente `id_municipio: StringType()`
- [ ] All values maintain 7 digits (no leading zero loss)
- [ ] JOINs com IBGE API: 100% match (no unmatched rows due to ID format)
- [ ] JOINs com SICONFI: 100% match
- [ ] DICIONARIO_DADOS.md explains: "ALWAYS STRING to preserve IBGE correspondence"
- [ ] Test suite passes (integration tests + sanity checks)

---

Última revisão: 2026-06-23 | Crítica em JOINs com APIs externas
