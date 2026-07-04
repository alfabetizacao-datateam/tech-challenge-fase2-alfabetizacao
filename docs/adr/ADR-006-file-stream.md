# ADR-006: File Stream vs Apache Kafka para Streaming

- **Status:** ACCEPTED | **Data:** 2026-06-15 | **Risco:** MÉDIO (Migração futura para Kafka é simples)

---

## 1. CONTEXTO

- **Desafio:** Tech Challenge precisa processar eventos em tempo real (novas medições SAEB, mudanças em dados de censo).

- **Opções arquiteturais:**
1. **Apache Kafka:** Message broker robusto (exactly-once, replay, múltiplos consumidores, escalável)
2. **File Stream:** Spark Structured Streaming lendo diretório JSON local

- **Restrição:** Desenvolvimento local em Windows com Docker limitado; produção não é crítica em 2026 (MVP).

---

## 2. DECISÃO

- **Escolha:** File Stream (Spark Structured Streaming) em dev/demo + Kafka pronto para produção.

- **Implementação:**
- Producer escreve eventos JSON em `dados_sample/streaming_events/` ou `/datalake_sample/streaming_events/`
- Spark lê continuamente: `spark.readStream.json("dados_sample/streaming_events")`
- Processa + escreve para `datalake_sample/streaming_refined/`
- Em produção futura: trocar apenas a fonte (Kafka) sem reescrever a lógica

---

## 3. CONSEQUÊNCIAS

**Vantagens:**
- Sem dependência Docker em dev (simplifica local setup)
- Código idêntico funciona em dev e produção (muda apenas a fonte de dados)
- Fast prototyping: 1 dia de implementação vs 3 dias com Kafka

**Limitações:**
- Sem exactly-once delivery (arquivo lido 1x, risco de duplicação em failover)
- Sem replay (se código quebrar, não recupera eventos antigos)
- Sem múltiplos consumidores (não escalável em equipes)
- Latência: batch cada N minutos (vs Kafka em milissegundos)

---

## 4. ALTERNATIVAS

| Alternativa | Vantagem | Por Quê Rejeitada |
|-------------|---------|------------------|
| **Kafka (Apache/Confluent)** | Exatly-once, Replay, Enterprise | Docker+Jvm+Zookeeper complexity em Windows 11; overkill para MVP |
| **Amazon Kinesis** | Managed, Scale automático | Cloud dependency; custo (não gratuito) |
| **Cloud Pub/Sub (GCP)** | Managed, integrado com BigQuery | Cloud dependency; projeto é local-first |
| **File Stream (Spark)** | Simples, sem infra externa | Sem garantias de entrega; não escalável |

---

## 5. PADRÃO DE IMPLEMENTAÇÃO

- **Estrutura de diretório:**
```
dados_sample/
├── streaming_events/ # Entrada (producer escreve aqui)
│ └── 2026-06-23T15-00.json
│ └── 2026-06-23T15-05.json
│ └── ...
└── streaming_refined/ # Saída (Spark Streaming escreve aqui)
    └── 2026-06-23/
        └── refined_events.parquet
```

- **Código:**
```python
# src/streaming/02_consumer_streaming.py
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession.builder.appName("StreamingConsumer").getOrCreate()

# Readstream do diretório
df_stream = spark.readStream \
    .json("dados_sample/streaming_events") \
    .withColumn("processed_at", f.current_timestamp())

# Processar (ex: enriquecer com dados Silver)
df_enriched = df_stream.join(spark.read.parquet("datalake_sample/silver/..."), on="id_municipio")

# Write (append mode)
query = df_enriched.writeStream \
    .format("parquet") \
    .mode("append") \
    .option("path", "datalake_sample/streaming_refined") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()
```

---

## 6. GATILHO DE MIGRAÇÃO PARA KAFKA

Quando migrar? Se/quando:
- [ ] Múltiplas equipes precisam ler o mesmo stream
- [ ] Latência < 1min se torna crítica (vs batch de 5min)
- [ ] Falhas de rede exigem replay automático
- [ ] Volume > 10k eventos/minuto

---

## 7. CRITÉRIOS DE ACEITAÇÃO

- [ ] Producer escreve JSON válido em `dados_sample/streaming_events/`
- [ ] Spark Structured Streaming `readStream` lê sem erro
- [ ] Checkpoint permite continuação após parada/reinício
- [ ] Output em Parquet com timestamp de processamento
- [ ] Documentação menciona "Pronto para Kafka em produção"

---
