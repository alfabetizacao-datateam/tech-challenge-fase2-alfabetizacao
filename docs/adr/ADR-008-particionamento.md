# ADR-008: Particionamento Silver = `ano + rede` vs `ano/UF/rede`

- **Status:** ACCEPTED | **Data:** 2026-06-15 | **Decisão:** Performante (queries 99% filtram ano+rede)

---

## 1. CONTEXTO

- **Dado:** OBT Silver com 23.995 municipios × 32 colunas, particionado por quê?

- **Análise de queries (histórico stakeholders):**
- 99% das queries: "Taxa por estado-rede em 2024" → filtra `ano=2024 AND rede='Municipal'`
- 0.5%: "Todos os dados de São Paulo" → filtra `sigla_uf='SP'` (sem filtro ano)
- 0.5%: Ad-hoc exploratórias

- **Opções:**
1. `partitionBy(ano, rede)` → ~40 partições (2 anos × 4 redes × 5 dedup)
2. `partitionBy(ano, sigla_uf, rede)` → ~400 partições (2 × 27 × 4 × dedup)
3. Sem partição → Spark escaneia 23.995 linhas em cada query

---

## 2. DECISÃO

- **Escolha:** Particionar por `(ano, rede)` apenas.

- **Razão:**
- Estrutura: `datalake/silver/alfabetizacao_municipios_obt/ano=2024/rede=Municipal/`
- Queries tipicamente filtram por AMBAS ano E rede
- Sem partição: Query típica varre 23k linhas
- Com particionamento: Query típica varre 1 subdir (~6k linhas)
- **Speedup:** ~4x em queries típicas

---

## 3. TRADE-OFF

- **Custo:** Queries por sigla_uf isolada escaneiam TODAS as partições

```sql
SELECT * FROM silver WHERE sigla_uf = 'SP'
-- Vai escanear: ano=2023/rede={Federal,Estadual,Municipal,Privada}
-- + ano=2024/rede={Federal,Estadual,Municipal,Privada}
-- Total: 8 subdiretórios vs 1
```

- **Frequência:** < 5% das queries. Aceitável.

---

## 4. IMPLEMENTAÇÃO

```python
# src/batch/02_silver_transform.py
df_silver.write \
    .format("parquet") \
    .mode("overwrite") \
    .partitionBy("ano", "rede") \
    .save("datalake/silver/alfabetizacao_municipios_obt")
```

- **Resultado:**
```
datalake/silver/alfabetizacao_municipios_obt/
├── ano=2023/
│ ├── rede=Federal/part-00000.parquet
│ ├── rede=Estadual/part-00000.parquet
│ ├── rede=Municipal/part-00000.parquet
│ └── rede=Privada/part-00000.parquet
├── ano=2024/
│ ├── rede=Federal/part-00000.parquet
│ ...
```

---

## 5. GATILHO DE REVISÃO

Se histórico expandir para 10+ anos:
- Partições + (ano, rede): ~80 (2x40) — ainda gerenciável
- Se expandir para escola/turma (granularidade fina): Revisar

---
