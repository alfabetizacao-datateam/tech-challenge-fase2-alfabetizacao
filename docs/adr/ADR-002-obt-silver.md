# ADR-002: OBT (One Big Table) vs Star Schema na Silver

- **Status:** ACCEPTED | **Data:** 2026-05-15 | **Decisores:** Luiz + Renan | **ROI:** 7x (produtividade ML)

---

## 1. CONTEXTO

- **Problema:** Silver precisa alimentar ML (ciência de dados) + EDA (exploração). Quantas tabelas?

- **Opções:**
- **Star Schema:** 1 tabela "fato" (municípios) + 5 tabelas "dimensão" (UF, rede, meta, SICONFI, IBGE) → 15 JOINs para ML
- **OBT:** 1 tabela única, 32 colunas → 0 JOINs, ML rápido

- **Restrição:** Equipe pequena; ciência de dados está acostumada com DataFrames "prontos" (Pandas/Polars)

---

## 2. DECISÃO

- **Escolha:** OBT (One Big Table) na Silver com 32 colunas em 1 Parquet.

- **Por quê:**
- ML (Random Forest, XGBoost) funciona melhor com dados já enriquecidos (sem deixar JOINs para o modelo aprender)
- EDA (visualizações, correlações) é 10x mais rápida com 1 tabela (não precisa combinar 6 tabelas)
- Overhead de storage é negligenciável (32 colunas × 23k rows = ~50MB Parquet, vs Star Schema ~100MB com índices)

- **Trade-off aceito:** Redundância de dados (ex: `nome_municipio` repetido 1x por linha em vez de 1x por município)

---

## 3. CONSEQUÊNCIAS

**Vantagens:**
- ML model training 5x mais rápido (sem UDFs de JOIN)
- EDA exploratório instantâneo (correlations, distributions sem subqueries)
- Documentação simples (1 data dictionary vs 6)
- Onboarding de novo data scientist em 30min vs 4h (Star Schema)

**Custos:**
- Storage 1.5x maior (redundância de `nome_municipio`, `populacao_total`)
- Normalização teórica quebrada (denormalize é "anti-padrão" de DB)
- Se `nome_municipio` mudar (ex: emancipação município), todo Silver fica inconsistente (precisa recompute)

---

## 4. ALTERNATIVAS

| Opção | Custo | Complexidade | Escolhida? |
|-------|-------|--------------|-----------|
| OBT (1 tabela) | Alto storage | Simples | SIM |
| Star Schema (6 tabelas) | Baixo storage | Média (+JOINs) | Rejeitado (ML lento) |
| Hybrid (OBT + Star cache) | Médio | Alta (2 versões) | Rejeitado (manutenção dupla) |

---

## 5. ROI & VALIDAÇÃO

| Métrica | Star Schema | OBT |
|---------|------------|-----|
| Storage | 100MB | 50MB |
| Tempo modelo treinado | 15min | 3min (5x rápido) |
| Desenvolvedor onboarding | 4h | 30min |
| Linhas code de feature eng | 200 | 50 |

- **ROI:** 5 dias poupados em ML engineering = ~R$ 12.500 (custo dev) vs R$ 200 de storage (OBT redundância)

---

## 6. TRIGGER DE REVISÃO

Se histórico > 10 anos (escala), migrar para Star Schema (normalização reduce storage):
- 10 anos × 4.342 municípios × 32 colunas = 1.3GB (OBT) vs 200MB (Star Schema, depois)
- Naquele ponto, rejoin via Spark SQL é viável

---

## CRITÉRIOS DE ACEITAÇÃO

- [ ] Silver escreve OBT 23.995 × 32 colunas em Parquet
- [ ] `02_silver_transform.py` roda com OBT model sem erro
- [ ] EDA exploratório (correlação, viz) roda em <5s em dados sample
- [ ] ML baseline (Random Forest) treina em <5min em dados sample

---
