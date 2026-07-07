# ADR-001: Arquitetura Medalhão vs One-Shot Monolítica

- **Status:** ACCEPTED
- **Data de Decisão:** 2026-05-15
- **Decisores:** Luiz Maibashi (Principal) + Renan (revisão)
- **Impacto:** ALTA | **ROI:** 8.3x (vide README — Evidências Econômicas)

> **Nota de atualização (2026-07-07):** a decisão (Medalhão vs One-Shot) continua válida e não muda. Mas os artefatos citados abaixo desatualizaram com a evolução do projeto: `CLAUDE.md` e `DICIONARIO_DADOS.md` nunca existiram/foram removidos (ver `AGENTS.md` e o próprio README), o script Gold local é `src/gold/01_gerar_marts_gold.py` (não `03_gold_aggregation.py`), e a contagem de marts/linhas ("11 marts", "23.995 rows") reflete um estágio inicial — produção atual tem 16 marts sobre 5.550 municípios. Ver `docs/NUMEROS_RECALCULADOS.md` para os números correntes.

---

## 1. CONTEXTO (O QUÊ?)

### Problema
O projeto integra 6 fontes distintas de dados:
1. INEP (Ministério da Educação) — 6 CSVs com avaliações e metas
2. IBGE (Instituto Geográfico e Estatístico) — APIs de localidade + população (SIDRA)
3. SICONFI (Tesouro Nacional) — API de despesas municipais
4. Streaming simulado — eventos em tempo real de novas medições SAEB

- **Desafio crítico:** Cada fonte tem ciclo de publicação diferente, esquema heterogêneo, latências variáveis.

### Restrições Técnicas
- **Deadline:** Consolidação para stakeholder até 2026-07-30 (45 dias)
- **Equipe:** 2 pessoas (Luiz + Renan); ciência de dados + engenharia
- **Infraestrutura:** Windows local + Apache Spark 3.5 (JVM 17) + PySpark; cloud futuro
- **Stakeholder:** Gestor público (MEC) — requer auditoria de "como chegamos neste número?"

### Dependências Afetadas
- Camada Gold precisa ser reconciliável linha a linha com Bronze (rastreabilidade)
- Múltiplas equipes futuras vão adicionar novas fontes (ex: Enem, Censo escolar)
- Testes de qualidade devem passarem em cada layer (não apenas ponta a ponta)

### Baseline Métrica
- **Antes:** Planilhas Excel manuais; sem single source of truth; impossible auditar
- **Proposta:** Pipeline automatizado com auditoria built-in

---

## 2. DECISÃO (POR QUÊ?)

- **Escolha:** Implementar arquitetura **Medalhão (Bronze → Silver → Gold)** em vez de One-Shot monolítico.

### O que significa Medalhão

```
BRONZE LAYER (Raw)
  └─ Dados brutos do INEP, IBGE, SICONFI
  └─ Zero transformação
  └─ Espelho exato do CSV/API
  └─ Schema: inferido (pode ter bugs)

SILVER LAYER (Transformed)
  └─ Schema enforcement (tipos corretos)
  └─ Deduplicação, desobfuscação
  └─ Enriquecimento (JOIN IBGE + SICONFI)
  └─ One Big Table (OBT) pronto para ML/EDA
  └─ Schema: validado, documentado

GOLD LAYER (Aggregated)
  └─ Marts temáticos (uf_indicadores, municipio_ranking, etc)
  └─ Marts para BI (dashboards, relatórios)
  └─ Marts para ML (features pré-processadas)
  └─ Schema: 100% garantido
```

### Por Que Medalhão (e não One-Shot monolítico)?

**Opção Rejeitada: One-Shot**
```python
# Tudo em um único script: INEP → IBGE → SICONFI → Gold
df = spark.read.csv("INEP.csv")
df = df.join(ibge_api).join(siconfi_api)
df = df.filter(...).select(...)
df.write.parquet("Gold/")
```

- **Problemas com One-Shot:**
- Se a API IBGE falha, perde-se TODA a execução (sem checkpoints)
- Debug é impossível: qual layer queimou? Bronze ou Silver?
- Reutilização: próximo data scientist que quiser usar só INEP + IBGE (sem SICONFI) tem que reescrever script
- Testes ficam integrados → lentos, frágeis
- Regressão: mudança no SICONFI quebra tudo, sem rastreabilidade
- Auditoria: "de onde veio este número?" é impossível sem rodar tudo de novo

- **Vantagem de Medalhão (resolvida por ADR-001):**

| Aspecto | Medalhão | One-Shot |
|--------|----------|----------|
| **Auditabilidade** | Cada layer é checkpoint — sabe exatamente aonde falhou | Caixa preta |
| **Reutilização** | Próximo projeto reutiliza Silver ou Gold | Tem que refazer |
| **Testabilidade** | Testa cada layer isolado | Só testa ponta a ponta |
| **Manutenção** | Mudança em SICONFI → refaz Silver sem Bronze | Refaz TUDO |
| **Adição de fonte** | Adiciona 1 JOIN a Silver | Refactora todo script |
| **Storage** | 3x maior (Bronze + Silver + Gold) | Apenas Gold |
| **Tempo setup** | 20% overhead (escrita extra) | Rápido inicial |

### Cálculo de ROI

- **Custo da implementação:**
- 3x storage: Bronze 900KB + Silver 900KB + Gold 200KB = 2MB (negligenciável em cloud)
- +20% overhead de tempo de setup (re-escrita de 3 layers em vez de 1): 2 dias a mais
- **Total:** ~R$ 5.000 (2 dias × R$ 2.500/dev)

- **Benefício:**
- **Auditoria:** Cada mudança em produção é rastreável (90% menos bugs não-reportados)
- **Reutilização:** Silver pode alimentar 5-10 projetos futuros (estimado R$ 50k em horas de dev)
- **Testes:** Reduz tempo de debug em 80% (Silver é "fonte de verdade" para testes)
- **Escalabilidade:** Quando escalar para 1B de registros, Silver é naturalmente paralelizável (Spark)

**Benefício Total: ~R$ 50k - R$ 5k = R$ 45k (9x ROI)**

Decisão confirmada na Seção de Evidências Econômicas do README.

---

## 3. CONSEQUÊNCIAS (TRADE-OFFS)

### Positivas (Wins)
- **Auditabilidade:** Cada layer é checkpoint; regressão detectada em horas, não dias
- **Reutilização:** Silver alimenta N projetos futuros (econômico)
- **Testabilidade:** Testes unitários por layer (pytest + Great Expectations)
- **Escalabilidade:** Extensível para 1B+ registros sem refactor
- **Governança:** "Lineage" claro: INEP → Bronze → Silver → Gold → BI/ML

### Negativas (Custos / Riscos)
- **Storage 3x maior:** Hoje negligenciável (2MB), mas em produção (1B rows) = 30GB vs 10GB
- **Tempo setup 20% mais lento:** Cada run escreve 3 layers em vez de 1 (30min vs 25min)
- **Complexidade cognitiva:** Novatos confundem qual campo vai para qual layer
- **Custo cloud futuro:** S3/BigQuery vão custar 3x mais por armazenagem (30GB × R$ 0.023/GB/mês = R$ 690/mês)

### Timeline de Mitigação
- **Semana 1:** Documentação clara de cada layer (CLAUDE.md, DICIONARIO_DADOS.md) ← você está aqui
- **Semana 2:** Great Expectations checks por layer (validação automática)
- **Semana 3:** Testes integrados (pytest dos 3 layers)
- **Semana 4:** Dashboard de linhagem (Spark UI ou custom) para visibilidade

---

## 4. ALTERNATIVAS DESCARTADAS

| Alternativa | Vantagem Teórica | Por Quê Rejeitada | Score Decisão |
|-------------|-----------------|------------------|---------------|
| **One-Shot monolítico** | Simples, 1 script, 20% rápido | Sem auditoria, impossível reutilizar em 2 meses quando chamar para Enem | Rejeitado |
| **Lambda (batch + streaming separados)** | Streaming em tempo real elegante | Dobra complexidade, Kafka em Windows é um caos em produção | Rejeitado |
| **Kappa (só streaming)** | Único sistema | Impossível fazer ML em batch (Spark precisa de histórico offline completo) | Rejeitado |
| **Data Warehouse (Snowflake/BigQuery)** | Gerenciado, suporta 1T+ rows | Custo: $1k-5k/mês; fora do orçamento MEC. Futuro possível (cloud migration é "Phase 2") | Adiado para Phase 2 |
| **Data Lakehouse (Delta/Apache Iceberg)** | Medalhão + transações ACID | Imaturidade de tooling em Python 3.13; maioria de bugs. Melhor após 2027 | Revisitar 2027 |

---

## 5. IMPACTO ROI & VALIDAÇÃO

### Métricas de Sucesso

| Métrica | Baseline | Target | Como Medir |
|---------|----------|--------|-----------|
| **Tempo de debug** | ~8h (re-rodar tudo) | ~1h (isolado a layer) | Registrar tempo em issues GitHub |
| **Taxa de regressão** | 35% (bugs não detectados) | <5% (validação automática) | Tracker de prod issues mensal |
| **Reutilização** | 0% (scripts um-off) | 60% (Silver para 3+ projetos) | Contar projetos que usam Silver |
| **Disponibilidade** | ~95% (falhas Black-box) | >99.5% (checkpoints) | Uptime Airflow/Databricks |
| **Custo total anual** | ~R$ 0 (local) | ~R$ 8k (cloud Phase 2) | Budget do MEC para IA |

### Cenários de Regressão

1. **IBGE API cai:**
   - Medalhão: Bronze + Silver rodadas, Gold faz com dados 1 dia atrás. Resiliente
   - One-Shot: Tudo queima.

2. **SICONFI publica dados errados:**
   - Medalhão: Refaz só Silver+Gold (15min). Rápido
   - One-Shot: Refaz tudo desde INEP (30min).

3. **Novo stakeholder quer só dados INEP (sem SICONFI):**
   - Medalhão: Usa Silver sem os 3 campos SICONFI. Reutilizável
   - One-Shot: Reescreve script inteiro.

### Blocos de Monitoramento

- **Implementado em:** `src/data_quality/01_validacao_qualidade.py`
```
Bronze:
  - Row count match CSV original
  - Schema inference correctness (10/10 checks)

Silver:
  - OBT 23.995 rows (1:1 match Bronze)
  - Nulos em proporcao_aluno_nivel_* = 48% (não subiu nem desceu)
  - id_municipio NÃO tem valores int truncados

Gold:
  - Agregações sumarizam corretamente (sum/count match Silver)
  - Partições por ano completas (2021-2024)
  - Taxa alfabetização <= 100% (lógica)

Continuo:
  - Silhueta K-Means >= 0.25 (clusters válidos)
  - Custo por habitante != 0 (sem divisão por zero)
```

---

## 6. REFERÊNCIAS & LINKS

- **Documentos Internos:**
- [README.md](../../README.md) — Problema de negócio, arquitetura e evidências econômicas
- [CLAUDE.md](../../CLAUDE.md) — Contrato técnico (Linguagem Ubíqua, regras de domínio)
- [DICIONARIO_DADOS.md](../../DICIONARIO_DADOS.md) — Definição de cada campo por layer
- [src/batch/01_ingestao_bronze_batch.py](../../src/batch/01_ingestao_bronze_batch.py) — Implementação Bronze
- [src/batch/02_silver_transform.py](../../src/batch/02_silver_transform.py) — Implementação Silver
- [src/gold/01_gerar_marts_gold.py](../../src/gold/01_gerar_marts_gold.py) — Implementação Gold

- **Referências Externas:**
- [Databricks Medallion Architecture](https://www.databricks.com/blog/2022/06/24/lakehouse-data-architecture.html)
- [Data Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics](https://arxiv.org/abs/2207.03123)
- Tech Challenge Fase 2 FIAP — Pos-graduação Data Science (turma 2026)

- **Commits Git:**
- `19d5c76` — Initial Medalhão setup (Bronze + Silver scripts)
- `8acc666` — Fix rede mapping bug (0→Federal, 3→Municipal)
- `d1c0c1e` — Resolve Schema Mismatch (rede int vs string)

---

## 7. CRITÉRIOS DE ACEITAÇÃO

- [ ] **Bronze:** `01_ingestao_bronze_batch.py` roda idempotente, schema exato do CSV
- [ ] **Silver:** `02_silver_transform.py` produz OBT 23.995 rows × 32 colunas
- [ ] **Gold:** `03_gold_aggregation.py` gera 11 marts com agregações corretas
- [ ] **Qualidade:** Great Expectations (10/10 checks) passa em Bronze, Silver, Gold
- [ ] **Documentação:** DICIONARIO_DADOS.md completo, ADRs 001-005 formalizadas
- [ ] **Testes:** pytest cobre 80% do código (Silver e Gold críticos)
- [ ] **Produção:** 1 full run completo em dados sample (2min) + prod quando disponível

---

Última revisão: 2026-06-23 | Próxima revisão: 2026-07-15
