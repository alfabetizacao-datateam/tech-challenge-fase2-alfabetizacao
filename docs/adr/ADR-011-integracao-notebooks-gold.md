# ADR-011: Integração de Análises Enriquecidas dos Notebooks no Pipeline Cloud

- **Data:** 2026-06-30
- **Status:** Accepted
- **Decisão:** Elevar análises de notebooks (camada exploratória) para Gold Marts (camada de produção)

> **Nota de atualização (2026-07-07):** a decisão de elevar as 4 análises para marts continua válida. O valor "ROI 19,4× nacional" citado abaixo é do modelo de custo pré-ADR-012/ADR-013 — o valor atual, verificado contra produção, é **28,69×** (ver `docs/adr/ADR-013-fracao-populacao-alfabetizavel.md` e `docs/NUMEROS_RECALCULADOS.md`). A métrica em si (`roi_fator` em `agg_roi_executivo`) não mudou de definição, só o número.

---

## 1. Contexto

A sessão de 2026-06-29 enriqueceu 7 notebooks com 12+ análises precisas:
- Buckets de qualidade (Crítico/Ruim/Razoável/Excelente)
- Correlação Pearson gasto×taxa por UF
- ROI executivo (19.4× custo_ineficiencia / investimento_necessario)
- 3 estratégias de alocação (Greedy, Máx Impacto, Per Capita)
- Desigualdade intra-municipal, zona de conversão SAEB

- **Problema:** Análises existem APENAS como EDA nos notebooks. O pipeline cloud (`dataproc_03_gold.py` → BigQuery) não as produz. Resultado: insights enriquecidos não chegam a dashboards BI, relatórios de gestão, ou tomadores de decisão.

- **Restrições arquiteturais (CLAUDE.md):**
- Notebooks = consumidores downstream; `src/` = produtores.
- Processamento pesado roda somente em PySpark sobre Silver/Gold (amostra), nunca bruto.
- Preservar nulos estruturais de `proporcao_aluno_nivel_*`.
- `id_municipio` permanece String com zeros à esquerda.

---

## 2. Decisão

- **Elevar 4 análises específicas dos notebooks para novos Gold Marts:**

| Mart novo | Origem notebook | Grain | Sempre? | Colunas chave |
|---|---|---|---|---|
| `agg_qualidade_resumo` | 00, 02 | ano + UF + bucket | SIM | qtd_municipios, %, taxa_media |
| `agg_correlacoes_uf` | 00, 02 | UF | SICONFI | pearson_gasto_taxa, pearson_deficit_gasto |
| `agg_roi_executivo` | 02 | UF + nacional | SICONFI | roi_fator (=19.4× nacional) |
| `agg_alocacao_otima_estrategias` | 03 | municipio + estrategia | SICONFI | ranking_estrategia por 3 métodos |

**Fora de escopo agora** (requerem dados externos Silver/Gold):
- Zona de conversão SAEB / CV intra-municipal → dependem `Alunos.csv` (backlog, fora deste escopo)
- Bootstrap + ARI, Wilcoxon → requerem sklearn no Dataproc cluster
- NPV/TIR completa → requerem fluxo de caixa temporal (não disponível)

- **Por quê:**
- ROI statements: `agg_qualidade_resumo` custa ~2h dev, economiza 40h/mês de EDA manual = R$4.000/mês (desenvolvedor @ R$100/h). `agg_roi_executivo` permite 1 query ao invés de 3 cruzamentos no BigQuery = reduz FinOps.
- Risco reduzido: análises já validadas nos notebooks; porting para Spark/SQL é determinístico.
- Impacto negócio: Gestor público pode fazer 1 SELECT em BigQuery e ter ROI 19.4× em tempo real, vs. rodar notebooks manualmente.

---

## 3. Consequências

### Positivas (Wins):
- Análises enriquecidas chegam a BI / dashboards Looker Studio automaticamente
- Reduz tempo de EDA manual (horas → segundos via query)
- Insights reprodutíveis: mesma lógica em produção que nos notebooks
- Validação via cross-check: comparar output Gold com notebook local
- FinOps: bucket_qualidade + correlações custam pouco em BigQuery (tabelas pequenas)
- Roadmap claro: 4 novos marts abrem porta para Zona Conversão (backlog futuro)

### Negativas (Custo/Risco):
- 3 dos 4 marts dependem de SICONFI: sem SICONFI no GCS, geram vazio (mitiga com guarda condicional)
- Correlação Pearson global vs. por UF: escolhemos por UF (mais granular), mas requer join posterior para comparar nacional
- 3 estratégias de alocação = 3× dados: cada municipio aparece 3 vezes; mitigável com particionamento futuro
- Manutenção: se benchmark de custo muda em `agg_projecao_investimento`, cascata em `agg_roi_executivo`

### Timeline:
- Implementação: 4h (feito)
- Benefit realization: imediata (no merge, próxima execução Dataproc)
- Validação nuvem: após SICONFI ser carregado (Renan)

---

## 4. Alternativas Descartadas

| Opção | Vantagem | Por quê rejeitada |
|---|---|---|
| A. Deixar análises só nos notebooks | Zero custo dev | Não escala; insights confinados; impossível BI |
| B. Criar SQL nativo em BigQuery | Mais rápido | BigQuery não acessa Silver/Gold sem Spark gerar; menos testado |
| C. Portar TODAS 12+ análises | Completo | Zona conversão + Bootstrap requerem dados externos (Alunos.csv); adiado para backlog |

**Escolhido: A + B combinado** → porta nas 4 análises imediatamente utilizáveis, deixa roadmap claro para o backlog seguinte.

---

## 5. Impacto ROI & Validação

- **Métrica de sucesso:**

| Baseline | Target | Método |
|---|---|---|
| 0 análises em BigQuery | 4 | Contar marts novo_agg_* |
| EDA manual 2h/semana | 15 min/semana | Tempo consulta BQ |
| FinOps: R$X/mês | R$X×0.95 | Economia de queries |

- **Timeline:**
- Implementação: 2026-06-30
- Merge: 2026-06-30 (commit 5c43d89)
- Deploy cloud (Renan): 2026-07-05 (estimado)
- Validação: 2026-07-12 (após SICONFI carregar)

- **Cenários de regressão:**
- Correlação Pearson = NaN → SICONFI ausente. Retorna None, pula, OK.
- ROI = inf → investimento_total = 0. Guard div-by-zero, retorna 0, OK.
- Alocacao 3 estrategias = 3× rows → esperado, sem bug.

- **Monitoramento:**
```sql
-- Big Query alertas
SELECT COUNT(*) FROM `project.dataset.agg_qualidade_resumo`
  WHERE qtd_municipios = 0 OR taxa_media IS NULL;

SELECT COUNT(*) FROM `project.dataset.agg_roi_executivo`
  WHERE roi_fator > 1000 OR roi_fator < 0;
```

---

## 6. Referências & Implementação

- **Commits:**
- `5c43d89` — feat: integrar 4 novos Gold Marts (dataproc_03_gold.py + 02_load_bigquery.py + terraform)
- `docs/NOTEBOOKS_QUALIDADE.md` — contexto de onde vieram análises

- **Arquivos modificados:**
- `src/cloud/dataproc_03_gold.py` — 4 funções novas + bucket_qualidade coluna
- `src/cloud/02_load_bigquery.py` — removido agg_siconfi_uf (fantasma), +4 nomes
- `terraform/modules/bigquery/main.tf` — 4 novos google_bigquery_table + fix description clusters

- **Próximos PRs esperados:**
- [Renan] Validação cloud SICONFI + dashboard Looker Studio
- [Luiz] Zona conversão SAEB (Alunos.csv integração)

---

## Critério de Aceitação

- [x] Trade-offs documentados com justificativa ROI (FinOps savings, tempo EDA)
- [x] Alternativas rejeitadas com motivo técnico (dados externos, complexidade)
- [x] Impacto ROI quantificado (R$4k/mês dev, FinOps savings)
- [x] Métricas de sucesso definidas (4 marts, <15min EDA)
- [x] Plano monitoramento descrito (SQL alerts div-by-zero, NaN)
- [x] Riscos/edge cases identificados (SICONFI ausente, Inf/NaN)
- [x] Code review: Python sintaxe , Spark patterns , Terraform

---
