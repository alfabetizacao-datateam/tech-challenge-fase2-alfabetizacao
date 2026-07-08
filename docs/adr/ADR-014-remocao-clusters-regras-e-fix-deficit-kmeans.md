# ADR-014: Remoção de `agg_clusters_municipios` (duplicata por regras) e correção de double-counting no K-Means

- **Status:** ACCEPTED | **Data:** 2026-07-08 | **Contexto:** auditoria da seção "ML" pré-entrega (ver `docs/HANDOFF_RENAN_2026-07-07.md`, seção 4)

---

## 1. CONTEXTO

Durante a auditoria de `agg_vulnerabilidade_ml` (K-Means, a única técnica de
IA/ML citada no README como "Análise de desigualdade educacional"),
identificamos que o pipeline cloud (`dataproc_03_gold.py`) gerava **três**
marts segmentando municípios pela mesma lógica de fundo (taxa de
alfabetização x déficit, em torno de 4 grupos):

| Mart | Método | Documentado? |
|---|---|---|
| `agg_priorizacao` | Regras + mediana, com **ranking** e peso de equidade fiscal | ADR-009 |
| `agg_clusters_municipios` | Regras + mediana (mais simples, sem peso de equidade) | Nenhum ADR — docstring do próprio código dizia "substitui o mart de clustering com segmentação baseada em regras (sem ML)" |
| `agg_vulnerabilidade_ml` | K-Means real (Spark MLlib) | README, seção "Aplicação em IA" |

`agg_clusters_municipios` é resíduo de uma versão anterior ao K-Means: a
própria docstring admitia ser um substituto do clustering "de verdade". Não
tinha ADR, não era citado no README como aplicação de IA, e duplicava
exatamente o que `agg_vulnerabilidade_ml` já fazia (com um método mais fraco
— corte por mediana, não otimização por distância).

Nota à parte: o script **local** `src/ml/01_clusterizar_municipios.py`
também escreve numa pasta chamada `agg_clusters_municipios`, mas usa K-Means
real via scikit-learn — mesmo nome, método diferente do mart cloud removido
aqui. Isso não afeta o BigQuery (que só recebe os marts cloud via
`02_load_bigquery.py`), mas é uma coincidência de nome que gerou confusão
nesta auditoria e vale nota para quem ler o código depois.

### Bug encontrado durante a auditoria (não relacionado à redundância acima)

`build_mart_vulnerabilidade_ml` calculava a feature de déficit como:

```python
spark_round(spark_sum("deficit_absoluto_proxy"), 0).alias("deficit_total")
```

`deficit_absoluto_proxy` é calculado **por linha** (`ano` x `rede`) usando
sempre a **população TOTAL do município** (não uma população específica da
rede — ver `02_silver_transform.py`). Um município pode ter até 6 linhas
(2 anos x 3 redes, confirmado na amostra local). Somar
`deficit_absoluto_proxy` entre essas linhas reutiliza a mesma população uma
vez por linha, inflando `deficit_total` em até ~6x — não por o município ser
mais vulnerável, mas por ter mais redes com dado reportado. Isso distorcia
diretamente a feature `deficit_per_capita` usada no K-Means, favorecendo
clusters "críticos" para municípios com reporte mais granular, independente
da taxa real.

Os dois números-bandeira do projeto (investimento R$1.218,3M e desperdício
R$34,96bi) **não são afetados** — `agg_projecao_investimento` e
`agg_custo_ineficiencia` usam `avg(taxa_alfabetizacao)` e
`avg(populacao_total)`, não a soma de `deficit_absoluto_proxy`.

---

## 2. DECISÃO

1. **Remover `agg_clusters_municipios`** (cloud) — função, chamada em
   `run_gold`, tabela Terraform/BigQuery, entrada em `02_load_bigquery.py` e
   referência no diagrama.
2. **Manter `agg_priorizacao` e `agg_vulnerabilidade_ml`** — não são
   redundantes entre si, apesar de operarem sobre o mesmo domínio:
   - `agg_priorizacao` devolve uma **ORDEM** (`ranking_prioridade`, um
     número por município) com um julgamento de política pública explícito
     (peso de equidade fiscal: metrópoles pesam 0.6, médias 0.8 — têm
     capacidade própria de arrecadação e não devem dominar a fila só por
     escala).
   - `agg_vulnerabilidade_ml` devolve um **PERFIL** (`cluster`,
     `nivel_vulnerabilidade`) via clustering data-driven, sem esse
     julgamento embutido — combina taxa, déficit per capita, população
     (log) e gasto per capita.
   - Um responde "quem atender primeiro"; o outro "que tipo de município é
     este". Ambos os docstrings foram atualizados para se referenciarem
     mutuamente.
3. **Corrigir o cálculo de déficit no K-Means**: trocar `SUM` por `AVG`
   (`deficit_absoluto_medio`), consistente com `taxa_media` (também `AVG`) e
   com `agg_priorizacao` (que já usava `AVG`). `deficit_per_capita`
   continua sendo a métrica de referência para vulnerabilidade/priorização
   (não `deficit_absoluto`, que carrega o viés de escala populacional);
   `deficit_absoluto_medio` fica disponível como coluna informativa, com a
   abordagem de cálculo documentada em comentário no código.
4. **Logar municípios excluídos do K-Means** por `dropna` (ex.: sem
   cobertura SICONFI) — antes era um descarte silencioso.

---

## 3. IMPLEMENTAÇÃO

| Arquivo | Mudança |
|---|---|
| `src/cloud/dataproc_03_gold.py` | Removida `build_mart_clusters_municipios` e sua chamada em `run_gold`; `build_mart_vulnerabilidade_ml` usa `avg` em vez de `sum` para déficit, loga municípios excluídos, docstring cruzada com `agg_priorizacao`; `build_mart_priorizacao` ganhou docstring cruzada com `agg_vulnerabilidade_ml` |
| `terraform/modules/bigquery/main.tf` | Removido `resource "google_bigquery_table" "agg_clusters_municipios"` |
| `src/cloud/02_load_bigquery.py` | Removida entrada `agg_clusters_municipios` da lista `MARTS` |
| `docs/DIAGRAMA_PIPELINE.md` | Contagem 16→15 marts; nó G4 sem `agg_clusters_municipios` |
| `README.md` | Contagem 16→15 marts; nota nova explicando por que priorização e IA/ML coexistem |
| `docs/DEPLOY_GCP.md` | Página 2 do Looker (Vulnerabilidade) atualizada para usar `agg_vulnerabilidade_ml` diretamente (antes apontava para o substituto `agg_clusters_municipios`, que não existe mais) |

**Pendente (infra, fora do escopo desta sessão de código):** a tabela
`agg_clusters_municipios` já existe hoje no BigQuery de produção (criada em
reprocessamento anterior). Rodar `terraform apply` no próximo
reprocessamento vai removê-la (via `terraform destroy` implícito no diff,
`deletion_protection = false`) — confirmar que isso é aceitável antes de
aplicar, já que a tabela deixa de existir para qualquer consulta/dashboard
que ainda a referencie.

---

## 4. CONSEQUÊNCIAS

**Vantagens:**
- Elimina uma aplicação de "IA" fantasma no discurso do projeto — antes da
  correção, alguém poderia confundir `agg_clusters_municipios` (regras) com
  uma segunda técnica de ML, quando é apenas uma versão mais fraca do
  K-Means já existente.
- `deficit_per_capita` no K-Means deixa de ser inflado por artefato de
  granularidade de reporte (redes com dado disponível), tornando os
  clusters mais fiéis à vulnerabilidade real.
- Reduz de 16 para 15 marts sem perda de cobertura funcional — nenhuma
  informação do `agg_clusters_municipios` não está já coberta (com mais
  rigor estatístico) por `agg_vulnerabilidade_ml`.

**Limitação (aceita, não é bug):**
- `deficit_per_capita` (calculado a partir da média) é, algebricamente,
  muito próximo de `(100 - taxa_media) / 100` quando a população é
  aproximadamente constante entre linhas do mesmo município — ou seja,
  parte da informação que essa feature carrega já está em `taxa_media`.
  Isso é uma correlação esperada entre duas leituras do mesmo fenômeno (taxa
  vs. déficit), não um erro de cálculo, e o Silhouette (0.41, documentado no
  handoff de 2026-07-07) já reflete essa estrutura de correlação nos dados
  reais. Não alteramos o conjunto de features do K-Means nesta sessão — é
  uma observação para uma futura calibração, não uma correção necessária.

---

## 5. GATILHO DE REVISÃO

- [ ] Se o Silhouette cair após o fix `sum→avg` (rodar
  `scripts/verificar_numeros_publicacao.py`-like check pontual em
  `agg_vulnerabilidade_ml`), investigar se `k=4` continua sendo o melhor
  corte com a feature corrigida (elbow/silhouette já eram calculados no
  notebook local antes do fix; recalcular após o reprocessamento).
- [ ] Ao rodar `terraform apply` no próximo reprocessamento em produção,
  confirmar a remoção da tabela `agg_clusters_municipios` do BigQuery e
  atualizar qualquer página do Looker que ainda a referencie.
