# Números Recalculados — Reprocessamento GCP 2026-07-04 (+ correção 2026-07-07)

> Substitui os números do README antigo (R$703M / ROI 19,4x / 2.815 municípios),
> calculados com o modelo de custo pré-ADR-012. Estes valores vêm do BigQuery
> (`tech-challenge-fase2-fiap.alfabetizacao_gold`), dataset de produção completo
> (5.550 municípios), pós-ADR-012 (custo marginal per capita) **e** pós-ADR-013
> (fração de população alfabetizável — ver abaixo, achado durante este
> reprocessamento).
>
> **Atualização 2026-07-07:** a tabela 3 (cobertura do knapsack) foi
> reprocessada de novo depois de um fix na auditoria ponta a ponta —
> `agg_alocacao_otima` usava a constante default de custo (R$20/hab/ponto) em
> vez do benchmark calibrado via SICONFI (~R$19,39/hab/ponto) que
> `agg_projecao_investimento` já usava. Tabelas 1 e 2 (investimento, ROI) não
> mudaram — não dependem desse benchmark no knapsack.

## Achado crítico durante o reprocessamento (ADR-013)

A primeira rodada produziu **R$93,7 bilhões** para atingir 80% — 133x maior
que o número antigo. Investigação encontrou a causa: a fórmula de custo
multiplicava `gap% × populacao_total`, onde `populacao_total` é a população
TOTAL do município (IBGE, todos os habitantes), não uma contagem de alunos.
Isso implicava **48,3 milhões de "alunos em déficit"** — maior que toda a
população de crianças de 6-10 anos do Brasil. Corrigido introduzindo
`FRACAO_POPULACAO_ALFABETIZAVEL = 0.013` (estimativa de coorte de idade única
via pirâmide etária IBGE) — ver `docs/adr/ADR-013-fracao-populacao-alfabetizavel.md`.

**Pendência conhecida (não corrigida nesta sessão):** o script local
(`src/gold/01_gerar_marts_gold.py`) nunca foi migrado para o modelo do
ADR-012 — ainda usa a fórmula antiga (R$200/ponto/1000hab) e tem o mesmo bug
de nome de coluna (`classificacao` vs `classificacao_eficiencia`) encontrado
no Task 7/10. Os números abaixo vêm exclusivamente do pipeline **cloud**
(BigQuery), que é o que reflete a decisão do ADR-012/013. Números gerados
rodando `src/gold/01_gerar_marts_gold.py` localmente continuam desatualizados
até esse script ser corrigido (fora do escopo desta sessão — ver `docs/PROXIMOS_PASSOS.md`).

## 1. Custo total para atingir 80% de alfabetização

```sql
SELECT ROUND(SUM(custo_estimado_para_atingir_80)/1e6, 1) AS custo_total_milhoes, COUNT(*) AS municipios_com_gap
FROM `tech-challenge-fase2-fiap.alfabetizacao_gold.agg_projecao_investimento`;
```

| Métrica | Valor | Modelo antigo (README) |
|---|---|---|
| Custo total | **R$ 1.218,3 milhões** (~R$1,22 bilhão) | R$703 milhões |
| Municípios com gap (<80%) | **4.679** | 2.830 |

## 2. ROI nacional (desperdício por ineficiência ÷ investimento necessário)

```sql
SELECT SUM(custo_total) AS desperdicio_total, SUM(investimento_total) AS investimento_total,
       ROUND(SUM(custo_total) / NULLIF(SUM(investimento_total), 0), 2) AS roi_fator_nacional
FROM `tech-challenge-fase2-fiap.alfabetizacao_gold.agg_roi_executivo`;
```

| Métrica | Valor | Modelo antigo (README) |
|---|---|---|
| Desperdício total (ineficiência) | **R$ 34,96 bilhões** | R$13,65 bilhões |
| Investimento necessário | R$ 1.218,3 milhões | R$703 milhões |
| **ROI (fator)** | **28,69x** | 19,4x |

> Desperdício maior que o antigo porque cobre dataset de produção completo
> (5.550 municípios) vs a amostra parcial usada antes. A tese qualitativa
> (desperdício ≫ investimento necessário) se confirma e fica **mais forte**
> (28,69x vs 19,4x), não mais fraca.

## 3. Cobertura do orçamento de R$500M (knapsack)

```sql
SELECT COUNT(*) AS total_com_gap, COUNTIF(selecionado_no_orcamento) AS selecionados_no_orcamento,
       ROUND(100.0 * COUNTIF(selecionado_no_orcamento) / COUNT(*), 1) AS pct_cobertura,
       ROUND(SUM(CASE WHEN selecionado_no_orcamento THEN beneficio_alunos_ate_80 ELSE 0 END), 0) AS alunos_beneficiados
FROM `tech-challenge-fase2-fiap.alfabetizacao_gold.agg_alocacao_otima`;
```

| Métrica | Valor | 2026-07-04 (benchmark default no knapsack) | Modelo antigo (README) |
|---|---|---|---|
| Municípios selecionados / total com gap | **2.331 / 4.679** | 2.329 / 4.679 | 2.815 / 2.815 |
| Cobertura do orçamento | **49,8%** | 49,8% | 99,96% |
| Alunos estimados beneficiados | **255.223** | 246.563 | (não reportado) |

> Com benchmark calibrado real (~R$1.939/aluno, vs R$20/aluno artificialmente
> baixo do modelo antigo), R$500M cobre metade dos municípios com gap, não
> quase todos. Isso é o número correto — o modelo antigo subestimava o custo
> real por aluno em ~97x.
>
> A coluna "2026-07-04" é o valor anterior à correção de 2026-07-07 (quando o
> knapsack ainda usava a constante default R$20/hab/ponto em vez do benchmark
> calibrado ~R$19,39/hab/ponto): levemente conservador, por isso a cobertura
> real (coluna atual) é um pouco maior.

## Reprocessamento — o que foi feito

1. Recriado Cloud NAT + cluster Dataproc (`spark-alfabetizacao`, efêmero).
2. `dataproc_04_siconfi.py` rodado de novo (reaproveitou cache GCS, 5.514/5.550
   municípios, 0 chamadas novas à API — script anterior no bucket usava
   fórmula pré-ADR-012, não sincronizada desde 02/07/2026).
3. `dataproc_03_gold.py` corrigido (safe_build já presente + ADR-013 novo) e
   rodado: **16/16 marts** salvos (incluindo `agg_evolucao_temporal` e
   `agg_vulnerabilidade_ml`, que falhavam silenciosamente antes).
4. BigQuery recarregado: **16/16 tabelas, 0 erros**.
5. Infraestrutura (cluster + NAT + router) deletada ao final.

## Reprocessamento 2 — 2026-07-07 (só o Gold, via Cloud Shell)

Silver e cache SICONFI já existiam no GCS — não foi necessário refazer
Bronze/Silver/SICONFI, só regerar o Gold com o fix do benchmark do knapsack.

1. Clonado o repo direto no Cloud Shell (`git clone`, evita duplicata de
   script solto no home).
2. Recriado Cloud NAT + cluster Dataproc (mesma receita).
3. `dataproc_03_gold.py` (com `build_mart_alocacao_otima` usando o benchmark
   calibrado via `resolve_custo_marginal_benchmark`) rodado: **16/16 marts**,
   0 erros. Log confirmou o benchmark usado: `R$19.39/hab/ponto (~R$1939.0/aluno)`.
4. BigQuery recarregado: **16/16 tabelas, 0 erros**.
5. Infraestrutura deletada ao final.
6. `scripts/verificar_numeros_publicacao.py` rodado: investimento/ROI/
   municípios com gap bateram exato; `alunos_beneficiados` e
   `selecionados_no_orcamento` do knapsack vieram maiores (esperado — ver
   tabela 3 acima). Script e README atualizados com os novos valores.
