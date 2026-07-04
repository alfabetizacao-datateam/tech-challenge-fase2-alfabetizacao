# Números Recalculados — Reprocessamento GCP 2026-07-04

> Substitui os números do README antigo (R$703M / ROI 19,4x / 2.815 municípios),
> calculados com o modelo de custo pré-ADR-012. Estes valores vêm do BigQuery
> (`tech-challenge-fase2-fiap.alfabetizacao_gold`), dataset de produção completo
> (5.550 municípios), pós-ADR-012 (custo marginal per capita) **e** pós-ADR-013
> (fração de população alfabetizável — ver abaixo, achado durante este
> reprocessamento).

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

| Métrica | Valor | Modelo antigo (README) |
|---|---|---|
| Municípios selecionados / total com gap | **2.329 / 4.679** | 2.815 / 2.815 |
| Cobertura do orçamento | **49,8%** | 99,96% |
| Alunos estimados beneficiados | **246.563** | (não reportado) |

> Com benchmark calibrado real (~R$1.939/aluno, vs R$20/aluno artificialmente
> baixo do modelo antigo), R$500M cobre metade dos municípios com gap, não
> quase todos. Isso é o número correto — o modelo antigo subestimava o custo
> real por aluno em ~97x.

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
