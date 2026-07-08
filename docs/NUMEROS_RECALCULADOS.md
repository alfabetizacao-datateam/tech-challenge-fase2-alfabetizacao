# Números Recalculados — Reprocessamento GCP 2026-07-04 (+ correções 2026-07-07 e 2026-07-08)

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

| Métrica | Valor (2026-07-08) | 2026-07-07 | 2026-07-04 (benchmark default) | Modelo antigo (README) |
|---|---|---|---|---|
| Municípios selecionados / total com gap | **2.334 / 4.679** | 2.331 / 4.679 | 2.329 / 4.679 | 2.815 / 2.815 |
| Cobertura do orçamento | **49,9%** | 49,8% | 49,8% | 99,96% |
| Alunos estimados beneficiados | **254.186** | 255.223 | 246.563 | (não reportado) |

> Com benchmark calibrado real (~R$1.939/aluno, vs R$20/aluno artificialmente
> baixo do modelo antigo), R$500M cobre metade dos municípios com gap, não
> quase todos. Isso é o número correto — o modelo antigo subestimava o custo
> real por aluno em ~97x.
>
> A coluna "2026-07-04" é o valor anterior à correção de 2026-07-07 (quando o
> knapsack ainda usava a constante default R$20/hab/ponto em vez do benchmark
> calibrado ~R$19,39/hab/ponto): levemente conservador, por isso a cobertura
> real ficou um pouco maior a partir de 2026-07-07.
>
> A pequena variação de 2026-07-08 (2.331→2.334 selecionados, 255.223→254.186
> alunos) vem da cobertura de `gap_meta` ter ido de ~44% para 100% após o KNN
> de imputação de metas (ver ADR-015) — municípios de redes não-Municipais
> que antes tinham `meta_alfabetizacao_2024` NULL agora entram no cálculo.
> `scripts/verificar_numeros_publicacao.py` confirmou tudo dentro da
> tolerância de 2% — investimento (R$1.218,3M) e ROI (28,69×) não mudaram
> (não dependem de `gap_meta` nem de `deficit_absoluto_proxy`).

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

## Reprocessamento 3 — 2026-07-08 (última rodada: KNN em produção + fix de deficit_per_capita)

Motivado pela auditoria completa do pipeline (ver ADR-014 e ADR-015):
correção de double-counting de `deficit_absoluto_proxy` em 10 pontos do
pipeline, remoção de `agg_clusters_municipios` (duplicata do K-Means sem
ADR), e primeira execução do KNN de imputação de metas em produção.

**Descoberta de infraestrutura durante a execução:** o bucket real do
projeto é `tc-alfabetizacao-fiap-879273` (confirmado via
`gcloud storage buckets list`) — o `terraform.tfvars` local apontava para
`alfabetizacao-datalake-fiap`, que **não existe**. O Terraform também
gerencia um dataset BigQuery chamado `gold` (vazio, 0 tabelas) que nunca
foi o caminho real de carga — a carga sempre foi feita via
`02_load_bigquery.py` direto no dataset `alfabetizacao_gold`. **O
`terraform apply` foi pulado nesta rodada** — não é o caminho real de dados,
só um artefato paralelo de IaC (provavelmente para atender ao requisito de
"Terraform gerencia os 3 módulos" do enunciado, sem estar de fato no
caminho crítico).

1. Cluster Dataproc recriado (`spark-alfabetizacao`, mesma receita); Cloud
   NAT não foi necessário (SICONFI já tinha cache do GCS).
2. Scripts sincronizados via `gsutil cp -r src/cloud gs://.../scripts/`
   (substituindo a pasta antiga).
3. **`dataproc_05_knn_metas.py` (novo, ADR-015) rodado pela primeira vez em
   produção:** cobertura de meta 43,6% → 94,5% (propagação) → 100% (KNN).
   Validação por holdout: **MAE 5,12pp, RMSE 7,26pp** (1.046/1.046
   municípios avaliados) — bem abaixo do limiar de alerta (10pp).
4. `dataproc_03_gold.py` rodado: **15/15 marts**, 0 erros (sem
   `agg_clusters_municipios`, removida no ADR-014). Silhouette do K-Means
   (`agg_vulnerabilidade_ml`) subiu de 0,41 para **0,46** após o fix de
   `deficit_per_capita` (eliminação do double-counting).
5. BigQuery recarregado via `02_load_bigquery.py`: **15/15 tabelas, 0
   erros**, direto no dataset `alfabetizacao_gold`.
6. Tabela órfã `agg_clusters_municipios` (de reprocessamentos anteriores,
   pré-ADR-014) removida manualmente (`bq rm -f -t`), já que o script de
   carga só atualiza/cria tabelas da sua lista — não apaga as que saíram.
7. `scripts/verificar_numeros_publicacao.py`: investimento e ROI exatos
   (não mudam); cobertura do knapsack variou dentro da tolerância de 2%
   (ver tabela 3 acima — efeito da cobertura de `gap_meta` ir a 100%).
8. Verificação pós-KNN em `agg_municipio_ranking`: distribuição de
   `status_risco` monotônica em `gap_meta_medio` (+11,97 a -32,5 pp entre
   os 4 buckets); 23/5.516 municípios (0,4%) com `gap_meta` além de ±50pp —
   proporção pequena e plausível, sem sinal de distorção sistêmica.
9. `agg_correlacoes_uf` confirmado com correlação real por UF (variando de
   -0,50 a 0,36 entre UFs — antes do fix repetia um único valor nacional em
   todas as linhas) e classificação por valor absoluto funcionando (AC com
   -0,50 corretamente rotulado "Moderada", não "Fraca").
10. Infraestrutura (só o cluster — NAT não foi criado desta vez) deletada
    ao final.

**Decisão sobre RandomForest (predição de risco):** mantido como protótipo
validado tecnicamente, não integrado à esteira Dataproc/BigQuery nesta
rodada — não alimenta nenhum outro mart, custo de integração desproporcional
ao ganho para esta entrega (ver ADR-015, seção 4).
