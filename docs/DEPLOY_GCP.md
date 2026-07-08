# Histórico de Deploy GCP — Pipeline de Alfabetização Municipal

> Consolida 4 documentos de sessão (2026-06-27 a 2026-07-02) numa única linha do tempo. Originais: `GCP_DEPLOYMENT_SESSION_REPORT.md`, `GCP_DEPLOY_SESSION_2026-07-01.md`, `GCP_PIPELINE_COMPLETO_2026-07-01.md`, `HANDOFF_SESSAO_PIPELINE_GCP_2026-07-02.md`.

## Estado Atual (2026-07-02)

| Etapa | Status |
|---|---|
| Bronze | Completo |
| Silver (+ enriquecimento IBGE) | Completo |
| SICONFI | Completo (5.514/5.550 municípios, 99,4%) |
| Gold | 14/16 marts salvos — `agg_evolucao_temporal` e `agg_vulnerabilidade_ml` falharam silenciosamente |
| BigQuery | 14/16 tabelas carregadas |
| Looker Studio | Em andamento — Página 1 (taxa por UF) sendo ajustada; Páginas 2-4 não criadas |

**Fix já escrito localmente, não sincronizado/re-executado no Cloud Shell:**
- `dataproc_03_gold.py` — cada mart agora usa `safe_build` isolado (se falhar, loga traceback completo em vez de sumir silenciosamente com `None`)
- `agg_alocacao_otima` — benefício e custo agora na mesma base (80%), corrigindo inflação do score

**Números econômicos desatualizados (pendência crítica):** README, ADR-010 e notebooks citam R$703M/ROI 19,4×/2.815 municípios calculados com o modelo de custo **antigo**. Após o modelo per capita (ADR-012), os números reais devem ser extraídos via query no BigQuery (queries prontas na seção "Como Recalcular" abaixo).

## Checklist do Que Falta

- [ ] Sincronizar `dataproc_03_gold.py` corrigido para o GCS (script no repo ≠ script no Cloud Shell)
- [ ] Apagar `dataproc_03_gold_(1).py` duplicado no home do Cloud Shell
- [ ] Recriar Cloud NAT + cluster, reprocessar só o Gold (SICONFI já enriqueceu o Silver)
- [ ] Recarregar BigQuery → meta: 16/16 tabelas
- [ ] Rodar as 3 queries de recálculo (seção abaixo) e atualizar README (seções "Evidências Econômicas"), ADR-010 e notebooks
- [ ] Terminar Looker Página 1 (trocar SUM→AVG na métrica, ver detalhe abaixo) + criar Páginas 2-4
- [ ] Screenshots do dashboard em `docs/screenshots/`
- [ ] Deletar infraestrutura (cluster, NAT, router) — comandos na seção "Infraestrutura"
- [ ] Commit final com scripts corrigidos (hoje só `dataproc_02_silver.py` e `dataproc_04_siconfi.py` estão commitados)

## Como Recalcular os Números Econômicos

```sql
-- Custo total para atingir 80% (substitui os R$703M do README)
SELECT ROUND(SUM(custo_estimado_para_atingir_80)/1e6, 1) AS custo_total_milhoes,
       COUNT(*) AS municipios_com_gap
FROM `tech-challenge-fase2-fiap.alfabetizacao_gold.agg_projecao_investimento`;

-- ROI real (substitui o "19,4x" do README)
SELECT SUM(custo_total) AS desperdicio_total,
       SUM(investimento_total) AS investimento_total,
       ROUND(SUM(custo_total) / NULLIF(SUM(investimento_total), 0), 2) AS roi_fator_nacional
FROM `tech-challenge-fase2-fiap.alfabetizacao_gold.agg_roi_executivo`;

-- Quantos municípios cabem em R$500M (substitui "2.815 municípios / 99,96%" do ADR-010)
SELECT COUNT(*) AS total_com_gap,
       COUNTIF(selecionado_no_orcamento) AS selecionados_no_orcamento,
       ROUND(100.0 * COUNTIF(selecionado_no_orcamento) / COUNT(*), 1) AS pct_cobertura
FROM `tech-challenge-fase2-fiap.alfabetizacao_gold.agg_alocacao_otima`;
```

## Correção do Looker Studio Página 1

**Problema:** o gráfico soma (`SUM`) a métrica `taxa_alfabetizacao_media`, o que é errado para percentual — infla barras e cria bucket "Outros" gigante.

**Correções:**
1. Métrica → trocar agregação de SUM para **Average**
2. Filtrar 1 ano (`ano = 2024` ou dropdown)
3. Estilo → número de barras = 27 (elimina "Outros")
4. Estilo → eixo Y máximo = 100
5. Ordenar por `taxa_alfabetizacao_media` desc

**Páginas 2-4 (planejadas):**
- **Página 2 — Vulnerabilidade:** fonte `agg_vulnerabilidade_ml` (K-Means, ADR-014 — mart substituto `agg_clusters_municipios` foi removido em 2026-07-08). Scatter: X=`taxa_media`, Y=`deficit_per_capita` (métrica de referência — não `deficit_absoluto_medio`, que mistura escala populacional), cor=`nivel_vulnerabilidade`.
- **Página 3 — ROI Executivo (tese central):** fonte `agg_roi_executivo` (26 linhas). Barras horizontais por `sigla_uf`, métrica `roi_fator` desc. Insight: `roi_fator > 1` = desperdício por ineficiência cobre o investimento necessário (problema de gestão, não de verba).
- **Página 4 — Alocação Ótima:** fonte `agg_alocacao_otima` (4.679 linhas). Tabela top 20 filtrada por `selecionado_no_orcamento = true`.

## Infraestrutura de Referência

| Recurso | Valor |
|---|---|
| Project ID | `tech-challenge-fase2-fiap` |
| Bucket GCS | `tc-alfabetizacao-fiap-879273` |
| Região | `us-central1` (zona `us-central1-a`) |
| Cluster Dataproc | `spark-alfabetizacao`, single-node, `c3-standard-4` (N1/N4 sem disponibilidade na região testada; C3/Sapphire Rapids funcionou) |
| Dataset BigQuery | `alfabetizacao_gold` |

**Comandos para recriar (Cloud NAT + cluster):**
```bash
gcloud compute routers create router-us-central1 \
  --region=us-central1 --network=default --project=tech-challenge-fase2-fiap

gcloud compute routers nats create nat-us-central1 \
  --router=router-us-central1 --region=us-central1 \
  --auto-allocate-nat-external-ips --nat-all-subnet-ip-ranges \
  --project=tech-challenge-fase2-fiap

gcloud dataproc clusters create spark-alfabetizacao \
  --region=us-central1 --zone=us-central1-a --project=tech-challenge-fase2-fiap \
  --single-node --master-machine-type=c3-standard-4 --master-boot-disk-size=50 \
  --image-version=2.2-debian12 --max-idle=30m --bucket=tc-alfabetizacao-fiap-879273
```

**Cleanup ao encerrar (evita custo contínuo — NAT ~R$1/dia, cluster ~R$0,50/hora):**
```bash
gcloud dataproc clusters delete spark-alfabetizacao --region=us-central1 --project=tech-challenge-fase2-fiap --quiet
gcloud compute routers nats delete nat-us-central1 --router=router-us-central1 --region=us-central1 --project=tech-challenge-fase2-fiap --quiet
gcloud compute routers delete router-us-central1 --region=us-central1 --project=tech-challenge-fase2-fiap --quiet
```

**Armadilha do ambiente:** o pipeline mais recente rodou em **Cloud Shell** (não Dataproc/máquina local) porque a API SICONFI recusa conexões vindas do IP do Cloud NAT (comportamento de blocklist para APIs governamentais), mas aceita o IP público do Cloud Shell. Fluxo usado: pré-popular cache SICONFI via Cloud Shell → job Dataproc lê o cache em vez de chamar a API. O diretório do repositório não existe no Cloud Shell — scripts ficam soltos no `~` (home); cuidado com duplicatas (`dataproc_03_gold_(1).py` apareceu assim).

---

## Linha do Tempo (histórico técnico condensado)

### 2026-06-27 — Primeiro deploy funcional (dados sem SICONFI)

Implementação inicial: Terraform modular (GCS + Dataproc + BigQuery), Dataproc Spark 3.5, BigQuery external tables. Custo ~R$15-20/execução.

**Bugs corrigidos nesta sessão:**
| # | Erro | Causa | Fix |
|---|---|---|---|
| 1 | `idle_delete_ttl` formato ilegal | Provider GCP v5 exige Duration protobuf (`"1800s"`, não `"30m"`) | `terraform/modules/dataproc/main.tf:32` |
| 2 | Compute SA sem permissão Storage | Novo projeto não concede `storage.admin` ao Compute default SA | `google_project_iam_member` em `terraform/main.tf:22-26` |
| 3 | Dataproc `properties` block inválido | Provider v5 removeu o campo | Bloco removido |
| 4 | BigQuery `**` glob não suportado | CLI/Terraform não interpretam `**` | Trocado por `/*` em todos os `source_uris` |
| 5 | Silver IBGE timeout | Workers Dataproc sem internet (VPC padrão) | Exception handling + log |
| 6 | `approxQuantile` retorna `[]` com coluna 100% null | Sem dados SICONFI | Helper `safe_quantile()` |
| 7 | BigQuery perdeu coluna `ano` | Partição Hive (`/ano=2023/`) não lida por external tables | Removido `.partitionBy("ano")` |

Resultado: 7 marts gerados (3 vazios, sem SICONFI), sem ML real (segmentação por regras). Pipeline rotulado "100% funcional" mas incompleto — SICONFI e KNN ficaram para depois.

### 2026-07-01 (sessão parcial) — Infra derruba e reconstrói; Bronze/Silver ok, resto falha

Sync do repo local com o público trouxe `dataproc_04_siconfi.py`, testes SICONFI e ADR-011. `terraform apply` com bucket novo destruiu o bucket antigo sem querer (tfvars divergente).

**Cluster Dataproc — 7 tentativas até funcionar:**
| # | Método | Zona/Máquina | Resultado |
|---|---|---|---|
| 1-3 | Terraform/CLI | us-central1-a/b/f, n1-standard-4 | UNAVAILABLE (sem recursos) |
| 4 | CLI | us-east1-b | Subnetwork not ready |
| 5-6 | Console auto-zone | us-central1-c, n4-standard-2 | UNAVAILABLE |
| 7 | Console (Desenvolvimento) | us-central1-a, **c3-standard-4** | Sucesso |

**Problema de rede:** cluster criado com `internalIpOnly: true` (causa não identificada — possivelmente padrão de VPC/subnet do projeto). Sem IP externo, nada acessa IBGE/SICONFI. Solução: Cloud NAT (criado e depois deletado ao encerrar, para não gerar custo).

**Bug Silver/IBGE:** script salvava JSON em `/tmp/` esperando filesystem local, mas Spark no Dataproc interpreta paths sem prefixo como HDFS (`hdfs://.../tmp/...`) — que não existe. `nome_municipio` ficou nulo. Fix identificado mas não testado nesta sessão (aplicado na sessão seguinte).

SICONFI não rodou (URLError em todos os municípios — sessão encerrada antes do NAT ficar disponível para o job).

### 2026-07-01 (sessão completa) — Silver corrigido, SICONFI via Cloud Shell, Gold com 3 bugs corrigidos, BigQuery 100%

**Fix Silver/IBGE aplicado:** troca de `open()`+`spark.read.json("/tmp/...")` por leitura direta via pandas → `spark.createDataFrame()`, sem arquivo temporário. `nome_municipio` e `populacao_total` (IBGE SIDRA) preenchidos para todos os municípios.

**SICONFI — API bloqueia IP de Cloud NAT:** diagnóstico por eliminação (IBGE funcionou via NAT, SICONFI funcionou via Cloud Shell direto) revelou que a API do Tesouro Nacional recusa IPs de cloud providers. Solução: pré-popular cache SICONFI direto do Cloud Shell (script com ThreadPoolExecutor, 4 threads), salvar em `gs://bucket/siconfi/cache.json`; o job Dataproc lê o cache em vez de chamar a API. Resultado: 5.514/5.550 municípios (99,4% — os 36 restantes não submeteram DCA ao Tesouro em 2024).

**3 bugs corrigidos em `dataproc_03_gold.py`:**
1. `agg_roi_executivo`: `.over()` sem `WindowSpec` → `TypeError`. Fix: `Window.rowsBetween(unboundedPreceding, unboundedFollowing)` para soma global. Também `"col".desc()` (string) → `col("col").desc()`.
2. `agg_alocacao_otima_estrategias`: colunas com nome errado (`taxa_media` → real é `taxa_alfabetizacao_media`; `deficit_total` não existia, calculado como `(gap_ate_80/100) * populacao_total`).
3. BigQuery load: `GOOGLE_APPLICATION_CREDENTIALS` inexistente no Cloud Shell (usa ADC automático) + URI `**/*.parquet` não suportada (trocado por `*.parquet`).

Resultado: 14 marts gerados, 14 tabelas carregadas no BigQuery, 0 erros — rotulado "PIPELINE 100% COMPLETO".

### 2026-07-02 — Handoff: na verdade 14/16 (2 marts sumiram sem erro aparente)

Execução completa no Cloud Shell revelou que o job Gold, apesar de terminar "com sucesso", **não salvou** `agg_evolucao_temporal` e `agg_vulnerabilidade_ml` (404 no BigQuery — arquivos não existem no GCS). Causa suspeita: o `save_mart` tinha um guard `if mart is None: print("PULADO")` que engolia exceções silenciosamente — provavelmente `agg_vulnerabilidade_ml` (usa Spark MLlib: VectorAssembler/StandardScaler/KMeans) falhou por nulos nas features ou libs ausentes no executor; `agg_evolucao_temporal` pode ter esbarrado em `deficit_absoluto_proxy` ausente.

Fix aplicado ao script local (não sincronizado com Cloud Shell ainda): `save_mart` isolado por mart (`safe_build`), loga traceback completo em vez de sumir. Dashboard Looker Studio iniciado — ver seção "Estado Atual" no topo deste documento para o checklist vivo.
