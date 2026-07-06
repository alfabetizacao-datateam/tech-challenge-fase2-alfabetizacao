# Tech Challenge Fase 2 — Pipeline Híbrido para Análise da Alfabetização no Brasil

Pipeline de dados em arquitetura Medalhão (Bronze/Silver/Gold), híbrida (Batch + Streaming), rodando em GCP, que integra o Indicador Criança Alfabetizada (INEP/Base dos Dados) com dados territoriais (IBGE) e financeiros (SICONFI) para apoiar priorização de investimento público em alfabetização infantil.

> Repositório sucessor de [`luizmaibashi/tech-challenge-fase-2`](https://github.com/luizmaibashi/tech-challenge-fase-2) (arquivado), reconstruído com Git Flow real. Ver [`AGENTS.md`](AGENTS.md) para o contrato de colaboração da dupla.

---

## Contexto do problema

A alfabetização na infância é um dos pilares do desenvolvimento educacional, social e econômico do país. O **Compromisso Nacional Criança Alfabetizada** mobiliza União, estados e municípios para que toda criança brasileira esteja alfabetizada até o final do 2º ano do ensino fundamental.

Em 2023, o INEP realizou a *Pesquisa Alfabetiza Brasil* e definiu **743 pontos** na escala de proficiência do Saeb como ponto de corte: acima disso, a criança é considerada alfabetizada. Esse corte deu origem ao **Indicador Criança Alfabetizada** — o percentual de estudantes que atingem esse patamar — com meta nacional de 100% até 2030.

O problema real que este projeto ataca: esse indicador, sozinho, não explica **por que** um município está atrás. Responder isso exige cruzar o indicador com metas oficiais, dados territoriais (IBGE), gasto público em educação (SICONFI) e microdados de avaliação — que vêm de fontes heterogêneas, em formatos diferentes, sem chave de junção padronizada por padrão (ver [ADR-005](docs/adr/ADR-005-id-string.md)).

## O desafio

Este repositório atua como um time de engenharia de dados de uma organização pública de análise educacional. Entrega uma pipeline que:

- integra as 6 entidades pedidas no enunciado (UF, Meta Brasil, Meta UF, Meta Município, Município, Dados de Alunos);
- garante qualidade e rastreabilidade em cada camada;
- roda em nuvem (GCP) com controle de custo explícito;
- vai além do indicador puro: converte gap de alfabetização em **estimativa de investimento em R$** e prioriza onde alocar orçamento público sob restrição (ver seção "Aplicação em IA").

## Arquitetura da solução

Arquitetura Medalhão híbrida (Batch + Streaming), com Bronze/Silver processados em PySpark local (dev) e no Dataproc (produção), Gold consumido via BigQuery/Looker Studio.

**Diagrama completo (arquitetura, fluxo de integração Silver, fluxo do modelo de custo):** [`docs/DIAGRAMA_PIPELINE.md`](docs/DIAGRAMA_PIPELINE.md).

Resumo por camada:

| Camada | Conteúdo | Onde |
|---|---|---|
| **Bronze** | Dados brutos — Indicador de Alfabetização, IBGE, SICONFI (cache API), microdados SAEB, eventos simulados | `src/batch/`, `src/siconfi/`, `src/streaming/` |
| **Silver** | *One Big Table* (`alfabetizacao_municipios_obt`) — join com IBGE, enriquecimento SICONFI, metas imputadas via KNN, integração de microdados | `src/batch/02_silver_transform.py`, `src/features/02_imputar_metas_knn.py` |
| **Gold** | 16 marts analíticos particionados por `ano` — executivos, priorização, financeiro, IA/ML, correlações | `src/gold/01_gerar_marts_gold.py` (local), `src/cloud/dataproc_03_gold.py` (produção) |

## Fluxo de dados

Resumo (diagrama detalhado em [`docs/DIAGRAMA_PIPELINE.md`](docs/DIAGRAMA_PIPELINE.md), seção 2):

1. Indicador de Alfabetização faz `JOIN` com IBGE por `id_municipio` — **sempre STRING**, nunca INT, para não perder zeros à esquerda ([ADR-005](docs/adr/ADR-005-id-string.md)).
2. `LEFT JOIN` com SICONFI (despesa em educação por município/ano/rede).
3. Metas oficiais do PDE só existem para rede Municipal (43,9% de cobertura); redes Estadual/Federal/Privada recebem meta **imputada via KNN (K=5, por UF)**, sempre marcadas com `is_imputado=True` — nunca reportadas como dado oficial ([ADR-004](docs/adr/ADR-004-knn-metas.md)).
4. `JOIN` com microdados de alunos (SAEB individual) para validação cruzada com o indicador publicado pelo INEP.
5. Nulos estruturais em `proporcao_aluno_nivel_*` (~48% dos registros) são **preservados**, nunca preenchidos com `0.0` — evita inverter o significado de "não avaliado" para "0% de proficiência" ([ADR-003](docs/adr/ADR-003-nulos-nivel.md)).

## Tecnologias utilizadas

| Ferramenta | Por quê |
|---|---|
| **PySpark 3.5** | Volume (dataset de produção: 5.550 municípios × múltiplos anos/redes) exige processamento distribuído; mesma API roda local (dev) e no Dataproc (prod) |
| **GCP (GCS + Dataproc + BigQuery)** | Dataproc é efêmero (sobe só para o job, cai depois — ver FinOps); BigQuery serve a camada Gold para BI sem manter cluster ligado |
| **Terraform** | Infra como código para os 3 módulos (GCS, Dataproc, BigQuery) — reprodutível e destruível sob comando |
| **scikit-learn** | KNN para imputação de metas ([ADR-004](docs/adr/ADR-004-knn-metas.md)) e RandomForest para o modelo preditivo de risco |
| **Spark MLlib (K-Means)** | Clustering de vulnerabilidade educacional em escala (roda no Dataproc, não teria como rodar em pandas/sklearn no volume de produção) |
| **Great Expectations (opcional) + validação manual** | Regras de qualidade de dados exigidas no enunciado; cai em fallback manual (`01_validacao_qualidade.py`) se a lib não estiver disponível |
| **Spark Structured Streaming (File Stream)** | Simula ingestão quase-tempo-real sem exigir Kafka/Docker em desenvolvimento local no Windows — ver decisão abaixo |
| **pytest** | 114 testes cobrindo ingestão, transformação, agregação, qualidade e ML |

## Decisões arquiteturais

Trade-offs completos em [`docs/adr/`](docs/adr/) (13 ADRs). Os três que o enunciado pede explicitamente:

**Batch vs Streaming** ([ADR-006](docs/adr/ADR-006-file-stream.md)) — dados de metas, município e agregados nacionais são batch (mudam por trimestre/ano). "Atualização de indicador" é simulada via Spark Structured Streaming lendo um diretório de eventos JSON, não Kafka: o MVP não tem múltiplos consumidores nem exige *exactly-once*, e Kafka/Zookeeper em Docker no Windows adicionava complexidade sem ganho real nesta fase. A troca para Kafka em produção é só de fonte, sem reescrever a lógica de consumo.

**Data lake vs Data warehouse** — GCS + Parquet particionado (`ano`, `rede` — [ADR-008](docs/adr/ADR-008-particionamento.md)) funciona como data lake para Bronze/Silver, onde o schema muda e o custo de storage bruto importa. BigQuery entra só na ponta Gold, como data warehouse de consumo para BI/Looker — não duplicamos o motor de query em toda a pipeline, só onde o consumo (dashboards, SQL ad-hoc) justifica o custo.

**Custo vs performance** — participação por `ano + rede` é ~4x mais rápida nas queries típicas (filtro por ano e rede em 99% dos casos) ao custo de queries por UF isolada varrerem mais partições ([ADR-008](docs/adr/ADR-008-particionamento.md)). No modelo de custo de investimento, escolhemos uma proxy per-capita simples (linear) em vez de um modelo econométrico de custo marginal real — mais barato de manter, ao custo de superestimar o custo dos últimos pontos até a meta (retornos decrescentes reais não capturados — ver [ADR-012](docs/adr/ADR-012-modelo-custo-marginal.md)).

## Qualidade de dados

Implementado em `src/data_quality/` (dois scripts, ~650 linhas): verificação de duplicidade, detecção de valores ausentes (distinguindo nulo estrutural de dado faltante), validação de chaves de relacionamento (`id_municipio` como STRING em 100% dos joins) e consistência entre tabelas (ex.: `agg_alunos_municipios` cruza taxa calculada dos microdados com a taxa publicada pelo INEP e sinaliza divergência `> 3pp`).

## Monitoramento e FinOps

**Monitoramento:** cada mart da camada Gold cloud é construído isoladamente (`safe_build`, em `dataproc_03_gold.py`) — se um mart falhar (ex.: K-Means sem dados suficientes), o traceback vai pro log do Dataproc e a pipeline **continua** para os demais marts, em vez de derrubar o job inteiro ou falhar em silêncio.

**FinOps** (o enunciado pede que o README explique isso especificamente):

- Cluster Dataproc **efêmero**: sobe só para rodar o job, `idle_delete_ttl=1800s` (auto-destrói após 30 min ocioso) e é deletado manualmente ao fim de cada sessão de processamento — sem cluster ligado 24/7.
- `n1-standard-4` (4 vCPU/15GB) com 2 workers por padrão — dimensionado para o volume atual, não superprovisionado.
- Região `us-central1` (uma das mais baratas do GCP) e discos `pd-standard` (não SSD) — sem necessidade de IOPS alto para este workload batch.
- Parquet particionado por `ano`/`rede` reduz volume escaneado por query em ~4x nos padrões de acesso mais comuns ([ADR-008](docs/adr/ADR-008-particionamento.md)).
- Cache local de SICONFI (~111KB JSON) evita re-consultar a API do Tesouro a cada execução ([ADR-007](docs/adr/ADR-007-siconfi-api.md)).
- **Gap conhecido, não implementado:** o bucket GCS não tem regras de *lifecycle*/storage class tiering (ex.: mover Bronze antigo para Coldline). Próximo passo se o histórico crescer.

## Aplicação em IA

A camada Gold foi desenhada para alimentar três usos de IA citados no enunciado:

- **Modelos de predição de alfabetização** — `src/ml/03_modelo_preditivo_risco.py`: RandomForestClassifier prevê risco (`taxa < 75%`) usando só features de **contexto** (população, gasto per capita, região) — exclui deliberadamente proficiência SAEB como feature para evitar vazamento de dado (a proficiência é, na prática, a própria definição de alfabetização).
- **Análise de desigualdade educacional** — `agg_vulnerabilidade_ml` (K-Means, k=4) segmenta municípios por vulnerabilidade combinando taxa, déficit per capita, população (escala log) e gasto; `agg_correlacoes_uf` mede correlação de Pearson gasto×taxa por UF.
- **Políticas públicas baseadas em dados** — `agg_projecao_investimento`, `agg_roi_executivo` e `agg_alocacao_otima` convertem o gap de alfabetização em custo estimado (R$) e usam um Knapsack Greedy para priorizar municípios sob um orçamento hipotético de R$500M ([ADR-010](docs/adr/ADR-010-knapsack.md), [ADR-012](docs/adr/ADR-012-modelo-custo-marginal.md), [ADR-013](docs/adr/ADR-013-fracao-populacao-alfabetizavel.md)).

**Números de referência** (dataset de produção, 5.550 municípios, via BigQuery — ver [`docs/NUMEROS_RECALCULADOS.md`](docs/NUMEROS_RECALCULADOS.md) para a auditoria completa):

| Métrica | Valor |
|---|---|
| Municípios com gap para 80% de alfabetização | 4.679 |
| Investimento total estimado | R$ 1.218,3 milhões |
| Desperdício por ineficiência de gasto | R$ 34,96 bilhões |
| ROI nacional (desperdício ÷ investimento necessário) | 28,69× |
| Cobertura do orçamento de R$500M (Knapsack) | 49,8% dos municípios com gap · ~246.563 alunos beneficiados |

> **Nota de escopo:** "desperdício" mede ineficiência de gasto educacional total (base: população do município); "investimento necessário" mede o custo específico de fechar o gap de alfabetização (base: fração alfabetizável, ADR-013). São dois recortes de gasto diferentes — a razão entre eles (28,69×) é uma leitura de "quanto já se desperdiça hoje vs. quanto seria preciso investir", não uma comparação de mesma base populacional.
>
> Números confirmados direto no BigQuery de produção em 2026-07-06 via `scripts/verificar_numeros_publicacao.py` — todas as métricas bateram exatamente com os valores acima. Rode o script de novo se os dados forem reprocessados no futuro.

## Estrutura do repositório

```
src/
├── batch/        # Ingestão e transformação Bronze -> Silver (dev/local)
├── siconfi/       # Ingestão SICONFI via API (cache local)
├── features/       # Imputação KNN de metas
├── streaming/       # Producer/consumer simulando tempo quase-real
├── gold/         # Geração dos marts Gold (execução local)
├── cloud/         # Equivalente Dataproc/GCS/BigQuery (produção)
├── data_quality/     # Validação e regras de qualidade
└── ml/          # Clustering, otimização de alocação, predição de risco (local)
docs/
├── adr/          # 13 Architecture Decision Records
├── guias/         # Guias operacionais (deploy, PySpark no Windows, etc.)
├── DIAGRAMA_PIPELINE.md # Diagrama da arquitetura e fluxo de dados
└── NUMEROS_RECALCULADOS.md # Auditoria dos números econômicos
notebooks/         # EDA e validação (consumidores downstream da Gold)
terraform/         # Infra como código (GCS, Dataproc, BigQuery)
tests/           # 114 testes (pytest)
```

## Como executar

```bash
pip install -r requirements.txt

# Local (amostra em dados_sample/), gera datalake_sample/{bronze,silver,gold}
python src/batch/01_ingestao_bronze_batch.py
python src/batch/02_silver_transform.py
python src/features/02_imputar_metas_knn.py
python src/gold/01_gerar_marts_gold.py

# Cloud (produção) — ver docs/DEPLOY_GCP.md e docs/guias/GUIA_CLOUD_DEPLOY.md
terraform -chdir=terraform apply
```

## Estado atual e limitações conhecidas

Este projeto é auditado com o mesmo rigor que se espera de um pipeline em produção — o que segue é o estado real, não uma versão polida para a entrega:

- **Testes:** 114 testes (`pytest`); no Windows, os que dependem de PySpark são pulados automaticamente (limitação conhecida do PySpark com JVM/Python serialization — ver [SPARK-15328](https://issues.apache.org/jira/browse/SPARK-15328)). Rodam completos em Docker/Linux.
- **Sem CI** ainda — os testes não rodam automaticamente em push/PR.
- **Looker Studio:** dashboard em construção (página 1 pronta; páginas adicionais pendentes).
- Ver [`docs/NUMEROS_RECALCULADOS.md`](docs/NUMEROS_RECALCULADOS.md) para o histórico de correções do modelo de custo (a primeira versão superestimou o investimento necessário em 77×, corrigido e documentado via ADR).

## Time

Luiz Maibashi & Renan — contrato de colaboração e Linguagem Ubíqua em [`AGENTS.md`](AGENTS.md).
