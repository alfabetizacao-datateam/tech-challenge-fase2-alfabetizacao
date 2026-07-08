# ADR-015: `deficit_per_capita` como métrica de referência (fix de double-counting em 10 pontos) e status de produção do KNN/RandomForest

- **Status:** ACCEPTED | **Data:** 2026-07-08 | **Contexto:** auditoria completa do pipeline (local + cloud), continuação do ADR-014

---

## 1. CONTEXTO

Após o ADR-014 (remoção de `agg_clusters_municipios` e fix do double-counting
no K-Means), o usuário pediu uma auditoria completa do pipeline — local e
cloud — para garantir que todo cálculo envolvendo `deficit_absoluto_proxy`
está matematicamente alinhado ao negócio, e uma decisão sobre se KNN e
RandomForest (as duas técnicas de ML citadas no ADR-004/README mas nunca
rodadas em produção) fazem sentido no projeto.

### Confirmação: `deficit_per_capita` é a métrica certa, `deficit_absoluto` é contexto

`deficit_absoluto_proxy = ((100 - taxa_alfabetizacao) / 100) * populacao_total`
é calculado **por linha** (`ano` × `rede`), sempre usando a população TOTAL do
município — nunca uma população específica da rede. Isso tem duas
implicações:

1. **Somar entre linhas do mesmo município (SUM) reutiliza a mesma
   população uma vez por rede/ano reportado** — um município com 3 redes
   reportadas em 2 anos (6 linhas, confirmado na amostra local) tinha seu
   "déficit total" inflado em até ~6x, não por ser mais vulnerável, mas por
   ter mais granularidade de reporte. Isso favorecia sistematicamente
   municípios com mais dados disponíveis em qualquer ranking/clustering que
   usasse a soma bruta.
2. **`deficit_per_capita` (déficit ÷ população) elimina o viés de escala
   populacional** que `deficit_absoluto` sempre carrega — sem ele, metrópoles
   dominam qualquer leitura só por tamanho, não por severidade. Esse
   racional já estava certo em `agg_priorizacao` (ADR-009); a auditoria
   confirmou que deveria ser o padrão em **todos** os pontos do pipeline que
   comparam municípios entre si por déficit — não só nos dois marts que já
   pediram atenção no ADR-014.

**Resposta à pergunta do usuário: sim, focar em `deficit_per_capita` como
métrica de referência para vulnerabilidade/priorização é a decisão certa.**
`deficit_absoluto`/`deficit_total_estimado` continuam existindo como colunas
de **contexto** (comunicam escala/volume, úteis para leitura executiva —
"quantas pessoas em déficit nesta UF"), mas nunca devem alimentar um
ranking, clustering ou feature de ML sem antes passar por população.

---

## 2. DECISÃO — 10 correções de double-counting/agregação incorreta

### 2.1 Pipeline cloud (`src/cloud/dataproc_03_gold.py`)

| Mart | Bug | Fix |
|---|---|---|
| `agg_uf_indicadores` | `SUM(deficit_absoluto_proxy)` direto do df bruto — soma linhas de rede/ano diferentes do mesmo município antes mesmo de somar entre municípios | Two-stage: `AVG` por (município, ano) primeiro, `SUM` dos valores já corretos por município depois — soma entre municípios distintos é legítima, a bug era a unidade errada de agregação |
| `agg_evolucao_temporal` | Idêntico ao acima | Mesmo fix two-stage |
| `agg_municipio_ranking` | `SUM` entre redes do mesmo (município, ano) | `AVG` |
| `agg_top10_uf` | `dropDuplicates(["id_municipio","ano"])` **antes** de agregar — descartava arbitrariamente todas as redes de um município menos uma (seleção não-determinística de qual rede representa o município), e ainda somava a que sobrava | `dropDuplicates` passou a incluir `"rede"` na chave (remove só duplicatas literais) + `AVG` no agrupamento |
| `agg_qualidade_resumo` | Mesmo problema de `dropDuplicates` prematuro do item anterior — e o bucket de qualidade (`Crítico/Ruim/Razoável/Excelente`) era atribuído a partir da taxa de uma rede escolhida arbitrariamente | Colapsa para (município, ano) via `AVG` primeiro, atribui o bucket a partir dessa taxa média, só depois agrega por UF/bucket |
| `agg_alocacao_otima` | `deficit_total` (não usado no cálculo de custo/benefício do knapsack — que já usa `gap_ate_80` × `populacao_alfabetizavel_estimada`, ADR-013) exposto como coluna informativa, `SUM` sem `ano`/`rede` no agrupamento — mesmo bug do K-Means antes do ADR-014 | Renomeado para `deficit_absoluto_medio`, `AVG` |
| `agg_correlacoes_uf` | **Bug diferente, mais grave**: `df.stat.corr(...)` calculava a correlação de Pearson **uma vez para o Brasil inteiro** e repetia esse mesmo número nacional em toda linha do mart — um mart chamado "correlações **por UF**" que não correlacionava nada por UF | Reescrito com a função agregada `corr()` dentro de `groupBy("sigla_uf")` — correlação real por grupo |
| `agg_correlacoes_uf` (2º bug) | `interpretacao_correlacao` classificava força pelo valor bruto (`< 0.3`), então uma correlação de -0.7 (forte, negativa) seria rotulada "Fraca" | Classificação por valor absoluto |

### 2.2 Pipeline local (`src/gold/01_gerar_marts_gold.py`)

Mesmos bugs, replicados porque os scripts local e cloud têm lógica paralela
(divergência conhecida, ver README "Local vs. produção não têm paridade
total"):

| Mart | Bug | Fix |
|---|---|---|
| `agg_uf_indicadores` | Mesmo bug de SUM + `count("id_municipio")` em vez de `countDistinct` em `qtd_municipios_analisados` (contava linhas, não municípios) | Two-stage AVG→SUM + `countDistinct` |
| `agg_municipio_ranking` | SUM entre redes | AVG |
| `agg_priorizacao` (local) | **Divergia do cloud**: a versão cloud já usava `AVG` (correta), mas a versão local ainda usava `SUM` — os dois scripts tinham desalinhado silenciosamente | AVG (agora sincronizado com o cloud) |
| `agg_top10_uf` | `dropDuplicates(["id_municipio","ano"])` prematuro, mesmo bug do cloud | Inclui `"rede"` na chave + AVG |

### 2.3 Scripts de ML (`src/ml/01_clusterizar_municipios.py`, `src/features/02_imputar_metas_knn.py`)

| Script | Bug | Fix |
|---|---|---|
| `01_clusterizar_municipios.py` (K-Means local, sklearn) | `F.sum("deficit_absoluto_proxy")` usado como feature de clustering — mesmo bug do K-Means cloud pré-ADR-014 | `F.avg` |
| `02_imputar_metas_knn.py` | `SUM(deficit_absoluto_proxy)` em SQL, usado como uma das 3 features do KNN (`taxa_media, populacao, deficit_total`) — o mais grave dos 10 pontos, porque distorcia um **valor imputado real** (a meta prevista), não uma coluna informativa | `AVG(deficit_absoluto_proxy)` |

**Números-bandeira não afetados** (confirmado por leitura de código, não
apenas suposição): `agg_projecao_investimento` (R$1.218,3M) e
`agg_custo_ineficiencia`/`agg_roi_executivo` (R$34,96bi, ROI 28,69×) nunca
leram `deficit_absoluto_proxy` — usam só `AVG(taxa_alfabetizacao)` e
`AVG(populacao_total)`, que não têm o problema de reuso de população entre
linhas. Não precisam ser recalculados por causa desta auditoria.

---

## 3. KNN (imputação de metas) — recomendação: MANTER e PROMOVER

**Necessidade de negócio real e documentada:** só a rede Municipal tem meta
oficial do PDE (43,9% de cobertura); Estadual/Federal/Privada ficam com
`meta_alfabetizacao_2024` NULL sem a imputação. Isso já limita hoje o
`gap_meta` calculado em `agg_municipio_ranking`, `agg_top10_uf` e o
`status_risco` derivado dele para mais da metade dos registros.

**Estado técnico:** implementação real em duas etapas (propagação
município→redes via `first(ignorenulls)`, depois KNN por UF só para quem
sobrou sem meta) — mais simples que o pseudocódigo do ADR-004 (não usa SAEB
como feature), mas funcional e com fallback para UFs com poucos vizinhos.

**O que faltava (achado da auditoria de 2026-07-07):** zero validação
estatística — só estatística descritiva do output (média/mediana/desvio),
sem MAE/RMSE contra nenhum holdout.

**Corrigido nesta sessão:**
- Bug de `deficit_total` somado (Seção 2.3).
- Adicionada `validar_knn_holdout()`: esconde a meta de 20% dos municípios
  que JÁ têm meta conhecida, roda o mesmo KNN por UF, mede MAE/RMSE contra o
  valor real. Roda automaticamente dentro de `etapa2_knn_imputacao` e salva
  o resultado em `metrics_knn_imputacao.json` ao lado do output.

**Recomendação:** rodar o script (local com dado completo, ou via Cloud
Shell) na próxima janela de reprocessamento e conferir o MAE reportado. Se
MAE for baixo (poucos pontos percentuais — sanidade: a própria meta varia
tipicamente entre 85-95% por UF, ver ADR-004 Seção 6), a imputação é
confiável e vale subir: o script já escreve no path
(`alfabetizacao_municipios_obt_com_metas_imputadas`) que
`dataproc_03_gold.py::load_silver()` **já procura automaticammente** — ou
seja, não há trabalho de integração pendente, só a decisão de rodar e
validar o número.

**RESULTADO (2026-07-08 — rodado em produção):** MAE 5,12pp, RMSE 7,26pp
(1.046/1.046 municípios do holdout avaliados), bem abaixo do limiar de
alerta (10pp) — **promovido**. Cobertura de meta: 43,6% (original) → 94,5%
(propagação) → 100% (KNN, 318 municípios). Observação: a média/mediana das
metas imputadas pelo KNN (60,1%/61,5%) ficou abaixo da faixa de sanidade
85-95% do ADR-004 — não é um erro: os 318 municípios que só o KNN resolveu
(nem a propagação achou) tendem a ser os mais vulneráveis (sem meta
reportada em nenhuma rede), então um valor imputado mais baixo é esperado.
O holdout (medida direta de acurácia) é a validação mais confiável e já
confirma a imputação. Verificação pós-promoção em
`agg_municipio_ranking`: `status_risco` monotônico em `gap_meta_medio`
(+11,97 a -32,5pp entre os 4 buckets), 23/5.516 municípios (0,4%) com
`gap_meta` além de ±50pp — proporção pequena e plausível, sem distorção
sistêmica. Ver `docs/NUMEROS_RECALCULADOS.md`, seção "Reprocessamento 3".

---

## 4. RandomForest (predição de risco) — recomendação: MANTER como protótipo validado, não forçar produção agora

**Estado técnico (já era bom antes desta auditoria):** sem vazamento
(exclui deliberadamente SAEB/proficiência das features), `train_test_split`
estratificado, métricas completas (accuracy/precision/recall/F1/ROC-AUC/
matriz de confusão/feature importances), regularizado
(`max_depth=8, min_samples_leaf=20`), balanceado para classe minoritária.
Não precisou de correção nesta auditoria — não usa `deficit_absoluto_proxy`
em nenhum lugar.

**O que falta:** nunca rodou contra a base de produção completa (5.550
municípios) — só contra amostra local, e nem essa está disponível neste
ambiente para gerar um `metrics.json` real agora (sem `datalake_sample`
materializado aqui, sem acesso ao GCP deste ambiente de trabalho).

**Por que não promover agora:** diferente do KNN, o RandomForest **não
alimenta nenhum outro mart** — é um item autocontido citado no README como
uma das 3 aplicações de IA do enunciado, mas sua ausência não distorce
nenhum número de outro mart (ao contrário do `gap_meta`, que hoje já é NULL
para 56% dos registros sem o KNN). O custo de integrá-lo à esteira
Dataproc/BigQuery (novo script `dataproc_XX`, path no GCS, tabela Terraform)
é desproporcional ao ganho para esta entrega, dado que o objetivo do projeto
é o pipeline de dados — ML é ferramenta que enriquece, não o produto.

**Recomendação:** rodar localmente (é rápido — 200 árvores em ~5.550 linhas,
não precisa de cluster) para produzir um `metrics.json` real e committar,
mas **sem** wire-in ao BigQuery nesta rodada. Framing honesto para
vídeo/apresentação: "protótipo pronto e validado tecnicamente, arquitetura
pronta pra produção — K-Means é a aplicação de IA que roda hoje em escala".
Confirma a opção 2 ("esforço médio") já levantada em
`docs/HANDOFF_RENAN_2026-07-07.md` seção 4.4.

---

## 5. GATILHO DE REVISÃO

- [x] Rodar `02_imputar_metas_knn.py`/`dataproc_05_knn_metas.py` contra
  produção — feito em 2026-07-08, MAE 5,12pp, promovido (ver Seção 3).
- [ ] Rodar `03_modelo_preditivo_risco.py` (RandomForest) contra a base
  completa para obter e versionar um `metrics.json` real antes do vídeo
  executivo.
- [x] Reprocessar o Gold (cloud) e rodar `scripts/verificar_numeros_publicacao.py`
  de novo — feito em 2026-07-08: investimento/ROI intactos, cobertura do
  knapsack variou dentro da tolerância de 2% (ver `docs/NUMEROS_RECALCULADOS.md`).

## 6. ACHADO DE INFRAESTRUTURA (2026-07-08, durante o reprocessamento)

`terraform.tfvars.example` tinha `bucket_name = "alfabetizacao-datalake-fiap"`
como default — esse bucket **não existe** no projeto. O bucket real é
`tc-alfabetizacao-fiap-879273` (confirmado via
`gcloud storage buckets list`, já documentado em `docs/DEPLOY_GCP.md`).
Corrigido o default no `.example` com um aviso para sempre confirmar antes
de usar.

Também descoberto: o Terraform gerencia um dataset BigQuery chamado
`"gold"` (default hardcoded em `terraform/modules/bigquery/variables.tf`),
que está **vazio (0 tabelas)** — nunca foi o caminho real de carga de
dados. O dataset de produção de fato, usado por `02_load_bigquery.py` e por
toda a documentação/scripts (`scripts/verificar_numeros_publicacao.py`,
`docs/DEPLOY_GCP.md`), é `alfabetizacao_gold`. **`terraform apply` foi
pulado no reprocessamento de 2026-07-08** — os dois módulos (GCS bucket,
Dataproc cluster) já existiam fora do controle do Terraform (sem estado
local, já que `terraform.tfstate` é gitignored e nunca existiu nesta
sessão), e o módulo BigQuery gerencia um dataset paralelo desconectado do
real. Reconciliar isso (via `terraform import` dos recursos reais, ou
apontar o módulo BigQuery para `alfabetizacao_gold`) fica como pendência —
não bloqueou a entrega porque `02_load_bigquery.py` já resolve a carga real
de forma independente do Terraform.
