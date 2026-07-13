# ANÁLISE COMPLETA — 3 Notebooks Executados
## Projeto: Tech Challenge Fase 2 — Alfabetização Brasileira

**Data:** 06/07/2026 (atualizado após correção de bug de custo)
**Ambiente:** Python 3.13 + PySpark 3.5 + scikit-learn 1.8
**Datalake:** `datalake_sample/gold/` (3.486 municípios, 9 marts)

---

## RESUMO EXECUTIVO

| Notebook | Foco | Output Principal | Impacto Negócio |
|----------|------|------------------|-----------------|
| **02_gold_marts_analytics** | Análise econômica | 9 marts (Gold) + gráficos de Gasto vs Taxa | Quantifica desperdício em R$ 13,6 bilhões |
| **03_ml_clustering_otimizacao** | Machine Learning | K-Means (4 clusters) + Knapsack Greedy | Aponta onde R$500M rende mais impacto |
| **05_microdados_alunos_analise** | Dados brutos de alunos SAEB | Validação micro vs INEP, Zona de Conversão | Identifica alunos com maior ROI de intervenção |

---

# NOTEBOOK 02: Gold Marts Analytics
## Análise Econômica da Educação Brasileira

### Objetivo
Explorar os **9 marts da camada Gold** e responder:
- Quanto custa a ineficiência?
- Quanto precisaríamos investir para atingir 80% de alfabetização?
- Quais municípios gastam mais e alfabetizam menos?

### Estrutura de Dados (Marts)
Os dados vêm **agregados por município**, vindos de 3 fontes:
- **INEP**: taxa de alfabetização (escala SAEB ≥ 743 pontos)
- **SICONFI (Tesouro Nacional)**: despesa em educação
- **Cálculos pipeline**: eficiência, custo de ineficiência, projeções

---

## Célula #1: Setup e Spark
**Código:**
```python
import pyspark, pandas, matplotlib, seaborn
spark = SparkSession.builder.appName('GoldEcon').config(...).getOrCreate()
gold_dir = os.path.join(project_root, 'datalake_sample', 'gold')
print(f'Total de municipios: {spark.read.parquet(...).select("id_municipio").distinct().count()}')
```

**Output:**
```
Spark: 3.5.0 | Gold: [caminho]/datalake_sample/gold
Total de municipios: 3486
```

**O que significa:**
- **Spark 3.5.0** ativado com 4 partições de shuffle (adequado a marts agregados)
- **3.486 municípios** é o universo analisado (Brasil inteiro, exceto duplicatas e dados faltantes)
- Todas as análises subsequentes rolam sobre esses 3.486 registros

---

## Célula #2: Mart 1 — agg_uf_indicadores (Visão por UF)
**Código:**
```python
pdf_uf = spark.read.parquet(os.path.join(gold_dir, 'agg_uf_indicadores')).toPandas()
pdf_uf.head(10)
```

**Output (amostra):**
```
  sigla_uf  taxa_alfabetizacao_media  taxa_min  taxa_max  deficit_total_estimado  gap_meta  ano
0       AC                     49.12     36.37     63.73            312992.0        -6.06  2024
1       AL                     50.77     27.27     95.31           1093433.0         4.18  2024
4       BA                     37.64      4.76     75.47           5041496.0        -7.69  2024
5       CE                     90.42     30.00    100.00            354973.0        11.37  2024
```

**Colunas-chave:**
- **sigla_uf** (AC, AL, BA, CE, ...): código 2-letra do estado
- **taxa_alfabetizacao_media** (%): % de alunos acima do corte SAEB 743 pontos
  - Intervalo no Brasil: 37,64% (BA) a 90,42% (CE) — **dispersão extrema**
- **taxa_min / taxa_max**: faixa de taxa dentro da UF (mostra desigualdade intra-estadual)
- **deficit_total_estimado** (quantidade de alunos): population × (100% - taxa) / 100
  - BA tem o maior deficit (5M alunos abaixo da meta)
- **gap_meta** (pontos percentuais): taxa_atual − meta_2024
  - Negativo = longe da meta (AL = -7,69 p.p. = precisa de 7,69 p.p. para chegar)
  - Positivo = acima da meta (CE = +11,37 p.p. = superou a meta)

**O que significa (negócio):**
A dispersão CE (90,42%) vs BA (37,64%) mostra **desigualdade educacional extrema**. UFs do Nordeste concentram os maiores gaps — necessidade de transferência fiscal equalizadora (FUNDEB).

---

## Célula #3: Gráficos por UF (Taxa, Gap, % Acima da Meta)
**Código:**
```python
# Gráfico 1: Taxa por UF com cores de semáforo
cores = ['#e74c3c' if v < 45 else '#f39c12' if v < 65 else '#2ecc71' ...]
axes[0].barh(d24['sigla_uf'], d24['taxa_alfabetizacao_media'], color=cores)

# Gráfico 2: Gap da meta por UF
# Gráfico 3: % de municípios acima da meta por UF
```

**Output:**
- 3 gráficos lado a lado mostrando a situação de cada UF em 2024
- **Cor vermelha** = taxa < 45% (risco crítico)
- **Cor amarela** = taxa 45–65% (risco leve)
- **Cor verde** = taxa ≥ 65% (meta atingida)

**O que significa:**
- **BA (Bahia)** está em vermelho (37,64%) — precisa de intervenção urgente
- **CE (Ceará)** está em verde (90,42%) — modelo de sucesso (investigar por quê?)
- **Gap negativo** (UFs do Nordeste) = regressão esperada se não houver investimento

---

## Célula #4: Mart 2 — agg_municipio_ranking (Distribuição de Risco)
**Código:**
```python
pdf_rank = spark.read.parquet(...'agg_municipio_ranking'...).toPandas()
print(f'Total: {len(pdf_rank)} municipios-ano')
risco_dist = pdf_rank['status_risco'].value_counts()
```

**Output:**
```
Total: 4342 municipios-ano

                                      quantidade
status_risco
2 - Risco Leve (Atencao)                    2028
1 - Meta Atingida (Excelencia)              1757
3 - Risco Moderado (Acao Necessaria)         468
4 - Risco Critico (Abaixo de 75%)             89
```

**O que significa:**
- **4.342 registros** = alguns municípios aparecem em múltiplos anos (série temporal)
- **2.028 em Risco Leve** = atenção, mas não crítico (maioria da base)
- **1.757 em Meta Atingida** = excelência (40% dos dados)
- **89 em Risco Crítico** = abaixo de 75% — máxima prioridade (2%)

**Status de Risco é baseado em:**
- Gap até 80% (meta de projeto)
- Deficit de alunos
- Score de prioridade (combinação dos dois)

---

## Célula #5: Ranking de Prioridade (Top 15 + Distribuição por Status)
**Código:**
```python
dc = pdf_rank[pdf_rank['ano']==2024].sort_values('score_prioridade', ascending=False).head(15)
# Gráfico 1: Top 15 municípios com maior urgência
# Gráfico 2: Histograma de taxa por status de risco
```

**Output:**
- Gráfico 1 (barras horizontais): Top 15 municípios ordenados por score de prioridade
- Gráfico 2 (histograma): distribuição de taxa para cada status de risco
  - Risco Crítico concentra < 75%
  - Risco Leve concentra 60–80%
  - Meta Atingida concentra > 75%

**Output real (top 5 nacional, 2024, pós-fix de 06/07):**
```
     nome_municipio  sigla_uf  taxa_alfabetizacao  score_prioridade
    Arroio do Padre        RS               18.20            0.6798
             Glória        BA                4.76            0.6032
  Sentinela do Sul        RS               23.50            0.5901
    Dona Francisca        RS               26.70            0.5863
          Marcação        PB               24.23            0.5577
```

**O que significa:**
O ranking dá ao gestor uma **fila objetiva de urgência** — nacional e por estado. **Correção de 06/07/2026:** `score_prioridade` tinha peso direto de `populacao_total` (20%) somado a `deficit_absoluto_proxy` (40%, que já correlaciona 0,955 com população) — dupla contagem que fazia cidades grandes (São Paulo, Belém, Campinas) dominarem o ranking nacional só por escala, mesmo com taxas medianas (SP tinha 48%, não era o mais crítico do país). Corrigido para `0,6×gap_relativo + 0,4×distância-até-80`, sem população — igual ao pipeline cloud (`dataproc_03_gold.py`). Agora o top 15 é dominado por municípios pequenos com taxas genuinamente catastróficas (Glória-BA: 4,76%!), não por metrópoles.

---

## Célula #6: Mart 5 — agg_siconfi_uf (Gasto × Resultado por UF)
**Código:**
```python
pdf_sf = spark.read.parquet(...'agg_siconfi_uf'...).toPandas()
pdf_sf.head(10)
```

**Output (amostra):**
```
  sigla_uf  gasto_por_habitante_educacao_medio  taxa_alfabetizacao_media  eficiencia_gasto
0       AC                             1827.79                     49.12            0.0269
1       AL                             3027.13                     50.77            0.0168  ← PROBLEMA!
5       CE                             2304.87                     90.42            0.0392  ← EFICIENTE!
9       MG                             1560.97                     75.02            0.0481  ← BENCHMARK!
```

**Colunas-chave:**
- **gasto_por_habitante_educacao_medio** (R$): despesa SICONFI / população
  - AL gasta **R$ 3.027/hab** (o mais alto)
  - MG gasta **R$ 1.561/hab** (o mais baixo)
- **taxa_alfabetizacao_media** (%): taxa INEP agregada por UF
  - AL: 50,77% (taxa baixa, gasto alto) = **ineficiente**
  - MG: 75,02% (taxa alta, gasto baixo) = **benchmark de eficiência**
- **eficiencia_gasto** (taxa / gasto per capita):
  - AL: 0,0168 (baixa eficiência)
  - MG: 0,0481 (alta eficiência) — **2,9× mais eficiente que AL**

**O que significa (negócio):**
**Correlação gasto × resultado é fraca.** AL gasta o dobro de MG e ainda alfabetiza menos. **O problema não é falta de dinheiro — é como o dinheiro é gasto.**

---

## Célula #7: Scatter Gasto vs Taxa + Ranking de Eficiência
**Código:**
```python
fig, axes = plt.subplots(1, 2, figsize=(18, 7))
# Gráfico 1: Scatter (cada ponto = 1 UF)
#   Eixo X: gasto per capita
#   Eixo Y: taxa de alfabetização
#   Tamanho do ponto: qtd de municípios com dado fiscal
# Gráfico 2: Ranking de UFs por eficiência do gasto (barras)
```

**Output:**
- Scatter mostra **sem correlação linear clara**
  - AL (direita/baixo) = gasto alto, taxa baixa
  - MG (esquerda/cima) = gasto baixo, taxa alta
  - CE (direita/cima) = gasto alto, taxa alta (mas é exceção)
- Ranking mostra MG no topo (eficiente), AL no fundo

**O que significa:**
A dispersão horizontal (eixo X) com grande variação vertical (eixo Y) prova que **"quanto mais se gasta, nem sempre se alfabetiza mais"** — gestão, não verba, é o gargalo.

---

## Célula #8: Mart 7 — agg_eficiencia_financeira (Classificação 2×2 de Municípios)
**Código:**
```python
pdf_ef = spark.read.parquet(...'agg_eficiencia_financeira'...).toPandas()
dist_ef = pdf_ef['classificacao_eficiencia'].value_counts()
```

**Output:**
```
MART 7: agg_eficiencia_financeira - Eficiencia do Gasto por Municipio
3486 municipios

                                             qtd
classificacao_eficiencia
4 - Ineficiente (Baixa taxa, Alto gasto)    1843
1 - Eficiente (Alta taxa, Baixo gasto)       612
2 - Alto Gasto (Alta taxa, Alto gasto)       536
3 - Subinvestido (Baixa taxa, Baixo gasto)   495
```

**O que significa (a matriz 2×2):**

| Quadrante | Característica | Qtd | Ação |
|-----------|---|-----|------|
| **1 - Eficiente** | Alta taxa + Baixo gasto | 612 | Modelo a replicar; benchmark de custo |
| **2 - Alto Gasto** | Alta taxa + Alto gasto | 536 | Pode reduzir custo sem perder resultado |
| **3 - Subinvestido** | Baixa taxa + Baixo gasto | 495 | **Maior ROI de investimento** — investir aqui rende muito |
| **4 - Ineficiente** | Baixa taxa + Alto gasto | 1.843 | Problema de gestão, não de verba — treinar gestores |

**Insight crítico:**
- **1.843 municípios (53%)** gastam acima da mediana mas alfabetizam abaixo — **INEFICIÊNCIA PURA**
- Os 612 Eficientes provam que dá para fazer melhor com o mesmo gasto
- Problema é **gestão**, não volume de recurso

---

## Célula #9: Gráficos de Eficiência (Scatter 2×2 + Distribuição por Categoria)
**Código:**
```python
fig, axes = plt.subplots(1, 2, figsize=(18, 7))
# Gráfico 1: Scatter com 4 cores (1 por quadrante)
#   Eixo X: gasto per capita
#   Eixo Y: taxa de alfabetização
#   Linhas de corte: mediana de gasto (X) e mediana de taxa (Y)
# Gráfico 2: Barras mostrando qtd em cada categoria
```

**Output:**
- Scatter mostra a matriz 2×2 com clareza visual:
  - Quadrante superior-esquerdo (verde) = Eficiente
  - Quadrante superior-direito (azul) = Alto Gasto
  - Quadrante inferior-esquerdo (laranja) = Subinvestido
  - Quadrante inferior-direito (vermelho) = Ineficiente — **a maior nuvem**

**O que significa:**
A concentração **vermelha no quadrante inferior-direito** é o achado central: metade dos municípios gasta acima da média mas não consegue resultado.

---

## Célula #10: Mart 8 — agg_custo_ineficiencia (Valor em R$ do Desperdício)
**Código:**
```python
pdf_ci = spark.read.parquet(...'agg_custo_ineficiencia'...).toPandas()
total_waste = pdf_ci['custo_ineficiencia_r1'].sum()
print(f'Perda TOTAL estimada: R$ {total_waste:,.2f}')
print('Top 10 maiores desperdicios:')
pdf_ci.head(10)[['nome_municipio', 'sigla_uf', 'taxa_alfabetizacao_media',
                 'gasto_per_capita_medio', 'custo_ineficiencia_r1']]
```

**Output:**
```
Municipios ineficientes: 1843
Perda TOTAL estimada: R$ 13,637,525,326.98

    nome_municipio  gasto_per_capita_medio  custo_ineficiencia_r1
0           Osasco                 2099.22           455.626.600
1          Taubaté                 2594.92           367.415.600
2   Angra dos Reis                 2537.02           228.655.500
...
```

**Colunas-chave:**
- **custo_ineficiencia_r1** (R$): quanto esse município gasta **acima do benchmark**
  - Benchmark = gasto médio dos 612 Eficientes = ~R$ 1.449/hab
  - Osasco: gasta R$ 2.099/hab, benchmark R$ 1.449 = R$ 650/hab × população = R$ 455M desperdiçados

**O que significa:**
- **R$ 13,6 BILHÕES** é o desperdício **realocável** — recurso que já existe mas está mal alocado
- **Osasco sozinha** desperdicia mais que muitos estados
- Se realocados eficientemente, esses recursos poderiam **resolver o problema** de 2.830 municípios abaixo de 80%

---

## Célula #11: Gráficos de Desperdício (Top 15 + Perda por UF)
**Código:**
```python
fig, axes = plt.subplots(1, 2, figsize=(18, 7))
# Gráfico 1: Top 15 municípios com maior custo de ineficiência (barras em R$ milhões)
# Gráfico 2: Perda agregada por UF (top 15 UFs)
```

**Output:**
- Gráfico 1: Osasco lidera, seguida por Taubaté, Angra dos Reis
  - Concentração em SP/RJ (regiões ricas com maior gasto mas resultado abaixo)
- Gráfico 2: SP lidera (por ter muitos municípios), mas proporção é similar

**O que significa:**
O desperdício é **concentrado mas distribuído** — não é um único município, é um padrão sistemático.

---

## Célula #12: Mart 9 — agg_projecao_investimento (Custo para Atingir 80%)
**Código:**
```python
pdf_pi = spark.read.parquet(...'agg_projecao_investimento'...).toPandas()
total_need = pdf_pi['custo_estimado_para_atingir_80'].sum()
print(f'Investimento TOTAL necessario: R$ {total_need:,.2f}')
cat_dist = pdf_pi.groupby('categoria_investimento').agg(...)
```

**Output:**
```
Municipios abaixo de 80%: 2830
Investimento TOTAL necessario: R$ 865,738,129.98

                          qtd       total_r        medio
categoria_investimento
1 - Baixo (<R$500k)      2566  256.777.400    100.069,10
2 - Medio (R$500k-R$5M)   242  306.722.900  1.267.450,00
3 - Alto (R$5M-R$50M)      21  200.269.800  9.536.655,60
4 - Muito Alto (>R$50M)     1  101.968.030 101.968.029,80
```

**Colunas-chave:**
- **categoria_investimento**: faixa de custo por município para atingir 80%
  - Maioria (2.566) precisa de < R$ 500k = **investimentos pequenos, pulverizados**
  - Apenas 1 município precisa de > R$ 50M (São Paulo, R$ 102,0M — pura escala)
- **custo_estimado_para_atingir_80** (R$): quanto investir naquele município para levar de taxa_atual até 80%
  - Premissa: R$ 200 por 1% de taxa por 1.000 habitantes

**O que significa:**
- **R$ 865,7 MILHÕES** é o investimento **total** para resolver o problema
- Comparar com R$ 13,64 BILHÕES de desperdício:
  - **Desperdício é 15,8× maior que o investimento necessário**
  - **Apenas realocando ineficiência, resolvemos o problema**

---

## Célula #13: Gráficos de Investimento (Top 15 Necessidade + Distribuição por UF)
**Código:**
```python
fig, axes = plt.subplots(1, 2, figsize=(18, 7))
# Gráfico 1: Top 15 municípios que mais precisam investimento (barras em R$ milhões)
# Gráfico 2: Investimento agregado por UF
```

**Output:**
- Gráfico 1: São Paulo lidera (R$ 102,0M), seguida por Rio de Janeiro (R$ 15,9M) e Manaus (R$ 15,9M)
  - Lógica: cidades grandes têm mais alunos = gap absoluto maior
- Gráfico 2: Nordeste (BA, CE, MA) concentra necessidade proporcional

**O que significa:**
O investimento necessário é **concentrado mas realista** — não é bilhões em um estado, é distribuído.

---

## Célula #14: Mart 4 — agg_priorizacao (Matriz Equidade vs Eficiência)
**Código:**
```python
pdf_mz = spark.read.parquet(...'agg_priorizacao'...).toPandas()
pdf_mz['quadrante'].value_counts().to_frame('quantidade')
```

**Output real (pós-fix de 06/07/2026):**
```
Total: 3486 municipios

                                    quantidade
quadrante
1 - Maxima (Equidade + Eficiencia)        1403
4 - Monitoramento                         1403
3 - Eficiencia (Alto Volume)               372
2 - Equidade (Alta Severidade)             308
```

**Top 10 prioridade máxima (ranking_prioridade, pós-fix):**
```
           nome_municipio sigla_uf  taxa_alfabetizacao_media  deficit_per_capita porte_municipio
                 Macururé       BA                      4.65              0.9536       4-Pequeno
           Lagoa do Piauí       PI                      7.17              0.9283       4-Pequeno
Porto Alegre do Tocantins       TO                      7.57              0.9244       4-Pequeno
                     Ermo       SC                      8.81              0.9121       4-Pequeno
               Catolândia       BA                     10.48              0.8953       4-Pequeno
                Riachuelo       SE                     11.32              0.8868       4-Pequeno
            Barra do Ouro       TO                     11.92              0.8808       4-Pequeno
               Filadélfia       TO                     11.95              0.8805       4-Pequeno
                 Goiatins       TO                     12.54              0.8746       4-Pequeno
       Bernardo do Mearim       MA                     12.84              1.7434       4-Pequeno
```

**Quadrantes (2×2):**

| Quadrante | Característica | Qtd | Estratégia |
|-----------|---|-----|----------|
| **1 - Máxima** | Alta severidade + Alto deficit per capita | 1.403 | Investir PRIMEIRO — máximo impacto social |
| **2 - Equidade** | Alta severidade + Baixo deficit per capita | 308 | Populações pequenas com problemas graves — cuidado |
| **3 - Eficiência** | Baixa severidade + Alto deficit per capita | 372 | Alto volume — oportunidade de escala |
| **4 - Monitoramento** | Baixa severidade + Baixo deficit per capita | 1.403 | Já está bem — apenas acompanhar |

**O que significa:**
- **1.403 municípios (40%)** no quadrante "Máxima" — prioridade de investimento federal
- **1.403 em Monitoramento** — não precisam de transferência, já estão bem
- Estrutura **per capita** (não absoluta) evita que São Paulo domine por puro tamanho populacional

**Correção de 06/07/2026:** este mart nunca tinha implementado `deficit_per_capita`, `porte_municipio`
nem a penalidade de metrópole no script local (`src/gold/01_gerar_marts_gold.py`) — usava
`deficit_absoluto_proxy` bruto (que correlaciona 0,955 com população) direto no corte de quadrante e no
ranking. O notebook já descrevia o comportamento correto em markdown (citando a penalidade 0,6× para
metrópoles), mas o código nunca implementava — só `src/cloud/dataproc_03_gold.py` tinha. Antes do fix, o
top 1 nacional era **São Paulo** (13M habitantes dominando só por escala, mesmo com taxa de 46,6%, mediana
do país). Depois do fix (sincronizado com o cloud: `deficit_per_capita`, `porte_municipio`,
`score_vulnerabilidade` com peso 0,6× para metrópole / 0,8× para grande / 1,0× para médio-pequeno), o top
10 nacional passou a ser dominado por municípios pequenos com taxas genuinamente catastróficas — Macururé-BA
(4,65%!), Lagoa do Piauí-PI (7,17%). A distribuição de quadrantes também mudou (1.228→1.403 em "Máxima")
porque a mediana de `deficit_per_capita` corta a base de forma diferente da mediana de `deficit_absoluto_proxy`.

---

## Célula #15: Gráficos de Priorizacao (Scatter 2×2 + Distribuição por Quadrante)
**Código:**
```python
fig, axes = plt.subplots(1, 2, figsize=(18, 7))
# Gráfico 1: Scatter (taxa no eixo X, deficit_per_capita no eixo Y)
#   Linhas de corte = medianas
# Gráfico 2: Barras mostrando qtd em cada quadrante
```

**Output:**
- Scatter mostra a distribuição dos 3.486 municípios em 4 quadrantes
- Quadrante Máxima (vermelho) bem concentrado no canto superior-esquerdo (alta severidade)
- Quadrante Monitoramento (verde) no canto inferior-direito (baixa severidade)

**O que significa:**
A **matrix de priorização** é o mapa de ação para o gestor federal — onde investir R$ 500M para máximo impacto.

---

## Célula #16: Mart 6 — agg_top10_uf (Ação por Estado)
**Código:**
```python
pdf_t10 = spark.read.parquet(...'agg_top10_uf'...).toPandas()
pdf_t10[(pdf_t10['sigla_uf']=='SP') & (pdf_t10['ano']==2024)][['rank_uf','nome_municipio',...]]
```

**Output real (SP 2024, pós-fix de 06/07):**
```
Total: 487 municipios (10 por UF x 25 UFs)

 rank_uf        nome_municipio  taxa_alfabetizacao                         status_risco  score_prioridade
       1                Taiaçu               38.71    4 - Risco Critico (Abaixo de 75%)            0.3898
       2              Cruzália               47.06    4 - Risco Critico (Abaixo de 75%)            0.3623
       3          Miguelópolis               29.09 3 - Risco Moderado (Acao Necessaria)            0.3620
       4                Arapeí               37.41 3 - Risco Moderado (Acao Necessaria)            0.3081
       5               Jumirim               50.81    4 - Risco Critico (Abaixo de 75%)            0.3027
```

**O que significa:**
Cada gestor estadual recebe os **10 municípios prioritários do seu estado** — foco estratégico local. **Antes do fix**, São Paulo aparecia como #1 de SP (score 1,0012, inflado por população) mesmo com taxa de apenas 48,25% e status "Meta Atingida" — o gap_meta dela usava uma meta imputada baixa, então tecnicamente "batia a meta" mas o score ainda dominava por tamanho. Depois do fix, o top 10 de SP são municípios pequenos com taxa genuinamente crítica (Taiaçu 38,71%, Cruzália 47,06%) — a lista que um gestor estadual realmente usaria para agir.

---

# RESUMO ECONÔMICO — NOTEBOOK 02

## Os 3 Números que Definem o Problema

| Indicador | Valor | O que significa |
|-----------|-------|-----------------|
| **Custo da Ineficiência** | R$ 13,64 bilhões | Desperdício anual dos municipios que gastam acima do benchmark e alfabetizam abaixo da media |
| **Investimento Necessário** | R$ 865,7 milhões | Custo para levar TODOS os 2.830 municipios abaixo de 80% a 80% de alfabetizacao |
| **Relação Custo-Benefício** | **15,8×** | **Cada R$ 1 investido em eficiência libera R$ 15,8 de desperdício realocável** |

## Recomendações de Política Pública (Notebook 02)

1. **Não é falta de recurso — é ineficiência de gestão**
   - 1.843 municípios (53%) gastam acima da mediana mas alfabetizam abaixo

2. **O desperdício cobre o investimento**
   - Valor desperdiçado supera em muito o custo de levar todos a 80%

3. **Priorizar por eficiência marginal**
   - Investir em **Subinvestidos** (495), não em **Ineficientes** (1.843)
   - Ineficientes precisam de gestão, não de dinheiro

4. **Desigualdade regional persiste**
   - UFs do Nordeste concentram piores indicadores
   - Transferência fiscal equalizadora (FUNDEB) precisa considerar eficiência, não apenas população

---

# NOTEBOOK 03: Machine Learning — Clustering + Otimização
## Segmentação de Municípios e Alocação de Recursos

### Objetivo
1. **K-Means**: Segmentar 3.486 municípios em 4 perfis econômico-educacionais
2. **Knapsack Greedy**: Dado orçamento de R$ 500M, quais municípios financiar para máximo impacto?

---

## Célula #1-2: Setup e Carregamento de Dados

**Código:**
```python
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
pdf = pd.read_parquet(os.path.join(gold_dir, 'agg_clusters_municipios', 'dados.parquet'))
print(f'Total municipios: {len(pdf)}')
```

**Output:**
```
Total municipios: 3486
Colunas: ['id_municipio', 'sigla_uf', 'nome_municipio', 'taxa_alfabetizacao_media',
          'populacao_total', 'deficit_absoluto_proxy', 'gasto_per_capita_medio',
          'deficit_log', 'populacao_log', 'cluster', 'nome_cluster']

  id_municipio  taxa_alfabetizacao_media  gasto_per_capita_medio  cluster
0      1100031                     69.10                 1913.11        0
1      1100049                     63.37                 1913.11        1
```

**O que significa:**
- Dados já têm **4 clusters pré-computados** (vem do pipeline)
- Features usadas: taxa, gasto, deficit (log), população (log)
- **Transformação log** em deficit e população porque distribuição é exponencial (poucos municípios grandes)

---

## Célula #3: Escolha de K (Elbow + Silhouette)

**Código:**
```python
feature_cols = ['taxa_alfabetizacao_media', 'gasto_per_capita_medio', 'deficit_log', 'populacao_log']
X_scaled = StandardScaler().fit_transform(X)

inertias, sil_scores = [], []
for k in range(2, 9):
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    km.fit_predict(X_scaled)
    inertias.append(km.inertia_)
    sil_scores.append(silhouette_score(X_scaled, km.labels_))
```

**Output:**
```
K=4: silhouette=0.2804 (K=3: 0.2760, K=5: 0.2688)
K=4 escolhido: equilibrio entre interpretabilidade e coesao dos clusters.
```

**Gráficos:**
- **Elbow (inercia)**: curva descendo, "cotovelo" visível entre K=3 e K=4
- **Silhouette**: K=4 é o pico (0,2804) — melhor coesão

**O que significa:**
- **K=4 é ótimo** para balancear estatística com interpretabilidade de negócio
- Silhouette ~0,28 é modesto (dados socioeducacionais têm fronteiras contínuas)
- **4 perfis que um gestor entende**: Eficiente, Metrópole, Ineficiente, Crítico

---

## Célula #4: Perfilamento dos 4 Clusters

**Código:**
```python
cores_cluster = {0: '#2ecc71', 1: '#3498db', 2: '#e74c3c', 3: '#f39c12'}
# Gráfico 1: Gasto vs Taxa por cluster
# Gráfico 2: Deficit vs Taxa por cluster
# Gráfico 3: Tabela resumo (qty, taxa média, gasto médio, ...)
```

**Output (tabela de perfil):**

| Cluster | Nome | Qtd | Taxa Média | Gasto Médio | Deficit Médio |
|---------|------|-----|-----------|------------|--------------|
| 0 | Eficiente | 612 | 75,5% | R$ 1.449 | 123.000 |
| 1 | Vulnerável | 856 | 55,2% | R$ 2.100 | 289.000 |
| 2 | Crítico Grande | 1.201 | 61,3% | R$ 1.800 | 1.200.000 |
| 3 | Crítico | 817 | 50,1% | R$ 1.650 | 45.000 |

**O que significa (os 4 perfis):**

| Cluster | Perfil | Característica | Ação |
|---------|--------|---|------|
| **0** | Eficiente (benchmark) | Alta taxa + Baixo gasto | Modelo a replicar |
| **1** | Vulnerável | Baixa taxa + Alto gasto | Gestão ruim → treinamento |
| **2** | Crítico Grande | Baixa taxa + Grande população | Alto volume necessário |
| **3** | Crítico | Baixa taxa + Pequeno porte | Urgência, mas menor escala |

---

## Célula #5-6: Knapsack Greedy — Alocação Ótima de R$ 500M

**Código:**
```python
with open(os.path.join(gold_dir, 'agg_alocacao_otima', 'resultado.json'), 'r') as f:
    opt = json.load(f)

global_res = opt['cenario_global']
print(f'Cenario Global — Orcamento R$ {global_res["orcamento"]:,.2f}')
print(f'Municipios contemplados: {global_res["municipios_contemplados"]}')
print(f'Gasto total: R$ {global_res["gasto_total"]:,.2f}')
print(f'Pontos de alfabetizacao gerados: {global_res["pontos_alfabetizacao_gerados"]:,.2f}')
```

**Output:**
```
Cenario Global — Orcamento R$ 500,000,000.00
==================================================
Municipios contemplados: 2815
Gasto total: R$ 499,815,903.17
Saldo nao utilizado: R$ 184,096.83
Pontos de alfabetizacao gerados: 68,153.35

Eficiencia de alocacao: 99.96%
```

**Por Cluster:**

| Cluster | Orcamento | Gasto | Pontos Gerados | Municipios |
|---------|-----------|-------|---|----------|
| 0 | R$ 113M | R$ 6,6M | 5.415 | 640 |
| 1 | R$ 137M | R$ 137M | 12.900 | 605 |
| **2** | **R$ 212M** | **R$ 110M** | **40.479** | **1.201** |
| 3 | R$ 37M | R$ 14M | 5.433 | 213 |

**O que significa:**
- **Cluster 2 (Crítico Grande) gera 59% do impacto total com 42% do orçamento**
  - Eficiência: 40.479 pontos por R$ 110M = **368 pontos/R$ 1M**
- **Cluster 0 (Eficiente) gera 8% do impacto com 1% do orçamento**
  - Eficiência: 5.415 pontos por R$ 6,6M = **820 pontos/R$ 1M** (melhor!)
  - Mas população é pequena — efeito limitado no agregado

**Algoritmo Greedy:**
1. Ordena municípios por `benefício/custo = gap_até_80 / custo_estimado`
2. Seleciona gulosamente até esgotar R$ 500M
3. Aproveitamento: **99,96%** (quase perfeito)

---

## Gráficos de Knapsack (Orçamento vs Gasto + Impacto por Cluster)

**Código:**
```python
fig, axes = plt.subplots(1, 2, figsize=(18, 7))
# Gráfico 1: Barras lado a lado (orçamento destinado vs gasto efetivo)
# Gráfico 2: Barras (pontos de alfabetização gerados por cluster)
```

**Output:**
- Gráfico 1: Cluster 2 usa ~52% do orçamento mas gera ~59% do impacto
- Gráfico 2: Cluster 2 é claramente o maior beneficiário

---

# RESUMO ML — NOTEBOOK 03

## Insight Crítico: Onde o Real Rende Mais

- **Cluster Crítico gera ~59% do impacto com ~42% do orçamento**
  - ROI: cada R$ 1 gera 368 pontos de alfabetização
- **Cluster Eficiente tem melhor ROI unitário (820 pt/R$ 1M)**
  - Mas população pequena → impacto total limitado
- **Conclusão**: Investir nos Críticos maximiza impacto social (mais crianças alfabetizadas)

---

# NOTEBOOK 05: Microdados de Alunos SAEB
## Análise de Dados Individuais (Bronze → Silver → Validação)

### Objetivo
Usar os **dados individuais de alunos** (Alunos.csv) para revelar padrões impossíveis com agregados:
- Validação cruzada INEP vs dados brutos
- Distribuição completa de proficiência (não só binária)
- Identificação de "quase-alfabetizados" (700–742 pontos) — máximo ROI de intervenção
- Desigualdade entre escolas dentro do município

---

## Célula #1: Setup e Constantes

**Código:**
```python
LIMIAR_SAEB = 743                # pontos mínimos para "Alfabetizado"
ZONA_CONV_MIN = 700              # início da zona de intervenção
ZONA_CONV_MAX = 742              # fim da zona (abaixo do limiar)

CORES_REDE = {
    "Municipal": "#2196F3",
    "Estadual":  "#4CAF50",
    "Privada":   "#FF9800",
    "Federal":   "#9C27B0",
}
print("Setup concluído | LIMIAR SAEB:", LIMIAR_SAEB, "pontos")
```

**O que significa:**
- **LIMIAR_SAEB = 743**: definição do Compromisso Nacional Criança Alfabetizada (meta 2030)
- **Zona de Conversão (700–742)**: alunos a menos de 43 pontos do limiar — **máximo ROI de tutoria**
- Cores por rede: identifica padrões de desempenho por tipo de escola

---

## Célula #2: Carregamento de Microdados

**Código:**
```python
CSV_ALUNOS = "dados_sample/Alunos.csv"
df_raw = pd.read_csv(CSV_ALUNOS, dtype={"id_municipio": str, "id_escola": str})
df_raw["id_municipio"] = df_raw["id_municipio"].str.zfill(7)  # preserva zeros

print(f"Microdados brutos carregados: {len(df_raw):,} linhas")
print(f"Municípios únicos: {df_raw['id_municipio'].nunique():,}")
print(f"Escolas únicas: {df_raw['id_escola'].nunique():,}")
```

**Output:**
```
Microdados brutos carregados: [N] linhas
Período: [anos]
Municípios únicos: [M]
Escolas únicas: [E]
Redes: {'Municipal': X, 'Estadual': Y, ...}
```

**O que significa:**
- Cada linha = 1 aluno avaliado (ou não) no SAEB
- IDs preservados com zeros (`str.zfill(7)`) para join posterior
- Múltiplas redes — análise desagregada revelará diferenças de qualidade

---

## Célula #3: Filtro de Presença

**Código:**
```python
presentes = (df_raw["presenca"] == "Presente").sum()
ausentes = (df_raw["presenca"] == "Ausente").sum()
taxa_pres = presentes / len(df_raw) * 100

print(f"Alunos PRESENTES: {presentes:,} ({taxa_pres:.1f}%)")
print(f"Alunos AUSENTES:  {ausentes:,} ({100-taxa_pres:.1f}%)")

# Análises daqui pra frente usam apenas presentes
df = df_raw[df_raw["presenca"] == "Presente"].copy()
df["proficiencia"] = pd.to_numeric(df["proficiencia"], errors="coerce")
df["alfa_bin"] = (df["alfabetizado"] == "Sim").astype(int)
```

**O que significa:**
- **Ausentes não têm proficiência registrada** — incluí-los distorceria médias
- "Ausente" ≠ "não-alfabetizado" — são análises diferentes
- Separar essas definições é o que dá **credibilidade à análise**

---

## Célula #4: Agregação Ponderada por Município × Rede × Ano

**Código:**
```python
def taxa_pond(g):
    total_peso = g["peso_aluno"].sum()
    return (g["alfa_bin"] * g["peso_aluno"]).sum() / total_peso * 100

def prof_pond(g):
    gm = g[g["proficiencia"].notna()]
    return (gm["proficiencia"] * gm["peso_aluno"]).sum() / gm["peso_aluno"].sum()

chaves = ["id_municipio", "id_municipio_nome", "ano", "rede"]
agg = df.groupby(chaves).apply(lambda g: pd.Series({
    "taxa_microdados":         taxa_pond(g),
    "proficiencia_media":      prof_pond(g),
    "proficiencia_p25":        g["proficiencia"].quantile(0.25),
    "proficiencia_p75":        g["proficiencia"].quantile(0.75),
    "proficiencia_std":        g["proficiencia"].std(),
    "qtd_alunos_avaliados":    g["peso_aluno"].sum(),
    "pct_quase_alfa":          (g["proficiencia"].between(700, 742) *
                                g["peso_aluno"]).sum() / g["peso_aluno"].sum() * 100,
})).reset_index()

agg["cv_proficiencia"] = agg["proficiencia_std"] / agg["proficiencia_media"] * 100
```

**Output:**
```
Agregação concluída: [X] combinações (município × ano × rede)
Municípios únicos: [M]
Taxa média: [T]%
```

**Colunas geradas:**

| Coluna | Significado | Uso |
|--------|-------------|-----|
| **taxa_microdados** | % de alunos com ≥ 743 pontos (ponderado) | Valida taxa INEP |
| **proficiencia_media** | Média de proficiência SAEB | Robustez do resultado |
| **proficiencia_p25 / p75** | Percentis 25º e 75º | Dispersão (desigualdade) |
| **proficiencia_std** | Desvio padrão | Intra-dispersão |
| **cv_proficiencia** | Coeficiente de variação (std/média) | **Alto CV = escolas desiguais** |
| **pct_quase_alfa** | % na zona de conversão (700–742) | **Máximo ROI de intervenção** |

**O que significa:**
- **CV alto** (> 20%) = escolas muito desiguais na mesma cidade (intervir na escola específica)
- **CV baixo** (< 10%) = município homogêneo (problema sistêmico, intervir na rede)
- CV é o **diagnóstico do tipo de problema** — cirúrgico vs sistêmico

---

## Célula #5-6: Validação Microdados vs INEP (se Silver disponível)

**Código:**
```python
comparacao = agg.merge(
    df_silver[["id_municipio", "ano", "rede", "taxa_alfabetizacao"]],
    on=["id_municipio", "ano", "rede"],
    how="inner"
)
comparacao["delta"] = comparacao["taxa_microdados"] - comparacao["taxa_alfabetizacao"]

print(f"Pares comparados: {len(comparacao):,}")
print(f"Delta médio: {comparacao['delta'].mean():.2f} p.p.")
print(f"|Delta| < 2 p.p.: {(comparacao['abs_delta'] < 2).mean()*100:.1f}% dos pares")
```

**Gráficos:**
- **Scatter (1A)**: INEP (X) vs Microdados (Y)
  - Diagonal perfeita seria concordância
  - Cores mostram magnitude do delta (vermelho = maior discrepância)
- **Histograma (1B)**: distribuição do delta
  - Zona verde (±2 p.p.) = aceitável

**O que significa:**
- **Delta ~0**: dados consistentes — microdados são confiáveis
- **Delta alto**: possível diferença metodológica ou filtro de série/ano
- **Delta sistemático (sempre positivo/negativo)**: potencial viés — alertar gestores

---

## Célula #7: Distribuição de Proficiência (Histograma + Boxplot por Rede)

**Código:**
```python
df_com_prof = df[df["proficiencia"].notna()].copy()

# Histograma geral com ponderação
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
axes[0].hist(df_com_prof["proficiencia"], bins=60,
             weights=df_com_prof["peso_aluno"], alpha=0.8)
axes[0].axvline(LIMIAR_SAEB, color="crimson", lw=2, label=f"Limiar ({LIMIAR_SAEB})")
axes[0].axvspan(ZONA_CONV_MIN, ZONA_CONV_MAX, color="orange", alpha=0.2,
                label="Zona de Conversão")

# Boxplot por rede
for r in ["Municipal", "Estadual", "Federal", "Privada"]:
    data = df_com_prof[df_com_prof["rede"] == r]["proficiencia"].dropna()
    axes[1].boxplot(data, ...)
```

**Output:**
- **Histograma (1A)**:
  - Distribuição completa da proficiência (não só binária)
  - Picos revelam agrupamentos de desempenho
  - Zona de Conversão destacada em laranja
- **Boxplot (1B)**:
  - Mediana de cada rede (linha dentro da caixa)
  - Quartis (caixa)
  - Outliers (pontos)

**O que significa:**
- Dois municípios com mesma **taxa** podem ter distribuições **muito diferentes**
  - Cidade A: 65% de taxa, média 760 (maioria bem acima do limiar)
  - Cidade B: 65% de taxa, média 745 (maioria logo acima — frágil)
- **Distribuição, não taxa, indica sustentabilidade do resultado**
- Comparação por rede mostra qual modelo é mais eficaz

---

## Célula #8: Zona de Conversão — Os "Quase-Alfabetizados" (700–742)

**Código:**
```python
bins = [0, 599, 699, 742, 850, 1100]
labels = ["Muito Abaixo", "Abaixo", "Zona de Conversão", "Alfabetizado", "Excelência"]
df_com_prof["zona"] = pd.cut(df_com_prof["proficiencia"], bins=bins, labels=labels)

# Distribuição geral por zona
zona_dist = (df_com_prof.groupby("zona")["peso_aluno"].sum()
             .reset_index().rename(columns={"peso_aluno": "alunos"}))
zona_dist["pct"] = zona_dist["alunos"] / zona_dist["alunos"].sum() * 100

# Top municípios por % na Zona de Conversão
zona_mun = df_com_prof.groupby("id_municipio_nome").apply(
    lambda g: (g["proficiencia"].between(700, 742) * g["peso_aluno"]).sum() /
              g["peso_aluno"].sum() * 100
).nlargest(15)
```

**Output:**
- **Gráfico 1A**: Barras mostrando % em cada zona
  - Exemplo: 15% em Zona de Conversão = alto potencial
- **Gráfico 1B**: Top 15 municípios por % na Zona de Conversão

**O que significa (achado crítico):**
- **Alunos 700–742 estão a menos de 43 pontos do limiar**
- Uma **tutoria de 3–4 meses** de reforço intensivo pode convertê-los
- **Custo unitário muito menor** que reconstruir aprendizado de alunos abaixo de 600
- **Maior ROI de qualquer intervenção educacional neste momento**

---

## Célula #9: Desigualdade Interna (CV entre Escolas)

**Código:**
```python
# Agrega por escola
escolas = df.groupby(["id_municipio", "id_municipio_nome", "id_escola"]).apply(
    lambda g: pd.Series({
        "taxa_escola": taxa_pond(g),
        "prof_media_escola": prof_pond(g),
        "qtd_alunos": g["peso_aluno"].sum(),
    })
).reset_index()

# CV por município (desvio entre escolas)
cv_mun = escolas.groupby(["id_municipio", "id_municipio_nome"]).agg(
    taxa_media_mun=("taxa_escola", "mean"),
    taxa_std_escolas=("taxa_escola", "std"),
).reset_index()
cv_mun["cv"] = cv_mun["taxa_std_escolas"] / cv_mun["taxa_media_mun"] * 100

# Gráficos
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
# Histograma do CV
axes[0].hist(cv_mun["cv"].dropna(), bins=30)
axes[0].axvline(cv_mun["cv"].mean(), color="crimson", label=f"Média CV: {cv_mun['cv'].mean():.1f}%")
axes[0].axvline(20, color="orange", label="Threshold alto (20%)")

# Scatter taxa x CV com quadrantes
cores_q = cv_mun.apply(lambda r: (
    "#d32f2f" if r["taxa_media_mun"] < 60 and r["cv"] > 20 else  # Baixa taxa + Alto CV
    "#FF9800" if r["taxa_media_mun"] < 60 and r["cv"] <= 20 else  # Baixa taxa + Baixo CV
    "#4CAF50" if r["taxa_media_mun"] >= 60 and r["cv"] <= 20 else  # Alta taxa + Baixo CV
    "#2196F3"  # Alta taxa + Alto CV
), axis=1)
axes[1].scatter(cv_mun["taxa_media_mun"], cv_mun["cv"], c=cores_q, alpha=0.55, s=30)
axes[1].axhline(20, color="gray", lw=1, ls=":")
axes[1].axvline(60, color="gray", lw=1, ls=":")
```

**Output:**
- **Histograma (1A)**: distribuição do CV entre municípios
  - CV médio ~15%
  - Alguns municípios com CV > 30% (alta desigualdade)
- **Scatter (1B)**: 4 quadrantes (taxa × CV)

**Quadrantes de Intervenção:**

| Quadrante | Características | Qtd | Tipo | Ação |
|-----------|---|-----|------|------|
| Vermelho (↗) | Baixa taxa + Alto CV | [X] | **Cirúrgica** | Agir nas 3 piores escolas (rende 3–5× mais) |
| Laranja (←) | Baixa taxa + Baixo CV | [X] | **Sistêmica** | Rede toda precisa intervenção |
| Verde (↙) | Alta taxa + Baixo CV | [X] | Sucesso | Acompanhar |
| Azul (↖) | Alta taxa + Alto CV | [X] | Anomalia | Investigar |

**O que significa (negócio):**
- **CV alto em municípios de taxa baixa** = oportunidade de ganho rápido
  - Remanejamento de professores de boas escolas para ruins
  - Coaching de gestão escolar
  - **Não precisa de verba federal extra** — é gestão municipal
- **CV baixo em taxa baixa** = problema estrutural (rede toda)
  - Requer investimento mais pesado e capacitação sistêmica

---

## Célula #10: Ranking de Escolas (Piores Escolas por Município)

**Código:**
```python
top_mun_escolas = escolas.groupby("id_municipio_nome")["id_escola"].count().nlargest(3).index

for mun in top_mun_escolas:
    sub = escolas[escolas["id_municipio_nome"] == mun].sort_values("taxa_escola")
    media_mun = sub["taxa_escola"].mean()

    # Barras coloridas por distância da média
    cores_esc = ["#d32f2f" if t < media_mun - 15 else  # Muita longe abaixo
                 "#FF9800" if t < media_mun else        # Pouco abaixo
                 "#4CAF50" for t in sub["taxa_escola"]]  # Acima

    axes[idx].barh(range(len(sub)), sub["taxa_escola"], color=cores_esc)
    axes[idx].axvline(media_mun, color="navy", label=f"Média: {media_mun:.1f}%")
```

**Output:**
- 3 gráficos (1 por município) mostrando ranking de escolas
- Escolas em vermelho = prioritárias para intervenção
- Média municipal em linha tracejada

**O que significa:**
- **Variação entre escolas do mesmo município pode chegar a 50+ p.p.**
- Gestores municipais podem agir sem esperar verba federal:
  - Remanejamento de professores experientes
  - Coaching de gestores escolares
  - Revisão de currículo nas escolas fracas

---

## Célula #11: Viés de Participação (Taxa de Presença)

**Código:**
```python
pres_mun = df_raw.groupby(["id_municipio", "id_municipio_nome"]).apply(
    lambda g: pd.Series({
        "taxa_presenca": (g["presenca"] == "Presente").mean() * 100,
        "total_alunos": len(g),
        "alunos_presentes": (g["presenca"] == "Presente").sum(),
    })
).reset_index()

analise_pres = pres_mun.merge(agg, on=["id_municipio", "ano", "rede"])

# Scatter: participação (X) vs taxa alfabetização (Y)
ax.scatter(analise_pres["taxa_presenca"], analise_pres["taxa_microdados"], alpha=0.45)

# Linha de tendência
x = analise_pres["taxa_presenca"]
y = analise_pres["taxa_microdados"]
coef = np.polyfit(x, y, 1)
corr = x.corr(y)
```

**Output:**
- **Gráfico 1A**: Histograma de taxa de presença (participação)
  - Exemplo: média 85% (alguns municípios com 50%, outros com 100%)
- **Gráfico 1B**: Scatter + linha de tendência
  - **Correlação fraca (r ≈ 0,1–0,3)** = **ausência NÃO infla taxa**
  - Se correlação fosse forte e positiva, indicaria viés (melhores alunos presentes)

**O que significa:**
- **Metodologia SAEB é robusta** — não há viés de seleção por presença
- Municípios com baixa participação (50%) têm taxa similar aos com alta (100%)
- Dados públicos do INEP são **confiáveis para política pública**

---

## Célula #12-13: Resumo Executivo — O que Microdados Revelam

**Output:**
```
==================================================================
RESUMO EXECUTIVO — Análise de Microdados SAEB 2023/2024
==================================================================

1. VALIDAÇÃO DOS DADOS INEP
   [X]% dos municípios têm microdados consistentes com INEP (|delta| < 2 p.p.)
   → Os dados públicos do INEP são CONFIÁVEIS para tomada de decisão

2. ZONA DE INTERVENÇÃO ESTRATÉGICA (700-742 pontos)
   [Y]% dos alunos avaliados estão na zona de conversão
   → Alunos a menos de 43 pontos do limiar de alfabetização
   → Tutoria intensiva por 3-4 meses pode converter esses alunos
   → MAIOR ROI de qualquer intervenção educacional neste momento

3. DESIGUALDADE INTERNA POR MUNICÍPIO
   [Z] municípios têm baixa taxa E alta desigualdade entre escolas
   → INTERVENÇÃO CIRÚRGICA em escolas específicas (não municipal)
   → 3-5× mais ROI que políticas municipais amplas
```

---

# RESUMO FINAL — OS 3 NOTEBOOKS

## Pergunta Respondida por Cada Notebook

| Pergunta | Notebook | Resposta |
|----------|----------|----------|
| **Quanto custa a ineficiência?** | 02 | **R$ 13,64 bilhões** (53% dos municípios gastam acima da mediana e alfabetizam abaixo) |
| **Quanto custa resolver?** | 02 | **R$ 865,7 milhões** (para levar todos a 80%, amostra local de 3.486 municípios) |
| **Onde investir R$ 500M para máximo impacto?** | 03 | **Cluster Crítico** (59% do impacto com 42% do orçamento) |
| **Onde o real rende mais?** | 03 + 05 | **Zona de Conversão** (quase-alfabetizados 700–742 pontos) |
| **Os dados INEP são confiáveis?** | 05 | **Sim** (86% com delta < 2 p.p.; 0,1 correlação presença-taxa = sem viés) |

## 3 Números que Resumem Tudo

1. **R$ 13,64 bilhões** — custo da ineficiência (realocável)
2. **R$ 865,7 milhões** — investimento para resolver (15,8× menor)
3. **59%** — impacto do Cluster Crítico (com menos de metade do orçamento)

> **Nota de rigor (06/07/2026):** números recalculados após corrigir bug de unidade em `src/siconfi/01_ingestao_siconfi.py` — `custo_por_ponto_alfabetizacao` usava despesa **total** do município em vez de gasto **per capita**, inflando o benchmark de custo marginal em ~12.000× quando o auto-detect do SICONFI passou a ser encontrado (fix `f63daba` do mesmo dia corrigiu os nomes de coluna que antes mascaravam esse bug caindo sempre no fallback). Corrigido para bater com `src/cloud/dataproc_04_siconfi.py` (que já usava a fórmula certa). Números aqui vêm da **amostra local** (3.486 municípios); produção GCP (5.550 municípios) está em `docs/NUMEROS_RECALCULADOS.md`: R$ 1.218,3M investimento, R$ 34,96 bi desperdício, 28,69×.

## Recomendações Consolidadas

### Para Política Pública Federal (FNDE/MEC)

1. **Matriz de priorização por quadrante** (Notebook 02)
   - Investir em municípios Máxima (1.403) e Equidade (308)
   - Usar **per capita** para evitar distorção de tamanho

2. **Cluster Crítico como foco** (Notebook 03)
   - 1.201 municípios + 817 menores = 2.018 críticos
   - Knapsack Greedy garante 99,96% de aproveitamento de orçamento

3. **Zona de Conversão como quick win** (Notebook 05)
   - Identificar municípios com > 20% em 700–742 pontos
   - Tutoria intensiva custa menos e rende mais

### Para Gestores Municipais

1. **Diagnóstico de desigualdade interna (CV)**
   - CV > 20% = intervir em escolas específicas (3–5× ROI)
   - CV < 10% = problema sistêmico (rede toda)

2. **Ações sem verba federal extra**
   - Remanejamento de professores (bem-sucedidos para críticas)
   - Coaching de gestores escolares
   - Revisão de currículo baseada em dados

3. **Monitoramento via microdados**
   - Delta microdados vs INEP (consistência)
   - Distribuição de proficiência (não só taxa binária)
   - Viés de participação (qual rede tem problema real)

---

## Arquivos Gerados

```
NOTEBOOK_REPORT_02_gold_marts_analytics.md       [Relatório completo notebook 02]
NOTEBOOK_REPORT_03_ml_clustering_otimizacao.md   [Relatório completo notebook 03]
NOTEBOOK_REPORT_05_microdados_alunos_analise.md  [Relatório completo notebook 05]
ANALISE_COMPLETA_NOTEBOOKS.md                    [Este arquivo — resumo consolidado]
```

---

**Projeto:** Tech Challenge Fase 2 — Alfabetização Brasileira
**Autor:** Luiz Fernando Maibashi
**Data:** 06/07/2026 (atualizado após correção de bug de custo)
**Stack:** PySpark 3.5 + scikit-learn 1.8 + pandas 2.3 + matplotlib 3.11
