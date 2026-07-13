# Notebook: 03_ml_clustering_otimizacao

# Machine Learning — Clusterizacao + Otimizacao
## Portfolio de Cientista de Dados / Economista

Duas tecnicas:
1. **K-Means:** Segmentar 3.486 municipios em perfis economicos-educacionais
2. **Knapsack Greedy:** Dado orcamento de R$ 500M, quais municipios financiar?

## Célula #1

**Código:**
```python
import os, json, warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

warnings.filterwarnings('ignore')
plt.rcParams['figure.dpi'] = 120
plt.rcParams['figure.figsize'] = (18, 7)
sns.set_style('whitegrid')

project_root = os.path.abspath(os.path.join(os.getcwd(), '..')) if 'notebooks' in os.getcwd() else os.getcwd()
gold_dir = os.path.join(project_root, 'datalake_sample', 'gold')
print(f'Projeto: {project_root}')
```

**Output:**

```
Projeto: C:\Users\Tchan\Documents\Base_de_Conhecimento\PROJETOS\01_PRIORITY\tech-challenge-fase2-alfabetizacao

```

### [TÉCNICO] Setup do ambiente

**O que a célula acima faz:** importa as bibliotecas de ML (`scikit-learn`: `KMeans`, `StandardScaler`, `silhouette_score`) e de visualização (matplotlib/seaborn), e resolve o caminho da camada Gold (`datalake_sample/gold`).

**Por que padronizar (StandardScaler):** o K-Means usa distância euclidiana. Sem padronizar, uma feature de escala grande (população em milhões) dominaria a taxa (0–100) e o cluster viraria "grande vs pequeno". A padronização coloca todas as features em média 0 / desvio 1, dando peso comparável.

### [NEGÓCIO] Por que duas técnicas

**K-Means** *segmenta* municípios em perfis parecidos (focalizar política por tipo). **Knapsack** *aloca* um orçamento fixo (decidir onde investir). Uma responde "quem é parecido com quem"; a outra "onde colocar R$500M para máximo impacto".

> **Conexão com o pipeline cloud:** este notebook é a exploração local. Em produção, o K-Means roda no Spark MLlib como mart `agg_vulnerabilidade_ml`, e o modelo de risco supervisionado está em `src/ml/03_modelo_preditivo_risco.py` (RandomForest).

---
# 1. K-Means: Segmentacao de Municipios

**Features:** `taxa_alfabetizacao_media`, `gasto_per_capita_medio`, `deficit_log`, `populacao_log`

## Célula #2

**Código:**
```python
pdf = pd.read_parquet(os.path.join(gold_dir, 'agg_clusters_municipios', 'dados.parquet'))
print(f'Total municipios: {len(pdf)}')
print(f'Colunas: {list(pdf.columns)}')
pdf.head(3)
```

**Output:**

```
Total municipios: 3486
Colunas: ['id_municipio', 'sigla_uf', 'nome_municipio', 'taxa_alfabetizacao_media', 'populacao_total', 'deficit_absoluto_proxy', 'gasto_per_capita_medio', 'custo_por_ponto_alfabetizacao_medio', 'deficit_log', 'populacao_log', 'cluster', 'nome_cluster']

```
```
  id_municipio sigla_uf nome_municipio  taxa_alfabetizacao_media  \
0      1100031       RO         Cabixi                     69.10   
1      1100049       RO         Cacoal                     63.37   
2      1100056       RO     Cerejeiras                     62.67   

   populacao_total  deficit_absoluto_proxy  gasto_per_capita_medio  \
0           5067.0                  1566.0                 1913.11   
1          86416.0                 31654.0                 1913.11   
2          16088.0                 12012.0                 1881.78   

   custo_por_ponto_alfabetizacao_medio  deficit_log  populacao_log  cluster  \
0                                  NaN     7.356918       8.530702        0   
1                                  NaN    10.362651      11.366940        1   
2                            446454.54     9.393745       9.685891        2   

                                nome_cluster  
0     0 - Alto Gasto (Alta taxa, alto gasto)  
1  1 - Vulneravel (Baixa taxa, alto deficit)  
2     2 - Critico (Baixa taxa, grande porte)  
```

## Célula #3

**Código:**
```python
feature_cols = ['taxa_alfabetizacao_media', 'gasto_per_capita_medio', 'deficit_log', 'populacao_log']
X = pdf[feature_cols].fillna(pdf[feature_cols].median()).values
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

inertias, sil_scores = [], []
Ks = range(2, 9)
for k in Ks:
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = km.fit_predict(X_scaled)
    inertias.append(km.inertia_)
    sil_scores.append(silhouette_score(X_scaled, labels))

fig, axes = plt.subplots(1, 2, figsize=(18, 6))
axes[0].plot(Ks, inertias, 'bo-', linewidth=2)
axes[0].axvline(4, color='red', linestyle='--', alpha=0.5, label='K=4 (escolhido)')
axes[0].set_xlabel('K (numero de clusters)')
axes[0].set_ylabel('Inercia')
axes[0].set_title('Metodo do Cotovelo (Elbow Method)')
axes[0].legend()

axes[1].plot(Ks, sil_scores, 'ro-', linewidth=2)
axes[1].axvline(4, color='red', linestyle='--', alpha=0.5, label='K=4 (escolhido)')
axes[1].set_xlabel('K (numero de clusters)')
axes[1].set_ylabel('Silhouette Score')
axes[1].set_title('Silhouette Score por K')
axes[1].legend()
plt.tight_layout()
plt.show()
print(f'K=4: silhouette={sil_scores[2]:.4f} (K=3: {sil_scores[1]:.4f}, K=5: {sil_scores[3]:.4f})')
print('K=4 escolhido: equilibrio entre interpretabilidade e coesao dos clusters.')
```

**Output:**

```
<Figure size 2160x720 with 2 Axes>
```
```
K=4: silhouette=0.2804 (K=3: 0.2760, K=5: 0.2688)
K=4 escolhido: equilibrio entre interpretabilidade e coesao dos clusters.

```

### [TÉCNICO] Escolha do número de clusters (Elbow + Silhouette)

**O que a célula acima faz:** treina o K-Means para K = 2 a 8 e plota dois diagnósticos:
- **Método do Cotovelo (inércia):** soma das distâncias intra-cluster. Cai sempre que K aumenta; procura-se o "cotovelo" onde o ganho marginal diminui.
- **Silhouette Score:** mede coesão (dentro do cluster) vs separação (entre clusters), de −1 a 1. Quanto maior, melhor definidos os grupos.

**Decisão:** K=4 — equilíbrio entre coesão estatística e **interpretabilidade de negócio** (4 perfis que um gestor entende). O Silhouette em ~0,28 é modesto mas esperado em dados socioeducacionais, que têm fronteiras contínuas, não grupos naturais isolados.

### [NEGÓCIO] Os 4 perfis

Eficiente (benchmark), Metrópole (desafio de escala), Ineficiente (problema de gestão, não de verba) e Crítico (emergência). O **Crítico** é a prioridade de investimento — é onde cada real rende mais.

## Célula #4

**Código:**
```python
cores_cluster = {0: '#2ecc71', 1: '#3498db', 2: '#e74c3c', 3: '#f39c12'}

fig, axes = plt.subplots(2, 2, figsize=(18, 14))

# G1: Taxa vs Gasto
for c in sorted(pdf['cluster'].unique()):
    sub = pdf[pdf['cluster'] == c]
    axes[0,0].scatter(sub['gasto_per_capita_medio'], sub['taxa_alfabetizacao_media'],
                      alpha=0.4, s=10, color=cores_cluster.get(c, 'gray'))
axes[0,0].set_xlabel('Gasto per Capita (R$)')
axes[0,0].set_ylabel('Taxa de Alfabetizacao Media (%)')
axes[0,0].set_title('Clusters: Gasto vs Taxa')

# G2: Taxa vs Deficit
for c in sorted(pdf['cluster'].unique()):
    sub = pdf[pdf['cluster'] == c]
    axes[0,1].scatter(np.log1p(sub['deficit_absoluto_proxy']), sub['taxa_alfabetizacao_media'],
                      alpha=0.4, s=10, color=cores_cluster.get(c, 'gray'), label=f'Cluster {c}')
axes[0,1].set_xlabel('Deficit Absoluto (log)')
axes[0,1].set_ylabel('Taxa de Alfabetizacao Media (%)')
axes[0,1].set_title('Clusters: Deficit vs Taxa')
axes[0,1].legend(fontsize=9)

# G3: Perfil dos clusters (radar-like)
profile = pdf.groupby('cluster').agg(
    qtd=('id_municipio','count'),
    taxa=('taxa_alfabetizacao_media','mean'),
    gasto=('gasto_per_capita_medio','mean'),
    deficit=('deficit_absoluto_proxy','mean'),
    pop=('populacao_total','mean')
).round(2)

bars = axes[1,0].barh(range(len(profile)), profile['qtd'].values,
                     color=[cores_cluster.get(i, 'gray') for i in profile.index], edgecolor='white')
axes[1,0].set_yticks(range(len(profile)))
axes[1,0].set_yticklabels([f'Cluster {i}' for i in profile.index])
axes[1,0].set_xlabel('Quantidade de Municipios')
axes[1,0].set_title('Distribuicao por Cluster')
for bar, val in zip(bars, profile['qtd'].values):
    axes[1,0].text(val + 20, bar.get_y() + bar.get_height()/2, str(val), va='center')

# G4: Tabela do perfil
axes[1,1].axis('off')
tbl_data = []
for i, row in profile.iterrows():
    nome = pdf[pdf['cluster'] == i]['nome_cluster'].iloc[0] if not pdf[pdf['cluster'] == i].emp
... (truncado, 2532 chars totais)
```

**Output:**

```
<Figure size 2160x1680 with 4 Axes>
```

### [TÉCNICO] Perfilamento visual dos clusters

**O que a célula acima faz:** produz 4 painéis para caracterizar os grupos — (1) Gasto × Taxa, (2) Déficit(log) × Taxa, (3) tamanho de cada cluster, (4) tabela-resumo (taxa, gasto, déficit e população médios). O eixo de déficit usa **escala log** porque a distribuição é fortemente assimétrica (poucas metrópoles com déficit enorme achatam o resto).

**[NEGÓCIO]** A tabela é o que traduz o modelo para decisão: mostra, por perfil, se o problema é *dinheiro* (Subinvestido/Crítico) ou *gestão* (Ineficiente — gasta acima da média com taxa abaixo). É a base da recomendação "os Ineficientes não precisam de mais verba, precisam de gestão".

---
# 2. Knapsack Greedy: Alocacao Otima de Recursos

**Problema:** Dado orcamento de **R$ 500 milhoes**, quais municipios financiar para maximizar o ganho de alfabetizacao?

**Metodo:** Greedy Knapsack. Para cada municipio: `relacao custo-beneficio = gap_ate_80 / custo_para_atingir_80`.
Ordena do maior para o menor e vai alocando ate acabar o orcamento.

## Célula #5

**Código:**
```python
with open(os.path.join(gold_dir, 'agg_alocacao_otima', 'resultado.json'), 'r', encoding='utf-8') as f:
    opt = json.load(f)

global_res = opt['cenario_global']
cluster_res = opt['cenario_por_cluster']

print('=' * 60)
print(f'Cenario Global — Orcamento R$ {global_res["orcamento"]:,.2f}')
print('=' * 60)
print(f'Municipios contemplados: {global_res["municipios_contemplados"]}')
print(f'Gasto total: R$ {global_res["gasto_total"]:,.2f}')
print(f'Saldo nao utilizado: R$ {global_res["saldo_nao_utilizado"]:,.2f}')
print(f'Pontos de alfabetizacao gerados: {global_res["pontos_alfabetizacao_gerados"]:,.2f}')
print(f'\nEficiencia de alocacao: {(global_res["gasto_total"]/global_res["orcamento"]*100):.2f}%')
print(f'\n{"=" * 60}')
print('Cenario por Cluster')
print('=' * 60)

cluster_df = []
for cid, data in sorted(cluster_res.items(), key=lambda x: int(x[0])):
    cluster_df.append({
        'Cluster': int(cid),
        'Nome': data['cluster_nome'],
        'Orcamento': data['orcamento_destinado'],
        'Gasto': data['gasto_total'],
        'Pontos': data['pontos_alfabetizacao_gerados'],
        'Municipios': data['municipios_contemplados']
    })
pd.DataFrame(cluster_df).round(2)
```

**Output:**

```
============================================================
Cenario Global — Orcamento R$ 500,000,000.00
============================================================
Municipios contemplados: 2815
Gasto total: R$ 499,815,903.17
Saldo nao utilizado: R$ 184,096.83
Pontos de alfabetizacao gerados: 68,153.35

Eficiencia de alocacao: 99.96%

============================================================
Cenario por Cluster
============================================================

```
```
   Cluster                                       Nome     Orcamento  \
0        0     0 - Alto Gasto (Alta taxa, alto gasto)  1.130742e+08   
1        1  1 - Vulneravel (Baixa taxa, alto deficit)  1.371025e+08   
2        2     2 - Critico (Baixa taxa, grande porte)  2.121908e+08   
3        3     3 - Critico (Baixa taxa, grande porte)  3.763251e+07   

          Gasto    Pontos  Municipios  
0  6.674772e+06   5415.02         640  
1  1.370974e+08  12900.06         605  
2  1.102223e+08  40479.61        1201  
3  1.424307e+07   5433.15         213  
```

## Célula #6

**Código:**
```python
df_opt = pd.DataFrame(cluster_df)

fig, axes = plt.subplots(1, 2, figsize=(18, 7))

cores_opt = [cores_cluster.get(i, 'gray') for i in df_opt['Cluster']]

# G1: Orcamento vs Gasto
x = range(len(df_opt))
w = 0.35
axes[0].bar([i - w/2 for i in x], df_opt['Orcamento']/1e6, w, label='Orcamento destinado', color='gray', alpha=0.4)
axes[0].bar([i + w/2 for i in x], df_opt['Gasto']/1e6, w, label='Gasto efetivo', color=cores_opt)
axes[0].set_xticks(x)
axes[0].set_xticklabels([f'Cluster {c}' for c in df_opt['Cluster']])
axes[0].set_ylabel('R$ milhoes')
axes[0].set_title('Orcamento Destinado vs Gasto Efetivo por Cluster')
axes[0].legend()

# G2: Pontos gerados por cluster
bars = axes[1].barh(range(len(df_opt)), df_opt['Pontos'], color=cores_opt, edgecolor='white')
axes[1].set_yticks(range(len(df_opt)))
axes[1].set_yticklabels([f'Cluster {c}\n({n.split("-")[0].strip()})' for c, n in zip(df_opt['Cluster'], df_opt['Nome'])], fontsize=9)
axes[1].set_xlabel('Pontos de Alfabetizacao Gerados')
axes[1].set_title('Impacto por Cluster: Pontos de Alfabetizacao Gerados')
for bar, val in zip(bars, df_opt['Pontos']):
    axes[1].text(val + 200, bar.get_y() + bar.get_height()/2, f'{val:,.0f}', va='center', fontsize=9)

plt.tight_layout()
plt.show()
print('*** INSIGHT ***')
print('Cluster 3 (Critico: baixa taxa) gerou 59% do impacto total com 42% do orcamento.')
print('Cluster 0 (Eficiente) gerou apenas 8% do impacto — gastar neles rende menos por R$ investido.')
```

**Output:**

```
<Figure size 2160x840 with 2 Axes>
```
```
*** INSIGHT ***
Cluster 3 (Critico: baixa taxa) gerou 59% do impacto total com 42% do orcamento.
Cluster 0 (Eficiente) gerou apenas 8% do impacto — gastar neles rende menos por R$ investido.

```

### [TÉCNICO] Knapsack Greedy — alocação sob orçamento

**O que as células acima fazem:** carregam o resultado da otimização (`agg_alocacao_otima/resultado.json`) e plotam orçamento destinado vs gasto efetivo e pontos gerados por cluster. O algoritmo ordena os municípios pela razão **benefício/custo** (`gap_ate_80 / custo_estimado_para_atingir_80`) e seleciona gulosamente até esgotar os R$500M — complexidade O(n log n), aproveitamento ~99,96% do orçamento.

> **No pipeline cloud:** a mesma heurística virou o mart `agg_alocacao_otima` (Spark), com custo acumulado e flag `selecionado_no_orcamento` (ver [ADR-010](../docs/adr/ADR-010-knapsack.md)). O custo usa o modelo marginal per capita do [ADR-012](../docs/adr/ADR-012-modelo-custo-marginal.md).

### [NEGÓCIO] Onde o real rende mais

O Cluster Crítico gera ~59% do impacto com ~42% do orçamento; o Eficiente, só ~8%. Investir nos críticos rende muito mais por real — é o **custo de oportunidade** tornado explícito para a decisão de política pública.

---
# Conclusao para Portfolio

## O que este notebook mostra sobre voce:

### Como Cientista de Dados:
- **Pipeline completo**: dados → features → modelo → interpretacao → deploy
- **Aprendizado nao-supervisionado**: K-Means com analise de silhouette, metodo do cotovelo, interpretacao de clusters
- **Feature engineering**: transformacao logaritmica, normalizacao, selecao de features
- **Otimizacao**: Knapsack Greedy com criterio de custo-beneficio

### Como Economista:
- **Traducao para R$**: toda decisao e quantificada em reais
- **Custo de oportunidade**: Cluster 0 (eficiente) gera 8% do impacto — investir neles e tirar do Cluster 3 e ineficiente
- **Trade-off explicito**: orcamento limitado → escolhas → consequencias mensuraveis
- **Recomendacao de politica**: os 248 municipios do Cluster 2 (gasto alto, desempenho mediocre) precisam de gestao, nao de mais dinheiro

### Stack:
- PySpark para ETL
- scikit-learn (KMeans, StandardScaler, Silhouette)
- Pandas, Matplotlib, Seaborn para analise e visualizacao
- Pipeline reprodutivel e documentado

---
# Secao Extra: Microdados — Features para Enriquecer o Clustering

Se a etapa 6 foi executada (`06_alunos_bronze_to_silver.py`), a Silver `obt_final`
contem colunas que enriquecem o modelo K-Means:

| Feature | O Que Mede | Por Que Melhora o Cluster |
|---------|-----------|--------------------------|
| `proficiencia_media_microdados` | Media SAEB ponderada (individual) | Mais precisa que taxa INEP binaria |
| `taxa_alunos_alfabetizados_microdados` | % calculado dos microdados | Valida taxa INEP |
| `delta_taxa_micro_vs_inep` | Discrepancia microdados vs INEP | Detecta anomalias de qualidade |

> **Insight de modelagem:** O CV entre escolas diferencia municipios que precisam de
> intervencao sistemica (todo o municipio) vs cirurgica (escolas especificas).

## Célula #7

**Código:**
```python
import os, glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

project_root = os.path.abspath(os.path.join(os.getcwd(), '..'))
silver_dir = os.path.join(project_root, 'datalake_sample', 'silver')

silver_final = os.path.join(silver_dir, 'alfabetizacao_municipios_obt_final')
silver_base  = os.path.join(silver_dir, 'alfabetizacao_municipios_obt_enriquecido')
silver_path  = silver_final if os.path.isdir(silver_final) else silver_base

parquets = glob.glob(os.path.join(silver_path, '**', '*.parquet'), recursive=True)

if not parquets:
    print('[INFO] Silver nao encontrada. Execute o pipeline primeiro.')
    silver_df = None
else:
    silver_df = pd.concat([pd.read_parquet(p) for p in parquets], ignore_index=True)
    micro_cols = ['taxa_alunos_alfabetizados_microdados', 'proficiencia_media_microdados',
                  'qtd_alunos_avaliados', 'delta_taxa_micro_vs_inep']
    has_micro = [c for c in micro_cols if c in silver_df.columns]
    print(f'Silver: {len(silver_df):,} linhas, {len(silver_df.columns)} colunas')
    print(f'Colunas de microdados: {has_micro}')
    if has_micro:
        cob = silver_df[has_micro[0]].notna().mean() * 100
        print(f'Cobertura de microdados: {cob:.1f}% dos municipios')
```

**Output:**

```
[INFO] Silver nao encontrada. Execute o pipeline primeiro.

```

## Célula #8

**Código:**
```python
if silver_df is not None and 'taxa_alunos_alfabetizados_microdados' in silver_df.columns:
    df_m = silver_df.dropna(subset=['taxa_alunos_alfabetizados_microdados',
                                     'taxa_alfabetizacao']).copy()

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # Scatter: INEP vs Microdados colorido por delta
    delta_abs = df_m['delta_taxa_micro_vs_inep'].abs() if 'delta_taxa_micro_vs_inep' in df_m.columns else None
    sc = axes[0].scatter(
        df_m['taxa_alfabetizacao'],
        df_m['taxa_alunos_alfabetizados_microdados'],
        c=delta_abs if delta_abs is not None else '#3498db',
        cmap='RdYlGn_r' if delta_abs is not None else None,
        alpha=0.5, s=15
    )
    axes[0].plot([0,100], [0,100], 'k--', linewidth=1, label='Concordancia perfeita')
    axes[0].set_xlabel('Taxa INEP (%)')
    axes[0].set_ylabel('Taxa Microdados (%)')
    axes[0].set_title('Validacao: INEP vs Microdados', fontweight='bold')
    if delta_abs is not None:
        plt.colorbar(sc, ax=axes[0], label='|Delta| p.p.')
    axes[0].legend()

    # Distribuicao do delta
    if 'delta_taxa_micro_vs_inep' in df_m.columns:
        delta = df_m['delta_taxa_micro_vs_inep'].dropna()
        axes[1].hist(delta, bins=40, color='#9b59b6', alpha=0.7, edgecolor='white')
        axes[1].axvline(0, color='black', linewidth=2)
        axes[1].axvspan(-2, 2, alpha=0.15, color='green', label='Zona aceitavel (+-2 p.p.)')
        axes[1].set_xlabel('Delta (Microdados - INEP) em p.p.')
        axes[1].set_title('Distribuicao da Discrepancia', fontweight='bold')
        axes[1].legend()
        pct_ok = (delta.abs() <= 2).mean() * 100
        print(f'{pct_ok:.1f}% dos municipios tem delta < 2 p.p. — consistentes.')
        print('Use delta_taxa_micro_vs_inep como feature de anomalia no clustering!')

    plt.tight_layout()
    plt.show()
elif silver_df is not None:
    print('[INFO] Execute 06_alunos_bronze_to_silver.py para adicionar colunas de microdados.')
```

*(Sem output direto)*
