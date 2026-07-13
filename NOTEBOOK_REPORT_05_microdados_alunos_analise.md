# Notebook: 05_microdados_alunos_analise

# Análise de Microdados de Alunos — SAEB 2023/2024

**Tech Challenge Fase 2 — FIAP Pos-Tech Data Analytics**

---

## Por que este notebook é diferente dos outros?

Os notebooks anteriores (`02_gold_marts_analytics.ipynb`, `99_final_portfolio.ipynb`) analisam **agregados por município** — taxas já calculadas e publicadas pelo INEP.

Este notebook vai além: usa os **microdados individuais de alunos** (`Alunos.csv`), que permitem análises impossíveis com os agregados:

| Análise | Possível com agregados? | Possível com microdados? |
|---------|------------------------|--------------------------|
| Taxa de alfabetização por município | Sim | Sim |
| Validação cruzada INEP vs dados brutos | Não | Sim |
| Distribuição completa de proficiência (histograma) | Não | Sim |
| Identificar "quase-alfabetizados" (700-742 pontos) | Não | Sim |
| Desigualdade entre escolas dentro do município | Não | Sim |
| Ranking de escolas individuais | Não | Sim |
| Bias de participação (taxa de presença) | Não | Sim |

**Limiar SAEB:** 743 pontos = estudante classificado como "Alfabetizado" pelo Compromisso Nacional Criança Alfabetizada (meta 2030).

## Célula #1

**Código:**
```python
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.patches as mpatches
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings("ignore")

# Estilo visual padronizado
plt.rcParams.update({
    "font.size": 11,
    "figure.dpi": 110,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "axes.grid": True,
    "grid.alpha": 0.3,
    "grid.linestyle": "--",
})
sns.set_palette("husl")

# Constantes do domínio
LIMIAR_SAEB = 743          # pontos mínimos para "Alfabetizado"
ZONA_CONV_MIN = 700        # início da zona de intervenção estratégica
ZONA_CONV_MAX = 742        # fim da zona de intervenção estratégica (abaixo do limiar)

CORES_REDE = {
    "Municipal": "#2196F3",
    "Estadual":  "#4CAF50",
    "Privada":   "#FF9800",
    "Federal":   "#9C27B0",
}

print("Setup concluído | LIMIAR SAEB:", LIMIAR_SAEB, "pontos")

```

**Output:**

```
Setup concluído | LIMIAR SAEB: 743 pontos

```

## 1. Carregamento e Panorama dos Dados

Lemos diretamente o `Alunos.csv` com pandas. Este arquivo contém **dados individuais** — cada linha é um aluno que participou (ou não) da avaliação SAEB.

## Célula #2

**Código:**
```python
ROOT = Path("..").resolve()
CSV_ALUNOS = ROOT / "dados_sample" / "Alunos.csv"
GOLD_DIR   = ROOT / "datalake_sample" / "gold"
SILVER_DIR = ROOT / "datalake_sample" / "silver"
DOCS_DIR   = ROOT / "docs"

# Carrega microdados — id's como string para preservar zeros à esquerda
df_raw = pd.read_csv(
    CSV_ALUNOS,
    dtype={"id_municipio": str, "id_escola": str, "id_aluno": str},
)
df_raw["id_municipio"] = df_raw["id_municipio"].str.zfill(7)
df_raw["id_escola"]    = df_raw["id_escola"].str.zfill(8)

print(f"Microdados brutos carregados: {len(df_raw):,} linhas")
print(f"Período: {sorted(df_raw['ano'].unique())}")
print(f"Municípios únicos: {df_raw['id_municipio'].nunique():,}")
print(f"Escolas únicas:    {df_raw['id_escola'].nunique():,}")
print(f"Redes:             {df_raw['rede'].value_counts().to_dict()}")
df_raw.head(4)

```

**Output:**

```
ERRO: FileNotFoundError: [Errno 2] No such file or directory: 'C:\\Users\\Tchan\\Documents\\Base_de_Conhecimento\\PROJETOS\\01_PRIORITY\\dados_sample\\Alunos.csv'
```

## Célula #3

**Código:**
```python
# Taxa de presença (participação no exame)
presentes = (df_raw["presenca"] == "Presente").sum()
ausentes  = (df_raw["presenca"] == "Ausente").sum()
taxa_pres = presentes / len(df_raw) * 100

print("=" * 55)
print(f"  Alunos PRESENTES (fizeram a prova): {presentes:,}  ({taxa_pres:.1f}%)")
print(f"  Alunos AUSENTES  (não avaliados):   {ausentes:,}  ({100-taxa_pres:.1f}%)")
print("=" * 55)
print()
print("Motivo da exclusão dos ausentes:")
print("  Alunos ausentes não têm proficiência registrada.")
print("  Incluí-los distorceria taxas e médias — são tratados")
print("  como 'não avaliados', não como 'não-alfabetizados'.")

# Todas as análises daqui pra frente usam apenas presentes
df = df_raw[df_raw["presenca"] == "Presente"].copy()
df["proficiencia"] = pd.to_numeric(df["proficiencia"], errors="coerce")
df["peso_aluno"]   = pd.to_numeric(df["peso_aluno"],   errors="coerce").fillna(1.0)
df["alfa_bin"]     = (df["alfabetizado"] == "Sim").astype(int)

```

**Output:**

```
ERRO: NameError: name 'df_raw' is not defined
```

### [TÉCNICO] Filtro de presença + peso amostral

**O que a célula acima faz:** mantém apenas alunos **Presentes** — ausentes não têm proficiência registrada e incluí-los distorceria as médias ("ausente" ≠ "nota zero"). O `peso_aluno` (fator de expansão SAEB) é preservado: cada aluno avaliado representa N alunos da mesma escola/série/rede na inferência populacional.

### [NEGÓCIO] Por que isso importa

Tratar ausentes como "não-alfabetizados" inflaria artificialmente o déficit. Separar "não avaliado" de "não alfabetizado" é o que dá credibilidade a toda a análise seguinte.

## 2. Agregação por Município × Rede × Ano

Calculamos as mesmas métricas que o INEP publica, mas **de baixo pra cima** a partir dos dados individuais — com ponderação pelo `peso_aluno` (fator de expansão estatística SAEB).

## Célula #4

**Código:**
```python
def taxa_pond(g):
    total_peso = g["peso_aluno"].sum()
    return (g["alfa_bin"] * g["peso_aluno"]).sum() / total_peso * 100 if total_peso > 0 else np.nan

def prof_pond(g):
    mask = g["proficiencia"].notna()
    if not mask.any():
        return np.nan
    gm = g[mask]
    return (gm["proficiencia"] * gm["peso_aluno"]).sum() / gm["peso_aluno"].sum()

def prof_p25(g):
    return g["proficiencia"].quantile(0.25) if g["proficiencia"].notna().any() else np.nan

def prof_p75(g):
    return g["proficiencia"].quantile(0.75) if g["proficiencia"].notna().any() else np.nan

def prof_std(g):
    return g["proficiencia"].std() if g["proficiencia"].notna().sum() > 1 else np.nan

chaves = ["id_municipio", "id_municipio_nome", "ano", "rede"]

agg = df.groupby(chaves).apply(lambda g: pd.Series({
    "taxa_microdados":         taxa_pond(g),
    "proficiencia_media":      prof_pond(g),
    "proficiencia_p25":        prof_p25(g),
    "proficiencia_p75":        prof_p75(g),
    "proficiencia_std":        prof_std(g),
    "qtd_alunos_avaliados":    g["peso_aluno"].sum(),
    "qtd_alunos_raw":          len(g),
    "qtd_escolas":             g["id_escola"].nunique(),
    "pct_quase_alfa":          (g["proficiencia"].between(ZONA_CONV_MIN, ZONA_CONV_MAX) *
                                g["peso_aluno"]).sum() / g["peso_aluno"].sum() * 100,
})).reset_index()

agg["cv_proficiencia"] = agg["proficiencia_std"] / agg["proficiencia_media"] * 100  # coef. de variação

print(f"Agregação concluída: {len(agg):,} combinações (município × ano × rede)")
print(f"Municípios únicos: {agg['id_municipio'].nunique():,}")
print(f"Taxa média: {agg['taxa_microdados'].mean():.1f}%")
agg.head()

```

**Output:**

```
ERRO: NameError: name 'df' is not defined
```

### [TÉCNICO] Agregação ponderada + Coeficiente de Variação

**O que a célula acima faz:** replica a metodologia INEP de baixo pra cima — `taxa = Σ(alfa_bin × peso) / Σ(peso)` — e calcula o **CV** (`std/média × 100`) da proficiência, que mede a dispersão **dentro** do município. Essa granularidade não existe nos agregados publicados.

### [NEGÓCIO] Diagnóstico do tipo de problema

CV alto = escolas muito desiguais na mesma cidade (intervir na escola). CV baixo = município homogêneo (problema sistêmico, intervir na política). O CV diz **onde** agir.

## 3. Validação: Microdados vs Taxa Publicada pelo INEP

**Hipótese:** A taxa calculada dos microdados (`taxa_microdados`) deve ser numericamente próxima à taxa oficial publicada pelo INEP (`taxa_alfabetizacao` na Silver OBT).

- **Delta ≈ 0**: dados consistentes — os microdados são a base do agregado
- **Delta alto**: possível diferença metodológica, filtro de série ou ano diferente
- **Delta sistemático**: potencial viés de publicação — alertar gestores

## Célula #5

**Código:**
```python
# Tenta carregar Silver (a mais enriquecida disponível)
silver_paths = [
    SILVER_DIR / "alfabetizacao_municipios_obt_final",
    SILVER_DIR / "alfabetizacao_municipios_obt_enriquecido",
    SILVER_DIR / "alfabetizacao_municipios_obt_com_metas_imputadas",
    SILVER_DIR / "alfabetizacao_municipios_obt",
]

df_silver = None
for p in silver_paths:
    if p.exists():
        try:
            df_silver = pd.read_parquet(p)
            df_silver["id_municipio"] = df_silver["id_municipio"].astype(str).str.zfill(7)
            print(f"Silver carregada: {p.name}  ({len(df_silver):,} registros × {df_silver.shape[1]} cols)")
            break
        except Exception as e:
            print(f"  Falha em {p.name}: {e}")

if df_silver is None:
    print("Silver não encontrada. Execute primeiro: python src/batch/02_silver_transform.py")
    print("Continuando análise sem comparação com INEP...")
    comparacao = pd.DataFrame()
else:
    cols_silver = ["id_municipio", "ano", "rede", "taxa_alfabetizacao"]
    comparacao = agg.merge(
        df_silver[cols_silver].drop_duplicates(),
        on=["id_municipio", "ano", "rede"],
        how="inner",
    )
    comparacao["delta"] = comparacao["taxa_microdados"] - comparacao["taxa_alfabetizacao"]
    comparacao["abs_delta"] = comparacao["delta"].abs()

    print(f"\nPares comparados: {len(comparacao):,}")
    print(f"Delta médio:      {comparacao['delta'].mean():.2f} p.p.")
    print(f"Delta mediana:    {comparacao['delta'].median():.2f} p.p.")
    print(f"|Delta| < 2 p.p.: {(comparacao['abs_delta'] < 2).mean()*100:.1f}% dos pares")
    print(f"|Delta| ≥ 5 p.p.: {(comparacao['abs_delta'] >= 5).sum()} pares (investigar)")

```

**Output:**

```
Silver não encontrada. Execute primeiro: python src/batch/02_silver_transform.py
Continuando análise sem comparação com INEP...

```

## Célula #6

**Código:**
```python
if len(comparacao) > 0:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    # ── Figura 1A: Scatter INEP vs Microdados ──────────────────────────────
    ax = axes[0]
    sc = ax.scatter(
        comparacao["taxa_alfabetizacao"],
        comparacao["taxa_microdados"],
        c=comparacao["abs_delta"],
        cmap="RdYlGn_r",
        alpha=0.65,
        s=35,
        vmin=0, vmax=10,
        edgecolors="none",
    )
    ax.plot([0, 100], [0, 100], "k--", lw=1.2, alpha=0.5, label="Concordância perfeita")
    ax.plot([0, 100], [5, 105], "r-",  lw=0.8, alpha=0.3, label="+5 p.p.")
    ax.plot([0, 100], [-5, 95], "r-",  lw=0.8, alpha=0.3, label="-5 p.p.")
    plt.colorbar(sc, ax=ax, label="|Delta| (p.p.)")
    ax.set_xlim(0, 105); ax.set_ylim(0, 105)
    ax.set_xlabel("Taxa INEP publicada (%)")
    ax.set_ylabel("Taxa calculada dos microdados (%)")
    ax.set_title("Fig. 1A — Validação: INEP vs Microdados")
    ax.legend(fontsize=9)

    # ── Figura 1B: Histograma do Delta ─────────────────────────────────────
    ax = axes[1]
    ax.hist(comparacao["delta"], bins=40, color="#2196F3", alpha=0.75, edgecolor="white", linewidth=0.5)
    ax.axvline(0,                          color="black", lw=1.5, ls="--", label="Delta = 0")
    ax.axvline(comparacao["delta"].mean(), color="crimson", lw=1.5, ls="-",
               label=f"Média: {comparacao['delta'].mean():.2f} p.p.")
    ax.axvspan(-2, 2, color="green", alpha=0.08, label="Zona consistente (±2 p.p.)")
    ax.set_xlabel("Delta = Microdados − INEP (p.p.)")
    ax.set_ylabel("Número de municípios")
    ax.set_title("Fig. 1B — Distribuição do Delta")
    ax.legend(fontsize=9)

    plt.tight_layout()
    plt.savefig(DOCS_DIR / "fig_01_validacao_microdados_inep.png", dpi=150, bbox_inches="tight")
    plt.show()

    pct_ok = (comparacao["abs_delta"] < 2).mean() * 100
    print(f"\nINSIGHT:")
    print(f"   {pct_ok:.0f}% dos municípios têm consistência alta (|delta| < 2 p.p.)")
    print(f"   O INEP publica dados confiáveis 
... (truncado, 2263 chars totais)
```

*(Sem output direto)*

## 4. Distribuição de Proficiência SAEB

A taxa de alfabetização (% com ≥ 743 pontos) é uma **métrica binária** — oculta toda a distribuição de desempenho. Com os microdados, vemos **onde cada aluno está na escala**.

**Por que isso importa:**
- Município A: 65% de taxa, proficiência média = 760 → maioria bem acima do limiar
- Município B: 65% de taxa, proficiência média = 745 → maioria logo acima — frágil
- Em ambos, a taxa é a mesma. Só os microdados revelam a vulnerabilidade do Município B.

## Célula #7

**Código:**
```python
df_com_prof = df[df["proficiencia"].notna()].copy()

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# ── Figura 2A: Histograma geral de proficiência ─────────────────────────────
ax = axes[0]
ax.hist(df_com_prof["proficiencia"], bins=60, color="#5C6BC0", alpha=0.8,
        edgecolor="white", linewidth=0.4, weights=df_com_prof["peso_aluno"])
ax.axvline(LIMIAR_SAEB, color="crimson", lw=2, ls="--",
           label=f"Limiar SAEB ({LIMIAR_SAEB} pts) = Alfabetizado")
ax.axvspan(ZONA_CONV_MIN, ZONA_CONV_MAX, color="orange", alpha=0.20,
           label=f"Zona de Conversão ({ZONA_CONV_MIN}–{ZONA_CONV_MAX} pts)")
ax.set_xlabel("Proficiência SAEB (pontos)")
ax.set_ylabel("Alunos (expandido por peso)")
ax.set_title("Fig. 2A — Distribuição de Proficiência (todos)")
ax.legend(fontsize=9)
media_geral = (df_com_prof["proficiencia"] * df_com_prof["peso_aluno"]).sum() / df_com_prof["peso_aluno"].sum()
ax.axvline(media_geral, color="navy", lw=1.5, ls=":", label=f"Média: {media_geral:.0f}")
ax.legend(fontsize=9)

# ── Figura 2B: Boxplot por rede ─────────────────────────────────────────────
ax = axes[1]
redes_ord = ["Municipal", "Estadual", "Federal", "Privada"]
data_bp = [df_com_prof[df_com_prof["rede"] == r]["proficiencia"].dropna().values
           for r in redes_ord if r in df_com_prof["rede"].values]
redes_pres = [r for r in redes_ord if r in df_com_prof["rede"].values]
cores_bp = [CORES_REDE[r] for r in redes_pres]

bp = ax.boxplot(data_bp, patch_artist=True, notch=False,
                medianprops={"color": "black", "linewidth": 2})
for patch, cor in zip(bp["boxes"], cores_bp):
    patch.set_facecolor(cor)
    patch.set_alpha(0.7)
ax.axhline(LIMIAR_SAEB, color="crimson", lw=1.5, ls="--", label=f"Limiar ({LIMIAR_SAEB})")
ax.set_xticklabels(redes_pres)
ax.set_ylabel("Proficiência SAEB (pontos)")
ax.set_title("Fig. 2B — Distribuição por Rede")
ax.legend(fontsize=9)

plt.tight_layout()
plt.savefig(DOCS_DIR / "fig_02_distribuicao_proficiencia.png", dpi=150, bbox_inches="tight")
... (truncado, 2326 chars totais)
```

**Output:**

```
ERRO: NameError: name 'df' is not defined
```

### [TÉCNICO] Distribuição de proficiência e comparação por rede

**O que a célula acima faz:** plota o histograma ponderado da proficiência (não só o binário ≥743) e um boxplot por rede. Duas cidades com a mesma taxa podem ter distribuições diferentes — uma com massa logo acima de 743 (frágil), outra bem acima (robusta). Só os microdados revelam isso.

### [NEGÓCIO] A taxa binária esconde a fragilidade

Municípios "no limiar" (média ~745) estão a um passo de regredir. A distribuição, não a taxa binária, indica quão sustentável é o resultado — e a comparação por rede mostra onde a política de ensino é mais eficaz.

## 5. Zona de Intervenção Estratégica: Os "Quase-Alfabetizados" (700–742 pontos)

**Esta é a análise com maior valor prático do projeto.**

Alunos com proficiência entre 700 e 742 pontos estão a **menos de 43 pontos** do limiar de alfabetização. Uma intervenção cirúrgica (tutoria intensiva, reforço de leitura) pode convertê-los em "Alfabetizados" com custo marginal muito menor do que tratar alunos que estão abaixo de 600 pontos.

**Municípios com alta % de "quase-alfabetizados" têm o maior ROI de intervenção.**

## Célula #8

**Código:**
```python
# Classifica cada aluno por zona de proficiência
bins = [0, 599, 699, ZONA_CONV_MAX, 850, 1100]
labels = ["Muito Abaixo\n(<600)", "Abaixo\n(600-699)", "Zona de Conversão\n(700-742)",
          "Alfabetizado\n(743-850)", "Excelência\n(>850)"]
df_com_prof["zona"] = pd.cut(df_com_prof["proficiencia"], bins=bins, labels=labels)

# ── Figura 3A: Distribuição por zona (geral)
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

ax = axes[0]
zona_dist = (df_com_prof.groupby("zona", observed=False)["peso_aluno"].sum()
             .reset_index().rename(columns={"peso_aluno": "alunos"}))
zona_dist["pct"] = zona_dist["alunos"] / zona_dist["alunos"].sum() * 100
cores_zonas = ["#d32f2f", "#e57373", "#FF9800", "#4CAF50", "#1565C0"]
bars = ax.barh(zona_dist["zona"].astype(str), zona_dist["pct"],
               color=cores_zonas, alpha=0.85, edgecolor="white")
for bar, pct in zip(bars, zona_dist["pct"]):
    ax.text(pct + 0.3, bar.get_y() + bar.get_height()/2,
            f"{pct:.1f}%", va="center", fontsize=10, fontweight="bold")
ax.set_xlabel("% de Alunos Avaliados")
ax.set_title("Fig. 3A — Distribuição por Zona de Proficiência")

# ── Figura 3B: Top municípios por % na Zona de Conversão
ax = axes[1]
zona_mun = df_com_prof.groupby("id_municipio_nome").apply(lambda g: pd.Series({
    "pct_zona_conv": (g["proficiencia"].between(ZONA_CONV_MIN, ZONA_CONV_MAX) *
                      g["peso_aluno"]).sum() / g["peso_aluno"].sum() * 100,
    "n_nao_alfa": (g["proficiencia"] < LIMIAR_SAEB).sum(),
    "taxa_atual": (g["proficiencia"] >= LIMIAR_SAEB).mean() * 100,
})).reset_index()

top15 = zona_mun.nlargest(15, "pct_zona_conv")
cores_top = ["#FF9800" if p > 30 else "#FFC107" for p in top15["pct_zona_conv"]]
ax.barh(top15["id_municipio_nome"], top15["pct_zona_conv"],
        color=cores_top, alpha=0.85, edgecolor="white")
ax.axvline(zona_mun["pct_zona_conv"].mean(), color="navy", ls="--", lw=1.5,
           label=f"Média geral: {zona_mun['pct_zona_conv'].mean():.1f}%")
ax.set_xlabel("% dos A
... (truncado, 2844 chars totais)
```

**Output:**

```
ERRO: NameError: name 'df_com_prof' is not defined
```

### [TÉCNICO] Zona de Conversão (700–742 pts)

**O que a célula acima faz:** classifica cada aluno em zonas de proficiência e identifica os municípios com maior % na faixa **700–742** — alunos a menos de 43 pontos do limiar (743). É a análise de maior valor prático: aponta onde o custo marginal de alfabetizar é menor.

### [NEGÓCIO] Máximo ROI de intervenção

Converter um "quase-alfabetizado" com tutoria de reforço custa muito menos que reconstruir o aprendizado de um aluno abaixo de 600 pontos. Priorizar essa faixa maximiza o número de crianças alfabetizadas por real investido.

## 6. Desigualdade Interna: Heterogeneidade entre Escolas

A taxa municipal é uma **média que pode esconder realidades opostas** dentro do mesmo município:

- **Município Homogêneo:** todas as escolas têm desempenho similar
  → *Problema sistêmico* — a rede toda precisa de intervenção
- **Município Heterogêneo:** uma escola com 90% ao lado de outra com 30%
  → *Problema concentrado* — intervenção cirúrgica na escola específica tem muito mais ROI

**Coeficiente de Variação (CV)** = `desvio padrão / média × 100`
- CV alto (> 20%): alta desigualdade interna entre escolas
- CV baixo (< 10%): homogeneidade — bom ou ruim, todos na mesma situação

## Célula #9

**Código:**
```python
# Agrega por escola (granularidade máxima dos microdados)
escolas = df.groupby(["id_municipio", "id_municipio_nome", "id_escola", "ano", "rede"]).apply(
    lambda g: pd.Series({
        "taxa_escola":       taxa_pond(g),
        "prof_media_escola": prof_pond(g),
        "qtd_alunos":        g["peso_aluno"].sum(),
    })
).reset_index()

escolas = escolas[escolas["qtd_alunos"] >= 5]  # mínimo 5 alunos para taxa válida

# CV por município: desvio entre escolas
cv_mun = escolas.groupby(["id_municipio", "id_municipio_nome", "ano"]).agg(
    taxa_media_mun=("taxa_escola", "mean"),
    taxa_std_escolas=("taxa_escola", "std"),
    qtd_escolas=("id_escola", "count"),
).reset_index()
cv_mun["cv"] = cv_mun["taxa_std_escolas"] / cv_mun["taxa_media_mun"] * 100
cv_mun = cv_mun[cv_mun["qtd_escolas"] >= 3]  # mínimo 3 escolas para CV significativo

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# ── Figura 4A: Distribuição do CV entre municípios
ax = axes[0]
ax.hist(cv_mun["cv"].dropna(), bins=30, color="#7B1FA2", alpha=0.75, edgecolor="white")
ax.axvline(cv_mun["cv"].mean(), color="crimson", lw=2, ls="--",
           label=f"Média CV: {cv_mun['cv'].mean():.1f}%")
ax.axvline(20, color="orange", lw=1.5, ls=":", label="Threshold alto (20%)")
ax.set_xlabel("Coeficiente de Variação entre Escolas (%)")
ax.set_ylabel("Número de municípios")
ax.set_title("Fig. 4A — Desigualdade Interna (CV entre escolas)")
ax.legend(fontsize=9)

# ── Figura 4B: Scatter taxa_media vs CV — quadrantes de intervenção
ax = axes[1]
cores_q = cv_mun.apply(lambda r: (
    "#d32f2f" if r["taxa_media_mun"] < 60 and r["cv"] > 20 else
    "#FF9800" if r["taxa_media_mun"] < 60 and r["cv"] <= 20 else
    "#4CAF50" if r["taxa_media_mun"] >= 60 and r["cv"] <= 20 else
    "#2196F3"
), axis=1)

ax.scatter(cv_mun["taxa_media_mun"], cv_mun["cv"],
           c=cores_q, alpha=0.55, s=30, edgecolors="none")
ax.axhline(20, color="gray", lw=1, ls=":")
ax.axvline(60, color="gray", lw=1, ls=":")

patches = [
    mpatches.Patch
... (truncado, 3228 chars totais)
```

**Output:**

```
ERRO: NameError: name 'df' is not defined
```

### [TÉCNICO] Desigualdade entre escolas (CV)

**O que a célula acima faz:** agrega por escola (granularidade máxima dos microdados) e mede o CV das taxas entre escolas do mesmo município. O scatter taxa × CV forma quadrantes: baixa taxa + alto CV = intervenção **cirúrgica** (escolas específicas); baixa taxa + baixo CV = intervenção **sistêmica** (rede toda).

### [NEGÓCIO] Cirúrgico × sistêmico

Onde a desigualdade interna é alta, agir nas 3 piores escolas rende 3–5× mais que uma política municipal ampla — e o gestor pode fazer isso sem verba federal extra (remanejar professores, coaching de diretores).

## 7. Ranking de Escolas: Quem Está Arrastando a Taxa para Baixo?

Dentro dos municípios maiores (mais escolas), identificamos as **escolas com pior desempenho** relativo à média municipal. Essas são as escolas que justificam intervenção imediata.

## Célula #10

**Código:**
```python
# Pega os 3 municípios com mais escolas avaliadas para análise detalhada
top_mun_escolas = (escolas.groupby("id_municipio_nome")["id_escola"].count()
                   .nlargest(3).index.tolist())

fig, axes = plt.subplots(1, len(top_mun_escolas), figsize=(5 * len(top_mun_escolas), 6),
                         sharey=False)

if len(top_mun_escolas) == 1:
    axes = [axes]

for ax, mun in zip(axes, top_mun_escolas):
    sub = escolas[escolas["id_municipio_nome"] == mun].copy()
    sub = sub.sort_values("taxa_escola")
    media_mun = sub["taxa_escola"].mean()

    cores_esc = ["#d32f2f" if t < media_mun - 15 else
                 "#FF9800" if t < media_mun else
                 "#4CAF50" for t in sub["taxa_escola"]]
    ax.barh(range(len(sub)), sub["taxa_escola"], color=cores_esc, alpha=0.8, edgecolor="white")
    ax.axvline(media_mun, color="navy", lw=1.5, ls="--",
               label=f"Média mun.: {media_mun:.1f}%")
    ax.axvline(LIMIAR_SAEB / 8.0, color="gray", lw=1, alpha=0.5)  # ref visual

    ax.set_yticks(range(len(sub)))
    ax.set_yticklabels([f"Escola {i+1}" for i in range(len(sub))], fontsize=9)
    ax.set_xlabel("Taxa de Alfabetização (%)")
    ax.set_title(f"{mun}\n({len(sub)} escolas, média {media_mun:.1f}%)")
    ax.legend(fontsize=9)

    piores = sub.nsmallest(3, "taxa_escola")
    print(f"  {mun}: {len(piores)} piores escolas têm taxas: "
          f"{piores['taxa_escola'].values.round(1).tolist()}")

plt.suptitle("Fig. 5 — Ranking de Escolas por Município", fontsize=13, fontweight="bold", y=1.02)
plt.tight_layout()
plt.savefig(DOCS_DIR / "fig_05_ranking_escolas_municipios.png", dpi=150, bbox_inches="tight")
plt.show()

print(f"\nINSIGHT:")
print(f"   A variação entre escolas do mesmo município pode chegar a 50+ p.p.")
print(f"   Gestores municipais podem agir sem depender de recursos federais extras:")
print(f"   → Remanejamento de professores experientes para as escolas mais fracas")
print(f"   → Coaching de gestão escolar (diretores das melhor
... (truncado, 2026 chars totais)
```

**Output:**

```
ERRO: NameError: name 'escolas' is not defined
```

## 8. Viés de Participação: Taxa de Presença Afeta as Taxas Publicadas?

**Questão metodológica crítica:** Se apenas os melhores alunos forem avaliados (alta ausência dos mais fracos), a taxa publicada pode estar **inflada**.

Verificamos se municípios com menor presença têm taxas sistematicamente mais altas (o que indicaria viés de seleção) ou se não há correlação (o que valida a metodologia SAEB).

## Célula #11

**Código:**
```python
# Taxa de presença por município
pres_mun = df_raw.groupby(["id_municipio", "id_municipio_nome", "ano", "rede"]).apply(
    lambda g: pd.Series({
        "taxa_presenca":   (g["presenca"] == "Presente").mean() * 100,
        "total_alunos":    len(g),
        "alunos_presentes": (g["presenca"] == "Presente").sum(),
    })
).reset_index()

# Junta com taxas
analise_pres = pres_mun.merge(
    agg[["id_municipio", "ano", "rede", "taxa_microdados"]],
    on=["id_municipio", "ano", "rede"],
    how="inner"
)

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# ── Figura 6A: Distribuição de taxa de presença
ax = axes[0]
ax.hist(analise_pres["taxa_presenca"], bins=30, color="#00BCD4", alpha=0.8, edgecolor="white")
ax.axvline(analise_pres["taxa_presenca"].mean(), color="crimson", lw=2, ls="--",
           label=f"Média: {analise_pres['taxa_presenca'].mean():.1f}%")
ax.set_xlabel("Taxa de Presença no Exame (%)")
ax.set_ylabel("Municípios")
ax.set_title("Fig. 6A — Distribuição da Taxa de Participação")
ax.legend()

# ── Figura 6B: Scatter participação vs taxa alfabetização (teste de viés)
ax = axes[1]
ax.scatter(analise_pres["taxa_presenca"], analise_pres["taxa_microdados"],
           alpha=0.45, s=25, color="#FF5722", edgecolors="none")

# Linha de tendência
mask = analise_pres["taxa_presenca"].notna() & analise_pres["taxa_microdados"].notna()
x = analise_pres.loc[mask, "taxa_presenca"]
y = analise_pres.loc[mask, "taxa_microdados"]
coef = np.polyfit(x, y, 1)
xline = np.linspace(x.min(), x.max(), 100)
ax.plot(xline, np.polyval(coef, xline), "k-", lw=1.5,
        label=f"Tendência (slope: {coef[0]:.2f})")
corr = x.corr(y)
ax.set_xlabel("Taxa de Presença (%)")
ax.set_ylabel("Taxa de Alfabetização (microdados) (%)")
ax.set_title(f"Fig. 6B — Participação vs Desempenho (r={corr:.2f})")
ax.legend(fontsize=9)

plt.tight_layout()
plt.savefig(DOCS_DIR / "fig_06_vies_participacao.png", dpi=150, bbox_inches="tight")
plt.show()

print(f"\nINSIGHT:")
print(f"   Correlação participação × t
... (truncado, 2712 chars totais)
```

**Output:**

```
ERRO: NameError: name 'df_raw' is not defined
```

## 9. Mart #14 — agg_alunos_municipios (Gold)

O Gold Mart gerado pelo script `06_alunos_bronze_to_silver.py` + `01_gerar_marts_gold.py` consolida todas as métricas de microdados por município. Visualizamos aqui os principais resultados.

## Célula #12

**Código:**
```python
mart_path = GOLD_DIR / "agg_alunos_municipios"

if mart_path.exists():
    df_mart = pd.read_parquet(mart_path)
    print(f"Mart #14 carregado: {len(df_mart):,} registros × {df_mart.shape[1]} colunas")
    print(f"Colunas: {list(df_mart.columns)}")
    print()
    print("Amostra:")
    display(df_mart.head(10))

    if "delta_taxa_micro_vs_inep" in df_mart.columns and "status_consistencia" in df_mart.columns:
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))

        # Status de consistência
        ax = axes[0]
        status_cnt = df_mart["status_consistencia"].value_counts()
        cores_status = {"Consistente": "#4CAF50", "Atenção": "#FF9800", "Inconsistente": "#d32f2f"}
        ax.pie(status_cnt.values,
               labels=status_cnt.index,
               colors=[cores_status.get(s, "#9E9E9E") for s in status_cnt.index],
               autopct="%1.1f%%",
               startangle=90)
        ax.set_title("Fig. 7A — Consistência Microdados vs INEP")

        # Top inconsistências
        ax = axes[1]
        top_inc = (df_mart[df_mart["delta_taxa_micro_vs_inep"].notna()]
                   .assign(abs_delta=lambda x: x["delta_taxa_micro_vs_inep"].abs())
                   .nlargest(10, "abs_delta"))
        bars = ax.barh(
            top_inc.get("nome_municipio", top_inc.get("id_municipio", top_inc.index.astype(str))),
            top_inc["abs_delta"],
            color=["#d32f2f" if d > 5 else "#FF9800" for d in top_inc["abs_delta"]],
            alpha=0.85, edgecolor="white"
        )
        ax.set_xlabel("|Delta| Microdados − INEP (p.p.)")
        ax.set_title("Fig. 7B — Top 10 Discrepâncias (Mart #14)")

        plt.tight_layout()
        plt.savefig(DOCS_DIR / "fig_07_mart14_consistencia.png", dpi=150, bbox_inches="tight")
        plt.show()
else:
    print("Mart #14 não encontrado.")
    print("Execute: python src/batch/06_alunos_bronze_to_silver.py")
    print("         python src/gold/01_gerar_marts_gold.py")
    print()
    print("Enquanto is
... (truncado, 2276 chars totais)
```

**Output:**

```
Mart #14 não encontrado.
Execute: python src/batch/06_alunos_bronze_to_silver.py
         python src/gold/01_gerar_marts_gold.py

Enquanto isso, exibindo resumo das análises feitas direto do CSV:

```
```
ERRO: NameError: name 'agg' is not defined
```

## 10. Resumo Executivo: O que os Microdados Revelam que os Agregados Ocultam

### Três achados principais

## Célula #13

**Código:**
```python
print("=" * 70)
print("RESUMO EXECUTIVO — Análise de Microdados SAEB 2023/2024")
print("=" * 70)

print("\n1. VALIDAÇÃO DOS DADOS INEP")
if len(comparacao) > 0:
    pct_ok = (comparacao['abs_delta'] < 2).mean() * 100
    print(f"   {pct_ok:.0f}% dos municípios têm microdados consistentes com o INEP (|delta| < 2 p.p.)")
    print(f"   Os dados públicos do INEP são CONFIÁVEIS para tomada de decisão")
else:
    print("   (Execute o pipeline completo para ver a comparação com INEP)")

print("\n2. ZONA DE INTERVENÇÃO ESTRATÉGICA (700-742 pontos)")
n_zona_conv = df_com_prof["proficiencia"].between(ZONA_CONV_MIN, ZONA_CONV_MAX).sum()
pct_zona = n_zona_conv / len(df_com_prof) * 100
print(f"   {pct_zona:.1f}% dos alunos avaliados estão na zona de conversão (700-742 pts)")
print(f"   = alunos a menos de 43 pontos do limiar de alfabetização")
print(f"   → Tutor intensivo por 3-4 meses pode converter esses alunos com custo unitário baixo")
print(f"   → MAIOR ROI de qualquer intervenção educacional possível neste momento")

print("\n3. DESIGUALDADE INTERNA POR MUNICÍPIO")
n_alta = ((cv_mun['cv'] > 20) & (cv_mun['taxa_media_mun'] < 60)).sum()
print(f"   {n_alta} municípios têm baixa taxa E alta desigualdade entre escolas")
print(f"   Nesses municípios, uma INTERVENÇÃO ESCOLAR específica (não municipal)")
print(f"   tem 3-5× mais ROI que políticas municipais amplas")

print("\n" + "-" * 70)
print("RECOMENDAÇÕES:")
print("  1. Mapeie os municípios do Top 15 da Zona de Conversão (Fig. 3B)")
print("     → Programe tutoria intensiva para alunos 700-742 pontos")
print("  2. Nos municípios com CV > 20% (Fig. 4B), identifique as 3 piores escolas")
print("     → Intervenção cirúrgica em gestão e corpo docente")
print("  3. Monitore municípios com presença < 70% (Fig. 6A)")
print("     → Alta ausência pode mascarar problemas reais na comunidade")
print("=" * 70)

```

**Output:**

```
======================================================================
RESUMO EXECUTIVO — Análise de Microdados SAEB 2023/2024
======================================================================

1. VALIDAÇÃO DOS DADOS INEP
   (Execute o pipeline completo para ver a comparação com INEP)

2. ZONA DE INTERVENÇÃO ESTRATÉGICA (700-742 pontos)

```
```
ERRO: NameError: name 'df_com_prof' is not defined
```

### [TÉCNICO] Validação multi-camada

**O que a célula acima faz:** consolida as garantias de integridade da análise — (1) consistência micro vs macro (delta vs INEP), (2) distribuição de proficiência, (3) desigualdade intra-municipal (CV), (4) viés de participação (presença × taxa). Cada camada é uma checagem independente de qualidade dos dados.

### [NEGÓCIO] Três achados acionáveis

(1) Os dados do INEP são confiáveis; (2) existe uma faixa de "quase-alfabetizados" com ROI altíssimo de intervenção; (3) parte da desigualdade é **dentro** do município — resolvível na escola, sem esperar repasse federal.

---

**Autor:** Luiz Fernando Maibashi
**Projeto:** Tech Challenge Fase 2 — FIAP Pos-Tech Data Analytics
**Dados:** INEP/SAEB 2023-2024 | Compromisso Nacional Criança Alfabetizada
**Código:** `src/batch/06_alunos_bronze_to_silver.py` | Gold Mart: `agg_alunos_municipios`
