# ADR-012: Modelo de Custo Marginal Per Capita para Projeção de Investimento

- **Status:** ACCEPTED | **Data:** 2026-07-01 | **Supersede:** parcialmente ADR-010 (parâmetro de custo)

---

## 1. CONTEXTO

Os marts financeiros (`agg_projecao_investimento`, `agg_alocacao_otima`,
`agg_alocacao_otima_estrategias`, `agg_roi_executivo`) dependem de um **custo
estimado para elevar um município à meta de 80%** de alfabetização. Esse custo
alimenta rankings de priorização e o knapsack de alocação orçamentária.

### Problema identificado (auditoria 2026-07-01)

O modelo anterior era **incoerente e economicamente inválido**:

1. **`custo_por_ponto_alfabetizacao = despesa_educacao_total / (taxa + 1)`** —
   dividia o **orçamento total** de educação pela taxa. Como a despesa total
   escala com a população, a métrica fazia **cidades grandes parecerem "caras
   por ponto" apenas por serem grandes**. Reintroduzia exatamente a distorção de
   escala populacional que o projeto já havia combatido nos rankings (problema SP).

2. **Explosão de escala com SICONFI.** O `custo_estimado_para_atingir_80` usava
   a fórmula `gap × custo_ponto × (pop/1000)`, com `custo_ponto` = 200 (default).
   Quando havia SICONFI, `custo_ponto` era **sobrescrito** pela mediana de
   `despesa_total/(taxa+1)` (~centenas de milhares). Plugado na mesma fórmula,
   **inflava as estimativas em ~1000×**, tornando-as sem sentido — justamente no
   cenário que roda em produção (com SICONFI).

3. **Três benchmarks conflitantes** no projeto: `200` (código), `R$2.000/aluno`
   (data_dictionary) e `~R$500k` (override SICONFI). Nenhum reconciliava.

---

## 2. DECISÃO

Adotar um **modelo de custo marginal PER CAPITA**, dimensionalmente coerente e
livre de distorção de escala.

### Definição

```
custo_estimado_para_atingir_80 (R$) =
    gap_ate_80 (pontos percentuais)
  × custo_marginal_per_capita (R$ por habitante por ponto percentual)
  × populacao_total (habitantes)
```

- **Unidades:** `pp × (R$/hab/pp) × hab = R$` (dimensionalmente correto).

### Benchmark do custo marginal (`custo_marginal_per_capita`)

- **Com SICONFI:** mediana, entre os municípios **eficientes** (alta taxa + baixo
  gasto per capita), de
  `custo_por_ponto_alfabetizacao = gasto_por_habitante_educacao / (taxa + 1)`.
  Note que a base agora é o **gasto por habitante** (per capita), não a despesa
  total → métrica livre de escala.
- **Sem SICONFI:** constante documentada
  `CUSTO_PONTO_PER_CAPITA_DEFAULT = 20.0` R$/hab/ponto.

### Calibração e interpretação

Custo por aluno a alfabetizar é **invariante** a gap e população:

```
custo_por_aluno = custo / alunos_no_deficit
               = (gap × c_pc × pop) / ((gap/100) × pop)
               = c_pc × 100
```

Com `c_pc = 20` R$/hab/pp → **R$ 2.000 por aluno**, alinhando com o benchmark de
R$2.000/aluno já citado no `DICIONARIO_DADOS.md` (ordem de grandeza compatível com
custo aluno-ano da educação básica pública brasileira / FUNDEB).

---

## 3. IMPLEMENTAÇÃO

| Arquivo | Mudança |
|---|---|
| `src/cloud/dataproc_04_siconfi.py` | `custo_por_ponto_alfabetizacao` passa a usar `gasto_por_habitante_educacao` (per capita) em vez de `despesa_educacao` (total) |
| `src/cloud/dataproc_03_gold.py` | Constante `CUSTO_PONTO_PER_CAPITA_DEFAULT = 20.0`; `build_mart_projecao_investimento` usa fórmula per capita; benchmark derivado (per capita) sem explosão; coluna `benchmark_custo_ponto_per_capita` |
| `src/cloud/dataproc_03_gold.py` | `build_mart_alocacao_otima` usa o mesmo custo per capita |

---

## 4. CONSEQUÊNCIAS

**Vantagens:**
- Elimina a distorção de escala populacional (per capita).
- Dimensionalmente correto; interpretável como R$/aluno.
- Reconcilia os três benchmarks divergentes num único parâmetro documentado.
- Benchmark empírico (SICONFI) e default (constante) agora na mesma ordem de grandeza.

**Limitação:**
- `custo_por_ponto` per capita é uma **proxy de custo médio** usada como
  aproximação do custo marginal (não é custo marginal estimado econometricamente).
  Suficiente para priorização relativa; não é peça orçamentária oficial.
- Assume linearidade do custo por ponto (retornos constantes), o que superestima
  o custo dos últimos pontos até 80% (retornos decrescentes reais).

---

## 5. GATILHO DE REVISÃO

- [ ] Disponibilidade de dados de custo-efetividade por programa (permite custo marginal real)
- [ ] Incorporação de retornos decrescentes (curva de custo não linear)
- [ ] Recalibração do `CUSTO_PONTO_PER_CAPITA_DEFAULT` com FUNDEB/censo escolar

---
