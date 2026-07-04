# ADR-013: Fração de População Alfabetizável no Custo de Projeção de Investimento

- **Status:** ACCEPTED | **Data:** 2026-07-04 | **Corrige:** ADR-012 (modelo de custo marginal per capita)

---

## 1. CONTEXTO

Ao reprocessar o pipeline no GCP com dados de produção completos (5.550
municípios), o cálculo de `custo_estimado_para_atingir_80`
(`agg_projecao_investimento`) e o ROI nacional (`agg_roi_executivo`)
produziram valores absurdos: **R$93,7 bilhões** para 4.679 municípios —
133x maior que os R$703M documentados no README antigo.

### Causa raiz

A fórmula do ADR-012 é:

```
custo_estimado_para_atingir_80 = gap_ate_80 × custo_marginal_per_capita × populacao_total
```

`populacao_total` é a **população TOTAL do município** (IBGE, todos os
habitantes — ver `DICIONARIO_DADOS.md`: "População do município. Estimativa
2021"). A calibração do ADR-012 (`custo_por_aluno = c_pc × 100 = R$2.000`)
assume implicitamente que `gap_ate_80% × populacao_total` representa o
**número de alunos em déficit**. Não representa: verificamos que essa conta
implica **48,3 milhões de "alunos em déficit"** no dataset de produção —
maior que toda a população de crianças de 6-10 anos do Brasil inteiro.

`populacao_total` já era usado como proxy de "volume humano" em
`deficit_absoluto_proxy` (ver `DICIONARIO_DADOS.md`, linha da coluna:
"NÃO é contagem de alunos avaliados — usa população total como proxy de
escala"). Isso é uma escolha correta e documentada para **ranking relativo**
(priorização entre municípios). O erro do ADR-012 foi reutilizar essa mesma
variável numa fórmula que converte gap para **R$ absolutos via custo-por-
aluno** — aí a diferença entre "população total" e "população de alunos"
deixa de ser cosmética e passa a inflar o resultado em ordens de magnitude.

---

## 2. DECISÃO

Introduzir uma fração demográfica documentada,
`FRACAO_POPULACAO_ALFABETIZAVEL`, que converte população total em uma
estimativa do número de crianças na coorte de idade do 2º ano do ensino
fundamental (o público do indicador), **apenas na fórmula de custo em R$**.
`populacao_total` bruta continua sendo usada, sem alteração, em toda métrica
de ranking/priorização (`deficit_absoluto_proxy`, `agg_municipio_ranking`,
etc.) — não há alteração de comportamento ali.

```python
# Coorte de idade única (~7 anos, 2o ano do ensino fundamental) como fracao
# da populacao total do municipio. Fonte: piramide etaria IBGE — nascimentos
# anuais no Brasil (~2,7-2,9 milhoes) sobre populacao total (~213 milhoes)
# aproxima uma coorte de idade unica a ~1,3% da populacao. Nao e dado direto
# do dataset (nao ha matricula/coorte por municipio nas fontes ingeridas) —
# e premissa demografica explicita, sujeita a recalibracao (ver Secao 5).
FRACAO_POPULACAO_ALFABETIZAVEL = 0.013

populacao_alfabetizavel = populacao_total * FRACAO_POPULACAO_ALFABETIZAVEL
custo_estimado_para_atingir_80 = gap_ate_80 * custo_marginal_per_capita * populacao_alfabetizavel
```

Consequência: `custo_por_aluno` continua sendo `c_pc × 100` = R$2.000
(inalterado, ainda alinhado ao benchmark FUNDEB), mas o número de "alunos"
multiplicado passa a ser uma estimativa de coorte escolar, não a população
inteira do município.

---

## 3. IMPLEMENTAÇÃO

| Arquivo | Mudança |
|---|---|
| `src/cloud/dataproc_03_gold.py` | Constante `FRACAO_POPULACAO_ALFABETIZAVEL = 0.013`; `build_mart_projecao_investimento` aplica a fração antes de multiplicar pelo custo marginal |
| `src/gold/01_gerar_marts_gold.py` | Mesma correção (script local tem a mesma fórmula, mesmo bug) |
| `agg_roi_executivo`, `agg_alocacao_otima`, `agg_alocacao_otima_estrategias` | Herdam a correção automaticamente (consomem `custo_estimado_para_atingir_80` já corrigido) |

---

## 4. CONSEQUÊNCIAS

**Vantagens:**
- Elimina a inflação de ~77x (48,3M "alunos" implícitos → ~628k, ordem de
  grandeza compatível com uma coorte real).
- `populacao_total` continua correta e inalterada onde já era usada
  corretamente (ranking/priorização relativa).
- Mantém o benchmark per capita (R$/hab/ponto) do ADR-012, que resolveu o
  problema anterior de distorção de escala entre municípios grandes/pequenos.

**Limitação (nova, a ser revisada):**
- `FRACAO_POPULACAO_ALFABETIZAVEL = 0.013` é uma **estimativa demográfica
  nacional aplicada uniformemente**, não uma coorte real por município.
  Municípios com pirâmide etária atípica (ex: forte êxodo de jovens, ou
  municípios universitários com população jovem inflada) terão erro maior
  nessa proxy. Não há dado de matrícula/coorte por município nas fontes já
  ingeridas para calibrar por município.

---

## 5. GATILHO DE REVISÃO

- [ ] Ingerir Censo Escolar (INEP) com matrícula real do 2º ano por
  município — substituiria a fração fixa por contagem direta.
- [ ] Validar `FRACAO_POPULACAO_ALFABETIZAVEL` contra IBGE/Censo 2022 por
  faixa etária (hoje é estimativa a partir de nascimentos anuais / população
  total nacional, não municipal).
