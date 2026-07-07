# ADR-010: Knapsack Greedy vs Programação Dinâmica (Otimização R$500M)

- **Status:** ACCEPTED (algoritmo) | **Números atualizados em 0.1 (2026-07-06)** | **Data:** 2026-06-15 | **Justificativa:** Computacionalmente viável + empiricamente ótimo

---

## 0. ATUALIZAÇÃO (2026-07-02) — números pendentes de recálculo [HISTÓRICO]

A **escolha do algoritmo** (Greedy vs DP, seção 2-4 abaixo) continua válida e não muda.

Os **números ilustrativos** deste documento (2.815 municípios, 99,96% de
aproveitamento, ~85% de cobertura populacional) foram calculados com o modelo
de custo **antigo**, substituído pelo **ADR-012** (2026-07-01). O novo modelo
de custo marginal per capita é dimensionalmente diferente do anterior (ver
ADR-012 seção 1) e resulta em custo por município ~100x maior no cenário
default. Isso significa que, com o `ORCAMENTO_ALOCACAO = R$500M` inalterado,
o número real de municípios cobertos pelo knapsack é **provavelmente muito
menor** que os 2.815 citados aqui.

**Não usar os números desta página em relatórios/apresentação até
recalcular** a partir do mart `agg_alocacao_otima` gerado após o ADR-012
(pipeline reprocessado em 2026-07-02, mas a contagem de
`selecionado_no_orcamento=True` ainda não foi extraída do BigQuery).

Se, após recalcular, a cobertura ficar baixa demais para ser útil como
narrativa (ex: dezenas de municípios em vez de milhares), considerar como
gatilho de revisão (seção 6): reavaliar `ORCAMENTO_ALOCACAO` ou o benchmark
`CUSTO_PONTO_PER_CAPITA_DEFAULT`.

## 0.1. RECÁLCULO CONCLUÍDO (2026-07-06) — números finais

A pendência da seção 0 foi fechada. Reprocessamento completo no GCP
(pós-ADR-012 e ADR-013), números extraídos do BigQuery e confirmados ao
vivo via `scripts/verificar_numeros_publicacao.py` (ver
`docs/NUMEROS_RECALCULADOS.md`):

| Métrica | Valor final | Ilustrativo (seção 0, obsoleto) |
|---|---|---|
| Municípios com gap (universo) | 4.679 | 2.815 (universo era outro) |
| Municípios selecionados no orçamento | 2.329 | 2.815 |
| Cobertura do orçamento | 49,8% dos municípios com gap | 99,96% |
| Alunos estimados beneficiados | 246.563 | não reportado |

A cobertura caiu de ~todos para ~metade — não porque o algoritmo piorou, mas
porque o benchmark de custo real (~R$1.939/aluno, calibrado via SICONFI) é
~97x maior que o R$20/aluno artificialmente baixo do modelo pré-ADR-012. O
número é mais baixo e também mais correto. Cobertura de 49,8% ainda é útil
como narrativa (não caiu para "dezenas de municípios") — o gatilho de revisão
da seção 6 não foi acionado.

---

## 1. CONTEXTO

- **Problema:** MEC tem orçamento de R$500M para alfabetização. Onde alocar para máximo impacto?

- **Formulação:**
- 3.486 municípios com gap de alfabetização
- Custo para atingir 80% por município (varia por contexto)
- Benefício: número de alunos que aprenderiam

- **Modelagem:** Problema Knapsack 0/1
```
Maximizar: Σ(beneficio_i × x_i)
Sujeito a: Σ(custo_i × x_i) ≤ R$500M
           x_i ∈ {0, 1}
```

- **Opções:**
1. **Programação Dinâmica Exata:** O(n × W) onde W = R$500M = inviável
2. **Greedy (razão benefício/custo):** O(n log n) — rápido mas aproximado
3. **Força Bruta:** O(2^n) — impossível para 3.486 municípios

---

## 2. DECISÃO

- **Escolha:** Algoritmo Greedy (razão benefício/custo máxima).

```python
# src/ml/02_otimizar_alocacao.py
def knapsack_greedy(municipios, budget=500_000_000):
    """
    Greedy: ordenar por razão benefício/custo decrescente
    """
    # Calcular razão
    municipios["razao"] = municipios["beneficio"] / municipios["custo"]

    # Ordenar decrescente
    municipios_sorted = municipios.sort_values("razao", ascending=False)

    # Pegar municípios até esgotar orçamento
    alocacao = []
    spent = 0
    for _, mun in municipios_sorted.iterrows():
        if spent + mun["custo"] <= budget:
            alocacao.append(mun)
            spent += mun["custo"]

    return pd.DataFrame(alocacao)
```

---

## 3. GARANTIAS

- **Complexidade:** O(n log n) — prático para 3.486 municípios (~50ms)

- **Empiricamente ótimo:** 99,96% de aproveitamento do orçamento

- **Não é ótimo teórico:** Pode deixar R$500k não-gastos (viável em Knapsack DP)

---

## 4. COMPARAÇÃO

| Abordagem | Complexidade | Qual Escolher | Resultado |
|-----------|--------------|---------------|-----------|
| Greedy | O(n log n) | MVP | 99,96% aproveitamento |
| DP Exato | O(n × R$500M) = O(n × 10^9) | Inviável | 100% aproveitamento (teórico) |

- **Diferença prática:** Greedy deixa ~R$500k de R$500M não-gasto (~0.1%). Trade-off aceitável.

---

## 5. IMPLEMENTAÇÃO

```python
# Resultado: 2.815 municípios alocados em R$500M
# Cobertura: ~85% da população brasileira em risco
resultado = knapsack_greedy(municipios_com_gap, budget=500_000_000)

# Validação
assert resultado["custo"].sum() <= 500_000_000
assert resultado["beneficio"].sum() > 0
print(f"Alocado: R$ {resultado['custo'].sum():,.0f} de R$ 500M")
print(f"Aproveitamento: {100 * resultado['custo'].sum() / 500_000_000:.2f}%")
```

---

## 6. GATILHO DE REVISÃO

Se:
- [ ] Orçamento muda para > R$2B (viabiliza DP)
- [ ] Margem 0.1% não-gasto vira inaceitável
- [ ] Stakeholder pede otimização teórica 100% (aceitar latência)

---
