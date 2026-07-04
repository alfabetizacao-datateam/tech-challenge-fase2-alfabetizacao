# ADR-009: Mediana como Corte dos Quadrantes de Eficiência

- **Status:** ACCEPTED | **Data:** 2026-06-15 | **Risco:** MÉDIO (Reclass em outliers é transitória)

---

## 1. CONTEXTO

- **Problema:** Municipios de diferentes tamanhos e contextos não são comparáveis com threshold absoluto.

- **Exemplo:** Threshold "eficiência_gasto > R$500/aluno" é:
- Fácil para São Paulo (12M hab, larga economia)
- Impossível para município de 5k hab (menor efeito de escala)

- **Solução:** Usar mediana como corte adaptativo.

---

## 2. DECISÃO

- **Escolha:** Separar municípios em 4 quadrantes usando MEDIANA de 2 eixos:
1. Eixo Y: Taxa de alfabetização
2. Eixo X: Gasto por habitante

```
            ACIMA MEDIANA

┌─────────────────────────────┐
│ Eficiente │ Alto Gasto│
│ (Bom custo) │ (Ineficiente)
├─────────────────┼─────────────┤
│Subinvestido │ Crítico │
│(Oportunidade) │ (Risco alto)│
└─────────────────────────────┘
   BAIXA TAXA ALTA TAXA
```

---

## 3. IMPLEMENTAÇÃO

```python
# Calcular medianas
taxa_mediana = df_silver["taxa_alfabetizacao"].median() # Ex: 84%
gasto_mediana = df_silver["gasto_por_habitante"].median() # Ex: R$2.000

# 4 quadrantes
df_silver["quadrante"] = np.select(
    [
        (df_silver["taxa"] >= taxa_mediana) & (df_silver["gasto"] >= gasto_mediana),
        (df_silver["taxa"] >= taxa_mediana) & (df_silver["gasto"] < gasto_mediana),
        (df_silver["taxa"] < taxa_mediana) & (df_silver["gasto"] >= gasto_mediana),
        (df_silver["taxa"] < taxa_mediana) & (df_silver["gasto"] < gasto_mediana),
    ],
    ["Alto Gasto", "Eficiente", "Crítico", "Subinvestido"],
    default="Unknown"
)
```

---

## 4. CONSEQUÊNCIAS

**Vantagens:**
- Robusta a outliers (São Paulo não distorce a mediana)
- Adaptativa ao contexto nacional (recalcula anualmente)
- Fácil interpretação (4 categorias claras)

**Limitação:**
- Município pode mudar de quadrante SEM mudança própria
- Ex: Se outro município melhora, mediana sobe → primeiro município cai de "Eficiente" para "Crítico"
- **Impacto:** Classificação é RELATIVA (contexto) não ABSOLUTA (capacidade)

---

## 5. COMPENSAÇÃO

- **Indicador de "Risco Absoluto":** Separado do quadrante.

```python
# Status baseado em threshold ABSOLUTO (complementar)
df_silver["status_risco"] = np.select(
    [
        df_silver["gap_meta"] > 0, # Acima da meta 2024
        df_silver["gap_meta"] >= -10, # Perto da meta
        df_silver["gap_meta"] >= -25, # Moderado risco
        df_silver["gap_meta"] < -25, # Crítico
    ],
    ["Verde", "Amarelo", "Laranja", "Vermelho"],
)
```

---

## 6. CRITÉRIOS DE ACEITAÇÃO

- [ ] Mediana calculada por ano
- [ ] Quadrante calculado para cada município
- [ ] Status_risco SEPARADO de quadrante
- [ ] Documentação explícita: "Quadrante é contexto, Status é risco absoluto"

---
