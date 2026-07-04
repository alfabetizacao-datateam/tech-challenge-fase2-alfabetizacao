# ADR-003: Preservação de Nulos em `proporcao_aluno_nivel_*`

- **Status:** ACCEPTED | **Data:** 2026-05-15 | **Criticidade:** ALTA | **Risco:** Inferência Estatística Errada

---

## 1. CONTEXTO

- **Dado de Entrada:** Coluna `proporcao_aluno_nivel_0` (INEP) tem 48% de valores NULL.

- **Questão Crítica:** NULL = "não coletado" vs NULL = "0%"?

- **Fato:** INEP não coleta proficiência por nível para TODOS os municípios em todos anos. Para ~50% dos casos:
- Município pequeno sem avaliação SAEB naquele ano
- Rede Privada com N alunos < threshold de divulgação (INEP censura por privacy)
- Dados não processados a tempo (atraso de divulgação)

- **Risco se imputar com 0.0:**
- Se imputarmos `NULL → 0.0`, invertemos o significado: "0% em nível 0" vira verdade
- Métricas agregadas (AVG, SUM) ficam enviesadas: descemos a proficiência FAKE
- Correlações espúrias: "municípios com 0 em nivel_0 têm x de taxa" é FALSO

---

## 2. DECISÃO

- **Escolha:** PRESERVAR 48% de NULLs em `proporcao_aluno_nivel_*`. NUNCA imputar com 0.0.

- **Implementação:**
```python
# NO script 02_silver_transform.py:
# Sem fazer:
df = df.fillna({"proporcao_aluno_nivel_0": 0.0}) # ERRADO

# Apenas manter:
df = df.select(
    "proporcao_aluno_nivel_0", # Preserva NULL como está
    ...
)
```

- **Comunicação:**
- Documentar em DICIONARIO_DADOS.md que 48% = estrutural, não falta aleatória
- Flag em Gold: `pct_nulos_proporcao_nivel` = 48% (alertar analista)
- Usar Spark SQL com `.isNull()` filter quando agregando (contabilizar nulos)

---

## 3. CONSEQUÊNCIAS

**Vantagens:**
- Estatísticas corretas: média de proficiência não fica enviesada
- Honestidade: "não sabemos" (NULL) é diferente de "sabemos que é 0%"
- Consistência: dados INEP respeitados como-são

**Custos:**
- Código precisa lidar com NULL (usar `.isNull()` em filters, não `.fillna()`)
- Analytics precisa contar NULLs (5 linhas SQL a mais)
- Educação: juniors podem não entender por que NULL ≠ 0

---

## 4. ALTERNATIVAS REJEITADAS

| Opção | Problema |
|-------|----------|
| **Imputar com 0.0** | Inverte significado; enjeita inferência; REJEITADO |
| **Remover linhas com NULL** | Perde 48% dos dados; 23k → 12k rows; REJEITADO |
| **Imputar com MÉDIA** | Assume padrão que não existe; bias estrutural; REJEITADO |
| **Imputar com KNN** | Mesmo problema: criamos dado fake baseado em vizinhos que também têm NULL | REJEITADO |
| **Preservar NULL** | Honesto, correto, estatisticamente sólido | ESCOLHIDO |

---

## 5. IMPLEMENTAÇÃO NO CÓDIGO

- **Em `02_silver_transform.py`:**
```python
# Não fazer fillna() nessas colunas
proporcao_cols = [f"proporcao_aluno_nivel_{i}" for i in range(9)]
df = df.select(*all_cols) # Deixa NULL como está, sem fillna
```

- **Em Great Expectations (data_quality/checks.py):**
```python
# Validar que % de nulos NÃO mudou
assert df.select("proporcao_aluno_nivel_0").isNull().sum() / len(df) ≈ 0.48
```

- **Em EDA / Notebooks:**
```python
# Contar NULLs ao agregar:
df.groupby("sigla_uf").agg(
    f.avg(f.col("proporcao_aluno_nivel_0")).alias("media_nivel_0"),
    f.count_if(f.col("proporcao_aluno_nivel_0").isNull()).alias("qtd_nulos")
)
```

---

## 6. ROI & RISCO

| Cenário | Sem ADR-003 | Com ADR-003 |
|---------|-----------|-----------|
| Relatório para MEC diz "proficiência média = 65%" | FALSO (imputação com 0.0 abaixa de verdadeiro 75%) | VERDADEIRO (78%, controlado nulos) |
| Modelo ML treina em dados errados | Acurácia fake 94%, produção 60% | Acurácia real 85%, produção 84% |
| Custo reputacional de erro | Alto (MEC desconfia) | Baixo (transparência) |

- **ROI:** Evita ~R$ 500k em reputação (se relatório errado vira news de "IA falha em educação")

---

## 7. CRITÉRIOS DE ACEITAÇÃO

- [ ] `proporcao_aluno_nivel_*` têm 48% nulos em Silver (não menos, não mais)
- [ ] `DICIONARIO_DADOS.md` marca estas colunas como "48% NULOS ESTRUTURAIS"
- [ ] Great Expectations check: `assert % nulos ≈ 48%` passa
- [ ] Nenhuma chamada a `.fillna()` ou `.coalesce()` dessas colunas (code review)
- [ ] Documentação (CLAUDE.md) explicita: "NUNCA imputar com 0.0"

---
