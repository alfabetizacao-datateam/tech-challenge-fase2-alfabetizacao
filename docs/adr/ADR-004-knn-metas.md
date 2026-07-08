# ADR-004: KNN Imputação de Metas (Cobertura 43.9% → 100%)

- **Status:** ACCEPTED | **Data:** 2026-05-18 | **Risco:** MÉDIO | **Limitação Explícita:** Sim

> **Nota de atualização (2026-07-07):** o algoritmo (KNN K=5 por UF) continua válido. A implementação real está em `src/features/02_imputar_metas_knn.py` (script dedicado), não em `02_silver_transform.py` como o texto abaixo descreve — refatoração posterior à escrita deste ADR. Ver README, seção "Fluxo de dados".
>
> **Nota de atualização (2026-07-08 — ver ADR-015):** a implementação real difere do pseudocódigo da Seção 5 em dois pontos: (1) roda em **duas etapas** — primeiro propaga a meta já existente dentro do mesmo município entre redes (`first(meta, ignorenulls=True) OVER (PARTITION BY id_municipio)`, uma decisão de negócio implícita — "meta é município-level, não rede-level" — antes não documentada aqui), e só então roda KNN nos municípios que sobraram sem meta nenhuma; (2) as features usadas são `taxa_alfabetizacao_media`, `populacao` e `deficit_absoluto_proxy` agregados por **município** (não por rede, e sem `medida_media_saeb`, que o texto abaixo cita mas o código nunca usou). Dois problemas reais também corrigidos nesta data: (a) `deficit_absoluto_proxy` estava sendo somado entre linhas (ano × rede) em vez de calculado como média, inflando a feature em até ~3x para municípios com mais redes reportadas — mesma classe de bug do ADR-015; (b) o script nunca tinha validação estatística (Seção 6 abaixo é só sanidade de range, não precisão) — adicionada validação por holdout (esconder a meta de municípios que já têm meta conhecida, medir MAE/RMSE da predição KNN contra o valor real), salva em `metrics_knn_imputacao.json` a cada execução. Isso não muda a decisão do KNN em si, mas agora há um número real para decidir se a cobertura reportada é confiável antes de usar em produção — ver ADR-015 para o racional completo e a recomendação de promoção.

---

## 1. CONTEXTO

- **Dado:** Coluna `meta_alfabetizacao_2024` vem do PDE (Plano de Desenvolvimento da Educação).

- **Problema:** PDE define metas APENAS para rede "Municipal" (3.486 municípios × 1 rede).
- Rede Federal: 0 metas (0%)
- Rede Estadual: 0 metas (0%)
- Rede Privada: 0 metas (0%)
- Rede Municipal: 3.486 metas

- **Cenário em Gold:** Relatório executivo quer "Qual o gap (meta - taxa) para TODAS as redes em todas UFs".
- Se deixarmos NULL, perde-se Estadual/Federal/Privada em análises
- Se imputarmos "ingenuamente" com 0.0, criamos fato falso: "meta = 0% é oficial"

- **Necessidade:** Inferir metas razoáveis para Estadual/Federal/Privada via ML (KNN)

---

## 2. DECISÃO

- **Escolha:** Usar K-Nearest Neighbors (KNN) com K=5 para imputar metas faltando.

- **Algoritmo:**
```
Para cada município com rede Estadual/Federal/Privada SEM meta:
  1. Encontrar 5 municípios vizinhos com MESMA sigla_uf E mesma rede E COM meta original
  2. Média ponderada de vizinhos: KNN(features=taxa_alfabetizacao + populacao + SAEB, weights=distance)
  3. Resultado: meta_alfabetizacao_2024_imputada
  4. MARCAR coluna com flag: is_imputado=True (para rastreabilidade)
```

- **Exemplo:**
```
Estadual em São Paulo SEM meta:
  → Encontra 5 Municipais em SP com meta (ex: 92%, 93%, 91%, 94%, 92%)
  → Média: 92.4% (meta imputada para Estadual)
  → Marca: meta_imputado=True
```

---

## 3. CONSEQUÊNCIAS

**Vantagens:**
- Cobertura: 43.9% → 100% de municípios têm meta (comparável)
- Rastreável: coluna `is_imputado` marca quais são inferenciais
- Contextual: KNN respeita sigla_uf (não compara SP com AC)
- Conservador: K=5 evita outliers; pesos distance fazem vizinhos próximos pesar mais

- **Custos & Limitações:**
- **Limitação crítica:** Metas imputadas para Estadual/Federal/Privada NÃO são dados INEP/PDE oficiais
- Não pode ser reportado ao MEC como "meta oficial de Estadual" (é inferência)
- Se padrão educacional mudar (ex: Estadual reforma currículo), KNN não captura (treinado em histórico Municipal)
- Dependência do K: K=5 é heurístico, não "ótimo"

---

## 4. ALTERNATIVAS REJEITADAS

| Opção | Problema | Decisão |
|-------|----------|---------|
| **Deixar NULL** | Perde Estadual/Federal/Privada em análises | Rejeitado |
| **Imputar com média geral** | Ignora contexto UF/rede; Minas Gerais ≠ São Paulo | Rejeitado |
| **Deixar em branco (0%)** | Inverte significado: "meta é 0%" é falso | Rejeitado |
| **Regressão Linear** | Assume relação linear (taxa → meta); não existe | Rejeitado |
| **KNN (K=5 com dist)** | Contextual, conservador, rastreável | ESCOLHIDO |

---

## 5. IMPLEMENTAÇÃO

- **Em `02_silver_transform.py`:**
```python
from sklearn.neighbors import KNeighborsRegressor

# Treina KNN em dados Municipal COM meta
municipal = df[df['rede'] == 'Municipal'].dropna(subset=['meta_alfabetizacao_2024'])

for rede in ['Estadual', 'Federal', 'Privada']:
    missing = df[(df['rede'] == rede) & (df['meta_alfabetizacao_2024'].isNull())]

    for uf in df['sigla_uf'].unique():
        # KNN por UF (não global)
        X = municipal[municipal['sigla_uf'] == uf][
            ['taxa_alfabetizacao', 'populacao_total', 'medida_media_saeb']
        ]
        y = municipal[municipal['sigla_uf'] == uf]['meta_alfabetizacao_2024']

        if len(X) >= 5: # Apenas se temos K vizinhos
            knn = KNeighborsRegressor(n_neighbors=5, weights='distance')
            knn.fit(X, y)

            X_missing = missing[missing['sigla_uf'] == uf][
                ['taxa_alfabetizacao', 'populacao_total', 'medida_media_saeb']
            ]
            y_pred = knn.predict(X_missing)

            df.loc[(df['rede'] == rede) & (df['sigla_uf'] == uf), 'meta_imputada'] = y_pred
            df.loc[(df['rede'] == rede) & (df['sigla_uf'] == uf), 'is_imputado'] = True
```

- **Em DICIONARIO_DADOS.md:**
```
`meta_alfabetizacao_2024_imputada` | DOUBLE
Origem: KNN (K=5) se municipal, direto se rede=Municipal
Limitação: Redes Estadual/Federal/Privada são ESTIMATIVAS, não dados INEP/PDE oficiais
is_imputado: Boolean flag para rastreabilidade
```

---

## 6. VALIDAÇÃO & MONITORAMENTO

- **Great Expectations Check:**
```python
assert df['meta_imputada'].notNull().sum() / len(df) >= 0.99 # 99%+ cobertura
assert df[df['is_imputado'] == True]['meta_imputada'].min() > 0 # Sanidade
assert df[df['is_imputado'] == True]['meta_imputada'].max() <= 100 # Sanidade
```

- **Métricas:**
- Cobertura antes: 43.9% (only Municipal)
- Cobertura depois: 100% (KNN + Municipal)
- % imputado: ~72% (Estadual/Federal/Privada)
- Diferença média Municipal (original vs rede imputada no estado): ~2% (razoável)

---

## 7. TRIGGER DE REVISÃO

Se em produção descobrirmos que:
- Redes Estadual/Federal têm metas divulgadas pelo PDE (fato novo)
- → Migrar para usar dados oficiais
- → Remover KNN (substitui por JOIN)

---

## 8. CRITÉRIOS DE ACEITAÇÃO

- [ ] KNN com K=5 implementado em Silver (02_silver_transform.py)
- [ ] Coluna `is_imputado` marca TRUE/FALSE corretamente
- [ ] Cobertura >= 99.9% (meta_imputada tem <0.1% nulos)
- [ ] DICIONARIO_DADOS.md marca "NÃO é dado INEP/PDE oficial" (limitação explícita)
- [ ] CLAUDE.md reforça: "Use is_imputado=True como filtro se precisa só dados oficiais"
- [ ] Média de meta_imputada [Estadual/Federal/Privada] entre 85-95% (sanidade range)

---
