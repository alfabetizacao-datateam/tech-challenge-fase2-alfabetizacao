# Great Expectations — Data Quality Governance

**Objetivo**: Automatizar validações de dados em produção com relatórios HTML, versionamento e integração CI/CD.

---

## O que é Great Expectations?

Framework open-source para **data quality** que oferece:

| Recurso | Benefício |
|---------|-----------|
| **Expectations** | Suites versionáveis de validações (como testes unitários para dados) |
| **Data Docs** | Relatórios HTML automáticos mostrando qualidade de cada coluna |
| **Checkpoints** | Roda a suite toda vez que novos dados chegam (automático) |
| **CI/CD** | Retorna exit code se validações falham (bloqueia pipeline) |

---

## Implementação no Tech Challenge

Temos **2 Scripts**:

### 1 `01_validacao_qualidade.py` (Manual/Simples)
- 10 validações manuais em PySpark
- Rápido, sem dependências extras
- **Usado para**: Testes, debugging rápido

```powershell
$env:ENV="dev"
python src/data_quality/01_validacao_qualidade.py
```

### 2 `02_great_expectations_dq.py` (Framework)
- 12 expectations com Great Expectations (quando instalado)
- Fallback automático para PySpark se GE não está disponível
- Valida **Silver + Gold**
- Gera relatórios JSON com timestamp

```powershell
$env:ENV="dev"
python src/data_quality/02_great_expectations_dq.py
```

**Estrutura de expectations**:
- **E01-E12**: Silver OBT (tipagem, nulos, ranges, duplicatas)
- **G01-G03**: Gold Marts (completude, deduplicação, ranges)

---

## Como Usar

### Setup (primeira vez)

Great Expectations já está em `requirements.txt`. Se não tiver:

```bash
pip install great-expectations
```

### Executar validações

```powershell
# Dev (amostra)
$env:ENV="dev"
python src/data_quality/02_great_expectations_dq.py

# Prod (dados completos)
$env:ENV="prod"
python src/data_quality/02_great_expectations_dq.py
```

### Output esperado

```
======================================================================
VALIDANDO CAMADA SILVER (OBT)
======================================================================
  [OK ] E01_id_municipio_not_null PASS
  [OK ] E02_id_municipio_string PASS
  [OK ] E03_taxa_range_0_100 PASS
  [OK ] E04_taxa_not_null PASS
  [OK ] E05_uf_no_unknown PASS
  [OK ] E06_meta_imputada_95pct PASS
  [OK ] E07_deficit_nao_negativo PASS
  [OK ] E08_nome_municipio_99pct PASS
  [OK ] E09_rede_valores_validos PASS
  [OK ] E11_populacao_positiva PASS
----------------------------------------------------------------------
  Resultado: 10 PASS | 0 FAIL | 0 SKIP
======================================================================

======================================================================
VALIDANDO CAMADA GOLD (MARTS)
======================================================================
  [OK ] G01_completude_taxa_alfabetizacao PASS
  [OK ] G02_gold_sem_duplicatas PASS
  [OK ] G03_range_taxa_alfabetizacao_media PASS
----------------------------------------------------------------------
  Resultado: 3 PASS | 0 FAIL | 0 SKIP
======================================================================

Todas as validacoes passaram!
```

### Relatórios

Cada execução gera 2 arquivos JSON em `docs/dq_reports/`:

1. **`dq_report_20260625_143022.json`** — Relatório Silver detalhado
2. **`dq_consolidado_20260625_143022.json`** — Silver + Gold consolidado

Exemplo:

```json
{
  "timestamp": "2026-06-25T14:30:22.123456",
  "ambiente": "dev",
  "silver": {
    "E01_id_municipio_not_null": "pass",
    "E02_id_municipio_string": "pass",
    ...
  },
  "gold_marts": {
    "agg_uf_indicadores": true,
    "agg_municipio_ranking": true,
    ...
  },
  "summary": {
    "silver_passed": 10,
    "gold_passed": 9,
    "overall_sucesso": true
  }
}
```

---

## 12 Expectations Definidas

### Silver OBT (E01-E12)

| # | Nome | Validação |
|---|------|-----------|
| E01 | id_municipio_not_null | id_municipio não pode ser nulo |
| E02 | id_municipio_string | id_municipio deve ser STRING (não int) |
| E03 | taxa_range_0_100 | taxa_alfabetizacao entre 0-100 |
| E04 | taxa_not_null | taxa_alfabetizacao obrigatória |
| E05 | uf_no_unknown | sigla_uf não pode ser "Unknown" |
| E06 | meta_imputada_95pct | meta_2024_imputada >= 95% cobertura |
| E07 | deficit_nao_negativo | deficit_proxy >= 0 |
| E08 | nome_municipio_99pct | nome_municipio >= 99% preenchido |
| E09 | rede_valores_validos | rede em {Federal, Estadual, Municipal, Privada} |
| E10 | sem_duplicatas_chave | (id_municipio, ano, rede) única |
| E11 | populacao_positiva | populacao_total > 0 (quando presente) |
| E12 | gasto_nao_negativo | gasto_habitante >= 0 (quando presente) |

### Gold Marts (G01-G03)

| # | Nome | Validação |
|---|------|-----------|
| G01 | completude | Agregados >= 90% preenchidos |
| G02 | sem_duplicatas | Chaves únicas por mart |
| G03 | range | Porcentagens entre 0-100 |

---

## Modo Fallback (sem Great Expectations)

Se GE não estiver instalado, o script **automaticamente** usa validações PySpark nativas:

```powershell
# Sem GE instalado:
python src/data_quality/02_great_expectations_dq.py

# Output:
# Modo FALLBACK: validacoes manuais PySpark
# [OK ] E01_id_municipio_not_null PASS
# ...
```

**Vantagem**: Sem dependência externa, funciona sempre.

---

## Integração com CI/CD

Se as validações falharem, o script retorna `exit code 1`:

```yaml
# .github/workflows/dq.yml
name: Data Quality
on: [push]
jobs:
  dq:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Great Expectations
        run: |
          python src/data_quality/02_great_expectations_dq.py
        # Se alguma validação falhar, workflow para aqui
```

---

## Próximos Passos (Roadmap)

### Hoje
- 12 expectations Silver
- 3 expectations Gold (sampling)
- Relatórios JSON
- Fallback PySpark

### Próximo (com Renan)
- [ ] Gerar **Data Docs HTML** (relatório visual)
- [ ] Criar **Checkpoint** (validação automática pós-pipeline)
- [ ] Integrar com **GitHub Actions** (bloquear PRs com dados ruins)
- [ ] Alertas **Slack** (notificar se DQ falha)

---

## Troubleshooting

### "ImportError: No module named 'great_expectations'"

```bash
pip install great-expectations
```

### "Silver OBT nao encontrada"

Certifique-se de rodar o pipeline antes:

```powershell
python src/batch/01_ingestao_bronze_batch.py
python src/batch/02_silver_transform.py
python src/gold/01_gerar_marts_gold.py
```

### "Esperanca E06 falhou (meta < 95%)"

Significa que `meta_alfabetizacao_2024_imputada` tem cobertura < 95%.
Rodar KNN imputation:

```powershell
python src/features/02_imputar_metas_knn.py
```

---

## Recursos

- **Great Expectations Docs**: https://docs.greatexpectations.io/
- **Nosso código**: `src/data_quality/02_great_expectations_dq.py`
- **Relatórios**: `docs/dq_reports/` (check JSON files)

