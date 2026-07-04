# ADR-007: SICONFI via API REST vs CSV Manual

- **Status:** ACCEPTED | **Data:** 2026-06-15 | **Risco:** BAIXO (Cache local mitiga downtime)

---

## 1. CONTEXTO

- **Dado:** Despesas municipais em educação da API SICONFI (Tesouro Nacional).

- **Problema:** Dados financeiros mudam a cada trimestre. CSV estático fica desatualizado.

- **Opções:**
1. **Arquivo CSV estático:** Download manual, versionado em git
2. **API REST SICONFI:** Requisições automáticas aos servidores do Tesouro

---

## 2. DECISÃO

- **Escolha:** API REST com cache JSON local (~111KB).

- **Implementação:**
```python
# src/siconfi/01_ingestao_siconfi.py
import requests
import json
from concurrent.futures import ThreadPoolExecutor

URL_SICONFI = "https://apidatalake.tesouro.gov.br/..."

# 8 workers paralelos → 5.550 municípios em ~6 minutos
def fetch_siconfi_paralelo():
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(fetch_by_id, mun_id) for mun_id in municipios]

    results = [f.result() for f in futures]

    # Cache local
    with open("models/siconfi_cache.json", "w") as f:
        json.dump(results, f)

    return results

# Fallback se API cair
def load_siconfi():
    try:
        return fetch_siconfi_paralelo()
    except requests.exceptions.ConnectionError:
        with open("models/siconfi_cache.json") as f:
            return json.load(f) # Use cache de última execução
```

---

## 3. CONSEQUÊNCIAS

**Vantagens:**
- Dados sempre atualizados (trimestre novo = novos valores)
- Sem manutenção manual (automático)
- Cache (~111KB) cabe em RAM; fallback instantâneo

**Custos:**
- Latência inicial: 6 minutos para fetch de 5.550 municípios
- Dependência de API externa (pode cair)
- Necessidade de retry logic + timeout handling

---

## 4. MITIGAÇÃO

| Risco | Mitigation |
|-------|-----------|
| API cai | Cache JSON local (última execução bem-sucedida) |
| Timeout | 8 workers com retry exponencial (max 3 tentativas) |
| Rate limiting | Delays de 100ms entre requests; batch de 50 por minuto |
| Dados obsoletos | Run mensal via Airflow/Cron (trigger: mês novo) |

---

## 5. CRITÉRIOS DE ACEITAÇÃO

- [ ] API REST fetch com 8 workers implementado
- [ ] Cache JSON salvo em `models/siconfi_cache.json` (111KB)
- [ ] Fallback para cache se API falha
- [ ] Retry com exponential backoff (max 3x)
- [ ] Timeout 30s por request
- [ ] Testado com 5.550 IDs (benchmark: < 6 min)

---
