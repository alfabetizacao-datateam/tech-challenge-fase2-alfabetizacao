# GUIA_CLOUD_DEPLOY.md — GCS + BigQuery + FinOps

Deploy da Camada Gold no Google Cloud. Custo efetivo: **R$ 0,00/mês** para este volume.

---

## TL;DR — Executar em 5 minutos

```powershell
# Pré-requisito: service-account.json na raiz do projeto (veja Seção 3)

.\.venv\Scripts\Activate.ps1

$env:GCS_BUCKET = "tc-fase2-gold-alfabetizacao"
$env:GOOGLE_CLOUD_PROJECT = "tc-fase2-alfabetizacao"
$env:GOOGLE_APPLICATION_CREDENTIALS = "C:\caminho\para\service-account.json"

# Testar sem fazer upload
$env:DRY_RUN = "true"
python src/cloud/01_deploy_gold_gcs.py

# Deploy real (dados sample)
$env:DRY_RUN = "false"; $env:ENV = "dev"
python src/cloud/01_deploy_gold_gcs.py
python src/cloud/02_load_bigquery.py

# Deploy prod (dados INEP completos)
$env:ENV = "prod"
python src/cloud/01_deploy_gold_gcs.py
python src/cloud/02_load_bigquery.py
```

---

## 1. Decisão FinOps: Por que GCS + BigQuery?

| Critério | AWS S3 + Athena | GCS + BigQuery | Decisão |
|----------|----------------|----------------|---------|
| Storage/GB/mês | US$ 0,023 | US$ 0,020 | GCS |
| Queries ad-hoc | US$ 5,00/TB | **US$ 0,00** (free tier 10 GB) | GCS/BQ |
| Free tier storage | 5 GB/mês | **10 GB/mês** | GCS |
| Custo total/mês (projeto) | ~US$ 0,12 | **US$ 0,00** | **GCS** |

Os 13 Marts (~50 MB Parquet) cabem inteiramente no free tier do BigQuery.

**Decisão**: GCS + BigQuery. Zero custo, integração nativa com Looker Studio e Jupyter.

---

## 2. Estimativa de Custo Real

| Recurso | Volume | Custo/mês |
|---------|--------|-----------|
| GCS Standard (dev) | ~5 MB | **US$ 0,00** |
| GCS Standard (prod) | ~50 MB | **US$ 0,00** |
| BigQuery storage (13 tabelas) | ~50 MB | **US$ 0,00** |
| BigQuery queries | ~100 MB/mês | **US$ 0,00** |
| **TOTAL** | | **US$ 0,00 / mês** |

Para 1 ano de dados (~500 MB): ainda < US$ 0,01/mês.

---

## 3. Setup (Primeira Vez)

### Passo 1: Criar projeto Google Cloud

```bash
gcloud projects create tc-fase2-alfabetizacao --name="Tech Challenge Fase 2"
gcloud config set project tc-fase2-alfabetizacao
```

Ou via Console: [console.cloud.google.com](https://console.cloud.google.com) → Menu > "Select a project" > "+ CREATE PROJECT".

### Passo 2: Ativar APIs

```bash
gcloud services enable storage.googleapis.com bigquery.googleapis.com
```

### Passo 3: Criar Service Account + chave JSON

```bash
gcloud iam service-accounts create tc-deploy-sa \
    --display-name="Tech Challenge Deploy SA"

# Roles necessárias
gcloud projects add-iam-policy-binding tc-fase2-alfabetizacao \
    --member="serviceAccount:tc-deploy-sa@tc-fase2-alfabetizacao.iam.gserviceaccount.com" \
    --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding tc-fase2-alfabetizacao \
    --member="serviceAccount:tc-deploy-sa@tc-fase2-alfabetizacao.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding tc-fase2-alfabetizacao \
    --member="serviceAccount:tc-deploy-sa@tc-fase2-alfabetizacao.iam.gserviceaccount.com" \
    --role="roles/bigquery.jobUser"

# Baixar chave JSON
gcloud iam service-accounts keys create service-account.json \
    --iam-account=tc-deploy-sa@tc-fase2-alfabetizacao.iam.gserviceaccount.com
```

**Importante**: `service-account.json` já está no `.gitignore`. Nunca commitar credenciais.

Via Console (alternativa): IAM & Admin > Service Accounts > + CREATE > Roles: `Storage Object Admin`, `BigQuery Data Editor`, `BigQuery Job User` > Create Key (JSON).

### Passo 4: Criar bucket GCS

```bash
# Região southamerica-east1 (São Paulo) — menor latência + LGPD compliant
gcloud storage buckets create gs://tc-fase2-gold-alfabetizacao \
    --location=southamerica-east1 \
    --uniform-bucket-level-access
```

### Passo 5: Script automatizado de setup

```powershell
# Ou simplesmente rodar o script de setup:
.\scripts\setup_gcs.ps1
# Script valida service-account, pede bucket e project ID, testa credenciais
```

---

## 4. Fluxo de Deploy

```
datalake_sample/gold/ (dev) ou datalake/gold/ (prod)
         │
         │  src/cloud/01_deploy_gold_gcs.py
         ▼
gs://tc-fase2-gold-alfabetizacao/gold/
├── agg_uf_indicadores/
├── agg_municipio_ranking/
├── agg_eficiencia_financeira/
└── ... (13 marts em Parquet)
         │
         │  src/cloud/02_load_bigquery.py  (load job gratuito)
         ▼
BigQuery: projeto.alfabetizacao_gold.*
├── agg_uf_indicadores       (49 linhas)
├── agg_municipio_ranking    (4.342 linhas)
├── agg_eficiencia_financeira (3.486 linhas)
└── ... (13 tabelas)
         │
         ▼
Looker Studio / Jupyter / Streamlit
```

### DEV (amostra — ~4 min)
```powershell
$env:ENV = "dev"
python src/cloud/01_deploy_gold_gcs.py   # ~30s, 19 Parquets ~1 MB
python src/cloud/02_load_bigquery.py      # ~20s, 13 tabelas
```

### PROD (dados completos INEP — ~7 min)
```powershell
$env:ENV = "prod"
python src/cloud/01_deploy_gold_gcs.py   # ~2 min, ~50 MB
python src/cloud/02_load_bigquery.py      # ~1 min
```

---

## 5. Validar Deploy

### GCS
```
https://console.cloud.google.com/storage/browser/tc-fase2-gold-alfabetizacao
```
Deve mostrar: `gold/agg_uf_indicadores/`, `gold/agg_municipio_ranking/`, etc.

### BigQuery
```
https://console.cloud.google.com/bigquery
```
Dataset `alfabetizacao_gold` com 13 tabelas.

### Query de teste
```sql
SELECT sigla_uf, taxa_alfabetizacao_media, qtd_municipios_analisados
FROM `tc-fase2-alfabetizacao.alfabetizacao_gold.agg_uf_indicadores`
WHERE ano = 2024
ORDER BY taxa_alfabetizacao_media DESC
LIMIT 10
```

---

## 6. Conectar ao Looker Studio (BI gratuito)

1. Acesse [lookerstudio.google.com](https://lookerstudio.google.com/)
2. "+ Create" > "Data Source" > Conector: **BigQuery**
3. Projeto: `tc-fase2-alfabetizacao` > Dataset: `alfabetizacao_gold`
4. Escolha o mart e crie gráficos dinamicamente

---

## 7. Conectar via Python/Jupyter

```python
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client(project="tc-fase2-alfabetizacao")

df = client.query("""
    SELECT sigla_uf, taxa_alfabetizacao_media
    FROM `tc-fase2-alfabetizacao.alfabetizacao_gold.agg_uf_indicadores`
    WHERE ano = 2024
    ORDER BY taxa_alfabetizacao_media DESC
""").to_dataframe()

print(df.head(10))
```

---

## 8. Troubleshooting

| Erro | Solução |
|------|---------|
| `GOOGLE_APPLICATION_CREDENTIALS não definido` | `$env:GOOGLE_APPLICATION_CREDENTIALS = "C:\...\service-account.json"` |
| `Bucket nao existe` | `gcloud storage buckets create gs://nome --location=southamerica-east1` |
| `Permission denied` | Verificar roles do Service Account (Storage Object Admin obrigatório) |
| `Tabela ja existe` | Script usa `WRITE_TRUNCATE` — sobrescreve por design (idempotente) |
| `API nao ativada` | `gcloud services enable storage.googleapis.com bigquery.googleapis.com` |
| `Gold path nao existe` | `python src/gold/01_gerar_marts_gold.py` primeiro |

---

## 9. Checklist Final

- [ ] Conta Google Cloud criada
- [ ] Projeto `tc-fase2-alfabetizacao` criado
- [ ] APIs Storage + BigQuery ativadas
- [ ] Service Account `tc-deploy-sa` criado com 3 roles
- [ ] `service-account.json` baixado e salvo na raiz (nunca commitar)
- [ ] Bucket GCS criado (`southamerica-east1`)
- [ ] `.\scripts\setup_gcs.ps1` executado com sucesso
- [ ] Dry run passou (`$env:DRY_RUN="true"`)
- [ ] Deploy DEV completou sem erros
- [ ] 13 tabelas visíveis no BigQuery
- [ ] Query de teste retornou resultados
- [ ] Dashboard Looker Studio criado (opcional)
