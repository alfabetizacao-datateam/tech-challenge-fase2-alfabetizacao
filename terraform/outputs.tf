output "bucket_url" {
  value       = module.gcs.bucket_url
  description = "URL do bucket para usar nos scripts Spark"
}

output "bucket_name" {
  value       = module.gcs.bucket_name
  description = "Nome do bucket GCS"
}

output "cluster_name" {
  value       = module.dataproc.cluster_name
  description = "Nome do cluster Dataproc para gcloud jobs submit"
}

output "cluster_region" {
  value       = module.dataproc.cluster_region
  description = "Região do cluster"
}

output "bigquery_dataset" {
  value       = module.bigquery.dataset_id
  description = "Dataset BigQuery para queries"
}

output "next_steps" {
  description = "Roteiro completo de deploy (sequencia obrigatoria)"
  value       = <<-EOT

    ===== ROTEIRO DE DEPLOY (sequencia obrigatoria) =====

    BUCKET  = ${module.gcs.bucket_url}
    CLUSTER = ${module.dataproc.cluster_name}
    REGION  = ${module.dataproc.cluster_region}

    -- ETAPA 0: Upload de dados e scripts --
    gsutil -m cp dados/*.csv ${module.gcs.bucket_url}/input/
    gsutil -m cp -r src/ ${module.gcs.bucket_url}/scripts/

    -- ETAPA 1: Bronze --
    gcloud dataproc jobs submit pyspark \
      ${module.gcs.bucket_url}/scripts/cloud/dataproc_01_bronze.py \
      --cluster=${module.dataproc.cluster_name} --region=${module.dataproc.cluster_region} \
      -- --bucket=${module.gcs.bucket_url}

    -- ETAPA 2: Silver (OBT base + IBGE) --
    gcloud dataproc jobs submit pyspark \
      ${module.gcs.bucket_url}/scripts/cloud/dataproc_02_silver.py \
      --cluster=${module.dataproc.cluster_name} --region=${module.dataproc.cluster_region} \
      -- --bucket=${module.gcs.bucket_url}

    -- ETAPA 3: SICONFI (enriquecimento financeiro) --
    CRITICO: sem esta etapa os marts financeiros ficam vazios no BigQuery!
    1a execucao: ~7 min (3.500 requests / 8 workers paralelos)
    Execucoes seguintes: segundos (cache em GCS/siconfi/cache.json)

    gcloud dataproc jobs submit pyspark \
      ${module.gcs.bucket_url}/scripts/cloud/dataproc_04_siconfi.py \
      --cluster=${module.dataproc.cluster_name} --region=${module.dataproc.cluster_region} \
      -- --bucket=${module.gcs.bucket_url} --ano=2024

    -- ETAPA 4: Gold Marts (14 marts) --
    Roda APOS SICONFI para ativar os 6 marts financeiros

    gcloud dataproc jobs submit pyspark \
      ${module.gcs.bucket_url}/scripts/cloud/dataproc_03_gold.py \
      --cluster=${module.dataproc.cluster_name} --region=${module.dataproc.cluster_region} \
      -- --bucket=${module.gcs.bucket_url}

    -- ETAPA 5: Habilitar BigQuery --
    Edite terraform.tfvars: create_bq_tables = true
    terraform apply

    -- ETAPA 6: Carga BigQuery --
    GCS_BUCKET=${module.gcs.bucket_name} \
    GOOGLE_CLOUD_PROJECT=${var.project_id} \
    GOOGLE_APPLICATION_CREDENTIALS=/caminho/service-account.json \
    python src/cloud/02_load_bigquery.py

    -- VERIFICACAO --
    gsutil ls ${module.gcs.bucket_url}/silver/alfabetizacao_municipios_obt_enriquecido/
    gsutil ls ${module.gcs.bucket_url}/gold/
    gsutil ls ${module.gcs.bucket_url}/siconfi/cache.json

    BigQuery:
    https://console.cloud.google.com/bigquery?project=${var.project_id}

  EOT
}
