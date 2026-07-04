resource "google_bigquery_dataset" "gold" {
  dataset_id  = var.dataset_id
  location    = var.location
  project     = var.project_id
  description = "Gold layer — marts analíticos alfabetização Brasil (Tech Challenge Fase 2)"

  delete_contents_on_destroy = true
}

# ============================================================
# EXTERNAL TABLES — criadas apenas após Spark gerar Parquet
# Fase 1: create_tables = false (padrão)
# Fase 2: create_tables = true (após pipeline Spark concluir)
# ============================================================

resource "google_bigquery_table" "agg_uf_indicadores" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_uf_indicadores"
  project             = var.project_id
  deletion_protection = false
  description         = "Visão executiva por UF e ano — 49 linhas, 13 colunas"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_uf_indicadores/*"]
  }
}

resource "google_bigquery_table" "agg_municipio_ranking" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_municipio_ranking"
  project             = var.project_id
  deletion_protection = false
  description         = "Ranking de priorização municipal — 4.342 linhas, 13 colunas"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_municipio_ranking/*"]
  }
}

resource "google_bigquery_table" "agg_rede_indicadores" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_rede_indicadores"
  project             = var.project_id
  deletion_protection = false
  description         = "Comparação entre redes (Municipal, Estadual, Federal, Privada)"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_rede_indicadores/*"]
  }
}

resource "google_bigquery_table" "agg_priorizacao" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_priorizacao"
  project             = var.project_id
  deletion_protection = false
  description         = "Matriz equidade vs eficiência — 4 quadrantes estratégicos"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_priorizacao/*"]
  }
}

resource "google_bigquery_table" "agg_eficiencia_financeira" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_eficiencia_financeira"
  project             = var.project_id
  deletion_protection = false
  description         = "Eficiência financeira por município (SICONFI vs alfabetização)"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_eficiencia_financeira/*"]
  }
}

resource "google_bigquery_table" "agg_custo_ineficiencia" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_custo_ineficiencia"
  project             = var.project_id
  deletion_protection = false
  description         = "Custo do desperdício por município — R$13,65bi estimado"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_custo_ineficiencia/*"]
  }
}

resource "google_bigquery_table" "agg_projecao_investimento" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_projecao_investimento"
  project             = var.project_id
  deletion_protection = false
  description         = "Projeção de investimento para atingir 80% de alfabetização"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_projecao_investimento/*"]
  }
}

resource "google_bigquery_table" "agg_top10_uf" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_top10_uf"
  project             = var.project_id
  deletion_protection = false
  description         = "Top 10 municípios prioritários por UF — 487 linhas"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_top10_uf/*"]
  }
}

resource "google_bigquery_table" "agg_clusters_municipios" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_clusters_municipios"
  project             = var.project_id
  deletion_protection = false
  description         = "Segmentação por regras — 4 perfis de município (taxa × deficit)"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_clusters_municipios/*"]
  }
}

resource "google_bigquery_table" "agg_alocacao_otima" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_alocacao_otima"
  project             = var.project_id
  deletion_protection = false
  description         = "ML: Knapsack Greedy — alocação ótima de R$500M"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_alocacao_otima/*"]
  }
}

resource "google_bigquery_table" "agg_qualidade_resumo" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_qualidade_resumo"
  project             = var.project_id
  deletion_protection = false
  description         = "Distribuição de municípios por bucket de qualidade e UF (Crítico/Ruim/Razoável/Excelente)"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_qualidade_resumo/*"]
  }
}

resource "google_bigquery_table" "agg_correlacoes_uf" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_correlacoes_uf"
  project             = var.project_id
  deletion_protection = false
  description         = "Correlação Pearson gasto×taxa por UF (requer SICONFI) — análise de força relação custo-resultado"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_correlacoes_uf/*"]
  }
}

resource "google_bigquery_table" "agg_roi_executivo" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_roi_executivo"
  project             = var.project_id
  deletion_protection = false
  description         = "ROI executivo por UF — custo da ineficiência vs investimento necessário (19.4× nacional)"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_roi_executivo/*"]
  }
}

resource "google_bigquery_table" "agg_alocacao_otima_estrategias" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_alocacao_otima_estrategias"
  project             = var.project_id
  deletion_protection = false
  description         = "3 estratégias de alocação de R$500M comparadas (Greedy, Máx Impacto, Menor Custo Per Capita)"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_alocacao_otima_estrategias/*"]
  }
}
