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
  description         = "Panorama por UF e ano: taxa de alfabetizacao, meta e gap"

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
  description         = "Ranking nacional de urgencia por municipio, para priorizar por onde comecar"

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
  description         = "Comparacao de desempenho entre redes de ensino (Municipal, Estadual, Federal, Privada)"

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
  description         = "Matriz equidade vs eficiencia: cruza severidade e deficit per capita em 4 quadrantes estrategicos"

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
  description         = "Classificacao de cada municipio por eficiencia do gasto (Eficiente, Alto Gasto, Subinvestido, Ineficiente)"

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
  description         = "Quanto do orcamento ja existente esta sendo desperdicado por ma gestao, por municipio"

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
  description         = "Quanto custaria levar cada municipio a 80% de alfabetizacao"

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
  description         = "Os 10 municipios mais prioritarios de cada UF, para acao do gestor estadual"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_top10_uf/*"]
  }
}

resource "google_bigquery_table" "agg_alocacao_otima" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_alocacao_otima"
  project             = var.project_id
  deletion_protection = false
  description         = "Alocacao otima de orcamento fixo entre municipios, via Knapsack Greedy"

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
  description         = "Distribuicao de municipios por qualidade de dados (bucket Critico/Ruim/Razoavel/Excelente), por UF"

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
  description         = "Forca da relacao entre gasto e taxa de alfabetizacao por UF (correlacao de Pearson, requer SICONFI)"

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
  description         = "ROI executivo por UF: custo da ineficiencia vs investimento necessario para fechar o gap"

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
  description         = "3 estrategias de alocacao de orcamento comparadas (Greedy, Maximo Impacto, Menor Custo Per Capita)"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_alocacao_otima_estrategias/*"]
  }
}

resource "google_bigquery_table" "agg_evolucao_temporal" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_evolucao_temporal"
  project             = var.project_id
  deletion_protection = false
  description         = "Evolucao ano a ano da taxa de alfabetizacao por UF, com variacao em pontos percentuais e %"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_evolucao_temporal/*"]
  }
}

resource "google_bigquery_table" "agg_vulnerabilidade_ml" {
  count               = var.create_tables ? 1 : 0
  dataset_id          = google_bigquery_dataset.gold.dataset_id
  table_id            = "agg_vulnerabilidade_ml"
  project             = var.project_id
  deletion_protection = false
  description         = "Segmentacao de municipios por vulnerabilidade educacional via K-Means (Spark MLlib) — educacao, territorio e financas"

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"
    source_uris   = ["gs://${var.bucket_name}/gold/agg_vulnerabilidade_ml/*"]
  }
}
