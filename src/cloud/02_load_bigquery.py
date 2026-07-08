"""
Carrega os Gold Marts do GCS para tabelas no BigQuery.

Prereq: 01_deploy_gold_gcs.py ja executado com sucesso.

Uso:
    $env:GCS_BUCKET="seu-bucket-nome"
    $env:GOOGLE_CLOUD_PROJECT="seu-project-id"
    $env:GOOGLE_APPLICATION_CREDENTIALS="C:/caminho/para/service-account.json"
    python src/cloud/02_load_bigquery.py

Variaveis opcionais:
    BQ_DATASET      dataset de destino no BigQuery (default: alfabetizacao_gold)
    GCS_PREFIX      prefixo no bucket (default: gold)
    DRY_RUN         true para listar tabelas sem carregar
"""

import os
import sys
from pathlib import Path

script_dir = Path(__file__).parent.resolve()
project_root = script_dir.parent.parent

MARTS = [
    # Base (sempre geradas)
    "agg_uf_indicadores",
    "agg_evolucao_temporal",
    "agg_municipio_ranking",
    "agg_rede_indicadores",
    "agg_priorizacao",
    "agg_top10_uf",
    "agg_vulnerabilidade_ml",
    "agg_alocacao_otima",
    "agg_qualidade_resumo",
    # Condicionais ao SICONFI (financeiras)
    "agg_eficiencia_financeira",
    "agg_custo_ineficiencia",
    "agg_projecao_investimento",
    "agg_correlacoes_uf",
    "agg_roi_executivo",
    "agg_alocacao_otima_estrategias",
]


def _check_prereqs():
    bucket = os.environ.get("GCS_BUCKET", "").strip()
    project = os.environ.get("GOOGLE_CLOUD_PROJECT", "").strip()
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

    errors = []
    if not bucket:
        errors.append("GCS_BUCKET nao definido")
    if not project:
        errors.append("GOOGLE_CLOUD_PROJECT nao definido (ex: meu-projeto-123)")
    # GOOGLE_APPLICATION_CREDENTIALS e OPCIONAL: em Cloud Shell / GCE a
    # autenticacao usa Application Default Credentials (ADC) automaticamente.
    # So validamos o arquivo se a variavel estiver explicitamente definida.
    if creds and not Path(creds).exists():
        errors.append(f"Arquivo de credenciais definido mas nao encontrado: {creds}")

    if errors:
        print("ERRO: Pre-requisitos faltando:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)

    return bucket, project


def load_to_bigquery(bucket: str, project: str, dataset: str, prefix: str, dry_run: bool):
    from google.cloud import bigquery

    client = bigquery.Client(project=project)

    # Cria o dataset se nao existir
    dataset_ref = bigquery.Dataset(f"{project}.{dataset}")
    dataset_ref.location = "US"

    if not dry_run:
        try:
            client.create_dataset(dataset_ref, exists_ok=True)
            print(f"Dataset: {project}.{dataset}")
        except Exception as e:
            print(f"Erro ao criar dataset: {e}")
            sys.exit(1)

    print(f"\n{'='*60}")
    print(f"CARGA GCS -> BIGQUERY")
    print(f"{'='*60}")
    print(f"Projeto   : {project}")
    print(f"Dataset   : {dataset}")
    print(f"Fonte GCS : gs://{bucket}/{prefix}/")
    print(f"Modo      : {'DRY RUN' if dry_run else 'CARGA REAL'}\n")

    loaded = 0
    failed = 0

    for mart in MARTS:
        # BigQuery nao suporta glob recursivo (**). O Gold salva Parquet plano
        # (sem particionamento Hive) — um nivel de *.parquet basta.
        gcs_uri = f"gs://{bucket}/{prefix}/{mart}/*.parquet"
        table_id = f"{project}.{dataset}.{mart}"

        if dry_run:
            print(f"  [dry] {gcs_uri} -> {table_id}")
            loaded += 1
            continue

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        try:
            load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
            load_job.result()  # aguarda conclusao
            table = client.get_table(table_id)
            print(f"  [ok] {mart}: {table.num_rows:,} linhas -> {table_id}")
            loaded += 1
        except Exception as e:
            print(f"  [ERRO] {mart}: {e}")
            failed += 1

    print(f"\n{'='*60}")
    print(f"RESULTADO: {loaded} tabelas carregadas, {failed} erros")
    print(f"{'='*60}")

    if not dry_run and failed == 0:
        print(f"\nQuery de verificacao:")
        print(f"  SELECT * FROM `{project}.{dataset}.agg_uf_indicadores` LIMIT 10")
        print(f"\nConsole BigQuery:")
        print(f"  https://console.cloud.google.com/bigquery?project={project}")
        print(f"\nConectar ao Looker Studio / Data Studio:")
        print(f"  Fonte: BigQuery -> {project} -> {dataset}")


def main():
    dry_run = os.environ.get("DRY_RUN", "false").lower() == "true"

    if dry_run:
        bucket = os.environ.get("GCS_BUCKET", "meu-bucket")
        project = os.environ.get("GOOGLE_CLOUD_PROJECT", "meu-projeto")
    else:
        bucket, project = _check_prereqs()

    dataset = os.environ.get("BQ_DATASET", "alfabetizacao_gold")
    prefix = os.environ.get("GCS_PREFIX", "gold")

    load_to_bigquery(bucket, project, dataset, prefix, dry_run)


if __name__ == "__main__":
    main()
