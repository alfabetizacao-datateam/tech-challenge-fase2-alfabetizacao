"""
Deploy Gold Marts (Parquet) para Google Cloud Storage.

Uso:
    # dev (datalake_sample/gold)
    $env:ENV="dev"
    $env:GCS_BUCKET="seu-bucket-nome"
    $env:GOOGLE_APPLICATION_CREDENTIALS="C:/caminho/para/service-account.json"
    python src/cloud/01_deploy_gold_gcs.py

    # prod (datalake/gold)
    $env:ENV="prod"
    python src/cloud/01_deploy_gold_gcs.py

    # dry run (sem upload real)
    $env:DRY_RUN="true"
    python src/cloud/01_deploy_gold_gcs.py

Variaveis de ambiente obrigatorias:
    GCS_BUCKET              nome do bucket (ex: tc-fase2-gold-alfabetizacao)
    GOOGLE_APPLICATION_CREDENTIALS  caminho para service account JSON

Variaveis opcionais:
    ENV                     dev (default) ou prod
    GCS_PREFIX              prefixo dentro do bucket (default: gold)
    DRY_RUN                 true para simular sem fazer upload
"""

import os
import sys
from pathlib import Path

script_dir = Path(__file__).parent.resolve()
project_root = script_dir.parent.parent

sys.path.insert(0, str(project_root))


def _resolve_gold_dir():
    env = os.environ.get("ENV", "dev")
    if env == "prod":
        return project_root / "datalake" / "gold"
    return project_root / "datalake_sample" / "gold"


def _collect_parquet_files(gold_dir: Path) -> list[tuple[Path, str]]:
    """Retorna lista de (caminho_local, caminho_relativo) para todos os .parquet."""
    files = []
    for f in gold_dir.rglob("*.parquet"):
        rel = f.relative_to(gold_dir)
        files.append((f, str(rel).replace("\\", "/")))
    return sorted(files, key=lambda x: x[1])


def deploy_to_gcs(bucket_name: str, gold_dir: Path, prefix: str, dry_run: bool):
    from google.cloud import storage

    parquet_files = _collect_parquet_files(gold_dir)

    if not parquet_files:
        print(f"Nenhum arquivo .parquet encontrado em: {gold_dir}")
        print("Execute o pipeline Gold primeiro:")
        print("  python src/gold/01_gerar_marts_gold.py")
        sys.exit(1)

    total_bytes = sum(f.stat().st_size for f, _ in parquet_files)
    total_mb = total_bytes / (1024 * 1024)

    print(f"\n{'='*60}")
    print(f"DEPLOY GOLD -> GCS")
    print(f"{'='*60}")
    print(f"Bucket    : gs://{bucket_name}/{prefix}/")
    print(f"Fonte     : {gold_dir}")
    print(f"Arquivos  : {len(parquet_files)} .parquet")
    print(f"Tamanho   : {total_mb:.2f} MB")
    print(f"Modo      : {'DRY RUN (sem upload)' if dry_run else 'UPLOAD REAL'}")
    print(f"{'='*60}\n")

    if dry_run:
        for _, rel in parquet_files:
            print(f"  [dry] gs://{bucket_name}/{prefix}/{rel}")
        print(f"\nTotal que seria enviado: {len(parquet_files)} arquivos ({total_mb:.2f} MB)")
        _print_finops(total_mb, bucket_name, prefix, len(parquet_files))
        return

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    uploaded = 0
    failed = 0
    for local_path, rel in parquet_files:
        blob_name = f"{prefix}/{rel}"
        blob = bucket.blob(blob_name)
        try:
            blob.upload_from_filename(str(local_path))
            print(f"  [ok] gs://{bucket_name}/{blob_name}")
            uploaded += 1
        except Exception as e:
            print(f"  [ERRO] {rel}: {e}")
            failed += 1

    print(f"\n{'='*60}")
    print(f"RESULTADO: {uploaded} enviados, {failed} erros")
    print(f"{'='*60}")

    if failed == 0:
        _print_finops(total_mb, bucket_name, prefix, uploaded)
        print("\nProximo passo: carregar no BigQuery")
        print("  python src/cloud/02_load_bigquery.py")
    else:
        print(f"\nVerifique permissoes no bucket e o service account.")
        sys.exit(1)


def _print_finops(total_mb: float, bucket_name: str, prefix: str, num_files: int):
    """Imprime estimativa de custo GCS."""
    total_gb = total_mb / 1024

    # GCS Standard Storage: US$0.020/GB/mes
    # GCS Network Egress (mesma regiao): gratuito
    # GCS Class A operations (upload): US$0.05/10k ops
    # BigQuery load: gratuito (ate 1TB/mes)
    storage_monthly = total_gb * 0.020
    ops_cost = (num_files / 10000) * 0.05

    print(f"\n{'='*60}")
    print(f"FINOPS - ESTIMATIVA DE CUSTO (GCS)")
    print(f"{'='*60}")
    print(f"Volume armazenado     : {total_mb:.2f} MB ({total_gb:.4f} GB)")
    print(f"Storage Standard/mes  : US$ {storage_monthly:.4f}  (~R$ {storage_monthly * 5.8:.3f})")
    print(f"Operacoes upload (1x) : US$ {ops_cost:.4f}")
    print(f"Download (leitura BI) : US$ 0.00  (BigQuery le direto do bucket)")
    print(f"BigQuery load         : US$ 0.00  (free tier 10 GB/mes)")
    print(f"BigQuery storage      : US$ 0.00  (free tier ate 10 GB)")
    print(f"{'='*60}")
    print(f"TOTAL MENSAL ESTIMADO : US$ {storage_monthly:.4f}  (menos de R$ 0.50/mes)")
    print(f"{'='*60}")
    print(f"\nLink: gs://{bucket_name}/{prefix}/")
    print(f"Console: https://console.cloud.google.com/storage/browser/{bucket_name}")


def _check_prereqs():
    bucket = os.environ.get("GCS_BUCKET", "").strip()
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

    errors = []
    if not bucket:
        errors.append("GCS_BUCKET nao definido (ex: $env:GCS_BUCKET='meu-bucket')")
    if not creds:
        errors.append("GOOGLE_APPLICATION_CREDENTIALS nao definido (caminho para service-account.json)")
    elif not Path(creds).exists():
        errors.append(f"Arquivo de credenciais nao encontrado: {creds}")

    if errors:
        print("\nERRO: Pre-requisitos faltando:")
        for e in errors:
            print(f"  - {e}")
        print("\nGuia de setup:")
        print("  1. Acesse: https://console.cloud.google.com/")
        print("  2. Crie um bucket GCS (Standard, regiao us-east1 ou southamerica-east1)")
        print("  3. Crie um Service Account com papel 'Storage Object Admin'")
        print("  4. Baixe a chave JSON e defina GOOGLE_APPLICATION_CREDENTIALS")
        print("\nOu rode em dry run primeiro:")
        print("  $env:DRY_RUN='true'; $env:GCS_BUCKET='qualquer-nome'; python src/cloud/01_deploy_gold_gcs.py")
        sys.exit(1)

    return bucket


def main():
    dry_run = os.environ.get("DRY_RUN", "false").lower() == "true"

    if dry_run:
        bucket = os.environ.get("GCS_BUCKET", "meu-bucket-alfabetizacao")
    else:
        bucket = _check_prereqs()

    prefix = os.environ.get("GCS_PREFIX", "gold")
    gold_dir = _resolve_gold_dir()

    if not gold_dir.exists():
        print(f"Diretorio gold nao existe: {gold_dir}")
        print("Execute primeiro: python src/gold/01_gerar_marts_gold.py")
        sys.exit(1)

    deploy_to_gcs(bucket, gold_dir, prefix, dry_run)


if __name__ == "__main__":
    main()
