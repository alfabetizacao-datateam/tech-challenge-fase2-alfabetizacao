import os, sys, warnings, logging
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, functions as F

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MLOptimization")

# Modelo de custo per capita (ver ADR-012/ADR-013). Usado apenas no fallback
# abaixo, quando agg_projecao_investimento nao existe localmente — precisa
# ficar em sincronia manual com as mesmas constantes em src/gold/01_gerar_marts_gold.py
# e src/cloud/dataproc_03_gold.py. Sem a fracao alfabetizavel, o custo usa
# populacao TOTAL do municipio (nao alunos) e infla o resultado em ~77x.
CUSTO_PONTO_PER_CAPITA_DEFAULT = 20.0  # R$/habitante/ponto percentual (ADR-012)
FRACAO_POPULACAO_ALFABETIZAVEL = 0.013  # coorte de idade unica ~7 anos (ADR-013)

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
hadoop_home = os.path.join(project_root, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + os.path.join(hadoop_home, "bin")
if "JAVA_HOME" in os.environ:
    java_bin = os.path.join(os.environ["JAVA_HOME"], "bin")
    if java_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")


def resolve_paths():
    env = os.environ.get("ENV", "dev")
    base = "datalake_sample" if env == "dev" else "datalake"
    gold_dir = os.path.join(project_root, base, "gold")
    return gold_dir


def load_clusters(gold_dir):
    path = os.path.join(gold_dir, "agg_clusters_municipios", "dados.parquet")
    if not os.path.exists(path):
        logger.warning(f"Clusters nao encontrados em {path}. Execute 01_clusterizar_municipios.py primeiro.")
        return None
    pdf = pd.read_parquet(path)
    logger.info(f"Clusters carregados: {len(pdf)} municipios, {len(pdf.columns)} colunas")
    return pdf


def load_projecao(gold_dir):
    spark = SparkSession.builder.appName("MLOptimization").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = spark.read.parquet(os.path.join(gold_dir, "agg_projecao_investimento"))
        pdf = df.toPandas()
        return pdf
    except Exception:
        return None
    finally:
        spark.stop()


def greedy_knapsack(pdf, budget, value_col, cost_col):
    pdf = pdf[pdf[cost_col] > 0].copy()
    pdf["relacao_custo_beneficio"] = pdf[value_col] / pdf[cost_col]
    pdf = pdf.sort_values("relacao_custo_beneficio", ascending=False)

    selected = []
    remaining = budget
    for _, row in pdf.iterrows():
        if row[cost_col] <= remaining:
            selected.append(row)
            remaining -= row[cost_col]
        if remaining <= 0:
            break

    df_result = pd.DataFrame(selected)
    total_cost = df_result[cost_col].sum() if not df_result.empty else 0
    total_value = df_result[value_col].sum() if not df_result.empty else 0

    logger.info(f"Orcamento: R$ {budget:,.2f}")
    logger.info(f"Gasto total: R$ {total_cost:,.2f}")
    logger.info(f"Saldo nao utilizado: R$ {remaining:,.2f}")
    logger.info(f"Valor total gerado: {total_value:.2f} pontos de alfabetizacao")
    logger.info(f"Municipios contemplados: {len(df_result)}")

    return df_result, total_cost, total_value, remaining


def optimize_with_clusters(pdf_proj, pdf_clusters, budget):
    pdf_proj = pdf_proj.copy()
    pdf_clusters = pdf_clusters[["id_municipio", "cluster", "nome_cluster"]].copy()
    pdf_proj["id_municipio"] = pdf_proj["id_municipio"].astype(str).str.strip()
    pdf_clusters["id_municipio"] = pdf_clusters["id_municipio"].astype(str).str.strip()
    pdf_merged = pdf_proj.merge(pdf_clusters, on="id_municipio", how="left")

    results = {}
    for cluster_id in sorted(pdf_merged["cluster"].dropna().unique()):
        subset = pdf_merged[pdf_merged["cluster"] == cluster_id].copy()
        budget_share = budget * (len(subset) / max(len(pdf_merged), 1))
        logger.info(f"\nCluster {int(cluster_id)} — orcamento proporcional: R$ {budget_share:,.2f}")
        df_sel, tc, tv, rem = greedy_knapsack(
            subset, budget_share,
            value_col="gap_ate_80",
            cost_col="custo_estimado_para_atingir_80"
        )
        cluster_name = subset["nome_cluster"].iloc[0] if not subset.empty else f"Cluster {int(cluster_id)}"
        results[int(cluster_id)] = {
            "cluster_nome": cluster_name,
            "orcamento_destinado": budget_share,
            "gasto_total": tc,
            "valor_gerado_pontos": tv,
            "municipios_contemplados": len(df_sel),
            "municipios_lista": df_sel.to_dict("records") if not df_sel.empty else []
        }

    return results


def main():
    logger.info("=" * 60)
    logger.info("ML - OTIMIZACAO DE ALOCACAO (KNAPSACK GREEDY)")
    logger.info("=" * 60)

    gold_dir = resolve_paths()

    pdf_clusters = load_clusters(gold_dir)
    if pdf_clusters is None:
        return

    pdf_proj = load_projecao(gold_dir)
    if pdf_proj is None:
        logger.warning(
            "Projecao de investimento nao encontrada. Usando dados dos clusters "
            f"com benchmark default (R${CUSTO_PONTO_PER_CAPITA_DEFAULT}/hab/ponto) — "
            "rode 01_gerar_marts_gold.py antes para usar o benchmark calibrado via SICONFI."
        )
        pdf_proj = pdf_clusters.copy()
        pdf_proj["gap_ate_80"] = (80 - pdf_proj["taxa_alfabetizacao_media"]).clip(lower=0)
        pdf_proj["populacao_alfabetizavel_estimada"] = (
            pdf_proj["populacao_total"] * FRACAO_POPULACAO_ALFABETIZAVEL
        )
        pdf_proj["custo_estimado_para_atingir_80"] = (
            pdf_proj["gap_ate_80"] * CUSTO_PONTO_PER_CAPITA_DEFAULT * pdf_proj["populacao_alfabetizavel_estimada"]
        )

    budget = 500_000_000
    logger.info(f"\n{'=' * 60}")
    logger.info(f"CENARIO 1: OTIMIZACAO GLOBAL — Orcamento R$ {budget:,.2f}")
    logger.info(f"{'=' * 60}")

    df_global, tc, tv, rem = greedy_knapsack(
        pdf_proj, budget,
        value_col="gap_ate_80",
        cost_col="custo_estimado_para_atingir_80"
    )

    logger.info(f"\n{'=' * 60}")
    logger.info(f"CENARIO 2: OTIMIZACAO POR CLUSTER — Orcamento R$ {budget:,.2f}")
    logger.info(f"{'=' * 60}")

    results = optimize_with_clusters(pdf_proj, pdf_clusters, budget)

    output = {
        "parametros": {
            "orcamento_total": budget,
            "metodo": "Knapsack Greedy (relacao valor/custo)",
            "coluna_valor": "gap_ate_80 (pontos de alfabetizacao para 80%)",
            "coluna_custo": "custo_estimado_para_atingir_80 (R$)"
        },
        "cenario_global": {
            "orcamento": budget,
            "gasto_total": float(tc),
            "saldo_nao_utilizado": float(rem),
            "pontos_alfabetizacao_gerados": float(tv),
            "municipios_contemplados": len(df_global),
            "municipios": df_global.to_dict("records") if not df_global.empty else []
        },
        "cenario_por_cluster": {
            str(k): {
                "cluster_nome": v["cluster_nome"],
                "orcamento_destinado": float(v["orcamento_destinado"]),
                "gasto_total": float(v["gasto_total"]),
                "pontos_alfabetizacao_gerados": float(v["valor_gerado_pontos"]),
                "municipios_contemplados": v["municipios_contemplados"]
            } for k, v in results.items()
        }
    }

    output_path = os.path.join(gold_dir, "agg_alocacao_otima", "resultado.json")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)

    logger.info(f"\nResultado salvo em: {output_path}")
    logger.info(f"\nResumo:\n{json.dumps(output, indent=2, ensure_ascii=False, default=str)[:2000]}")
    logger.info("Done.")


if __name__ == "__main__":
    import json
    main()
