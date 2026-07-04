"""
src/ml/03_modelo_preditivo_risco.py
Modelo preditivo de RISCO de alfabetizacao por municipio.

Pergunta de negocio
-------------------
Com base em contexto TERRITORIAL e FISCAL (sem usar a propria taxa como
preditor), quais municipios estao estruturalmente em risco de baixa
alfabetizacao? Responde a "modelos preditivos de alfabetizacao por municipio"
citado no enunciado (Aplicacao em IA).

Modelagem
---------
Target (binario): risco = 1 se taxa_alfabetizacao_media < LIMIAR_RISCO (75),
                  senao 0. 75 e o corte do bucket "Bom/Razoavel".
Features (contextuais — SEM vazamento do target):
  - log1p(populacao_total)          [territorio / escala]
  - gasto_por_habitante_educacao    [financas SICONFI, quando disponivel]
  - regiao (one-hot: N, NE, CO, SE, S)   [contexto territorial]
Por que NAO usar medida_media_saeb / proporcao_aluno_nivel_* como feature?
  Sao praticamente a propria definicao de alfabetizacao (leakage). O objetivo e
  prever risco a partir de contexto estrutural, nao redescrever a taxa.

Modelo: RandomForestClassifier — robusto, sem necessidade de escalar, e fornece
importancia de features (explicabilidade para o gestor publico).

Saidas (em <base>/gold/agg_predicao_risco/):
  - metrics.json      : accuracy, precision, recall, f1, ROC-AUC, importancias
  - predicoes.parquet : id_municipio, sigla_uf, nome, prob_risco, risco_previsto
"""
import os
import sys
import json
import warnings
import logging

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, functions as F

warnings.filterwarnings("ignore")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MLPredicaoRisco")

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(script_dir, "..", ".."))
hadoop_home = os.path.join(project_root, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] = os.environ.get("PATH", "") + os.pathsep + os.path.join(hadoop_home, "bin")
if "JAVA_HOME" in os.environ:
    java_bin = os.path.join(os.environ["JAVA_HOME"], "bin")
    if java_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")

LIMIAR_RISCO = 75.0  # taxa media abaixo disso => municipio "em risco"

# Regiao por prefixo do codigo IBGE da UF (1o digito do id_municipio)
REGIAO_POR_UF = {
    "RO": "Norte", "AC": "Norte", "AM": "Norte", "RR": "Norte", "PA": "Norte", "AP": "Norte", "TO": "Norte",
    "MA": "Nordeste", "PI": "Nordeste", "CE": "Nordeste", "RN": "Nordeste", "PB": "Nordeste",
    "PE": "Nordeste", "AL": "Nordeste", "SE": "Nordeste", "BA": "Nordeste",
    "MG": "Sudeste", "ES": "Sudeste", "RJ": "Sudeste", "SP": "Sudeste",
    "PR": "Sul", "SC": "Sul", "RS": "Sul",
    "MS": "Centro-Oeste", "MT": "Centro-Oeste", "GO": "Centro-Oeste", "DF": "Centro-Oeste",
}


def get_spark_session():
    return SparkSession.builder.appName("MLPredicaoRisco").getOrCreate()


def resolve_paths():
    env = os.environ.get("ENV", "dev")
    base = "datalake_sample" if env == "dev" else "datalake"
    silver_dir = os.path.join(project_root, base, "silver")
    gold_dir = os.path.join(project_root, base, "gold")
    return silver_dir, gold_dir


def load_municipio_features(spark, silver_dir):
    """Carrega Silver (enriquecida se disponivel) e agrega por municipio."""
    path_enriched = os.path.join(silver_dir, "alfabetizacao_municipios_obt_enriquecido")
    path_obt = os.path.join(silver_dir, "alfabetizacao_municipios_obt")

    tem_siconfi = os.path.isdir(path_enriched) and any(
        f.endswith(".parquet") for _, _, fs in os.walk(path_enriched) for f in fs
    )
    df = spark.read.parquet(path_enriched if tem_siconfi else path_obt)
    logger.info(f"Silver carregado ({'enriquecido/SICONFI' if tem_siconfi else 'OBT base'}): {df.count()} linhas")

    aggs = [
        F.max("nome_municipio").alias("nome_municipio"),
        F.round(F.avg("taxa_alfabetizacao"), 2).alias("taxa_alfabetizacao_media"),
        F.round(F.avg("populacao_total"), 0).alias("populacao_total"),
    ]
    if "gasto_por_habitante_educacao" in df.columns:
        aggs.append(F.round(F.avg("gasto_por_habitante_educacao"), 2).alias("gasto_por_habitante_educacao"))

    pdf = df.groupBy("id_municipio", "sigla_uf").agg(*aggs).toPandas()
    return pdf, ("gasto_por_habitante_educacao" in pdf.columns)


def build_dataset(pdf, tem_siconfi):
    pdf = pdf.dropna(subset=["taxa_alfabetizacao_media", "populacao_total"]).copy()
    pdf = pdf[pdf["populacao_total"] > 0]

    pdf["regiao"] = pdf["sigla_uf"].map(REGIAO_POR_UF).fillna("Outra")
    pdf["log_populacao"] = np.log1p(pdf["populacao_total"])
    pdf["risco"] = (pdf["taxa_alfabetizacao_media"] < LIMIAR_RISCO).astype(int)

    feature_cols = ["log_populacao"]
    if tem_siconfi:
        # imputa gasto ausente pela mediana (municipios sem infra fiscal reportada)
        med = pdf["gasto_por_habitante_educacao"].median()
        pdf["gasto_por_habitante_educacao"] = pdf["gasto_por_habitante_educacao"].fillna(med)
        feature_cols.append("gasto_por_habitante_educacao")

    dummies = pd.get_dummies(pdf["regiao"], prefix="regiao")
    pdf = pd.concat([pdf, dummies], axis=1)
    feature_cols += list(dummies.columns)

    return pdf, feature_cols


def train_and_evaluate(pdf, feature_cols):
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import (
        accuracy_score, precision_score, recall_score, f1_score, roc_auc_score,
        confusion_matrix,
    )

    X = pdf[feature_cols].values
    y = pdf["risco"].values

    if len(np.unique(y)) < 2:
        logger.warning("Apenas uma classe presente — modelo nao treinavel. Abortando.")
        return None, None

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y
    )

    model = RandomForestClassifier(
        n_estimators=200, max_depth=8, min_samples_leaf=20,
        class_weight="balanced", random_state=42, n_jobs=-1,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]

    metrics = {
        "limiar_risco_taxa": LIMIAR_RISCO,
        "n_municipios": int(len(pdf)),
        "pct_em_risco": round(float(y.mean()) * 100, 1),
        "features": feature_cols,
        "accuracy": round(float(accuracy_score(y_test, y_pred)), 4),
        "precision": round(float(precision_score(y_test, y_pred, zero_division=0)), 4),
        "recall": round(float(recall_score(y_test, y_pred, zero_division=0)), 4),
        "f1": round(float(f1_score(y_test, y_pred, zero_division=0)), 4),
        "roc_auc": round(float(roc_auc_score(y_test, y_proba)), 4),
        "confusion_matrix": confusion_matrix(y_test, y_pred).tolist(),
        "feature_importances": {
            f: round(float(imp), 4)
            for f, imp in sorted(zip(feature_cols, model.feature_importances_),
                                 key=lambda kv: kv[1], reverse=True)
        },
    }

    logger.info(f"Accuracy={metrics['accuracy']} | ROC-AUC={metrics['roc_auc']} | "
                f"Recall={metrics['recall']} | Precision={metrics['precision']}")
    logger.info(f"Top features: {list(metrics['feature_importances'].items())[:3]}")

    # Predicao para TODOS os municipios (probabilidade de risco)
    pdf = pdf.copy()
    pdf["prob_risco"] = model.predict_proba(X)[:, 1].round(4)
    pdf["risco_previsto"] = (pdf["prob_risco"] >= 0.5).astype(int)
    predicoes = pdf[["id_municipio", "sigla_uf", "nome_municipio", "regiao",
                     "taxa_alfabetizacao_media", "prob_risco", "risco_previsto"]] \
        .sort_values("prob_risco", ascending=False)

    return metrics, predicoes


def save_outputs(metrics, predicoes, gold_dir):
    out_dir = os.path.join(gold_dir, "agg_predicao_risco")
    os.makedirs(out_dir, exist_ok=True)

    with open(os.path.join(out_dir, "metrics.json"), "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    predicoes.to_parquet(os.path.join(out_dir, "predicoes.parquet"), index=False)

    logger.info(f"Saidas salvas em: {out_dir}")
    logger.info(f"  metrics.json + predicoes.parquet ({len(predicoes)} municipios)")


def main():
    logger.info("=" * 60)
    logger.info("MODELO PREDITIVO DE RISCO DE ALFABETIZACAO")
    logger.info("=" * 60)

    silver_dir, gold_dir = resolve_paths()
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        pdf, tem_siconfi = load_municipio_features(spark, silver_dir)
    finally:
        spark.stop()

    pdf, feature_cols = build_dataset(pdf, tem_siconfi)
    logger.info(f"Dataset: {len(pdf)} municipios | {int(pdf['risco'].sum())} em risco "
                f"({pdf['risco'].mean()*100:.1f}%) | features: {feature_cols}")

    metrics, predicoes = train_and_evaluate(pdf, feature_cols)
    if metrics is None:
        sys.exit(1)

    save_outputs(metrics, predicoes, gold_dir)
    logger.info("\nResumo:\n" + json.dumps(metrics, indent=2, ensure_ascii=False)[:1500])
    logger.info("Done.")


if __name__ == "__main__":
    main()
