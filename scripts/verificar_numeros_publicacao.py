"""
Confere, direto no BigQuery de producao, se os numeros publicados no README
e em docs/NUMEROS_RECALCULADOS.md ainda batem com os dados atuais.

Uso:
    pip install google-cloud-bigquery
    gcloud auth application-default login   # precisa de acesso ao projeto GCP
    python scripts/verificar_numeros_publicacao.py

Roda as 3 queries documentadas em docs/NUMEROS_RECALCULADOS.md e compara com
os valores que estao no README. Nao altera nada — so leitura.
"""
import sys
from google.cloud import bigquery

PROJETO_DATASET = "tech-challenge-fase2-fiap.alfabetizacao_gold"

# Valores atualmente publicados no README (docs/NUMEROS_RECALCULADOS.md)
ESPERADO = {
    "custo_total_milhoes": 1218.3,
    "municipios_com_gap": 4679,
    "roi_fator_nacional": 28.69,
    "total_com_gap": 4679,
    "selecionados_no_orcamento": 2329,
    "pct_cobertura": 49.8,
    "alunos_beneficiados": 246563,
}

TOLERANCIA_PCT = 0.02  # 2% — variacao pequena eh esperada se a base mudou


def compara(nome, valor_real, valor_esperado):
    if valor_esperado is None or valor_real is None:
        status = "SEM REFERENCIA"
    else:
        diff_pct = abs(valor_real - valor_esperado) / max(abs(valor_esperado), 1e-9)
        status = "OK" if diff_pct <= TOLERANCIA_PCT else "MISMATCH"
    print(f"  {nome:32s} atual={valor_real!s:>16} esperado={valor_esperado!s:>12}  [{status}]")
    return status


def main():
    client = bigquery.Client()
    print("=" * 70)
    print("VERIFICACAO DOS NUMEROS ECONOMICOS PUBLICADOS")
    print(f"Projeto/dataset: {PROJETO_DATASET}")
    print("=" * 70)

    mismatches = []

    print("\n1. Custo total para atingir 80%")
    row = list(client.query(f"""
        SELECT ROUND(SUM(custo_estimado_para_atingir_80)/1e6, 1) AS custo_total_milhoes,
               COUNT(*) AS municipios_com_gap
        FROM `{PROJETO_DATASET}.agg_projecao_investimento`
    """).result())[0]
    for campo in ["custo_total_milhoes", "municipios_com_gap"]:
        if compara(campo, row[campo], ESPERADO[campo]) == "MISMATCH":
            mismatches.append(campo)

    print("\n2. ROI nacional (desperdicio / investimento)")
    row = list(client.query(f"""
        SELECT SUM(custo_total) AS desperdicio_total, SUM(investimento_total) AS investimento_total,
               ROUND(SUM(custo_total) / NULLIF(SUM(investimento_total), 0), 2) AS roi_fator_nacional
        FROM `{PROJETO_DATASET}.agg_roi_executivo`
    """).result())[0]
    print(f"  desperdicio_total = R$ {row['desperdicio_total']:,.0f}")
    print(f"  investimento_total = R$ {row['investimento_total']:,.0f}")
    if compara("roi_fator_nacional", row["roi_fator_nacional"], ESPERADO["roi_fator_nacional"]) == "MISMATCH":
        mismatches.append("roi_fator_nacional")

    print("\n3. Cobertura do orcamento de R$500M (knapsack)")
    row = list(client.query(f"""
        SELECT COUNT(*) AS total_com_gap, COUNTIF(selecionado_no_orcamento) AS selecionados_no_orcamento,
               ROUND(100.0 * COUNTIF(selecionado_no_orcamento) / COUNT(*), 1) AS pct_cobertura,
               ROUND(SUM(CASE WHEN selecionado_no_orcamento THEN beneficio_alunos_ate_80 ELSE 0 END), 0) AS alunos_beneficiados
        FROM `{PROJETO_DATASET}.agg_alocacao_otima`
    """).result())[0]
    for campo in ["total_com_gap", "selecionados_no_orcamento", "pct_cobertura", "alunos_beneficiados"]:
        if compara(campo, row[campo], ESPERADO[campo]) == "MISMATCH":
            mismatches.append(campo)

    print("\n" + "=" * 70)
    if mismatches:
        print(f"ATENCAO: {len(mismatches)} numero(s) divergente(s) do README: {mismatches}")
        print("Atualize o README/docs/NUMEROS_RECALCULADOS.md antes de publicar.")
        sys.exit(1)
    else:
        print("Todos os numeros batem com o README (dentro da tolerancia). OK para publicar.")


if __name__ == "__main__":
    main()
