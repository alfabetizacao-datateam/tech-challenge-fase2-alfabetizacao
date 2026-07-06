# Diagrama da Pipeline — Alfabetização no Brasil

> Renderiza nativamente no GitHub (Mermaid). Complementa o README com a visão
> de arquitetura e o fluxo de dados exigidos no enunciado do Tech Challenge.

## 1. Arquitetura geral (Medalhão híbrido: Batch + Streaming, GCP)

```mermaid
flowchart TB
    subgraph FONTES["Fontes de Dados (Base dos Dados / INEP / IBGE / Tesouro)"]
        F1["Indicador Criança Alfabetizada\n(UF, Município, Brasil, Metas)"]
        F2["IBGE\n(Município, População)"]
        F3["SICONFI — API Tesouro Nacional\n(Despesa em Educação)"]
        F4["Microdados de Alunos\n(SAEB individual)"]
        F5["Eventos simulados\n(atualização de indicadores)"]
    end

    subgraph BRONZE["BRONZE — dados brutos, sem transformação"]
        B1["01_ingestao_bronze_batch.py"]
        B2["06_alunos_bronze_to_silver.py (raw)"]
        B3["01_ingestao_siconfi.py\n(cache local + API paralela)"]
        B4["01_producer_eventos.py\n(JSON simulado)"]
    end

    subgraph SILVER["SILVER — One Big Table + integração"]
        S1["alfabetizacao_municipios_obt\n(join Indicador + IBGE, id_municipio STRING)"]
        S2["+ SICONFI\nalfabetizacao_municipios_obt_enriquecido"]
        S3["+ Metas KNN (K=5, por UF)\nalfabetizacao_municipios_obt_com_metas_imputadas"]
        S4["+ Microdados alunos\nalfabetizacao_municipios_obt_final"]
        S5["02_consumer_streaming.py\n(Spark Structured Streaming)"]
    end

    subgraph GOLD["GOLD — 16 marts analíticos (partição: ano)"]
        G1["Executivos:\nagg_uf_indicadores, agg_evolucao_temporal,\nagg_rede_indicadores, agg_qualidade_resumo"]
        G2["Priorização:\nagg_municipio_ranking, agg_top10_uf,\nagg_priorizacao"]
        G3["Financeiro (ADR-012/013):\nagg_eficiencia_financeira, agg_custo_ineficiencia,\nagg_projecao_investimento, agg_roi_executivo"]
        G4["IA / ML:\nagg_clusters_municipios, agg_vulnerabilidade_ml (K-Means),\nagg_alocacao_otima(_estrategias) (Knapsack)"]
        G5["Correlação:\nagg_correlacoes_uf"]
    end

    subgraph CONSUMO["Consumo"]
        C1[("BigQuery\ntech-challenge-fase2-fiap.alfabetizacao_gold")]
        C2["Looker Studio\n(dashboards executivos)"]
        C3["Notebooks\n(EDA / validação)"]
        C4["Modelo preditivo de risco\n(RandomForest — sem leakage)"]
    end

    F1 --> B1
    F2 --> B1
    F4 --> B2
    F3 --> B3
    F5 --> B4

    B1 --> S1
    B2 --> S4
    B3 --> S2
    B4 --> S5

    S1 --> S2 --> S3 --> S4
    S5 -.enriquece com Silver.-> S4

    S4 --> G1
    S4 --> G2
    S4 --> G3
    S4 --> G4
    S4 --> G5

    G1 --> C1
    G2 --> C1
    G3 --> C1
    G4 --> C1
    G5 --> C1
    C1 --> C2
    C1 --> C3
    G4 --> C4
```

## 2. Fluxo de dados — camada Silver (integração das bases)

```mermaid
flowchart LR
    A["Indicador Alfabetização\n(Base dos Dados)"] -->|"JOIN id_municipio (STRING)\nADR-005"| B["OBT Silver\nalfabetizacao_municipios_obt"]
    IBGE["IBGE\n(nome_municipio, população)"] -->|"enriquecimento"| B
    B -->|"LEFT JOIN\nid_municipio + ano + rede"| C["+ SICONFI\ndespesa_educacao,\ngasto_por_habitante_educacao"]
    C -->|"KNN K=5 por UF\n(taxa + população + déficit)\nADR-004"| D["+ Metas imputadas\nmeta_alfabetizacao_2024_imputada\n(flag is_imputado)"]
    D -->|"JOIN microdados SAEB"| E["OBT final\n+ taxa_alunos_alfabetizados_microdados"]
    E --> F["GOLD (16 marts)"]

    note1["proporcao_aluno_nivel_* preserva NULL\n(ADR-003 — nunca fillna(0))"]
    note1 -.-> B
```

## 3. Fluxo de custo/priorização (ADR-012 + ADR-013 — o modelo econômico)

```mermaid
flowchart TB
    P["populacao_total (IBGE)\ntodos os habitantes"] -->|"× 0,013\n(coorte de idade única ~7 anos)"| Q["populacao_alfabetizavel_estimada"]
    T["taxa_alfabetizacao_media"] -->|"80 − taxa"| G["gap_ate_80 (pp)"]
    S["SICONFI: gasto_por_habitante_educacao\ndos municípios Eficientes"] -->|"mediana"| CM["custo_marginal_per_capita\n(R$/hab/ponto — fallback R$20)"]
    G --> X["custo_estimado_para_atingir_80"]
    Q --> X
    CM --> X
    X --> R1["agg_projecao_investimento"]
    X --> R2["agg_roi_executivo\n(desperdício ÷ investimento)"]
    X --> R3["agg_alocacao_otima\n(Knapsack Greedy, orçamento R$500M)"]
```

## Notas de leitura

- **Batch** cobre Indicador de Alfabetização, IBGE, SICONFI e microdados — dados históricos, reprocessados periodicamente.
- **Streaming** (Spark Structured Streaming / File Stream, ver ADR-006) simula atualização quase-tempo-real de indicadores; arquitetura pronta para trocar a fonte por Kafka sem reescrever a lógica de consumo.
- Os 3 diagramas cobrem exatamente os itens do enunciado: "descrição da arquitetura da solução", "diagrama da pipeline" e "fluxo de dados".
- Fonte de verdade do modelo econômico: `src/cloud/dataproc_03_gold.py` (produção/BigQuery) e, após o fix de 2026-07-06, também `src/gold/01_gerar_marts_gold.py` (local) — ver `docs/adr/ADR-012-modelo-custo-marginal.md` e `ADR-013-fracao-populacao-alfabetizavel.md`.
