#  Dashboard Analítico — Inteligência de Dados para Apoio à Alfabetização no Brasil

Este diretório reúne o dashboard desenvolvido para o projeto **Tech Challenge - Fase 2**, responsável por transformar os dados consolidados da camada **Gold** em informações estratégicas para apoio à tomada de decisão na gestão pública.

O dashboard foi desenvolvido no **Looker Studio**, utilizando como fonte de dados tabelas analíticas armazenadas no **BigQuery**, permitindo análises interativas sobre alfabetização infantil, investimentos públicos e priorização de municípios.

---

## Objetivo

Disponibilizar uma visão executiva sobre a situação da alfabetização no Brasil, respondendo perguntas como:

- Qual é o panorama nacional da alfabetização?
- Onde estão os municípios mais críticos?
- O investimento realizado explica os resultados?
- Quanto custa não agir?
- Quais municípios deveriam receber prioridade nos investimentos?

---

# Estrutura do Dashboard

O dashboard foi organizado em seis páginas, conduzindo o usuário por uma narrativa analítica.

## 1. Panorama Nacional

Apresenta uma visão consolidada dos principais indicadores nacionais.

Principais informações:

- Taxa média de alfabetização
- Municípios abaixo da meta nacional
- Investimento estimado necessário
- Ranking dos estados
- Evolução dos estados com melhor desempenho

---

## 2. Onde Está o Problema?

Identifica os municípios prioritários para intervenção.

Inclui:

- Ranking dos municípios mais críticos
- Classificação por nível de risco
- Gap para a meta de alfabetização
- Comparação entre redes de ensino

---

## 3. Por Que Isso Acontece?

Analisa a relação entre investimento e desempenho educacional.

Visualizações:

- Dispersão entre gasto por aluno e alfabetização
- Correlação de Pearson por estado
- Identificação de padrões de eficiência

---

## 4. A Tese Central: O Custo da Inércia

Compara o custo de resolver o problema com o desperdício decorrente da ineficiência na aplicação dos recursos públicos.

Indicadores apresentados:

- ROI nacional
- ROI por estado
- Investimento necessário
- Estimativa de desperdício por ineficiência

---

## 5. Onde Investir

Propõe uma estratégia de priorização baseada em impacto social e eficiência.

Análises disponíveis:

- Matriz de priorização
- Ranking dos municípios prioritários
- Segmentação por quadrantes estratégicos

---

## 6. Plano de Ação

Simula a utilização de um orçamento limitado para maximizar o impacto social.

Indicadores:

- Municípios contemplados
- Cobertura do orçamento
- Alunos beneficiados
- Ranking por custo-benefício

---

# Fonte dos Dados

Os indicadores foram construídos a partir da integração de bases públicas:

- INEP
- IBGE
- SICONFI

Todos os dados passaram pelo pipeline analítico desenvolvido em ambiente Cloud, seguindo a Arquitetura Medalhão (**Bronze → Silver → Gold**), antes de serem disponibilizados no BigQuery para consumo pelo dashboard.

---

# Tecnologias Utilizadas

- Google Looker Studio
- Google BigQuery
- Google Cloud Storage
- Google Dataproc
- PySpark
- SQL
- Google Cloud Platform (GCP)

---

# Objetivo Analítico

Mais do que apresentar indicadores, este dashboard foi desenvolvido para apoiar gestores públicos na identificação de prioridades, otimização da alocação de recursos e formulação de políticas educacionais baseadas em dados.

A proposta central do projeto demonstra que decisões orientadas por dados podem contribuir para uma gestão pública mais eficiente, transparente e direcionada à melhoria da alfabetização infantil no Brasil.