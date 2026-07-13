# Apresentação do Projeto

Este diretório reúne os materiais utilizados na apresentação do **Tech Challenge – Fase 2**, desenvolvida para demonstrar a solução proposta para o problema da alfabetização infantil no Brasil.

A apresentação resume as principais decisões de negócio, arquitetura de dados, dashboards analíticos e aplicações de Inteligência Artificial desenvolvidas ao longo do projeto.

---

# Objetivo da Apresentação

Apresentar, de forma executiva, como a solução foi construída para transformar dados públicos em informações estratégicas capazes de apoiar políticas públicas voltadas à alfabetização.

Durante a apresentação são abordados:

- O problema de negócio
- A arquitetura analítica desenvolvida
- O pipeline de dados na Google Cloud Platform
- Os dashboards executivos
- O potencial de aplicação de Inteligência Artificial
- As principais conclusões do projeto

---

# Estrutura da Apresentação

A apresentação está organizada em seis etapas:

## 1. O Desafio da Alfabetização

Apresenta o cenário nacional da alfabetização infantil, destacando os principais indicadores e o desafio enfrentado pelos gestores públicos.

---

## 2. Arquitetura da Solução

Apresenta a arquitetura Lakehouse implementada utilizando serviços da Google Cloud Platform.

Fluxo da solução:

```
Fontes Públicas
        ↓
Cloud Storage
        ↓
Dataproc (PySpark)
        ↓
Bronze → Silver → Gold
        ↓
BigQuery
        ↓
Dashboards e IA
```

---

## 3. Valor da Pipeline

Mostra como a camada Gold disponibiliza dados analíticos para construção de dashboards executivos, permitindo análises descritivas, diagnósticas e apoio à tomada de decisão.

---

## 4. Potencial de Inteligência Artificial

Apresenta as aplicações desenvolvidas utilizando Machine Learning.

Modelos utilizados:

- K-Means
- KNN
- Random Forest

---

## 5. Conclusão

Resume os principais resultados alcançados e demonstra como a solução pode apoiar gestores públicos na definição de prioridades e na otimização da alocação de recursos.

---

# Tecnologias Apresentadas

- Google Cloud Platform (GCP)
- Cloud Storage
- Dataproc
- PySpark
- BigQuery
- SQL
- Looker Studio
- Machine Learning
- Arquitetura Lakehouse
- Arquitetura Medalhão

---

# Arquivo

Este diretório contém a apresentação oficial utilizada na defesa do projeto.

```
📄 Tech Challenge - apresentação.pdf
```

---
