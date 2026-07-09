# Simulação da Arquitetura Medalhão no Databricks

Esta pasta contém uma simulação da implementação da Arquitetura Medalhão utilizando Databricks, PySpark e Delta Lake.

> **Importante:** esta implementação não representa o pipeline completo do projeto principal. O projeto final foi desenvolvido em ambiente Cloud, com maior volume de dados, mais componentes de infraestrutura e uma arquitetura mais ampla.
> Os notebooks desta pasta têm como objetivo demonstrar que o mesmo fluxo conceitual também pode ser implementado dentro do Databricks.

## Objetivo

Demonstrar a construção de um pipeline de dados em Databricks seguindo a Arquitetura Medalhão, organizada em três camadas:

* **Bronze:** ingestão dos dados brutos;
* **Silver:** tratamento, padronização e validação;
* **Gold:** criação de tabelas analíticas para consumo em dashboards e análises.

O tema utilizado na simulação é a avaliação de alfabetização com dados do INEP, incluindo resultados por município, UF e metas de alfabetização.

## Tecnologias utilizadas

* Databricks
* Apache Spark
* PySpark
* Delta Lake
* Unity Catalog
* DBFS / Volumes
* Arquitetura Medalhão

## Estrutura dos notebooks

```text
notebooks databricks/
├── bronze.ipynb
├── silver.ipynb
└── gold.ipynb
```

## Arquitetura do pipeline

```text
Arquivos CSV
    ↓
Camada Bronze
    ↓
Camada Silver
    ↓
Camada Gold
    ↓
Dashboards / Análises
```

## Camada Bronze

A camada Bronze é responsável pela ingestão dos arquivos CSV no Databricks.

Nesta etapa, os dados são carregados de forma próxima ao seu estado original, sem aplicação de regras de negócio. O objetivo é preservar os dados brutos e adicionar metadados técnicos para rastreabilidade.

### Principais operações

* Leitura automática dos arquivos CSV armazenados em volume Databricks;
* Criação do schema `workspace.bronze`;
* Ingestão dos arquivos como tabelas Delta;
* Escrita com `overwrite` e `overwriteSchema`;
* Adição de colunas técnicas de auditoria.

### Metadados adicionados

```text
_id_linha
_data_ingestao
_arquivo_origem
_caminho_arquivo
_camada
```

## Camada Silver

A camada Silver realiza o tratamento dos dados provenientes da Bronze.

Nesta etapa, os dados passam por padronização, validação e conversão de tipos, tornando-se mais consistentes para uso analítico.

### Principais operações

* Leitura das tabelas Delta da Bronze;
* Criação do schema `workspace.silver`;
* Conversão de tipos numéricos;
* Padronização de textos com `trim` e `upper`;
* Remoção de duplicidades;
* Validação de colunas obrigatórias;
* Inclusão da coluna `_data_processamento_silver`;
* Escrita das tabelas tratadas em Delta Lake.

### Exemplos de tratamentos

* `ano` convertido para inteiro;
* `taxa_alfabetizacao` convertida para double;
* `media_portugues` convertida para double;
* `sigla_uf` padronizada em maiúsculas;
* campos de rede e metas tratados conforme configuração de cada tabela.

## Camada Gold

A camada Gold cria tabelas analíticas preparadas para consumo em dashboards, relatórios e análises de negócio.

Nesta etapa, os dados tratados da Silver são combinados com as metas de alfabetização para gerar indicadores, rankings e comparações.

### Principais operações

* Leitura das tabelas Silver;
* Criação do schema `workspace.gold`;
* Comparação dos resultados de alfabetização com a meta de 2030;
* Cálculo do gap em relação à meta;
* Classificação dos registros como `Meta atingida`, `Abaixo da meta`, `Sem resultado informado` ou `Sem meta informada`;
* Criação de rankings por UF e município;
* Geração de tabelas analíticas em Delta Lake.

### Tabelas analíticas criadas

```text
workspace.gold.indicador_municipio_meta_2030
workspace.gold.indicador_uf_meta_2030
workspace.gold.ranking_uf_alfabetizacao
workspace.gold.resumo_status_meta_uf
workspace.gold.ranking_municipios_maior_gap_meta_2030
```

## Exemplo de indicador criado

A tabela `indicador_municipio_meta_2030` compara o resultado de alfabetização de cada município com a meta projetada para 2030.

Campos principais:

```text
ano
id_municipio
serie
rede
resultado_alfabetizacao
media_portugues
meta_alfabetizacao_2030
gap_para_meta_2030
status_meta_2030
_data_processamento_gold
```

O campo `gap_para_meta_2030` representa a diferença entre a meta de alfabetização para 2030 e o resultado atual observado.

## Boas práticas demonstradas

Este conjunto de notebooks demonstra boas práticas importantes em pipelines de dados:

* Separação clara entre camadas Bronze, Silver e Gold;
* Uso de Delta Lake para armazenamento transacional;
* Organização por schemas no Unity Catalog;
* Inclusão de metadados de auditoria e linhagem;
* Funções reutilizáveis para leitura, tratamento e escrita;
* Padronização de tipos e textos;
* Criação de tabelas analíticas voltadas para consumo;
* Estrutura orientada à escalabilidade e manutenção.

## Relação com o projeto principal

O pipeline principal do projeto foi desenvolvido em ambiente Cloud, utilizando uma quantidade maior de dados e uma arquitetura mais completa.

A implementação presente nesta pasta foi criada como uma simulação complementar no Databricks, com o objetivo de demonstrar que a mesma lógica de organização em camadas também pode ser aplicada em um ambiente Lakehouse com PySpark, Delta Lake e Unity Catalog.

Portanto, estes notebooks devem ser interpretados como uma prova de conceito técnica, e não como a versão final completa do projeto.

## Conclusão

A simulação demonstra a aplicação prática da Arquitetura Medalhão no Databricks, passando por ingestão, tratamento e criação de indicadores analíticos.

O fluxo implementado mostra como dados brutos podem ser transformados em tabelas confiáveis e preparadas para análise, mantendo rastreabilidade, organização e separação clara de responsabilidades entre as camadas do pipeline.
