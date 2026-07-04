# 05. Guia Rápido: Arquitetura Medalhão no Databricks

**Objetivo:** Permitir que Luiz replique a Arquitetura Medalhão construída localmente (PySpark) dentro da plataforma **Databricks** de forma 100% gratuita, acompanhando o trabalho do Renan.

---

## 1. Criando sua Conta Gratuita (Community Edition)
O Databricks tem versões pagas pesadas, mas oferece um ambiente educacional gratuito onde você ganha um cluster Spark "de brinquedo" (15GB de RAM) que desliga sozinho quando você para de usar.

1. Acesse: [https://community.cloud.databricks.com/login.html](https://community.cloud.databricks.com/login.html)
2. Se não tiver conta, procure a opção "Sign up for Community Edition" (não coloque cartão de crédito).
3. Após criar a conta e logar, você verá o painel principal.

---

## 2. Ligando o "Motor" (Criando o Cluster)
Para rodar qualquer código PySpark, você precisa ligar o computador na nuvem.
1. No menu lateral esquerdo, clique em **Compute**.
2. Clique no botão azul **Create Compute**.
3. Dê um nome (ex: `cluster_alfabetizacao`).
4. A versão do *Databricks Runtime* pode ser a padrão (algo como 13.x ou 14.x LTS).
5. Clique em **Create Compute** e espere uns 3 minutos até a bolinha ficar verde.

---

## 3. Subindo os Dados (Camada Bronze no Unity Catalog)
O Databricks atualizou sua arquitetura e agora utiliza o **Unity Catalog Volumes** (muito superior ao antigo DBFS) para gerenciar arquivos brutos.
1. No menu lateral esquerdo, clique em **Catalog**.
2. Navegue pela estrutura do seu catálogo (ex: `datalakeraw` -> `default` -> `datalakeraw`).
3. Crie um Volume (se não existir) e dentro dele crie a pasta `raw/`.
4. Faça o **Upload** dos seus arquivos `.csv` da pasta `dados_sample` do seu computador para lá.
5. O caminho moderno e seguro para eles será: `/Volumes/datalakeraw/default/datalakeraw/raw/dados.csv`.

---

## 4. O Código: Do Local para a Nuvem
A magia do PySpark é que **o código é o mesmo**.
No seu ambiente local, você tem um arquivo chamado `02_silver_transform.py`. No Databricks, você cria um **Notebook**:

1. Menu esquerdo -> **Workspace** -> Create -> **Notebook**.
2. Linguagem: **Python**.
3. Conecte o notebook ao seu cluster que está verdinho.

### O Ajuste de Rota (Caminhos)
No seu código local, você faz isso:
```python
df = spark.read.csv("C:/Users/Luiz.../dados.csv")
```

No notebook do Databricks, você aponta para o Unity Catalog Volume:
```python
# Célula 1: Ingestão Bronze -> Silver
# Using Unity Catalog Volume instead of DBFS FileStore
df_bronze = spark.read.csv("/Volumes/datalakeraw/default/datalakeraw/raw/dados.csv", header=True)
print(f" Loaded {df_bronze.count()} rows")
df_bronze.printSchema()

# Célula 2: A mesma transformação que fizemos
from pyspark.sql.functions import col
df_silver = df_bronze.filter(col("id_municipio").isNotNull())

# Célula 3: Salvando na camada Silver (em Parquet)
df_silver.write.mode("overwrite").parquet("/Volumes/datalakeraw/default/datalakeraw/silver/obt_alfabetizacao")
display(df_silver) # O Databricks gera tabelas bonitas com esse comando!
```

---

## Dica de Ouro para a Call com o Renan
Quando for conversar com ele, mostre que você domina a diferença entre rodar PySpark Local vs Databricks atualizado. Diga:

> *"Renan, a essência do nosso pipeline Batch com PySpark é agnóstica. A única diferença de rodar as transformações na minha máquina ou no seu Databricks é que você está usando a nova arquitetura do Unity Catalog Volumes como Datalake, enquanto eu uso as pastas locais simulando o HDFS. O core das regras de negócio que colocamos na Silver (limpeza de nulos e chaves) roda igual nos dois ambientes!"*
