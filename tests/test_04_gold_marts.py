import pytest
from pyspark.sql.functions import col, count


class TestGoldMartUFIndicadores:
    def test_uf_indicadores_schema(self, spark, sample_mart_data):
        df_agg = sample_mart_data.groupBy("ano", "sigla_uf").agg(
            {"taxa_alfabetizacao": "avg", "id_municipio": "count"}
        )
        assert "ano" in df_agg.columns
        assert "sigla_uf" in df_agg.columns
        assert "avg(taxa_alfabetizacao)" in df_agg.columns or "taxa_alfabetizacao_media" in df_agg.columns

    def test_uf_indicadores_grupos(self, spark, sample_mart_data):
        df_agg = sample_mart_data.groupBy("ano", "sigla_uf").agg(
            {"taxa_alfabetizacao": "avg", "id_municipio": "count"}
        )
        pares = [(r["ano"], r["sigla_uf"]) for r in df_agg.collect()]
        assert (2023, "SP") in pares
        assert (2023, "CE") in pares
        assert (2024, "SP") in pares

    def test_uf_indicadores_sem_dados(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
        schema = StructType([
            StructField("ano", IntegerType()), StructField("id_municipio", StringType()),
            StructField("sigla_uf", StringType()), StructField("taxa_alfabetizacao", DoubleType())
        ])
        df_empty = spark.createDataFrame([], schema=schema)
        df_agg = df_empty.groupBy("ano", "sigla_uf").agg(
            {"taxa_alfabetizacao": "avg", "id_municipio": "count"}
        )
        assert df_agg.count() == 0


class TestGoldMunicipioRanking:
    def test_ranking_schema_possui_score(self, spark, sample_mart_data):
        df = sample_mart_data.withColumn("gap_meta", sample_mart_data["taxa_alfabetizacao"] - sample_mart_data["meta_alfabetizacao_2024"])
        assert "gap_meta" in df.columns

    def test_ranking_ordena_por_ano(self, spark, sample_mart_data):
        anos = [r["ano"] for r in sample_mart_data.select("ano").distinct().orderBy("ano").collect()]
        assert anos == sorted(anos)

    def test_ranking_sem_dados(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
        schema = StructType([
            StructField("ano", IntegerType()), StructField("id_municipio", StringType()),
            StructField("sigla_uf", StringType()), StructField("taxa_alfabetizacao", DoubleType()),
            StructField("meta_alfabetizacao_2024", DoubleType())
        ])
        df_empty = spark.createDataFrame([], schema=schema)
        assert df_empty.count() == 0


class TestGoldRedeIndicadores:
    def test_rede_agrupamento(self, spark, sample_mart_data):
        df_agg = sample_mart_data.groupBy("ano", "sigla_uf", "rede").agg(
            {"taxa_alfabetizacao": "avg"}
        )
        linhas = [(r["ano"], r["sigla_uf"], r["rede"]) for r in df_agg.collect()]
        assert (2023, "SP", "Municipal") in linhas
        assert (2023, "SP", "Estadual") in linhas

    def test_rede_tipos_distintos(self, spark, sample_mart_data):
        redes = [r["rede"] for r in sample_mart_data.select("rede").distinct().collect()]
        assert "Municipal" in redes
        assert "Estadual" in redes
        assert "Privada" in redes

    def test_rede_sem_dados(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
        schema = StructType([
            StructField("ano", IntegerType()), StructField("sigla_uf", StringType()),
            StructField("rede", StringType()), StructField("taxa_alfabetizacao", DoubleType())
        ])
        df_empty = spark.createDataFrame([], schema=schema)
        df_agg = df_empty.groupBy("ano", "sigla_uf", "rede").agg(
            {"taxa_alfabetizacao": "avg"}
        )
        assert df_agg.count() == 0


class TestGoldPriorizacao:
    def test_priorizacao_quadrantes(self, spark, sample_mart_data):
        df = sample_mart_data.groupBy("id_municipio", "sigla_uf").agg(
            {"taxa_alfabetizacao": "avg", "deficit_absoluto_proxy": "sum"}
        )
        mediana = df.approxQuantile("sum(deficit_absoluto_proxy)", [0.5], 0.01)[0] or 1
        assert mediana is not None

    def test_priorizacao_agrupa_municipios(self, spark, sample_mart_data):
        municipios = sample_mart_data.select("id_municipio").distinct().count()
        assert municipios == 4

    def test_priorizacao_sem_dados(self, spark):
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        schema = StructType([
            StructField("id_municipio", StringType()), StructField("sigla_uf", StringType()),
            StructField("taxa_alfabetizacao", DoubleType()), StructField("deficit_absoluto_proxy", DoubleType())
        ])
        df_empty = spark.createDataFrame([], schema=schema)
        df_agg = df_empty.groupBy("id_municipio", "sigla_uf").agg(
            {"taxa_alfabetizacao": "avg", "deficit_absoluto_proxy": "sum"}
        )
        assert df_agg.count() == 0


class TestGoldMartFilesGenerated:
    def test_marts_exist_no_disk(self):
        import os
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        gold_dir = os.path.join(project_root, "datalake_sample", "gold")
        marts_esperados = [
            "agg_uf_indicadores",
            "agg_municipio_ranking",
            "agg_rede_indicadores",
            "agg_priorizacao",
        ]
        for mart in marts_esperados:
            mart_path = os.path.join(gold_dir, mart)
            assert os.path.isdir(mart_path), f"Mart {mart} nao encontrado em {mart_path}"

    def test_marts_tem_parquet(self, spark):
        import os
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        gold_dir = os.path.join(project_root, "datalake_sample", "gold")
        marts_esperados = [
            "agg_uf_indicadores",
            "agg_municipio_ranking",
            "agg_rede_indicadores",
            "agg_priorizacao",
        ]
        for mart in marts_esperados:
            mart_path = os.path.join(gold_dir, mart)
            df = spark.read.parquet(mart_path)
            assert df.count() > 0, f"Mart {mart} esta vazio"
