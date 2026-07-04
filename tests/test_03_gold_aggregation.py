import pytest
from pyspark.sql.functions import col, avg, count


class TestGoldAggregation:
    def test_aggregation_groupby(self, spark, sample_municipio_data):
        df_agg = sample_municipio_data.groupBy("ano", "rede").agg(
            avg("taxa_alfabetizacao").alias("taxa_alfabetizacao_media"),
            count("id_municipio").alias("qtd_municipios_analisados")
        )

        assert df_agg.count() == 4
        sp_2023_mun = df_agg.filter(
            (col("ano") == 2023) & (col("rede") == 3)
        ).collect()[0]
        assert sp_2023_mun["qtd_municipios_analisados"] == 3
        assert sp_2023_mun["taxa_alfabetizacao_media"] == pytest.approx(93.163, abs=0.1)

    def test_aggregation_partitioned_by_ano(self, spark, sample_municipio_data):
        df_agg = sample_municipio_data.groupBy("ano", "rede").agg(
            avg("taxa_alfabetizacao").alias("taxa_alfabetizacao_media"),
            count("id_municipio").alias("qtd_municipios_analisados")
        )
        anos = df_agg.select("ano").distinct().collect()
        assert {row["ano"] for row in anos} == {2023, 2024}

    def test_gold_schema(self, spark, sample_municipio_data):
        df_agg = sample_municipio_data.groupBy("ano", "rede").agg(
            avg("taxa_alfabetizacao").alias("taxa_alfabetizacao_media"),
            count("id_municipio").alias("qtd_municipios_analisados")
        )
        assert "ano" in df_agg.columns
        assert "taxa_alfabetizacao_media" in df_agg.columns
        assert "qtd_municipios_analisados" in df_agg.columns

    def test_gold_empty_input(self, spark):
        df_empty = spark.createDataFrame([], schema="ano int, id_municipio string, rede int, taxa_alfabetizacao double")
        df_agg = df_empty.groupBy("ano", "rede").agg(
            avg("taxa_alfabetizacao").alias("taxa_alfabetizacao_media"),
            count("id_municipio").alias("qtd_municipios_analisados")
        )
        assert df_agg.count() == 0