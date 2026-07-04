import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.functions import col, count, when, isnan, isnull


def check_duplicates(df, subset, label):
    total = df.count()
    dup = df.groupBy(subset).count().filter(col("count") > 1).count()
    return dup == 0


def check_null_pct(df, col_name, max_pct):
    total = df.count()
    nulls = df.filter(col(col_name).isNull()).count()
    pct = (nulls / total * 100) if total > 0 else 0
    return pct <= max_pct


def check_range(df, col_name, min_val, max_val):
    violacoes = df.filter((col(col_name) < min_val) | (col(col_name) > max_val)).count()
    return violacoes == 0


def check_type(df, col_name, expected_type_name):
    campo = df.schema[col_name]
    return campo.dataType.typeName() == expected_type_name


class TestCheckDuplicates:
    def test_no_duplicates(self, spark):
        df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
        assert check_duplicates(df, ["id"], "test") is True

    def test_with_duplicates(self, spark):
        df = spark.createDataFrame([(1, "a"), (1, "b")], ["id", "val"])
        assert check_duplicates(df, ["id"], "test") is False

    def test_compound_key_no_dup(self, spark):
        df = spark.createDataFrame([(1, "a", 10), (1, "b", 20)], ["id", "rede", "v"])
        assert check_duplicates(df, ["id", "rede"], "test") is True

    def test_compound_key_dup(self, spark):
        df = spark.createDataFrame([(1, "a", 10), (1, "a", 20)], ["id", "rede", "v"])
        assert check_duplicates(df, ["id", "rede"], "test") is False


class TestCheckNullPct:
    def test_no_nulls(self, spark):
        df = spark.createDataFrame([(1,), (2,)], ["val"])
        assert check_null_pct(df, "val", 10.0) is True

    def test_all_nulls(self, spark):
        schema = StructType([StructField("val", DoubleType())])
        df = spark.createDataFrame([(None,), (None,)], schema=schema)
        assert check_null_pct(df, "val", 10.0) is False

    def test_within_tolerance(self, spark):
        df = spark.createDataFrame([(1,), (None,), (3,), (4,)], ["val"])
        assert check_null_pct(df, "val", 50.0) is True

    def test_exceeds_tolerance(self, spark):
        schema = StructType([StructField("val", DoubleType())])
        df = spark.createDataFrame([(1.0,), (None,), (None,), (4.0,)], schema=schema)
        assert check_null_pct(df, "val", 25.0) is False


class TestCheckRange:
    def test_all_in_range(self, spark):
        df = spark.createDataFrame([(50.0,), (75.0,), (100.0,)], ["taxa"])
        assert check_range(df, "taxa", 0.0, 100.0) is True

    def test_below_min(self, spark):
        df = spark.createDataFrame([(-1.0,)], ["taxa"])
        assert check_range(df, "taxa", 0.0, 100.0) is False

    def test_above_max(self, spark):
        df = spark.createDataFrame([(101.0,)], ["taxa"])
        assert check_range(df, "taxa", 0.0, 100.0) is False

    def test_on_boundary(self, spark):
        df = spark.createDataFrame([(0.0,), (100.0,)], ["taxa"])
        assert check_range(df, "taxa", 0.0, 100.0) is True

    def test_negative_deficit(self, spark):
        df = spark.createDataFrame([(-5.0,)], ["deficit"])
        assert check_range(df, "deficit", 0.0, float("inf")) is False

    def test_positive_deficit(self, spark):
        df = spark.createDataFrame([(0.0,), (1000.0,)], ["deficit"])
        assert check_range(df, "deficit", 0.0, float("inf")) is True


class TestCheckType:
    def test_correct_type(self, spark):
        df = spark.createDataFrame([("3550308",)], ["id_municipio"])
        assert check_type(df, "id_municipio", "string") is True

    def test_wrong_type(self, spark):
        df = spark.createDataFrame([(3550308,)], ["id_municipio"])
        assert check_type(df, "id_municipio", "string") is False


class TestQualityIntegration:
    def test_silver_full_validation(self, spark, sample_municipio_data):
        df = sample_municipio_data.withColumn("sigla_uf", sample_municipio_data["id_municipio"])
        assert check_duplicates(df, ["id_municipio", "ano", "rede"], "chave") is True
        assert check_range(df, "taxa_alfabetizacao", 0.0, 100.0) is True

    def test_gold_full_validation(self, spark, sample_gold_data):
        assert check_range(sample_gold_data, "taxa_alfabetizacao_media", 0.0, 100.0) is True
        assert check_range(sample_gold_data, "qtd_municipios_analisados", 1, float("inf")) is True