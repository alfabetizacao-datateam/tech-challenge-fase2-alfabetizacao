from pyspark.sql.functions import col, when, substring
from pyspark.sql.types import StringType, DoubleType


UF_MAPPING = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA", "16": "AP", "17": "TO",
    "21": "MA", "22": "PI", "23": "CE", "24": "RN", "25": "PB", "26": "PE", "27": "AL",
    "28": "SE", "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP", "41": "PR",
    "42": "SC", "43": "RS", "50": "MS", "51": "MT", "52": "GO", "53": "DF",
}


def apply_rede_mapping(df):
    return df.withColumn("rede",
        when(col("rede") == 0, "Federal")
        .when(col("rede") == 2, "Estadual")
        .when(col("rede") == 3, "Municipal")
        .when(col("rede") == 5, "Privada")
        .otherwise(col("rede").cast(StringType()))
    )


def apply_uf_mapping(df):
    state_code = substring(col("id_municipio"), 1, 2)
    mapping_expr = when(state_code == "11", "RO")
    for code, uf in UF_MAPPING.items():
        if code != "11":
            mapping_expr = mapping_expr.when(state_code == code, uf)
    mapping_expr = mapping_expr.otherwise("Unknown")
    return df.withColumn("sigla_uf", mapping_expr)


class TestRedeMapping:
    def test_map_federal(self, spark):
        df = spark.createDataFrame([(0,)], ["rede"])
        result = apply_rede_mapping(df).collect()[0][0]
        assert result == "Federal"

    def test_map_estadual(self, spark):
        df = spark.createDataFrame([(2,)], ["rede"])
        result = apply_rede_mapping(df).collect()[0][0]
        assert result == "Estadual"

    def test_map_municipal(self, spark):
        df = spark.createDataFrame([(3,)], ["rede"])
        result = apply_rede_mapping(df).collect()[0][0]
        assert result == "Municipal"

    def test_map_privada(self, spark):
        df = spark.createDataFrame([(5,)], ["rede"])
        result = apply_rede_mapping(df).collect()[0][0]
        assert result == "Privada"

    def test_map_unknown(self, spark):
        df = spark.createDataFrame([(99,)], ["rede"])
        result = apply_rede_mapping(df).collect()[0][0]
        assert result == "99"


class TestUfMapping:
    def test_sp(self, spark):
        df = spark.createDataFrame([("3550308",)], ["id_municipio"])
        result = apply_uf_mapping(df).collect()[0]["sigla_uf"]
        assert result == "SP"

    def test_ce(self, spark):
        df = spark.createDataFrame([("2304400",)], ["id_municipio"])
        result = apply_uf_mapping(df).collect()[0]["sigla_uf"]
        assert result == "CE"

    def test_am(self, spark):
        df = spark.createDataFrame([("1302603",)], ["id_municipio"])
        result = apply_uf_mapping(df).collect()[0]["sigla_uf"]
        assert result == "AM"

    def test_df(self, spark):
        df = spark.createDataFrame([("5300108",)], ["id_municipio"])
        result = apply_uf_mapping(df).collect()[0]["sigla_uf"]
        assert result == "DF"

    def test_unknown(self, spark):
        df = spark.createDataFrame([("0012345",)], ["id_municipio"])
        result = apply_uf_mapping(df).collect()[0]["sigla_uf"]
        assert result == "Unknown"

    def test_all_ufs(self, spark):
        uf_codes = list(UF_MAPPING.keys())
        data = [(f"{code}{'0'*5}",) for code in uf_codes]
        df = spark.createDataFrame(data, ["id_municipio"])
        result = apply_uf_mapping(df).collect()
        siglas = {row["sigla_uf"] for row in result}
        expected = set(UF_MAPPING.values())
        assert siglas == expected


class TestSchemaEnforcement:
    def test_id_municipio_as_string(self, spark):
        df = spark.createDataFrame([(3550308,)], ["id_municipio"])
        df = df.withColumn("id_municipio", col("id_municipio").cast(StringType()))
        assert df.schema["id_municipio"].dataType.typeName() == "string"

    def test_taxa_alfabetizacao_as_double(self, spark):
        df = spark.createDataFrame([(85.5,)], ["taxa_alfabetizacao"])
        df = df.withColumn("taxa_alfabetizacao", col("taxa_alfabetizacao").cast(DoubleType()))
        assert df.schema["taxa_alfabetizacao"].dataType.typeName() == "double"


class TestSilverTransform:
    def test_obt_join_structure(self, spark, sample_municipio_data, sample_meta_municipio_data):
        df_muni = apply_rede_mapping(sample_municipio_data)
        df_meta_dedup = sample_meta_municipio_data.drop("ano").dropDuplicates(subset=["id_municipio", "rede"])
        join_cond = [
            df_muni.id_municipio == df_meta_dedup.id_municipio,
            df_muni.rede == df_meta_dedup.rede
        ]
        df_obt = df_muni.join(df_meta_dedup, join_cond, "left")
        assert df_obt.count() == 6

    def test_obt_preserves_nulls_proporcao(self, spark, sample_municipio_data):
        row_with_nulls = sample_municipio_data.filter(col("id_municipio") == "1302603").collect()[0]
        assert row_with_nulls["proporcao_aluno_nivel_0"] is None

    def test_silver_output_columns(self, spark, sample_municipio_data, sample_meta_municipio_data):
        df_muni = apply_rede_mapping(sample_municipio_data)
        df_meta_dedup = sample_meta_municipio_data.drop("ano").dropDuplicates(subset=["id_municipio", "rede"])
        join_cond = [
            df_muni.id_municipio == df_meta_dedup.id_municipio,
            df_muni.rede == df_meta_dedup.rede
        ]
        df_obt = df_muni.join(df_meta_dedup, join_cond, "left") \
            .drop(df_meta_dedup.id_municipio) \
            .drop(df_meta_dedup.rede)
        df_obt = apply_uf_mapping(df_obt)

        expected_cols = {"ano", "id_municipio", "serie", "rede", "taxa_alfabetizacao",
                         "sigla_uf", "meta_alfabetizacao_2024", "meta_alfabetizacao_2025",
                         "meta_alfabetizacao_2026", "meta_alfabetizacao_2027",
                         "meta_alfabetizacao_2028", "meta_alfabetizacao_2029", "meta_alfabetizacao_2030"}
        assert expected_cols.issubset(set(df_obt.columns))

    def test_obt_deduplicates_meta(self, spark, sample_municipio_data, sample_meta_municipio_data):
        df_meta_dedup = sample_meta_municipio_data.drop("ano").dropDuplicates(subset=["id_municipio", "rede"])
        assert df_meta_dedup.count() < sample_meta_municipio_data.count()

    def test_uf_mapping_on_obt(self, spark, sample_municipio_data, sample_meta_municipio_data):
        df_muni = apply_rede_mapping(sample_municipio_data)
        df_meta_dedup = sample_meta_municipio_data.drop("ano").dropDuplicates(subset=["id_municipio", "rede"])
        join_cond = [
            df_muni.id_municipio == df_meta_dedup.id_municipio,
            df_muni.rede == df_meta_dedup.rede
        ]
        df_obt = df_muni.join(df_meta_dedup, join_cond, "left") \
            .drop(df_meta_dedup.id_municipio) \
            .drop(df_meta_dedup.rede)
        df_obt = apply_uf_mapping(df_obt)

        sp_rows = df_obt.filter(col("id_municipio") == "3550308").collect()
        for r in sp_rows:
            assert r["sigla_uf"] == "SP"