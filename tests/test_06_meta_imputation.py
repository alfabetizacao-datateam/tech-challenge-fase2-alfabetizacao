import pytest
from pyspark.sql.functions import col


class TestMetaImputation:
    def test_propagacao_municipio(self, spark, sample_mart_data):
        from pyspark.sql.window import Window
        from pyspark.sql.functions import first, coalesce

        janela = Window.partitionBy("id_municipio")
        df_prop = sample_mart_data.withColumn(
            "meta_propagada",
            first("meta_alfabetizacao_2024", ignorenulls=True).over(janela)
        )
        df_prop = df_prop.withColumn(
            "meta_final",
            coalesce(col("meta_alfabetizacao_2024"), col("meta_propagada"))
        )

        registros_com_meta = df_prop.filter(col("meta_final").isNotNull()).count()
        total = df_prop.count()

        assert registros_com_meta > 0
        assert registros_com_meta >= total / 2

    def test_propagacao_municipio_preserva_original(self, spark, sample_mart_data):
        from pyspark.sql.window import Window
        from pyspark.sql.functions import first, coalesce

        janela = Window.partitionBy("id_municipio")
        df_prop = sample_mart_data.withColumn(
            "meta_propagada",
            first("meta_alfabetizacao_2024", ignorenulls=True).over(janela)
        )
        df_prop = df_prop.withColumn(
            "meta_final",
            coalesce(col("meta_alfabetizacao_2024"), col("meta_propagada"))
        )

        diffs = df_prop.filter(
            col("meta_alfabetizacao_2024").isNotNull() &
            (col("meta_alfabetizacao_2024") != col("meta_final"))
        ).count()

        assert diffs == 0

    def test_imputacao_knn_faixa_esperada(self, spark, sample_mart_data):
        import numpy as np
        from sklearn.preprocessing import MinMaxScaler
        from sklearn.neighbors import KNeighborsRegressor

        ref = sample_mart_data.filter(col("meta_alfabetizacao_2024").isNotNull())
        tgt = sample_mart_data.filter(col("meta_alfabetizacao_2024").isNull()).limit(1)

        if ref.count() == 0 or tgt.count() == 0:
            pytest.skip("Dados insuficientes para KNN")

        pdf_ref = ref.select("populacao_total", "taxa_alfabetizacao",
                             "deficit_absoluto_proxy", "meta_alfabetizacao_2024").toPandas()
        pdf_tgt = tgt.select("populacao_total", "taxa_alfabetizacao",
                             "deficit_absoluto_proxy").toPandas()

        X_ref = pdf_ref[["taxa_alfabetizacao", "populacao_total", "deficit_absoluto_proxy"]].values
        y_ref = pdf_ref["meta_alfabetizacao_2024"].values
        X_tgt = pdf_tgt.values

        scaler = MinMaxScaler()
        X_ref_s = scaler.fit_transform(X_ref)
        X_tgt_s = scaler.transform(X_tgt)

        knn = KNeighborsRegressor(n_neighbors=3, weights="distance")
        knn.fit(X_ref_s, y_ref)
        pred = knn.predict(X_tgt_s)[0]

        meta_min = float(pdf_ref["meta_alfabetizacao_2024"].min())
        meta_max = float(pdf_ref["meta_alfabetizacao_2024"].max())

        assert meta_min <= pred <= meta_max, f"Predicao {pred:.1f} fora da faixa [{meta_min:.1f}, {meta_max:.1f}]"

    def test_imputacao_knn_mantem_taxa(self, spark, sample_mart_data):
        import numpy as np
        from sklearn.preprocessing import MinMaxScaler
        from sklearn.neighbors import KNeighborsRegressor

        ref = sample_mart_data.filter(col("meta_alfabetizacao_2024").isNotNull())
        tgt = sample_mart_data.filter(col("meta_alfabetizacao_2024").isNull()).limit(1)

        if ref.count() == 0 or tgt.count() == 0:
            pytest.skip("Dados insuficientes")

        pdf_ref = ref.select("populacao_total", "taxa_alfabetizacao",
                             "deficit_absoluto_proxy", "meta_alfabetizacao_2024").toPandas()
        pdf_tgt = tgt.select("populacao_total", "taxa_alfabetizacao",
                             "deficit_absoluto_proxy").toPandas()

        X_ref = pdf_ref[["taxa_alfabetizacao", "populacao_total", "deficit_absoluto_proxy"]].values
        y_ref = pdf_ref["meta_alfabetizacao_2024"].values
        X_tgt = pdf_tgt.values

        scaler = MinMaxScaler()
        X_ref_s = scaler.fit_transform(X_ref)
        X_tgt_s = scaler.transform(X_tgt)

        knn = KNeighborsRegressor(n_neighbors=3, weights="distance")
        knn.fit(X_ref_s, y_ref)
        pred = knn.predict(X_tgt_s)[0]

        assert 0 <= pred <= 100, f"Predicao {pred:.1f} fora de [0, 100]"
