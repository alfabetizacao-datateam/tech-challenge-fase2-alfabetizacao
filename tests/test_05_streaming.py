import json
import os
import time
import random
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

UFS = ["AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA",
       "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR", "RJ", "RN",
       "RO", "RR", "RS", "SC", "SE", "SP", "TO"]

STATUS_META = ["atingida", "em_progresso", "critico"]

MUNICIPIOS_EXEMPLO = {
    "SP": ("3550308", "Sao Paulo"),
    "RJ": ("3304557", "Rio de Janeiro"),
    "CE": ("2304400", "Fortaleza"),
    "BA": ("2927408", "Salvador"),
    "MG": ("3106200", "Belo Horizonte"),
    "PE": ("2611606", "Recife"),
    "AM": ("1302603", "Manaus"),
    "RS": ("4314902", "Porto Alegre"),
    "PR": ("4106902", "Curitiba"),
    "DF": ("5300108", "Brasilia"),
}

STREAMING_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("sigla_uf", StringType(), True),
    StructField("id_municipio", StringType(), True),
    StructField("nome_municipio", StringType(), True),
    StructField("nova_medicao_saeb", DoubleType(), True),
    StructField("status_meta", StringType(), True),
    StructField("meta_atingida", BooleanType(), True),
])


def generate_mock_event():
    uf = random.choice(UFS)
    mun_id, mun_nome = MUNICIPIOS_EXEMPLO.get(uf, ("0000000", "Desconhecido"))
    nova_taxa = round(random.uniform(700, 800), 2)
    meta_atual = random.choice(STATUS_META)
    return {
        "event_id": f"evt_{int(time.time() * 1000000)}_{random.randint(1000, 9999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "sigla_uf": uf,
        "id_municipio": mun_id,
        "nome_municipio": mun_nome,
        "nova_medicao_saeb": nova_taxa,
        "status_meta": meta_atual,
        "meta_atingida": meta_atual == "atingida",
    }


def start_producer(landing_zone_path, interval_sec=0.1, max_events=None):
    os.makedirs(landing_zone_path, exist_ok=True)
    count = 0
    while max_events is None or count < max_events:
        event = generate_mock_event()
        file_name = f"event_{int(time.time() * 1000)}_{count:04d}.json"
        file_path = os.path.join(landing_zone_path, file_name)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(event, f, ensure_ascii=False)
        count += 1
        time.sleep(interval_sec)
    return count


class TestProducer:
    def test_generate_mock_event_structure(self):
        event = generate_mock_event()
        expected_keys = {"event_id", "timestamp", "sigla_uf", "id_municipio",
                         "nome_municipio", "nova_medicao_saeb", "status_meta",
                         "meta_atingida"}
        assert set(event.keys()) == expected_keys

    def test_generate_mock_event_types(self):
        for _ in range(50):
            event = generate_mock_event()
            assert isinstance(event["event_id"], str)
            assert isinstance(event["sigla_uf"], str)
            assert isinstance(event["id_municipio"], str)
            assert isinstance(event["nova_medicao_saeb"], float)
            assert isinstance(event["status_meta"], str)
            assert isinstance(event["meta_atingida"], bool)
            assert "nova_medicao_saeb" not in event or event["nova_medicao_saeb"] >= 700.0

    def test_unique_event_ids(self):
        events = [generate_mock_event() for _ in range(20)]
        ids = [e["event_id"] for e in events]
        assert len(set(ids)) == 20

    def test_producer_generates_correct_file_count(self, tmp_path):
        count = start_producer(str(tmp_path), interval_sec=0.05, max_events=5)
        assert count == 5
        files = list(tmp_path.iterdir())
        assert len(files) == 5

    def test_producer_files_are_valid_json(self, tmp_path):
        start_producer(str(tmp_path), interval_sec=0.05, max_events=3)
        for f in tmp_path.iterdir():
            with open(str(f), "r") as fh:
                event = json.load(fh)
            assert isinstance(event, dict)

    def test_producer_with_zero_events(self, tmp_path):
        count = start_producer(str(tmp_path), interval_sec=0.05, max_events=0)
        assert count == 0
        assert len(list(tmp_path.iterdir())) == 0


class TestConsumer:
    def test_streaming_schema_definition(self):
        fields = {f.name: f.dataType.typeName() for f in STREAMING_SCHEMA.fields}
        assert fields["event_id"] == "string"
        assert fields["sigla_uf"] == "string"
        assert fields["nova_medicao_saeb"] == "double"
        assert fields["status_meta"] == "string"
        assert fields["meta_atingida"] == "boolean"
        assert fields["id_municipio"] == "string"
        assert fields["nome_municipio"] == "string"
        assert fields["timestamp"] == "string"

    def test_read_producer_output_with_schema(self, spark, tmp_path):
        start_producer(str(tmp_path), interval_sec=0.05, max_events=3)

        df = spark.read.schema(STREAMING_SCHEMA).json(str(tmp_path))
        assert df.count() == 3
        columns = set(df.columns)
        expected = {"event_id", "timestamp", "sigla_uf", "id_municipio",
                    "nome_municipio", "nova_medicao_saeb", "status_meta",
                    "meta_atingida"}
        assert expected.issubset(columns)

    def test_schema_reads_with_null_columns(self, spark, tmp_path):
        partial_file = tmp_path / "partial.json"
        partial_file.write_text('{"event_id": "evt_001", "sigla_uf": "SP"}', encoding="utf-8")

        df = spark.read.schema(STREAMING_SCHEMA).json(str(tmp_path))
        row = df.collect()[0]
        assert row["event_id"] == "evt_001"
        assert row["sigla_uf"] == "SP"
        assert row["nova_medicao_saeb"] is None
        assert row["meta_atingida"] is None

    def test_producer_and_consumer_roundtrip(self, spark, tmp_path):
        landing = tmp_path / "landing"
        output = tmp_path / "bronze_stream"
        landing.mkdir()

        start_producer(str(landing), interval_sec=0.05, max_events=5)

        df = spark.read.schema(STREAMING_SCHEMA).json(str(landing))
        df.write.format("parquet").mode("overwrite").save(str(output))

        df_read = spark.read.parquet(str(output))
        assert df_read.count() == 5
        assert "sigla_uf" in df_read.columns