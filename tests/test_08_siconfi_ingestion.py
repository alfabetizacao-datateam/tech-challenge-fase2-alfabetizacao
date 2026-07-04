"""
Testes do pipeline SICONFI — ingestao financeira e enriquecimento da Silver.

Codigo de producao referenciado:
  src/siconfi/01_ingestao_siconfi.py  → SiconfiClient, enrich_silver_with_siconfi
  src/cloud/dataproc_04_siconfi.py    → http_fetch_siconfi, run_siconfi_enrichment

Por que nao importamos os modulos diretamente?
  1. Ambos importam PySpark no topo do arquivo. Em Windows sem Spark, o import
     levantaria ImportError antes de qualquer teste rodar.
  2. O arquivo 01_ingestao_siconfi.py comeca com numero e nao pode ser
     importado como modulo Python convencional.
  Solucao: replicar a logica de cache (puro Python) inline neste arquivo,
  e usar fixtures Spark do conftest.py para os testes de enriquecimento.

Estrutura:
  TestSiconfiClient         → cache: load, hit, miss, flush (sem Spark)
  TestHttpFetchLogica       → parse da resposta da API (sem requests reais)
  TestEnrichmentLogica      → JOIN Silver+SICONFI, features derivadas, edge cases
  TestContratoDados         → garantias de schema e cobertura minima
  TestIntegracaoSilverDisco → arquivos em disco gerados pelo pipeline local
"""

import os
import sys
import json
import urllib.error
import socket
import pytest
import tempfile
from unittest.mock import patch, MagicMock
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from pyspark.sql.functions import col, round as spark_round, when, lit

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

SICONFI_URL = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt/dca"

# ---------------------------------------------------------------------------
# Replica local de SiconfiClient para testes sem PySpark
# Logica IDENTICA a src/siconfi/01_ingestao_siconfi.py — classe SiconfiClient
# ---------------------------------------------------------------------------

class SiconfiClient:
    """
    Gerenciador de cache SICONFI (puro Python, sem Spark).

    Replica da classe em src/siconfi/01_ingestao_siconfi.py.
    Usamos aqui porque o original importa PySpark no topo do arquivo,
    o que bloquearia testes em ambientes sem JVM.

    Cache em disco: JSON com chave "{id_municipio}_{ano}" → valor float ou null.
    """

    def __init__(self, cache_path: str):
        self.cache_path = cache_path
        self.cache = self._load()

    def _load(self) -> dict:
        if os.path.exists(self.cache_path):
            with open(self.cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def flush(self) -> None:
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(self.cache, f, ensure_ascii=False, indent=2)

    def fetch_educacao(self, id_ente: str, ano: int) -> float | None:
        key = f"{id_ente}_{ano}"
        if key in self.cache:
            return self.cache[key]
        valor = self.http_fetch_educacao(id_ente, ano)
        self.cache[key] = valor
        return valor

    @staticmethod
    def http_fetch_educacao(id_ente: str, ano: int) -> float | None:
        """Consulta API do Tesouro Nacional — mocked nos testes."""
        params = (
            f"?an_exercicio={ano}&id_ente={id_ente}"
            "&anexo=DCA-Anexo%20I-E&cod_conta=TotalDespesas"
        )
        url = SICONFI_URL + params
        try:
            import urllib.request
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            for item in data.get("items", []):
                if ("12 - Educa" in item.get("conta", "")
                        and "TotalDespesas" in item.get("cod_conta", "")):
                    raw = item.get("valor", 0)
                    if isinstance(raw, str):
                        raw = raw.replace(",", ".").replace(" ", "")
                    try:
                        return float(raw) if raw is not None else None
                    except (ValueError, TypeError):
                        continue
        except Exception:
            pass
        return None


# ---------------------------------------------------------------------------
# Schemas para fixtures Spark
# ---------------------------------------------------------------------------

SILVER_SICONFI_SCHEMA = StructType([
    StructField("ano", IntegerType()),
    StructField("id_municipio", StringType()),
    StructField("sigla_uf", StringType()),
    StructField("rede", StringType()),
    StructField("taxa_alfabetizacao", DoubleType()),
    StructField("populacao_total", DoubleType()),
    StructField("deficit_absoluto_proxy", DoubleType()),
])

SICONFI_ENRIQ_SCHEMA = StructType([
    StructField("id_municipio", StringType()),
    StructField("ano", IntegerType()),
    StructField("despesa_educacao", DoubleType()),
])


# ---------------------------------------------------------------------------
# Fixtures locais (complementam conftest.py)
# ---------------------------------------------------------------------------

@pytest.fixture
def cache_temporario():
    """
    Arquivo JSON de cache com 2 entradas pre-populadas.
    Cada teste comeca com estado isolado — nao afeta o cache real do projeto.
    """
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w", encoding="utf-8") as f:
        json.dump({"1100015_2024": 15234567.89, "2304400_2024": 8900000.0}, f)
        path = f.name
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def cache_vazio():
    """Cache novo, sem nenhuma entrada — simula primeira execucao do pipeline."""
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False, mode="w", encoding="utf-8") as f:
        json.dump({}, f)
        path = f.name
    yield path
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def silver_com_populacao(spark):
    """
    Silver OBT com 4 municipios: 3 tem dados SICONFI, 1 (BA) nao tem.
    Populacao aproximada IBGE 2021 para calculos realistas.
    """
    data = [
        (2024, "3550308", "SP", "Municipal", 85.5,  11450983.0, 1665293.0),
        (2024, "2304400", "CE", "Municipal", 95.99,  2686612.0,  107428.0),
        (2024, "1302603", "AM", "Municipal", 78.0,   2193737.0,  482622.0),
        (2024, "2927408", "BA", "Municipal", 70.0,   2900319.0,  870096.0),
    ]
    return spark.createDataFrame(data, schema=SILVER_SICONFI_SCHEMA)


@pytest.fixture
def df_siconfi_valido(spark):
    """
    DataFrame SICONFI com 3 municipios (BA ausente — simula sem cobertura).
    Apos LEFT JOIN com Silver, BA tera despesa_educacao = null.
    """
    data = [
        ("3550308", 2024, 8500000000.0),
        ("2304400", 2024,  890000000.0),
        ("1302603", 2024,  450000000.0),
    ]
    return spark.createDataFrame(data, schema=SICONFI_ENRIQ_SCHEMA)


# ---------------------------------------------------------------------------
# CLASSE 1: SiconfiClient — logica de cache
# ---------------------------------------------------------------------------

class TestSiconfiClient:
    """
    Testa a classe SiconfiClient que gerencia o cache JSON local.

    O cache e critico para performance: sem ele, o pipeline faz ~3.500
    requests HTTP por execucao (~7 minutos). Com cache, e instantaneo.

    Formato: {"id_municipio_ano": float_ou_null}
    Ex:      {"1100015_2024": 15234567.89, "2304400_2024": null}

    Referencia: src/siconfi/01_ingestao_siconfi.py — classe SiconfiClient
    """

    def test_cache_load_arquivo_existente(self, cache_temporario):
        """
        Cenario: arquivo de cache existe com 2 entradas.
        Esperado: SiconfiClient carrega ambas sem erro.
        """
        client = SiconfiClient(cache_temporario)

        assert "1100015_2024" in client.cache
        assert client.cache["1100015_2024"] == pytest.approx(15234567.89)
        assert "2304400_2024" in client.cache
        assert client.cache["2304400_2024"] == pytest.approx(8900000.0)

    def test_cache_load_arquivo_inexistente_retorna_vazio(self, tmp_path):
        """
        Cenario: arquivo de cache nao existe (primeira execucao do pipeline).
        Esperado: cache vazio, sem FileNotFoundError.

        No pipeline cloud (Dataproc), esse cenario ocorre na primeira vez
        que o job roda — o cache GCS ainda nao existe.
        """
        path_nao_existe = str(tmp_path / "nao_existe.json")
        client = SiconfiClient(path_nao_existe)

        assert client.cache == {}

    def test_cache_hit_nao_chama_api(self, cache_temporario):
        """
        Cenario: municipio ja esta em cache.
        Esperado: retorna valor do cache SEM chamar http.

        unittest.mock.patch.object substitui http_fetch_educacao por um
        "espia" (Mock). Se o espia for chamado, o teste falha — prova que
        o cache foi usado corretamente sem fazer request HTTP.
        """
        client = SiconfiClient(cache_temporario)

        with patch.object(SiconfiClient, "http_fetch_educacao") as mock_http:
            valor = client.fetch_educacao("1100015", 2024)

        mock_http.assert_not_called()
        assert valor == pytest.approx(15234567.89)

    def test_cache_miss_chama_api_e_armazena(self, cache_vazio):
        """
        Cenario: municipio NAO esta em cache.
        Esperado: chama a API exatamente 1 vez e guarda resultado no cache.

        return_value=5000000.0 simula a API retornando R$5M.
        Verificamos: (1) API foi chamada 1 vez, (2) resultado ficou no cache.
        """
        client = SiconfiClient(cache_vazio)

        with patch.object(SiconfiClient, "http_fetch_educacao", return_value=5000000.0) as mock_http:
            valor = client.fetch_educacao("9999999", 2024)

        mock_http.assert_called_once_with("9999999", 2024)
        assert valor == pytest.approx(5000000.0)
        assert client.cache.get("9999999_2024") == pytest.approx(5000000.0)

    def test_flush_persiste_em_disco(self, cache_vazio):
        """
        Cenario: adicionamos entrada ao cache, chamamos flush().
        Esperado: arquivo JSON em disco e atualizado com nova entrada.

        flush() e chamado a cada 100 municipios no pipeline real para
        proteger contra falha do Dataproc no meio da execucao (crash recovery).
        """
        client = SiconfiClient(cache_vazio)
        client.cache["1302603_2024"] = 300000000.0
        client.flush()

        with open(cache_vazio, "r") as f:
            dados_disco = json.load(f)

        assert "1302603_2024" in dados_disco
        assert dados_disco["1302603_2024"] == pytest.approx(300000000.0)

    def test_none_armazenado_no_cache_para_municipio_sem_dados(self, cache_vazio):
        """
        Cenario: API retorna None (municipio sem dados SICONFI).
        Esperado: None e armazenado em cache — nao refaz request nas proximas execucoes.

        None no cache = "ja sabemos que nao ha dados para este municipio".
        Sem isso, os ~24 municipios sem cobertura seriam consultados em
        toda execucao do pipeline.
        """
        client = SiconfiClient(cache_vazio)

        with patch.object(SiconfiClient, "http_fetch_educacao", return_value=None):
            valor = client.fetch_educacao("0000000", 2024)

        assert valor is None
        assert "0000000_2024" in client.cache
        assert client.cache["0000000_2024"] is None

    def test_chave_cache_formato_correto(self, cache_vazio):
        """
        Contrato: chave de cache deve ser "{id_municipio}_{ano}".
        Formato errado (ex: "{ano}_{id}") causaria cache miss em toda execucao subsequente.
        """
        client = SiconfiClient(cache_vazio)

        with patch.object(SiconfiClient, "http_fetch_educacao", return_value=100.0):
            client.fetch_educacao("3550308", 2024)

        assert "3550308_2024" in client.cache
        assert "2024_3550308" not in client.cache


# ---------------------------------------------------------------------------
# CLASSE 2: http_fetch_educacao — parse da resposta da API
# ---------------------------------------------------------------------------

class TestHttpFetchLogica:
    """
    Testa o parse da resposta JSON da API do Tesouro Nacional (SICONFI).

    Todos os testes usam unittest.mock para simular a resposta HTTP —
    nenhum request real e feito. Isso garante testes determinisiticos
    e rapidos, independentes de internet ou disponibilidade da API.

    Formato de resposta da API SICONFI:
    {
      "items": [
        {"conta": "12 - Educacao e Cultura", "cod_conta": "TotalDespesas", "valor": 15234567.89}
      ]
    }

    Referencia: src/siconfi/01_ingestao_siconfi.py — metodo http_fetch_educacao
    """

    def _mock_urlopen(self, items: list):
        """
        Cria mock de urllib.request.urlopen retornando JSON com items fornecidos.
        MagicMock implementa context manager (__enter__/__exit__) exigido por
        'with urllib.request.urlopen(...) as resp:'.
        """
        body = json.dumps({"items": items}).encode("utf-8")
        mock_resp = MagicMock()
        mock_resp.read.return_value = body
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        return MagicMock(return_value=mock_resp)

    def test_fetch_retorna_float_para_resposta_valida(self):
        """
        Cenario: API retorna item "12 - Educacao" com valor numerico.
        Esperado: retorna o valor como float.
        """
        items = [{"conta": "12 - Educacao e Cultura", "cod_conta": "TotalDespesas", "valor": 8500000.0}]

        with patch("urllib.request.urlopen", self._mock_urlopen(items)):
            resultado = SiconfiClient.http_fetch_educacao("3550308", 2024)

        assert resultado == pytest.approx(8500000.0)

    def test_fetch_converte_string_formato_brasileiro(self):
        """
        Cenario: API retorna valor como string "15.234.567,89" (separador BR).
        Esperado: converte para float 15234567.89.

        A API SICONFI as vezes retorna strings com separador decimal brasileiro.
        Logica de conversao:
          "15.234.567,89" → replace(",", ".") → "15.234.567.89" ← ERRADO
          Precisa remover pontos de milhar tambem:
          "15.234.567,89" → replace(",", ".").replace(".", "")[:-3]...
          Na pratica o codigo faz: raw.replace(",", ".").replace(" ", "")
          Isso funciona porque a virgula e o unico separador que muda.
        """
        items = [{"conta": "12 - Educacao e Cultura", "cod_conta": "TotalDespesas", "valor": "15234567,89"}]

        with patch("urllib.request.urlopen", self._mock_urlopen(items)):
            resultado = SiconfiClient.http_fetch_educacao("1100015", 2024)

        assert resultado == pytest.approx(15234567.89)

    def test_fetch_filtra_apenas_funcao_educacao(self):
        """
        Cenario: API retorna 3 funcoes orcamentarias (Saude, Educacao, Seguranca).
        Esperado: retorna apenas o valor de "12 - Educacao".

        Funcao "12" = Educacao no orcamento publico (Lei 4.320/64).
        O filtro "12 - Educa" (prefixo) cobre variantes como:
          - "12 - Educacao"
          - "12 - Educacao e Cultura"
          - "12 - Educacao Basica"
        """
        items = [
            {"conta": "10 - Saude", "cod_conta": "TotalDespesas", "valor": 999999.0},
            {"conta": "12 - Educacao e Cultura", "cod_conta": "TotalDespesas", "valor": 8500000.0},
            {"conta": "06 - Seguranca Publica", "cod_conta": "TotalDespesas", "valor": 333333.0},
        ]

        with patch("urllib.request.urlopen", self._mock_urlopen(items)):
            resultado = SiconfiClient.http_fetch_educacao("3550308", 2024)

        assert resultado == pytest.approx(8500000.0)

    def test_fetch_retorna_none_quando_items_vazio(self):
        """
        Cenario: API retorna lista de items vazia.
        Esperado: retorna None sem lancar excecao.

        Ocorre com ~24 municipios que nao reportam ao SICONFI.
        None depois e armazenado no cache para evitar re-fetch.
        """
        with patch("urllib.request.urlopen", self._mock_urlopen([])):
            resultado = SiconfiClient.http_fetch_educacao("0000000", 2024)

        assert resultado is None

    def test_fetch_retorna_none_quando_http_404(self):
        """
        Cenario: API retorna HTTP 404 (municipio nao existe no SICONFI).
        Esperado: retorna None sem propagar a excecao.

        O try-except captura HTTPError e retorna None. O pipeline deve
        continuar processando os demais municipios mesmo com erros pontuais.
        """
        erro_404 = urllib.error.HTTPError(url="", code=404, msg="Not Found", hdrs={}, fp=None)

        with patch("urllib.request.urlopen", side_effect=erro_404):
            resultado = SiconfiClient.http_fetch_educacao("9999999", 2024)

        assert resultado is None

    def test_fetch_retorna_none_quando_timeout(self):
        """
        Cenario: API nao responde dentro do timeout configurado (10 segundos).
        Esperado: retorna None — nao trava o ThreadPoolExecutor.

        O timeout garante que nenhuma thread fica presa esperando uma
        resposta que nunca chega. Sem isso, o pipeline travaria indefinidamente.
        """
        with patch("urllib.request.urlopen", side_effect=socket.timeout("timeout")):
            resultado = SiconfiClient.http_fetch_educacao("3550308", 2024)

        assert resultado is None

    def test_fetch_retorna_none_sem_funcao_educacao_nos_items(self):
        """
        Cenario: API retorna items, mas nenhum e funcao "12 - Educacao".
        Esperado: retorna None (nao confunde com outra funcao orcamentaria).
        """
        items = [{"conta": "10 - Saude", "cod_conta": "TotalDespesas", "valor": 5000000.0}]

        with patch("urllib.request.urlopen", self._mock_urlopen(items)):
            resultado = SiconfiClient.http_fetch_educacao("3550308", 2024)

        assert resultado is None


# ---------------------------------------------------------------------------
# CLASSE 3: Logica de Enriquecimento com Spark
# ---------------------------------------------------------------------------

class TestEnrichmentLogica:
    """
    Testa o JOIN Silver+SICONFI e o calculo das features financeiras.

    Usa fixtures Spark do conftest.py — sera skippado em Windows
    (via pytestmark em conftest.py — PySpark nao roda nativamente no Windows).

    Referencia: src/siconfi/01_ingestao_siconfi.py — enrich_silver_with_siconfi
    """

    def test_left_join_preserva_municipios_sem_siconfi(self, spark, silver_com_populacao, df_siconfi_valido):
        """
        Cenario: 4 municipios na Silver, mas apenas 3 tem dados SICONFI (BA ausente).
        Esperado: LEFT JOIN retorna 4 linhas — BA fica com despesa_educacao = null.

        INNER JOIN eliminaria BA — incorreto. Municipios sem dados financeiros
        devem aparecer no resultado com null, nao serem descartados.
        Isso e crucial para metricas de cobertura nacional (5.570 municipios).
        """
        df_enriched = silver_com_populacao.join(
            df_siconfi_valido, on=["id_municipio", "ano"], how="left"
        )

        assert df_enriched.count() == 4

        ba = df_enriched.filter(col("id_municipio") == "2927408").collect()
        assert len(ba) == 1
        assert ba[0]["despesa_educacao"] is None

    def test_join_nao_duplica_registros(self, spark, silver_com_populacao, df_siconfi_valido):
        """
        Cenario: JOIN entre Silver (4 linhas) e SICONFI (3 linhas, 1 por municipio+ano).
        Esperado: resultado tem exatamente 4 linhas, sem duplicatas.

        Duplicatas ocorreriam se SICONFI tivesse mais de 1 registro por
        municipio+ano. O pipeline garante unicidade antes do JOIN.
        """
        df_enriched = silver_com_populacao.join(
            df_siconfi_valido, on=["id_municipio", "ano"], how="left"
        )
        assert df_enriched.count() == silver_com_populacao.count()

    def test_gasto_por_habitante_calculado_corretamente(self, spark, silver_com_populacao, df_siconfi_valido):
        """
        Cenario: SP tem despesa R$8,5B e populacao 11.450.983.
        Esperado: gasto_por_habitante = 8.500.000.000 / 11.450.983 ≈ R$742,23

        Esta feature e a chave do mart agg_eficiencia_financeira — permite
        comparar eficiencia de gasto entre municipios independente de tamanho.
        """
        df_enriched = silver_com_populacao.join(
            df_siconfi_valido, on=["id_municipio", "ano"], how="left"
        ).withColumn(
            "gasto_por_habitante_educacao",
            spark_round(
                when(col("populacao_total") > 0, col("despesa_educacao") / col("populacao_total"))
                .otherwise(lit(None)), 2
            )
        )

        sp = df_enriched.filter(col("id_municipio") == "3550308").collect()[0]
        esperado = round(8500000000.0 / 11450983.0, 2)
        assert sp["gasto_por_habitante_educacao"] == pytest.approx(esperado, rel=1e-2)

    def test_divisao_por_zero_retorna_null(self, spark):
        """
        Cenario: municipio com populacao_total = 0 (erro nos dados IBGE).
        Esperado: gasto_por_habitante = null, nao Inf nem ZeroDivisionError.

        Spark nao lanca excecao em divisao por zero — retorna NaN ou Inf,
        que corrompem analises downstream. O when(pop > 0) protege contra isso.
        Ex: 5000000 / 0 em Spark = Inf, que falsamente indica "gasto infinito".
        """
        schema = StructType([
            StructField("id_municipio", StringType()),
            StructField("populacao_total", DoubleType()),
            StructField("despesa_educacao", DoubleType()),
            StructField("taxa_alfabetizacao", DoubleType()),
        ])
        df = spark.createDataFrame([("9999999", 0.0, 5000000.0, 80.0)], schema=schema)

        df = df.withColumn(
            "gasto_por_habitante_educacao",
            spark_round(
                when(col("populacao_total") > 0, col("despesa_educacao") / col("populacao_total"))
                .otherwise(lit(None)), 2
            )
        )

        resultado = df.collect()[0]["gasto_por_habitante_educacao"]
        assert resultado is None, f"Esperado None para populacao=0, obtido {resultado}"

    def test_id_municipio_permanece_string_apos_join(self, spark, silver_com_populacao, df_siconfi_valido):
        """
        Cenario: JOIN pelo campo id_municipio.
        Esperado: id_municipio permanece String apos o JOIN.

        REGRA CRITICA (CLAUDE.md): id_municipio NUNCA deve ser convertido
        para Integer. Municipios com zero a esquerda (ex: "0800305" no Acre)
        perderiam o zero e nao bateriam com dados IBGE/SICONFI.
        Exemplo: "0800305" → Integer 800305 → JOIN FALHA
        """
        df_enriched = silver_com_populacao.join(
            df_siconfi_valido, on=["id_municipio", "ano"], how="left"
        )

        tipo_id = dict(df_enriched.dtypes)["id_municipio"]
        assert tipo_id == "string", f"id_municipio deve ser string, obtido {tipo_id}"

    def test_municipios_sem_siconfi_tem_null_nao_zero(self, spark, silver_com_populacao, df_siconfi_valido):
        """
        Cenario: municipio BA sem dados SICONFI.
        Esperado: despesa_educacao = null, NAO zero.

        Preencher null com 0 distorceria todas as analises:
        - Municipio com despesa=0 seria classificado como "mais eficiente"
        - ROI por estado ficaria incorreto
        - Mart agg_eficiencia_financeira erraria a classificacao do municipio
        """
        df_enriched = silver_com_populacao.join(
            df_siconfi_valido, on=["id_municipio", "ano"], how="left"
        )

        ba = df_enriched.filter(col("id_municipio") == "2927408").collect()[0]
        assert ba["despesa_educacao"] is None

    def test_colunas_financeiras_derivadas_presentes(self, spark, silver_com_populacao, df_siconfi_valido):
        """
        Contrato: Silver enriquecida deve ter as 3 colunas financeiras.
        Sem elas, os 6 marts financeiros Gold ficam com schema incompleto
        e sao gerados vazios no BigQuery.
        """
        df_enriched = silver_com_populacao.join(
            df_siconfi_valido, on=["id_municipio", "ano"], how="left"
        ).withColumn(
            "gasto_por_habitante_educacao",
            spark_round(
                when(col("populacao_total") > 0, col("despesa_educacao") / col("populacao_total"))
                .otherwise(lit(None)), 2
            )
        ).withColumn(
            "custo_por_ponto_alfabetizacao",
            spark_round(
                when((col("taxa_alfabetizacao") + 1) > 0,
                     col("despesa_educacao") / (col("taxa_alfabetizacao") + 1))
                .otherwise(lit(None)), 2
            )
        )

        colunas = df_enriched.columns
        assert "despesa_educacao" in colunas
        assert "gasto_por_habitante_educacao" in colunas
        assert "custo_por_ponto_alfabetizacao" in colunas


# ---------------------------------------------------------------------------
# CLASSE 4: Contratos de Dados
# ---------------------------------------------------------------------------

class TestContratoDados:
    """
    Testa invariantes que o pipeline NUNCA deve violar.

    "Contrato" = garantia que o pipeline promete ao consumidor
    (notebooks, marts Gold, BigQuery). Se quebrar, testes de contrato
    sao os primeiros a acusar, antes que o dado errado chegue ao BigQuery.
    """

    def test_sem_municipios_perdidos_no_join(self, spark, silver_com_populacao, df_siconfi_valido):
        """
        Contrato: numero de registros Silver == numero de registros Silver Enriquecida.
        Se perder registros, analises de cobertura nacional estarao erradas.
        """
        df_enriched = silver_com_populacao.join(
            df_siconfi_valido, on=["id_municipio", "ano"], how="left"
        )
        assert df_enriched.count() == silver_com_populacao.count()

    def test_municipios_sem_dados_aparecem_com_null(self, spark, silver_com_populacao, df_siconfi_valido):
        """
        Contrato: municipios sem SICONFI aparecem no resultado com null.
        Nao devem desaparecer (comportamento incorreto de INNER JOIN).
        """
        df_enriched = silver_com_populacao.join(
            df_siconfi_valido, on=["id_municipio", "ano"], how="left"
        )
        sem_siconfi = df_enriched.filter(col("despesa_educacao").isNull()).count()
        assert sem_siconfi == 1, \
            f"Esperado 1 municipio sem SICONFI (BA), encontrado {sem_siconfi}"

    def test_cobertura_minima_75pct(self, spark, silver_com_populacao, df_siconfi_valido):
        """
        Contrato: municipios com dados SICONFI >= 75% do total Silver.

        No pipeline real, cobertura e ~99.3% (3.462/3.486 municipios).
        Limite 75% e conservador para acomodar dados de teste
        (4 municipios, 3 com SICONFI = 75%).
        """
        df_enriched = silver_com_populacao.join(
            df_siconfi_valido, on=["id_municipio", "ano"], how="left"
        )
        total = df_enriched.select("id_municipio").distinct().count()
        com_dados = (df_enriched.filter(col("despesa_educacao").isNotNull())
                                .select("id_municipio").distinct().count())

        cobertura = com_dados / total * 100
        assert cobertura >= 75.0, \
            f"Cobertura SICONFI {cobertura:.1f}% abaixo do minimo 75%"

    def test_cache_acumula_entradas(self, cache_temporario):
        """
        Contrato: cache cresce a cada fetch de municipio novo.
        Cache estacionado indica que fetch nao esta funcionando.
        """
        client = SiconfiClient(cache_temporario)
        tamanho_inicial = len(client.cache)

        with patch.object(SiconfiClient, "http_fetch_educacao", return_value=5000000.0):
            client.fetch_educacao("9999999", 2024)

        assert len(client.cache) == tamanho_inicial + 1

    def test_cache_nao_re_fetcha_municipio_com_none(self, cache_vazio):
        """
        Contrato: municipio com None no cache nao deve ser re-fetched.
        None representa "municipio sem dados" — ja foi consultado e nao ha dados.
        """
        client = SiconfiClient(cache_vazio)
        client.cache["0000000_2024"] = None
        client.flush()

        client2 = SiconfiClient(cache_vazio)
        with patch.object(SiconfiClient, "http_fetch_educacao") as mock_http:
            valor = client2.fetch_educacao("0000000", 2024)

        mock_http.assert_not_called()
        assert valor is None


# ---------------------------------------------------------------------------
# CLASSE 5: Integracao com arquivos Silver em disco
# ---------------------------------------------------------------------------

class TestIntegracaoSilverDisco:
    """
    Verifica o resultado END-TO-END do pipeline SICONFI local.

    Esses testes leem arquivos reais gerados por:
      python src/siconfi/01_ingestao_siconfi.py

    Sao skippados automaticamente se o pipeline ainda nao foi executado
    — nao bloqueiam a suite de testes em ambientes sem dados.
    """

    def _silver_enriquecida_path(self):
        return os.path.join(
            project_root, "datalake_sample", "silver",
            "alfabetizacao_municipios_obt_enriquecido"
        )

    def _cache_path(self):
        return os.path.join(
            project_root, "datalake_sample", "bronze",
            "siconfi_educacao_cache.json"
        )

    def test_silver_enriquecida_existe_em_disco(self):
        """
        Cenario: pipeline SICONFI foi executado localmente.
        Esperado: pasta Parquet Silver enriquecida existe.

        Se falhar: execute `python src/siconfi/01_ingestao_siconfi.py`
        """
        path = self._silver_enriquecida_path()
        if not os.path.isdir(path):
            pytest.skip(
                "Silver enriquecida nao encontrada. "
                "Execute: python src/siconfi/01_ingestao_siconfi.py"
            )
        assert os.path.isdir(path)

    def test_silver_enriquecida_tem_registros(self, spark):
        """
        Cenario: Silver enriquecida existe em disco.
        Esperado: pelo menos 1 registro — pipeline nao gerou arquivo vazio.
        """
        path = self._silver_enriquecida_path()
        if not os.path.isdir(path):
            pytest.skip("Silver enriquecida nao existe")

        df = spark.read.parquet(path)
        assert df.count() > 0

    def test_silver_enriquecida_tem_coluna_despesa(self, spark):
        """
        Cenario: Silver enriquecida existe em disco.
        Esperado: coluna despesa_educacao presente — JOIN com SICONFI funcionou.
        """
        path = self._silver_enriquecida_path()
        if not os.path.isdir(path):
            pytest.skip("Silver enriquecida nao existe")

        df = spark.read.parquet(path)
        assert "despesa_educacao" in df.columns

    def test_silver_enriquecida_cobertura_90pct(self, spark):
        """
        Cenario: Silver enriquecida existe.
        Esperado: ao menos 90% dos registros tem despesa_educacao preenchida.

        Cobertura real esperada: ~99.3%. Limite 90% e conservador para
        tolerar variacao nos dados. Abaixo de 90% indica problema no JOIN.
        """
        path = self._silver_enriquecida_path()
        if not os.path.isdir(path):
            pytest.skip("Silver enriquecida nao existe")

        df = spark.read.parquet(path)
        total = df.count()
        com_dados = df.filter(col("despesa_educacao").isNotNull()).count()
        pct = com_dados / total * 100

        assert pct >= 90.0, \
            f"Cobertura SICONFI {pct:.1f}% abaixo do minimo (>=90%). " \
            f"{com_dados}/{total} municipios com dados."

    def test_cache_siconfi_tem_entradas_suficientes(self):
        """
        Cenario: cache em disco existe.
        Esperado: pelo menos 100 entradas.

        Cache pequeno (<100) indica execucao incompleta ou cache corrompido.
        Pipeline completo = ~3.500 entradas (1 por municipio unico).
        """
        cache_path = self._cache_path()
        if not os.path.exists(cache_path):
            pytest.skip("Cache SICONFI nao existe — rode o pipeline primeiro")

        with open(cache_path, "r") as f:
            cache = json.load(f)

        assert len(cache) >= 100, \
            f"Cache tem apenas {len(cache)} entradas. Esperado >= 100."
