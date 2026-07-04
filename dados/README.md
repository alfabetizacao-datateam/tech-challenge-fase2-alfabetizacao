# dados/ — Fonte de Dados Brutos

CSVs originais, versionados (7,8MB total, dentro do limite do GitHub):
- `Alunos.csv` — microdados individuais SAEB
- `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_brasil.csv.gz` — meta nacional
- `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_municipio.csv.gz` — meta por município
- `br_inep_avaliacao_alfabetizacao_meta_alfabetizacao_uf.csv.gz` — meta por UF
- `br_inep_avaliacao_alfabetizacao_municipio.csv.gz` — indicador de alfabetização por município
- `br_inep_avaliacao_alfabetizacao_uf.csv.gz` — indicador de alfabetização por UF

Fonte: [Indicador Criança Alfabetizada - Base dos Dados](https://basedosdados.org/dataset/indicador-crianca-alfabetizada) (ver PDF em `referencia/`).

IBGE (população/nomes de município) e SICONFI (despesas) NÃO estão aqui — são obtidos em runtime via API (`src/siconfi/01_ingestao_siconfi.py`, enriquecimento IBGE no `src/batch/02_silver_transform.py`), não são CSV estático.

`dados_sample/` é um subset destes mesmos 6 arquivos (ex: `Alunos.csv` 5.001 linhas vs 57.782 no completo) usado por padrão em dev (`$env:ENV="dev"`). Trocar para `$env:ENV="prod"` usa `dados/` completo.
