# Landing Zone de Amostras (`dados_sample`)

**Papel na Arquitetura:**
Esta pasta atua como o ponto de entrada (Landing Zone) para os arquivos brutos em formato original (geralmente CSV, JSON ou TXT) que representam uma **fração representativa** (amostra) da base de dados massiva.

## Perspectiva do Engenheiro de Dados
- **Imutabilidade:** Os arquivos nesta pasta são estritamente **somente leitura**. Uma vez gerados e depositados aqui, eles não devem ser alterados ou abertos para edição no Excel/Bloco de Notas, pois isso pode quebrar o *encoding* (ex: UTF-8) ou alterar delimitadores.
- **Nomenclatura Padrão:** Utilize um padrão claro para rastreabilidade. Exemplo: `YYYYMMDD_nome_do_dataset_amostra.csv`.
- **Isolamento de Ambiente:** Esta pasta existe para suportar a **Arquitetura Híbrida** no ambiente local (`ENV=dev`). Ela garante que o pipeline possa ser testado ponta a ponta em segundos, sem onerar a memória RAM ou CPU com os arquivos originais pesados.

## Perspectiva do Cientista de Dados
- **Representatividade Estatística:** Uma boa amostra não é apenas pegar as primeiras 1.000 linhas (`head`). Sempre que possível, o notebook gerador de amostras deve realizar uma **amostragem estratificada** (ex: garantir que tenhamos municípios de todas as regiões/estados na amostra) para que a pipeline seja testada contra a variância real dos dados.
- **Análise Rápida:** É a partir daqui que os cientistas de dados podem rapidamente abrir um arquivo no Pandas para entender a "cara" inicial do dado antes mesmo dele ser ingerido no Data Lake.
- **Preservação do Ruído:** O arquivo bruto da amostra deve conter os mesmos erros, nulos e inconsistências da base original. Isso garante que as lógicas de limpeza (que ocorrerão na camada Silver) sejam testadas e validadas.
