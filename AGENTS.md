# AGENTS.md: Contrato Técnico e Governança da Dupla (Luiz & Renan)

## 0. Este é o repositório titular

Repositório único do projeto, com Git Flow real desde o primeiro commit — todo PR tem branch dedicada e revisão cruzada Luiz/Renan.

Este documento é a "Memória de Contexto" e o Contrato de Trabalho para o projeto *Tech Challenge Fase 2*. Qualquer IA agindo neste repositório (Claude/Agentes) deve ler isso antes de iniciar.

## 1. Colaboração da Dupla (Luiz e Renan)
- **Git Flow e PRs:** Toda feature (Streaming, Qualidade, Nuvem) deve ser desenvolvida em *branches* (`feature/kafka`, `feature/great-expectations`). Nenhuma integração ao `main` ocorre sem uma **Pull Request (PR)** clara explicando o problema resolvido, para que Luiz possa revisar o código de Renan, e vice-versa.
- **Não quebrar a arquitetura do outro:** A arquitetura já foi acordada e reflete a documentação do `BLUEPRINT_PROJETO.md`.

## 2. Regras de Domínio e Arquitetura (Linguagem Ubíqua)
- **Arquitetura Medalhão:**
  - `Bronze`: Dados crus, sem modificação de schema.
  - `Silver`: Dados tratados. *One Big Table* (`alfabetizacao_municipios_obt`). Regra de Ouro: **Nunca remover Zeros à esquerda de `id_municipio`** (deve ser forçado como `String` para garantir Joins espaciais/IBGE).
  - `Gold`: Agregações analíticas particionadas por `ano` e `sigla_uf`.
- **Preservação de Nulos:** As colunas `proporcao_aluno_nivel_*` possuem quase metade de vazios. É proibido imputar com `0.0`. Devem permanecer `NaN`/`Null` para não distorcer médias na Gold.
- **A Execução Híbrida (FinOps/Token Saver):**
  - O processamento pesado roda via **PySpark Local / Cloud Batch**.
  - As IAs (como Eu) são usadas **exclusivamente na pasta `dados_sample` ou na camada `Gold`** para exploração de dados, EDA e geração de Insights (para não torrar a cota do cartão de crédito da dupla nem travar a máquina).

## 3. Governança do Agente IA (AI Jail)
- **Sem Vibe Coding Cego:** Agentes não devem deletar, sobrescrever arquivos estruturais ou rodar scripts pesados de migração sem aprovação humana expressa do "Navegador".
- **Refatoração Segura:** Nenhuma refatoração pesada ou mudança de pacote sem antes garantir a qualidade e testabilidade do que já estava rodando.
