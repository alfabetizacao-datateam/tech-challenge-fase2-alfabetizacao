# Winutils — Dependência Externa (Windows)

PySpark no Windows precisa de `winutils.exe` + `hadoop.dll` em `hadoop/bin/` (não são binários Apache oficiais para Windows — são build da comunidade). Por isso NÃO são versionados neste repo (ver `.gitignore`).

## Como obter

1. Baixar da pasta compatível com Hadoop 3.3.x (a versão que o PySpark 3.5.0 empacota) em https://github.com/cdarlint/winutils
2. Colocar `winutils.exe` e `hadoop.dll` em `hadoop/bin/` (criar a pasta se não existir)
3. Definir a variável de ambiente antes de rodar qualquer script Spark:
   ```powershell
   $env:HADOOP_HOME = (Resolve-Path ".\hadoop").Path
   $env:PATH += ";$env:HADOOP_HOME\bin"
   ```

Versão já validada neste projeto: `winutils.exe` (112KB) + `hadoop.dll` (92KB) — se pegar de `cdarlint/winutils`, usar a pasta `hadoop-3.3.x` mais próxima dessa versão.
