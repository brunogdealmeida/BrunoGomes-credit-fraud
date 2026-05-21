# Data Lakehouse — Instruções de Configuração / Setup Instructions

> Documento bilíngue: **Português (Brasil)** primeiro, **English** na segunda metade.  
> Bilingual document: **Portuguese (Brazil)** first, **English** in the second half.

---

# PORTUGUÊS (BRASIL)

## Visão Geral da Arquitetura

Pipeline de detecção de fraudes seguindo arquitetura medallion (Bronze → Silver → Gold) com qualidade de dados via dbt.

```
CSV (~1M linhas)
      │
      ▼
 [Bronze Layer]          Spark 3.5.1 + Iceberg 1.5.2
 Iceberg raw table       Nessie catalog (API v1)
 s3a://lakehouse/         MinIO object storage
      │
      ▼
 [Silver Layer]          Validação + enriquecimento
 tb_fraud_credit          ~1.031.544 linhas válidas
 + [Quarantine]           linhas rejeitadas separadas
      │
      ▼
 [Gold Layer]            5 tabelas de agregação
 fraud_by_region          Spark jobs
 fraud_by_type
 daily_fraud_metrics
 high_risk_transactions
 risk_profile
      │
      ▼
 [dbt Quality]           53 testes (porta de qualidade pura)
 Dremio OSS 24.3.0        Testa tabelas Spark diretamente via Dremio
 dbt-dremio 1.7.0         Não materializa nenhum modelo
      │
      ▼
 [Dashboard]             Apache Superset 3.1.3
      │
      ▼
 [Data Catalog]          OpenMetadata 1.5.0
 Lineage Airflow          Metadados MinIO + pipelines
      │
      ▼
 [Monitoring]            Prometheus + Grafana
 cAdvisor                 Métricas de containers e PostgreSQL
```

### Stack completa

| Componente        | Tecnologia                        | Versão     |
|-------------------|-----------------------------------|------------|
| Object Storage    | MinIO                             | 2024-03    |
| Iceberg Catalog   | Project Nessie                    | 0.76.6     |
| Processing        | Apache Spark + PySpark            | 3.5.1      |
| Table Format      | Apache Iceberg                    | 1.5.2      |
| Orchestration     | Apache Airflow                    | 2.9.3      |
| Query Engine      | Dremio OSS                        | 24.3.0     |
| Data Quality      | dbt + dbt-dremio                  | 1.7.9      |
| Dashboard         | Apache Superset                   | 3.1.3      |
| Data Catalog      | OpenMetadata                      | 1.5.0      |
| Monitoring        | Prometheus + Grafana              | v2.51 / 10.x |
| Metadata DB       | PostgreSQL                        | 15         |

---

## Pré-requisitos

- **Docker** >= 24.0 e **Docker Compose** >= 2.20
- Mínimo **12 GB RAM** disponíveis para Docker (recomendado 16 GB)
- Mínimo **30 GB de espaço em disco** livre (Dremio + Spark + imagens Docker acumulam facilmente 20 GB)
- Portas livres: `9000`, `9001`, `9047`, `19120`, `8080`, `8082`, `8088`, `5432`, `7077`, `31010`, `45678`

---

## Estrutura do Projeto

```
datalakehouse-project/
├── .env                          # Variáveis de ambiente (credenciais)
├── docker-compose.yml            # Orquestração de todos os serviços
├── dataset/
│   └── df_fraud_credit.csv       # ~1M linhas de dados de fraude
├── airflow/
│   ├── Dockerfile                # Imagem customizada com Spark + dbt
│   └── dags/
│       └── fraud_lakehouse_pipeline.py  # DAG principal
├── spark/
│   ├── Dockerfile                # Imagem Spark com JARs Iceberg/Nessie
│   └── jobs/
│       ├── bronze_ingestion.py
│       ├── silver_transformation.py
│       └── gold_aggregation.py
├── dbt/
│   ├── profiles.yml              # Conexão dbt → Dremio
│   ├── dbt_project.yml           # models: {} — dbt não materializa nada
│   ├── packages.yml
│   ├── macros/
│   │   └── test_not_null.sql     # Override para aspas em nomes de colunas
│   └── models/
│       └── sources.yml           # 6 fontes + 53 testes (Silver + 5 Gold)
├── openmetadata/
│   └── ingestion/
│       ├── airflow.yaml          # Config ingestion Airflow → OpenMetadata
│       └── minio.yaml            # Config ingestion MinIO → OpenMetadata
└── scripts/
    ├── setup_dremio.py           # Configura fonte Nessie no Dremio automaticamente
    └── setup_openmetadata.py     # Bootstrap OpenMetadata (login + ingestion)
```

---

## Configuração do Ambiente (Passo a Passo)

### 1. Clonar o repositório e configurar variáveis

```bash
git clone <url-do-repositorio>
cd datalakehouse-project
```

Edite o arquivo `.env` com suas credenciais. O arquivo padrão contém:

```env
# MinIO (Object Storage)
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password123

# PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow123
POSTGRES_DB=airflow

# Airflow
AIRFLOW__CORE__FERNET_KEY=ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=
AIRFLOW__WEBSERVER__SECRET_KEY=a25mQ1FHTUJoMnZ1cFR6RzR3Z1E=
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin123

# Dremio
DREMIO_USERNAME=adm-datalab
DREMIO_PASSWORD=DataEnv2025-

# Superset
SUPERSET_SECRET_KEY=superset-secret-change-in-production
SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=admin123
SUPERSET_ADMIN_EMAIL=admin@superset.com

# Nessie — versões diferentes de API para Spark e Dremio
NESSIE_URI=http://nessie:19120/api/v1       # Spark usa v1
NESSIE_URI_V2=http://nessie:19120/api/v2    # Dremio usa v2

# Spark
SPARK_MASTER_URL=spark://spark-master:7077
MINIO_ENDPOINT=http://minio:9000
```

> **IMPORTANTE**: `NESSIE_URI` (v1) é usado pelo Spark. `NESSIE_URI_V2` (v2) é usado pelo `setup_dremio.py` ao registrar a fonte no Dremio. Não misture as versões.

### 2. Subir todos os serviços

```bash
docker compose up -d --build
```

O processo levará alguns minutos na primeira execução (download + build das imagens).

Acompanhe a inicialização:

```bash
docker compose logs -f dremio-setup   # Aguarda Dremio iniciar e configura a fonte Nessie
docker compose ps                      # Verifica status de todos os containers
```

### 3. Verificar serviços disponíveis

| Serviço          | URL                          | Usuário / Senha                    |
|------------------|------------------------------|------------------------------------|
| Airflow UI       | http://localhost:8082        | admin / admin123                   |
| Dremio UI        | http://localhost:9047        | ver `.env` (DREMIO_USERNAME/PASSWORD) |
| MinIO Console    | http://localhost:9001        | ver `.env` (MINIO_ROOT_USER/PASSWORD) |
| Superset         | http://localhost:8088        | admin / admin123                   |
| OpenMetadata     | http://localhost:8585        | admin@openmetadata.org / admin     |
| Grafana          | http://localhost:3001        | admin / admin                      |
| Prometheus       | http://localhost:9090        | —                                  |
| Nessie API       | http://localhost:19120       | —                                  |
| Spark Master UI  | http://localhost:8080        | —                                  |

### 4. Disparar o pipeline no Airflow

1. Acesse http://localhost:8082
2. Ative a DAG `fraud_lakehouse_pipeline`
3. Clique em **Trigger DAG**
4. Acompanhe as 4 tasks: `bronze_ingestion` → `silver_transformation` → `gold_aggregation` → `dbt_quality_tests`

---

## Configurações Críticas e Problemas Resolvidos

Esta seção documenta cada configuração não-óbvia que foi necessária para o pipeline funcionar corretamente.

---

### C1. Endpoint MinIO no Dremio: sem `http://` no prefixo

**Arquivo afetado**: `scripts/setup_dremio.py`

**Problema**: O cliente S3 nativo do Dremio (`dremioS3`) deriva o protocolo a partir do parâmetro `secure`. Se o endpoint incluir `http://minio:9000`, o Dremio constrói URLs no formato virtual-hosted (`http://lakehouse.minio:9000/...`), que falha na resolução DNS e resulta em timeout de **165 segundos** ao tentar ler arquivos Iceberg.

**Solução**: Definir o endpoint **sem o prefixo de protocolo**:

```python
# CORRETO — Dremio adiciona http:// automaticamente quando secure=false
{"name": "fs.s3a.endpoint", "value": "minio:9000"}

# ERRADO — causa timeout de 165s por URL virtual-hosted inválida
{"name": "fs.s3a.endpoint", "value": "http://minio:9000"}
```

A configuração completa da fonte Nessie enviada via API (`POST /api/v3/catalog`):

```python
payload = {
    "entityType": "source",
    "name": "nessie_lakehouse",
    "type": "NESSIE",
    "config": {
        "nessieEndpoint": "http://nessie:19120/api/v2",  # v2 para Dremio
        "nessieAuthType": "NONE",
        "awsAccessKey": MINIO_ACCESS_KEY,
        "awsAccessSecret": MINIO_SECRET_KEY,
        "awsRootPath": "lakehouse",
        "credentialType": "ACCESS_KEY",
        "propertyList": [
            {"name": "fs.s3a.endpoint",               "value": "minio:9000"},  # SEM http://
            {"name": "fs.s3a.path.style.access",      "value": "true"},
            {"name": "dremio.s3.compat",              "value": "true"},
            {"name": "fs.s3a.connection.ssl.enabled", "value": "false"},
            {"name": "fs.s3a.aws.credentials.provider",
             "value": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"},
        ],
    },
}
```

---

### C2. dbt aponta diretamente para o catálogo Nessie (sem wrapper views)

**Arquivos afetados**: `dbt/models/sources.yml`, `airflow/dags/fraud_lakehouse_pipeline.py`

**Contexto**: O Dremio 24.x historicamente exigia `AT BRANCH main` em queries Nessie. A solução anterior criava wrapper views no home space do usuário antes de cada execução do dbt. Essa abordagem foi removida.

**Solução atual**: As sources do dbt apontam diretamente para `nessie_lakehouse.*`. O Dremio resolve o branch via a fonte Nessie registrada, sem necessidade de wrapper views ou `AT BRANCH` explícito nas queries geradas pelo dbt.

```yaml
# dbt/models/sources.yml
sources:
  - name: silver
    database: nessie_lakehouse   # catálogo Nessie registrado no Dremio
    schema: silver
    tables:
      - name: tb_fraud_credit

  - name: gold
    database: nessie_lakehouse
    schema: gold
    tables:
      - name: fraud_by_region
      # ... mais 4 tabelas gold
```

A task `dbt_quality_tests` no DAG executa apenas:

```bash
dbt deps --profiles-dir /opt/dbt
dbt test --profiles-dir /opt/dbt --target dev
```

**Sem** criação de views, **sem** `dbt run`. O dbt é uma porta de qualidade pura (53 testes, 0 modelos).

---

### C3. Coluna `timestamp` é palavra reservada no Dremio

**Arquivo afetado**: `dbt/macros/test_not_null.sql` (arquivo novo criado)

**Problema**: O teste `not_null` padrão do dbt gera SQL sem aspas nas colunas:

```sql
select timestamp from {{ model }} where timestamp is null
```

O Dremio interpreta `timestamp` como palavra-chave SQL (tipo de dado), causando erro de parse:

```
ERROR: Encountered "timestamp from" at line 13, column 8.
```

**Soluções que NÃO funcionaram**:
- Adicionar `quoting: identifier: true` no `dbt_project.yml` — quebrou o `dbt run` porque o `@adm-datalab` no `database` começou a receber aspas duplas aninhadas, gerando erro de lexer no Dremio
- Macro com `SELECT COUNT(*) ... WHERE "column" IS NULL` — dbt interpreta qualquer resultado > 0 como FAIL; `COUNT(*)` sempre retorna 1 linha, fazendo todos os 18 testes `not_null` falharem

**Solução correta**: Criar `dbt/macros/test_not_null.sql` com override do teste built-in:

```sql
{% test not_null(model, column_name) %}
  select "{{ column_name }}"
  from {{ model }}
  where "{{ column_name }}" is null
{% endtest %}
```

Este macro sobrescreve o `not_null` nativo do dbt, adicionando aspas duplas em todos os nomes de colunas. O dbt conta as linhas retornadas — 0 linhas = PASS, qualquer linha = FAIL.

> **Nota**: O `dbt_project.yml` final **não deve ter** nenhuma seção `quoting`. Deixe o arquivo sem ela.

---

### C4. Spark driver no container do Airflow

**Arquivo afetado**: `airflow/dags/fraud_lakehouse_pipeline.py`

**Problema**: O Airflow usa `LocalExecutor`, então o processo `spark-submit` é executado dentro do container `airflow-scheduler`. O worker Spark tenta conectar de volta ao driver via hostname — se o hostname não estiver configurado, a conexão falha.

**Solução**: Definir no `SPARK_CONF`:

```python
SPARK_CONF = {
    # ... outras configs ...
    "spark.driver.host": "airflow-scheduler",    # hostname do container Airflow
    "spark.driver.bindAddress": "0.0.0.0",       # escuta em todas as interfaces
}
```

---

### C5. Conexão Spark no Airflow: formato JSON

**Arquivo afetado**: `docker-compose.yml`

O Airflow espera a connection `spark_default` em formato JSON (não URI):

```yaml
# CORRETO
AIRFLOW_CONN_SPARK_DEFAULT: '{"conn_type":"spark","host":"spark://spark-master:7077"}'

# ERRADO (formato URI antigo — pode causar falhas de parse)
AIRFLOW_CONN_SPARK_DEFAULT: 'spark://spark-master:7077'
```

---

### C6. Versões diferentes da API Nessie

O Nessie expõe duas versões de API no mesmo endpoint (`port 19120`):

| Cliente | Endpoint           | Motivo                                         |
|---------|--------------------|------------------------------------------------|
| Spark   | `/api/v1`          | Extensão Nessie para Spark 3.5 usa somente v1  |
| Dremio  | `/api/v2`          | Dremio 24.x suporta somente v2                 |

No `.env`:

```env
NESSIE_URI=http://nessie:19120/api/v1      # usado pelo Spark (NESSIE_URI)
NESSIE_URI_V2=http://nessie:19120/api/v2   # usado pelo setup_dremio.py
```

---

## Todos os Arquivos Modificados / Criados

### `scripts/setup_dremio.py` — Modificado

**O que mudou**: `fs.s3a.endpoint` alterado de `"http://minio:9000"` para `"minio:9000"`.

**Por quê**: O cliente S3 nativo do Dremio adiciona o protocolo automaticamente baseado no parâmetro `secure`. Com `http://` no endpoint, ele constrói URLs inválidas (virtual-hosted), causando timeout de 165 segundos.

### `scripts/setup_openmetadata.py` — CRIADO (novo arquivo)

Script de bootstrap do OpenMetadata: obtém token JWT via login, renderiza configs de ingestion com variáveis de ambiente, e executa `metadata ingest` para MinIO e Airflow.

**Detalhes não-óbvios**:
- A senha deve ser enviada em **Base64** para a API de login
- O endpoint de health check retorna **HTTP 401** quando o servidor está saudável mas requer autenticação — tratar 401 como "saudável"
- O conector Dremio **não existe** no OpenMetadata 1.5.0 — ignorar
- Email padrão do admin: `admin@openmetadata.org` (sem hífen)

### `openmetadata/ingestion/airflow.yaml` — CRIADO (novo arquivo)

Config de ingestion do Airflow para o OpenMetadata. Usa conexão **Postgres** (banco de metadados do Airflow) em vez de conexão HTTP:

```yaml
connection:
  type: Postgres
  username: "${POSTGRES_USER}"
  authType:
    password: "${POSTGRES_PASSWORD}"
  hostPort: "postgres:5432"
  database: "${POSTGRES_DB}"
```

### `airflow/dags/fraud_lakehouse_pipeline.py` — Modificado

**O que mudou**:
1. Adicionado `spark.driver.host=airflow-scheduler` e `spark.driver.bindAddress=0.0.0.0` ao `SPARK_CONF`
2. Task `dbt_quality_tests` simplificada — removidos criação de wrapper views e `dbt run`:
   ```bash
   dbt deps --profiles-dir /opt/dbt
   dbt test --profiles-dir /opt/dbt --target dev
   ```

### `dbt/dbt_project.yml` — Modificado

`models: {}` — dbt não materializa nenhum modelo. É exclusivamente uma porta de qualidade.

### `dbt/models/sources.yml` — Modificado (reescrito)

Aponta Silver e Gold diretamente para `nessie_lakehouse.*`. Contém 53 testes distribuídos em 6 fontes (1 Silver + 5 Gold). Removidos modelos SQL do Gold (duplicavam o trabalho do Spark).

### `dbt/macros/test_not_null.sql` — Mantido (arquivo existente)

```sql
{% test not_null(model, column_name) %}
  select "{{ column_name }}"
  from {{ model }}
  where "{{ column_name }}" is null
{% endtest %}
```

Override necessário para escapar nomes de colunas com aspas duplas, resolvendo o conflito da coluna `timestamp` com palavra reservada SQL no Dremio.

---

## Notas Operacionais

### Acúmulo de disco no diretório de trabalho do Spark

O Spark grava artefatos de job em `/opt/spark/work` dentro do container `spark-worker`. Cada execução do pipeline acumula ~400 MB. Após ~60 execuções isso pode consumir 25+ GB e causar erros "No space left on device" no Spark.

Limpeza manual quando necessário:

```bash
docker exec spark-worker sh -c "cd /opt/spark/work && for d in app-2*; do rm -rf \"\$d\"; done"
```

### Dremio OOMKill sob pressão de memória

O Dremio está configurado com `-Xmx1500m`. Durante execuções de jobs Spark, a VM Docker pode ficar sem memória e matar o Dremio (exit code 137).

O design do pipeline mitiga isso: as tasks Spark completam primeiro e o Dremio só é necessário para o `dbt_quality_tests` ao final.

Se o Dremio for morto: `docker compose up -d dremio`

**Requisito de RAM**: O Docker Desktop deve ter **mínimo 12 GB** alocados (Settings → Resources → Memory). Com menos, o Dremio compete com Spark e é OOMKilled.

### Catálogo Nessie desatualizado após deleção direta no MinIO

Deletar arquivos diretamente no MinIO (sem passar pelo Spark/Iceberg) deixa ponteiros de metadados órfãos no Nessie, causando `NotFoundException` na próxima execução do pipeline.

**Diagnóstico**: `org.apache.iceberg.exceptions.NotFoundException: Failed to open input stream for file: s3a://lakehouse/bronze/...`

**Solução**: Use a API REST do Nessie v2 para commitar operações DELETE nas entradas de tabela afetadas:

```bash
# Verificar estado atual do branch main
curl http://localhost:19120/api/v2/trees/main

# Deletar entrada de tabela órfã via commit Nessie
curl -X POST "http://localhost:19120/api/v2/trees/main/history/commit" \
  -H "Content-Type: application/json" \
  -d '{"message": "remove stale bronze entry", "operations": [{"type": "DELETE", "key": {"elements": ["bronze", "tb_fraud_credit"]}}]}'
```

### Reiniciar o Dremio corretamente

**SEMPRE use `docker compose restart`, NUNCA `docker restart`**:

```bash
# CORRETO
docker compose restart dremio

# ERRADO — pode corromper o índice Lucene e desconectar da rede lakehouse-net
docker restart dremio
```

**Explicação**: `docker restart` pode remover o container da rede Docker (`lakehouse-net`), tornando-o inacessível pelos outros containers. Também pode corromper o índice Lucene interno do Dremio se a parada não for graciosa.

### Erro 400 em todas as queries do Dremio (AlreadyClosedException)

Se todas as queries no Dremio retornarem erro 400 com mensagem `Unexpected error occurred` nos logs:

```bash
docker compose logs dremio | grep AlreadyClosedException
# org.apache.lucene.store.AlreadyClosedException: this IndexWriter is closed
```

**Causa**: Esgotamento de disco durante operação do Spark fechou o `IndexWriter` do Lucene.

**Solução**:
```bash
# Verificar espaço em disco
df -h /var/lib/docker

# Limpar diretório de trabalho do Spark
docker exec spark-worker sh -c "cd /opt/spark/work && for d in app-2*; do rm -rf \"\$d\"; done"

# Reiniciar Dremio graciosamente
docker compose restart dremio
```

### Monitorar espaço em disco

```bash
docker system df           # mostra uso total
docker builder prune -f    # remove cache de builds (seguro)
docker image prune -f      # remove imagens não utilizadas
```

### OpenMetadata — lockout por tentativas de login

O OpenMetadata bloqueia o admin após múltiplas tentativas de senha incorretas. O estado de bloqueio é em memória:

```bash
docker restart openmetadata-server   # limpa o contador de tentativas
```

---

## Verificação do Pipeline

Após disparar a DAG no Airflow, verifique cada layer:

### Bronze
```bash
docker exec -it spark-master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions" \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
  --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1 \
  --conf spark.sql.catalog.nessie.ref=main \
  --conf spark.sql.catalog.nessie.warehouse=s3a://lakehouse/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  -e "SELECT COUNT(*) FROM nessie.bronze.fraud_transactions;"
```

### Silver + Quarantine
```bash
# Contar registros válidos (esperado: ~1.031.544)
# Contar quarentena (linhas rejeitadas com location_region='0')
docker exec -it spark-master /opt/spark/bin/spark-sql \
  [mesmas configs] \
  -e "SELECT COUNT(*) FROM nessie.silver.fraud_transactions; SELECT COUNT(*) FROM nessie.quarantine.fraud_transactions;"
```

### Gold
```bash
# Verificar tabelas de agregação
docker exec -it spark-master /opt/spark/bin/spark-sql \
  [mesmas configs] \
  -e "SHOW TABLES IN nessie.gold;"
```

### dbt (resultado esperado)
```
dbt deps: Installing dbt-labs/dbt_utils (1.3.0)

dbt test: Found 53 tests, 6 sources, 0 models
          Concurrency: 1 threads (target='dev')
          Finished running 53 tests
          Completed successfully
          PASS=53 WARN=0 ERROR=0 SKIP=0 TOTAL=53
```

---

## Solução Manual da Fonte Nessie no Dremio

Se o container `dremio-setup` falhar, configure manualmente:

1. Abra http://localhost:9047
2. Clique em **Add Source** → **Nessie**
3. Preencha:
   - **Name**: `nessie_lakehouse`
   - **Nessie Endpoint URL**: `http://nessie:19120/api/v2`
   - **Authentication**: None
4. Na aba **Storage**:
   - **AWS Access Key**: `admin`
   - **AWS Access Secret**: `password123`
   - **AWS Root Path**: `lakehouse`
   - **Connection Properties**:
     ```
     fs.s3a.endpoint               = minio:9000          ← SEM http://
     fs.s3a.path.style.access      = true
     dremio.s3.compat              = true
     fs.s3a.connection.ssl.enabled = false
     fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
     ```

---

---

# ENGLISH

## Architecture Overview

Fraud detection pipeline following the medallion architecture (Bronze → Silver → Gold) with data quality via dbt.

```
CSV (~1M rows)
      │
      ▼
 [Bronze Layer]          Spark 3.5.1 + Iceberg 1.5.2
 Iceberg raw table       Nessie catalog (API v1)
 s3a://lakehouse/         MinIO object storage
      │
      ▼
 [Silver Layer]          Validation + enrichment
 tb_fraud_credit          ~1,031,544 valid rows
 + [Quarantine]           rejected records isolated
      │
      ▼
 [Gold Layer]            5 aggregation tables
 fraud_by_region          Spark jobs
 fraud_by_type
 daily_fraud_metrics
 high_risk_transactions
 risk_profile
      │
      ▼
 [dbt Quality]           53 tests (pure quality gate)
 Dremio OSS 24.3.0        Tests Spark tables directly via Dremio
 dbt-dremio 1.7.0         Materializes no models
      │
      ▼
 [Dashboard]             Apache Superset 3.1.3
      │
      ▼
 [Data Catalog]          OpenMetadata 1.5.0
 Airflow lineage          MinIO + pipeline metadata
      │
      ▼
 [Monitoring]            Prometheus + Grafana
 cAdvisor                 Container and PostgreSQL metrics
```

### Full Stack

| Component         | Technology                        | Version    |
|-------------------|-----------------------------------|------------|
| Object Storage    | MinIO                             | 2024-03    |
| Iceberg Catalog   | Project Nessie                    | 0.76.6     |
| Processing        | Apache Spark + PySpark            | 3.5.1      |
| Table Format      | Apache Iceberg                    | 1.5.2      |
| Orchestration     | Apache Airflow                    | 2.9.3      |
| Query Engine      | Dremio OSS                        | 24.3.0     |
| Data Quality      | dbt + dbt-dremio                  | 1.7.9      |
| Dashboard         | Apache Superset                   | 3.1.3      |
| Data Catalog      | OpenMetadata                      | 1.5.0      |
| Monitoring        | Prometheus + Grafana              | v2.51 / 10.x |
| Metadata DB       | PostgreSQL                        | 15         |

---

## Prerequisites

- **Docker** >= 24.0 and **Docker Compose** >= 2.20
- Minimum **12 GB RAM** available to Docker (16 GB recommended)
- Minimum **30 GB free disk space** (Dremio + Spark + Docker images easily accumulate 20 GB)
- Free ports: `9000`, `9001`, `9047`, `19120`, `8080`, `8082`, `8088`, `5432`, `7077`, `31010`, `45678`

---

## Project Structure

```
datalakehouse-project/
├── .env                          # Environment variables (credentials)
├── docker-compose.yml            # All services orchestration
├── dataset/
│   └── df_fraud_credit.csv       # ~1M rows fraud dataset
├── airflow/
│   ├── Dockerfile                # Custom image with Spark + dbt
│   └── dags/
│       └── fraud_lakehouse_pipeline.py  # Main DAG
├── spark/
│   ├── Dockerfile                # Spark image with Iceberg/Nessie JARs
│   └── jobs/
│       ├── bronze_ingestion.py
│       ├── silver_transformation.py
│       └── gold_aggregation.py
├── dbt/
│   ├── profiles.yml              # dbt → Dremio connection
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── macros/
│   │   └── test_not_null.sql     # Critical override for "timestamp" column
│   └── models/
│       ├── sources.yml           # Silver points to Dremio wrapper view
│       ├── silver/
│       └── gold/
└── scripts/
    └── setup_dremio.py           # Automatically configures Nessie source in Dremio
```

---

## Environment Setup (Step by Step)

### 1. Clone the repository and configure variables

```bash
git clone <repository-url>
cd datalakehouse-project
```

Edit the `.env` file with your credentials. The default file contains:

```env
# MinIO (Object Storage)
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password123

# PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow123
POSTGRES_DB=airflow

# Airflow
AIRFLOW__CORE__FERNET_KEY=ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=
AIRFLOW__WEBSERVER__SECRET_KEY=a25mQ1FHTUJoMnZ1cFR6RzR3Z1E=
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin123

# Dremio
DREMIO_USERNAME=adm-datalab
DREMIO_PASSWORD=DataEnv2025-

# Superset
SUPERSET_SECRET_KEY=superset-secret-change-in-production
SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=admin123
SUPERSET_ADMIN_EMAIL=admin@superset.com

# Nessie — different API versions for Spark vs Dremio
NESSIE_URI=http://nessie:19120/api/v1       # Spark uses v1
NESSIE_URI_V2=http://nessie:19120/api/v2    # Dremio uses v2

# Spark
SPARK_MASTER_URL=spark://spark-master:7077
MINIO_ENDPOINT=http://minio:9000
```

> **IMPORTANT**: `NESSIE_URI` (v1) is used by Spark. `NESSIE_URI_V2` (v2) is used by `setup_dremio.py` when registering the source in Dremio. Do not mix the versions.

### 2. Start all services

```bash
docker compose up -d --build
```

The process will take a few minutes on first run (image download + build).

Monitor initialization:

```bash
docker compose logs -f dremio-setup   # Waits for Dremio to start and configures Nessie source
docker compose ps                      # Check status of all containers
```

### 3. Verify available services

| Service          | URL                          | Username / Password                  |
|------------------|------------------------------|--------------------------------------|
| Airflow UI       | http://localhost:8082        | admin / admin123                     |
| Dremio UI        | http://localhost:9047        | see `.env` (DREMIO_USERNAME/PASSWORD) |
| MinIO Console    | http://localhost:9001        | see `.env` (MINIO_ROOT_USER/PASSWORD) |
| Superset         | http://localhost:8088        | admin / admin123                     |
| OpenMetadata     | http://localhost:8585        | admin@openmetadata.org / admin       |
| Grafana          | http://localhost:3001        | admin / admin                        |
| Prometheus       | http://localhost:9090        | —                                    |
| Nessie API       | http://localhost:19120       | —                                    |
| Spark Master UI  | http://localhost:8080        | —                                    |

### 4. Trigger the pipeline in Airflow

1. Open http://localhost:8082
2. Enable the `fraud_lakehouse_pipeline` DAG
3. Click **Trigger DAG**
4. Monitor the 4 tasks: `bronze_ingestion` → `silver_transformation` → `gold_aggregation` → `dbt_quality_tests`

---

## Critical Configurations and Resolved Issues

This section documents each non-obvious configuration required for the pipeline to work correctly.

---

### C1. MinIO endpoint in Dremio: no `http://` prefix

**Affected file**: `scripts/setup_dremio.py`

**Problem**: Dremio's native S3 client (`dremioS3`) derives the protocol from the `secure` parameter. If the endpoint includes `http://minio:9000`, Dremio builds virtual-hosted URLs (`http://lakehouse.minio:9000/...`), which fails DNS resolution and causes a **165-second timeout** when reading Iceberg metadata files.

**Solution**: Set the endpoint **without the protocol prefix**:

```python
# CORRECT — Dremio adds http:// automatically when secure=false
{"name": "fs.s3a.endpoint", "value": "minio:9000"}

# WRONG — causes 165s timeout due to invalid virtual-hosted URL
{"name": "fs.s3a.endpoint", "value": "http://minio:9000"}
```

Complete Nessie source config sent via API (`POST /api/v3/catalog`):

```python
payload = {
    "entityType": "source",
    "name": "nessie_lakehouse",
    "type": "NESSIE",
    "config": {
        "nessieEndpoint": "http://nessie:19120/api/v2",  # v2 for Dremio
        "nessieAuthType": "NONE",
        "awsAccessKey": MINIO_ACCESS_KEY,
        "awsAccessSecret": MINIO_SECRET_KEY,
        "awsRootPath": "lakehouse",
        "credentialType": "ACCESS_KEY",
        "propertyList": [
            {"name": "fs.s3a.endpoint",               "value": "minio:9000"},  # NO http://
            {"name": "fs.s3a.path.style.access",      "value": "true"},
            {"name": "dremio.s3.compat",              "value": "true"},
            {"name": "fs.s3a.connection.ssl.enabled", "value": "false"},
            {"name": "fs.s3a.aws.credentials.provider",
             "value": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"},
        ],
    },
}
```

---

### C2. dbt points directly to the Nessie catalog (no wrapper views)

**Affected files**: `dbt/models/sources.yml`, `airflow/dags/fraud_lakehouse_pipeline.py`

**Context**: Dremio 24.x historically required `AT BRANCH main` in Nessie queries. The previous approach created wrapper views in the user's home space before each dbt run. This approach has been removed.

**Current solution**: dbt sources point directly to `nessie_lakehouse.*`. Dremio resolves the branch through the registered Nessie source, with no wrapper views or explicit `AT BRANCH` required in dbt-generated queries.

```yaml
# dbt/models/sources.yml
sources:
  - name: silver
    database: nessie_lakehouse   # registered Nessie catalog in Dremio
    schema: silver
    tables:
      - name: tb_fraud_credit

  - name: gold
    database: nessie_lakehouse
    schema: gold
    tables:
      - name: fraud_by_region
      # ... 4 more gold tables
```

The `dbt_quality_tests` DAG task runs only:

```bash
dbt deps --profiles-dir /opt/dbt
dbt test --profiles-dir /opt/dbt --target dev
```

**No** view creation, **no** `dbt run`. dbt is a pure quality gate (53 tests, 0 models).

---

### C3. `timestamp` column is a reserved word in Dremio

**Affected file**: `dbt/macros/test_not_null.sql` (new file created)

**Problem**: The standard dbt `not_null` test generates SQL without quoted column names:

```sql
select timestamp from {{ model }} where timestamp is null
```

Dremio interprets `timestamp` as a SQL keyword (data type), causing a parse error:

```
ERROR: Encountered "timestamp from" at line 13, column 8.
```

**Solutions that did NOT work**:
- Adding `quoting: identifier: true` in `dbt_project.yml` — broke `dbt run` because `@adm-datalab` in the `database` field started receiving nested double quotes, causing a Dremio lexer error
- Macro using `SELECT COUNT(*) ... WHERE "column" IS NULL` — dbt counts returned rows as failures; `COUNT(*)` always returns 1 row, making all 18 `not_null` tests report "FAIL 1"

**Correct solution**: Create `dbt/macros/test_not_null.sql` with an override of the built-in test:

```sql
{% test not_null(model, column_name) %}
  select "{{ column_name }}"
  from {{ model }}
  where "{{ column_name }}" is null
{% endtest %}
```

This macro overrides dbt's native `not_null`, adding double quotes around all column names. dbt counts returned rows — 0 rows = PASS, any row = FAIL.

> **Note**: The final `dbt_project.yml` must **NOT have** any `quoting` section. Leave the file without one.

---

### C4. Spark driver inside the Airflow container

**Affected file**: `airflow/dags/fraud_lakehouse_pipeline.py`

**Problem**: Airflow uses `LocalExecutor`, so the `spark-submit` process runs inside the `airflow-scheduler` container. The Spark worker tries to connect back to the driver via hostname — if the hostname is not configured, the connection fails.

**Solution**: Set in `SPARK_CONF`:

```python
SPARK_CONF = {
    # ... other configs ...
    "spark.driver.host": "airflow-scheduler",    # Airflow container hostname
    "spark.driver.bindAddress": "0.0.0.0",       # listen on all interfaces
}
```

---

### C5. Spark connection in Airflow: JSON format

**Affected file**: `docker-compose.yml`

Airflow expects the `spark_default` connection in JSON format (not URI):

```yaml
# CORRECT
AIRFLOW_CONN_SPARK_DEFAULT: '{"conn_type":"spark","host":"spark://spark-master:7077"}'

# WRONG (old URI format — may cause parsing failures)
AIRFLOW_CONN_SPARK_DEFAULT: 'spark://spark-master:7077'
```

---

### C6. Different Nessie API versions

Nessie exposes two API versions on the same endpoint (port 19120):

| Client  | Endpoint           | Reason                                            |
|---------|--------------------|---------------------------------------------------|
| Spark   | `/api/v1`          | Nessie Spark 3.5 extension only supports v1       |
| Dremio  | `/api/v2`          | Dremio 24.x only supports v2                      |

In `.env`:

```env
NESSIE_URI=http://nessie:19120/api/v1      # used by Spark (NESSIE_URI)
NESSIE_URI_V2=http://nessie:19120/api/v2   # used by setup_dremio.py
```

---

## All Modified / Created Files

### `scripts/setup_dremio.py` — Modified

**What changed**: `fs.s3a.endpoint` value changed from `"http://minio:9000"` to `"minio:9000"`.

**Why**: Dremio's native S3 client adds the protocol automatically based on the `secure` parameter. With `http://` in the endpoint, it builds invalid virtual-hosted URLs, causing a 165-second timeout.

### `airflow/dags/fraud_lakehouse_pipeline.py` — Modified

**What changed**:
1. Added `spark.driver.host=airflow-scheduler` and `spark.driver.bindAddress=0.0.0.0` to `SPARK_CONF`
2. Task `dbt_quality_tests` changed from a simple dbt call to a `BashOperator` with:
   - Inline Python script that creates/checks the wrapper view in Dremio before dbt runs
   - `dbt deps`, `dbt run`, `dbt test` in sequence with `set -e`
   - `append_env=True` to preserve the container `PATH` (needed to find `dbt` and `python3`)

### `dbt/models/sources.yml` — Modified

**What changed**: The `silver_layer` source was updated:
- `database`: changed from `nessie_lakehouse` to `"@{{ env_var('DREMIO_USERNAME', 'admin') }}"` (points to the user's home space in Dremio)
- `schema`: remains `silver`

**Why**: Dremio requires `AT BRANCH main` in Nessie queries. The wrapper view in the home space encapsulates this transparently for dbt.

### `dbt/macros/test_not_null.sql` — CREATED (new file)

**Content**:
```sql
{% test not_null(model, column_name) %}
  select "{{ column_name }}"
  from {{ model }}
  where "{{ column_name }}" is null
{% endtest %}
```

**Why**: Override required so that column names are always escaped with double quotes in the generated SQL, resolving the conflict between the `timestamp` column and the SQL reserved word of the same name in Dremio.

---

## Operational Notes

### Spark work directory disk accumulation

Spark writes job artifacts to `/opt/spark/work` inside the `spark-worker` container. Each pipeline run accumulates ~400 MB. After ~60 runs this can consume 25+ GB and cause "No space left on device" errors in Spark.

Manual cleanup when needed:

```bash
docker exec spark-worker sh -c "cd /opt/spark/work && for d in app-2*; do rm -rf \"\$d\"; done"
```

### Dremio OOMKill under memory pressure

Dremio is configured with `-Xmx1500m`. During Spark job execution the Docker VM may run out of memory and OOMKill Dremio (exit code 137).

The pipeline design mitigates this: Spark tasks complete first and Dremio is only needed for `dbt_quality_tests` at the end.

If Dremio is killed: `docker compose up -d dremio`

**RAM requirement**: Docker Desktop must have **at least 12 GB** allocated (Settings → Resources → Memory). With less, Dremio competes with Spark and gets OOMKilled.

### Stale Nessie catalog after direct MinIO deletion

Deleting files directly from MinIO (bypassing Spark/Iceberg) leaves orphaned metadata pointers in Nessie, causing `NotFoundException` on the next pipeline run.

**Diagnosis**: `org.apache.iceberg.exceptions.NotFoundException: Failed to open input stream for file: s3a://lakehouse/bronze/...`

**Fix**: Use the Nessie v2 REST API to commit DELETE operations on the affected table entries:

```bash
curl -X POST "http://localhost:19120/api/v2/trees/main/history/commit" \
  -H "Content-Type: application/json" \
  -d '{"message": "remove stale bronze entry", "operations": [{"type": "DELETE", "key": {"elements": ["bronze", "tb_fraud_credit"]}}]}'
```

### Restarting Dremio correctly

**ALWAYS use `docker compose restart`, NEVER `docker restart`**:

```bash
# CORRECT
docker compose restart dremio

# WRONG — may corrupt the Lucene index and disconnect from lakehouse-net
docker restart dremio
```

**Explanation**: `docker restart` may remove the container from the Docker network (`lakehouse-net`), making it unreachable by other containers. It can also corrupt Dremio's internal Lucene index if the shutdown is not graceful.

### Error 400 on all Dremio queries (AlreadyClosedException)

If all queries in Dremio return error 400 with `Unexpected error occurred` in the logs:

```bash
docker compose logs dremio | grep AlreadyClosedException
# org.apache.lucene.store.AlreadyClosedException: this IndexWriter is closed
```

**Cause**: Disk exhaustion during a Spark operation caused the Lucene `IndexWriter` to be forcibly closed.

**Solution**:
```bash
# Check disk space
df -h /var/lib/docker

# Clean Spark work directory
docker exec spark-worker sh -c "cd /opt/spark/work && for d in app-2*; do rm -rf \"\$d\"; done"

# Gracefully restart Dremio
docker compose restart dremio
```

### Monitor disk space

```bash
docker system df           # show total usage
docker builder prune -f    # remove build cache (safe)
docker image prune -f      # remove unused images
```

### OpenMetadata — login lockout

OpenMetadata locks out the admin after multiple failed login attempts. The lockout state is in-memory:

```bash
docker restart openmetadata-server   # clears the attempt counter
```

---

## Pipeline Verification

After triggering the DAG in Airflow, verify each layer:

### Bronze
```bash
docker exec -it spark-master /opt/spark/bin/spark-sql \
  --conf spark.sql.extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions" \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
  --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1 \
  --conf spark.sql.catalog.nessie.ref=main \
  --conf spark.sql.catalog.nessie.warehouse=s3a://lakehouse/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  -e "SELECT COUNT(*) FROM nessie.bronze.fraud_transactions;"
```

### Silver + Quarantine
```bash
# Count valid records (expected: ~1,031,544)
# Count quarantine (rejected rows with location_region='0')
docker exec -it spark-master /opt/spark/bin/spark-sql \
  [same configs] \
  -e "SELECT COUNT(*) FROM nessie.silver.fraud_transactions; SELECT COUNT(*) FROM nessie.quarantine.fraud_transactions;"
```

### Gold
```bash
# Check aggregation tables
docker exec -it spark-master /opt/spark/bin/spark-sql \
  [same configs] \
  -e "SHOW TABLES IN nessie.gold;"
```

### dbt (expected result)
```
dbt deps: Installing dbt-labs/dbt_utils (1.3.0)

dbt test: Found 53 tests, 6 sources, 0 models
          Concurrency: 1 threads (target='dev')
          Finished running 53 tests
          Completed successfully
          PASS=53 WARN=0 ERROR=0 SKIP=0 TOTAL=53
```

---

## Manual Nessie Source Setup in Dremio

If the `dremio-setup` container fails, configure manually:

1. Open http://localhost:9047
2. Click **Add Source** → **Nessie**
3. Fill in:
   - **Name**: `nessie_lakehouse`
   - **Nessie Endpoint URL**: `http://nessie:19120/api/v2`
   - **Authentication**: None
4. In the **Storage** tab:
   - **AWS Access Key**: `admin`
   - **AWS Access Secret**: `password123`
   - **AWS Root Path**: `lakehouse`
   - **Connection Properties**:
     ```
     fs.s3a.endpoint               = minio:9000          ← NO http://
     fs.s3a.path.style.access      = true
     dremio.s3.compat              = true
     fs.s3a.connection.ssl.enabled = false
     fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
     ```
