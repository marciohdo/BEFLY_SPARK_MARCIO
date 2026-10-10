# Desafio BeFly – Pipeline de Dados em PySpark (Arquitetura Bronze–Silver–Gold)

## Visão geral

Mini-pipeline em **PySpark (API de DataFrames)** que processa o dataset público
*Hotel Booking Demand* seguindo a arquitetura **Medalhão**:

| Camada | Script | O que faz | Saída |
|---|---|---|---|
| Bronze | [`scripts/raw_to_bronze.py`](scripts/raw_to_bronze.py) | Lê os 3 CSVs com **schema explícito**, trata `"NULL"` como nulo e grava Parquet/Snappy com colunas de linhagem | `data/bronze/{bookings,countries,hotels}/` (`bookings` particionado por `arrival_date_year`) |
| Silver | [`scripts/bronze_to_silver.py`](scripts/bronze_to_silver.py) | Tipagem, `arrival_date`, nulos, filtros de qualidade, colunas derivadas e joins com as referências | `data/silver/bookings_enriched/` (particionado por `arrival_date_year` / `arrival_date_month_num`) |
| Gold | [`scripts/silver_to_gold.py`](scripts/silver_to_gold.py) | 5 visões agregadas (`groupBy` + `agg`) para as perguntas de negócio | `data/gold/<visão>/` (Parquet) |

```
.
├── README.md
├── requirements.txt
├── env.sh                      # ativa a venv (e o JDK local, se existir)
├── scripts/
│   ├── download_dataset.py     # baixa o dataset via kagglehub
│   ├── raw_to_bronze.py
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   └── run_pipeline.sh         # roda as 3 etapas em sequência
├── utils/
│   ├── config.yaml             # caminhos de entrada/saída e configs do Spark
│   ├── common.py               # SparkSession, leitura de config, logging
│   └── log4j2.properties       # reduz o log do Spark para WARN
└── data/
    ├── raw/                    # hotel_bookings.csv, country_metadata.csv, hotel_metadata.csv
    ├── bronze/
    ├── silver/
    └── gold/
```

## Execução

### Requisitos

- **Python 3.13** (testado com 3.13.9)
- **PySpark 3.5.3**
- **Java 17** (o Spark 3.5 roda em Java 8/11/17)

### Instalação

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

O `env.sh` ativa a `.venv` e, se existir um JDK em `.tools/jdk17/`, aponta o
`JAVA_HOME` para ele. Sem esse diretório, ele usa o Java instalado no sistema.

### Obtenção dos dados

Para iniciar a ingestão, criamos o script
[`scripts/download_dataset.py`](scripts/download_dataset.py), que baixa o
dataset *Hotel Booking Demand* direto do Kaggle usando a biblioteca
[`kagglehub`](https://github.com/Kaggle/kagglehub):

```bash
source env.sh
python scripts/download_dataset.py
```

O script imprime o caminho local onde o `kagglehub` salvou os arquivos. A partir
dali, o `hotel_bookings.csv` deve ser copiado para `data/raw/`, junto com as
tabelas de referência `country_metadata.csv` e `hotel_metadata.csv` (fornecidas
em anexo ao teste).

### Caminhos dos arquivos

Todos os caminhos de entrada e saída ficam em [`utils/config.yaml`](utils/config.yaml).
Para apontar para CSVs em outro local, edite a seção `raw:`. Caminhos iniciados
por `data/` são relativos à raiz do projeto; qualquer outro valor é usado como
está (por exemplo, um caminho absoluto). Também é possível passar outro arquivo
de configuração com `--config`.

### Rodando o pipeline

Tudo de uma vez:

```bash
./scripts/run_pipeline.sh
```

Ou etapa por etapa:

```bash
source env.sh
spark-submit scripts/raw_to_bronze.py
spark-submit scripts/bronze_to_silver.py
spark-submit scripts/silver_to_gold.py
# opcional: spark-submit scripts/<etapa>.py --config /caminho/outro_config.yaml
```

Cada etapa grava com `mode("overwrite")`, então o pipeline é idempotente e
pode ser reexecutado do zero. A execução completa leva cerca de 1–2 minutos
num laptop, a maior parte em subir a JVM três vezes.

## Recorte do dataset

Optei por **manter o dataset completo** (`hotel_bookings.csv`, ~119.390 reservas
entre 2015 e 2017, dois hotéis), sem aplicar nenhum recorte. O volume é pequeno
e roda confortavelmente em PySpark local, então não há ganho em reduzi-lo.

## Bronze – ingestão

- **Schema explícito** (`StructType`) para os 3 arquivos, com `header=True`,
  `sep=","`, `nullValue="NULL"` e `mode=FAILFAST`. Se a fonte mudar de formato,
  a ingestão quebra em vez de gravar nulos em silêncio.
- `children` é lido como **string**, porque a fonte usa `"NA"` (e não `"NULL"`)
  para nulos nessa coluna. `reservation_status_date` também fica como string
  crua. As duas conversões acontecem na Silver, mantendo a Bronze fiel à origem.
- Colunas técnicas adicionadas: `_source_file` e `_ingested_at` (linhagem).
- Saída em Parquet + Snappy, com `bookings` particionado por `arrival_date_year`.

| Tabela | Linhas |
|---|---|
| bookings | 119.390 |
| countries | 252 |
| hotels | 2 |

## Silver – principais transformações

1. **Tipos:** as colunas inteiras listadas no enunciado viram `int`, `adr` vira
   `double` e `reservation_status_date` vira `date`.
2. **`arrival_date`:** o mês por extenso é mapeado para 1–12
   (`arrival_date_month_num`, via `create_map`), e a data é montada com
   `concat_ws` + `lpad` + `to_date`. Se alguma data ficar nula, a etapa falha.
3. **Nulos:** `children` passa de nulo para `0` (4 registros) e `country` passa
   de nulo para `"UNK"` (488 registros). Após o join, `country_name` e
   `continent` dessas reservas recebem `"Unknown"`.
4. **Filtros de qualidade** (contagens registradas no log):

   | Filtro | Removidos |
   |---|---|
   | Sem hóspedes (`adults + children + babies = 0`) | 180 |
   | `adr < 0` | 1 |

   Resultado: **119.209** reservas na Silver.
5. **Colunas derivadas:**

   | Coluna | Regra |
   |---|---|
   | `total_nights` | `stays_in_weekend_nights + stays_in_week_nights` |
   | `total_guests` | `adults + children + babies` |
   | `is_family` | 1 se `children > 0` ou `babies > 0` |
   | `is_long_stay` | 1 se `total_nights > 7` |
   | `revenue` | `adr * total_nights` se não cancelada, senão 0 |
   | `booking_status` | `CheckedOut` / `Canceled` / `NoShow` |
   | `booking_date` | `arrival_date - lead_time` (data em que a reserva foi feita) |
   | `lead_time_bucket` | faixa de antecedência (`1: 0-7d` … `6: 365d+`) |
   | `is_room_changed` | 1 se o quarto atribuído difere do reservado |

6. **Enriquecimento:** LEFT JOIN com `country_metadata` (traz `country_name` e
   `continent`) e com `hotel_metadata` (traz `hotel_city`, `hotel_country_code`,
   `hotel_star_rating` e `hotel_opened_year`). As referências passam por
   `dropDuplicates` na chave e entram via `broadcast`. A etapa **compara a
   contagem antes e depois do join** e falha se houver duplicação.
7. Gravação em Parquet particionado por `arrival_date_year` / `arrival_date_month_num`.

## Gold – visões analíticas

Em todas as visões, uma reserva é **cancelada** quando `is_canceled = 1`, o que
inclui `Canceled` e `No-Show`, e **efetiva** caso contrário.

| Visão | Granularidade | Métricas | Pergunta de negócio |
|---|---|---|---|
| `revenue_by_hotel_month` | hotel × ano × mês | `total_bookings`, `effective_bookings`, `cancelled_bookings`, `total_revenue`, `avg_adr` (só efetivas), `total_nights_sold`, `cancellation_rate` | Quanto cada hotel fatura mês a mês? |
| `cancellation_by_segment` | `market_segment` × `distribution_channel` × `customer_type` | `total_bookings`, `cancelled_bookings`, `cancellation_rate`, `avg_lead_time`, `avg_total_special_requests` | Quais segmentos e canais cancelam mais? Com quanta antecedência reservam? |
| `cancellation_by_lead_time` | `lead_time_bucket` × `deposit_type` × `is_repeated_guest` | `total_bookings`, `cancelled_bookings`, `cancellation_rate`, `avg_previous_cancellations`, `revenue_at_risk` | Que perfis de reserva têm risco alto de cancelamento (alvo de campanhas de retenção)? |
| `top_countries_by_revenue` | `country` × `country_name` × `continent` (Top 20) | `effective_bookings`, `total_revenue`, `avg_ticket`, `avg_lead_time` | De que países vêm os hóspedes mais valiosos? (exclui `UNK`) |
| `guest_profile_by_hotel` | hotel × `customer_type` | `effective_bookings`, `avg_total_nights`, `avg_total_guests`, `pct_long_stay`, `pct_family` | Qual a estadia média e o perfil dos hóspedes? |

Alguns achados da execução:

- O cancelamento cresce com a antecedência: ~10% para reservas feitas até 7 dias
  antes e ~68% para as feitas com mais de 1 ano.
- `Groups` via `TA/TO` com cliente `Transient` (7.573 reservas) cancela ~98%,
  com antecedência média de 225 dias.
- Portugal lidera em receita, mas Reino Unido, França e Alemanha têm ticket
  médio maior e reservam com bem mais antecedência.

## Comentário arquitetural – produção na AWS / Databricks

Em produção, cada camada viraria um prefixo em **S3** (por exemplo,
`s3://befly-datalake-{bronze,silver,gold}/reservia/bookings/`), com os dados em
formato de tabela aberta (**Delta Lake** no Databricks ou **Apache Iceberg** no
Glue/Athena), o que garante ACID, `MERGE` incremental, *time travel* e evolução
de schema. A ingestão Raw→Bronze passaria a ser incremental: arquivos chegando
num *landing zone* processados por **Auto Loader** ou **Glue Jobs** com
*bookmarks*, ou CDC das bases transacionais via **DMS**/Kafka. A Bronze seria
particionada por data de ingestão, e a Silver/Gold por data de negócio, com
*Z-order*/*clustering* nas colunas de filtro (hotel, país). Os mesmos scripts
PySpark rodariam como **Databricks Jobs / Delta Live Tables** ou **AWS Glue**,
orquestrados por **Airflow (MWAA)**, **Databricks Workflows** ou **Step
Functions**, com caminhos e parâmetros injetados por ambiente (dev/hml/prd) no
lugar do `config.yaml` local, e deploy via CI/CD (Databricks Asset Bundles ou
Terraform).

Os filtros e asserts que hoje estão no código viram **regras de qualidade
declarativas** (expectations do DLT, Great Expectations ou Glue Data Quality),
com quarentena dos registros reprovados em vez do descarte e métricas
publicadas no CloudWatch/alertas. A governança ficaria no **Unity Catalog** ou
no **Glue Data Catalog + Lake Formation**: catálogo único das tabelas,
linhagem, permissões por camada e por coluna (por exemplo, mascarar dados
pessoais de hóspedes, conforme a LGPD) e criptografia via KMS. A Gold seria
exposta para BI pelo **Athena** ou por um **Databricks SQL Warehouse**, servindo
Power BI/QuickSight, e as marcas do grupo consumiriam os dados por
*data sharing* sem cópia (Delta Sharing ou Lake Formation cross-account).
