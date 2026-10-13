# Desafio BeFly – Pipeline de Dados em PySpark (Arquitetura Bronze–Silver–Gold)

## Visão geral

Mini-pipeline em **PySpark (API de DataFrames)** que processa o dataset público
*Hotel Booking Demand* seguindo a arquitetura **Medalhão**:

| Camada | Script | O que faz | Saída |
|---|---|---|---|
| Bronze | [`scripts/raw_to_bronze.py`](scripts/raw_to_bronze.py) | Lê os 3 CSVs com **schema explícito**, trata `"NULL"` como nulo e grava Parquet/Snappy com colunas de linhagem | `data/bronze/{bookings,countries,hotels}/` (`bookings` particionado por `arrival_date_year`) |
| Silver | [`scripts/bronze_to_silver.py`](scripts/bronze_to_silver.py) | Tipagem, `arrival_date`, nulos, filtros de qualidade, colunas derivadas e joins com as referências | `data/silver/bookings_enriched/` (particionado por `arrival_date_year` / `arrival_date_month_num`) |
| Gold | [`scripts/silver_to_gold.py`](scripts/silver_to_gold.py) | 6 visões agregadas (`groupBy` + `agg`) para as perguntas de negócio, mais um score de risco de cancelamento por reserva | `data/gold/<visão>/` (Parquet) |

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

- **Python 3.11 ou 3.12** (recomendado). O pipeline foi desenvolvido e testado
  com o Python 3.13.9 e funcionou, mas o suporte oficial do PySpark 3.5 vai até
  o Python 3.12. Para reproduzir sem surpresas, prefira 3.11 ou 3.12.
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

> **Rode `source env.sh` antes de qualquer comando Spark** (`spark-submit`,
> `python` com PySpark). Se a máquina não tiver Java instalado no sistema (caso
> comum no macOS), o Spark falha ao iniciar com `Unable to locate a Java Runtime`
> / `JAVA_GATEWAY_EXITED`. O `env.sh` resolve isso apontando para o JDK 17 em
> `.tools/jdk17/` (diretório fora do Git, listado no `.gitignore`).

Para quem clonar o repositório (sem `.tools/`), instale o Java 17 no sistema:

```bash
# macOS (Homebrew)
brew install openjdk@17
export JAVA_HOME="$(brew --prefix openjdk@17)/libexec/openjdk.jdk/Contents/Home"
export PATH="$JAVA_HOME/bin:$PATH"

# Linux (Debian/Ubuntu)
sudo apt-get install -y openjdk-17-jdk

java -version   # deve mostrar a versão 17
```

### Obtenção dos dados

Para iniciar a ingestão, criamos o script
[`scripts/download_dataset.py`](scripts/download_dataset.py), que baixa o
dataset *Hotel Booking Demand* direto do Kaggle usando a biblioteca
[`kagglehub`](https://github.com/Kaggle/kagglehub):

```bash
source env.sh
python scripts/download_dataset.py
```

O script grava o `hotel_bookings.csv` direto em `data/raw/` e é **idempotente**:
o `kagglehub` reaproveita o cache local, e o arquivo só é copiado se ainda não
existir em `data/raw/` ou se o conteúdo for diferente. Pode ser reexecutado sem
efeitos colaterais. Para forçar um novo download e sobrescrever, use
`python scripts/download_dataset.py --force`.

As tabelas de referência `country_metadata.csv` e `hotel_metadata.csv` não vêm
do Kaggle. Elas foram fornecidas em anexo ao teste e já estão em `data/raw/`.

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

As saídas de cada camada (`data/bronze/`, `data/silver/` e `data/gold/`) estão
versionadas no repositório, para consulta sem precisar rodar o pipeline.
Cada etapa grava com `mode("overwrite")`, então o pipeline é idempotente e
pode ser reexecutado do zero. A execução completa leva cerca de 1–2 minutos
num laptop, a maior parte em subir a JVM três vezes.

## Recorte do dataset

Optei por **manter o dataset completo** (`hotel_bookings.csv`, ~119.390 reservas
entre 2015 e 2017, dois hotéis), sem aplicar nenhum recorte. O volume é pequeno
e roda confortavelmente em PySpark local, então não há ganho em reduzi-lo.

O dataset tem apenas **2 hotéis** (`City Hotel` e `Resort Hotel`), por limitação
da própria fonte pública. Veja a nota na
[pergunta 1](#1-quanto-cada-hotel-está-faturando-mês-a-mês).

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

### Checklist – requisitos do enunciado (seção 4.1)

| Requisito | Status | Onde / como |
|---|---|---|
| Ler os 3 CSVs em PySpark | ✅ | `read_csv()` para bookings, countries e hotels |
| `header = True` | ✅ | `.option("header", True)` |
| Separador `,` | ✅ | `.option("sep", ",")` |
| `inferSchema` **ou** schema explícito (**bônus**) | ✅ bônus | `BOOKINGS_SCHEMA`, `COUNTRIES_SCHEMA` e `HOTELS_SCHEMA` com `StructType` |
| Tratar `"NULL"` como nulo | ✅ | `.option("nullValue", "NULL")` (`agent`, `company`, `country`) |
| Recorte de volume (opcional) | ✅ | Dataset completo mantido (ver [Recorte do dataset](#recorte-do-dataset)) |
| Parquet com Snappy em `data/bronze/` | ✅ | `.option("compression", "snappy")` em `bookings/`, `countries/` e `hotels/` |
| Particionar `bookings` por `arrival_date_year` (**bônus**) | ✅ bônus | `partitionBy("arrival_date_year")`, que gera as pastas 2015, 2016 e 2017 |

## Silver – principais transformações

1. **Tipos:** as colunas inteiras listadas no enunciado viram `int`, `adr` vira
   `double` e `reservation_status_date` vira `date`.
2. **`arrival_date`:** o mês por extenso é mapeado para 1–12
   (`arrival_date_month_num`, via `create_map`), e a data é montada com
   `concat_ws` + `lpad` + `to_date`. Se alguma data ficar nula, a etapa falha.
   Validado na saída Silver: coluna do tipo `date`, 0 nulos em 119.209 linhas,
   ano/mês/dia/nome do mês extraídos de `arrival_date` batem com as colunas de
   origem em todas as linhas, e o intervalo vai de `2015-07-01` a `2017-08-31`.
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

### Checklist – requisitos do enunciado (seção 4.2)

| Requisito | Status | Onde / como |
|---|---|---|
| Colunas inteiras → `integer` (lista do enunciado) | ✅ | `cast_types()`, com a lista `INT_COLUMNS` |
| `adr` → `double` | ✅ | `cast_types()` |
| `reservation_status_date` → `date` | ✅ | `to_date(..., "yyyy-MM-dd")` em `cast_types()` |
| Criar `arrival_date` com `to_date`, `concat_ws` e `lpad` | ✅ | `add_arrival_date()`; o mês é mapeado para 1–12 via `create_map` |
| `children` nulo → `0` | ✅ | `handle_nulls()` (4 registros, incluindo os `"NA"` da fonte) |
| `country` nulo → `"UNK"` | ✅ | `handle_nulls()` (488 registros) |
| Filtrar reservas sem hóspedes | ✅ | `filter_invalid()`: 180 removidas |
| Filtrar `adr < 0` | ✅ | `filter_invalid()`: 1 removida |
| Documentar quantos registros cada filtro removeu | ✅ | Log de cada filtro e a tabela acima |
| Pelo menos 3 colunas derivadas | ✅ | 9 colunas em `add_derived_columns()`, incluindo todas as 6 sugeridas |
| LEFT JOIN com `country_metadata` (`country_name`, `continent`) | ✅ | `enrich()` |
| LEFT JOIN com `hotel_metadata` (`hotel_city`, `hotel_star_rating`, `hotel_opened_year`) | ✅ | `enrich()` |
| Join sem duplicar registros | ✅ | `dropDuplicates` na chave das referências e comparação de contagem antes e depois (a etapa falha se divergir) |
| Parquet em `data/silver/bookings_enriched/` | ✅ | `main()` |
| Particionar por `arrival_date_year` + `arrival_date_month_num` (**bônus**) | ✅ bônus | `partitionBy("arrival_date_year", "arrival_date_month_num")` |
| Usar a API de DataFrames, sem SQL direto | ✅ | Nenhum `spark.sql()` no projeto |

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
| `booking_cancellation_risk` | reserva | `risk_score`, `risk_level`, mais as dimensões do perfil e `is_canceled` | Quais reservas acionar numa campanha de retenção? |
| `cancellation_risk_summary` | `risk_level` | `total_bookings`, `cancelled_bookings`, `cancellation_rate`, `pct_of_bookings`, `avg_risk_score`, `revenue_at_risk` | O score de risco separa bem quem cancela? |

Os resultados de cada visão estão em
[Respostas às perguntas de negócio](#respostas-às-perguntas-de-negócio).

### Checklist – requisitos do enunciado (seção 4.3)

| Requisito | Status | Onde / como |
|---|---|---|
| Pelo menos uma visão Gold (idealmente 2 ou 3) | ✅ | 7 visões em [`silver_to_gold.py`](scripts/silver_to_gold.py) |
| Sugestão 1: receita e ocupação por hotel × mês | ✅ | `revenue_by_hotel_month`, com todas as métricas sugeridas |
| Sugestão 2: cancelamentos por segmento | ✅ | `cancellation_by_segment`, com todas as métricas sugeridas |
| Sugestão 3: Top 20 países por receita | ✅ | `top_countries_by_revenue`, ordenada por `total_revenue` desc com `limit(20)` |
| Sugestão 4: estadia média e perfil de hóspedes | ✅ | `guest_profile_by_hotel`, com granularidade `(hotel, customer_type)` |
| Visão proposta pelo candidato | ✅ extra | `cancellation_by_lead_time`, sobre risco de cancelamento para retenção |
| Visões propostas pelo candidato: score de risco | ✅ extra | `booking_cancellation_risk` (score por reserva) e `cancellation_risk_summary` (validação) |
| DataFrame agregado (`groupBy` + `agg`) | ✅ | As 6 visões agregadas; `booking_cancellation_risk` é a única por reserva e usa `groupBy` + `agg` para calcular as taxas dos perfis |
| Salvo em `data/gold/` em Parquet ou CSV | ✅ | Parquet com Snappy, um arquivo por visão |
| Nomes de colunas autoexplicativos | ✅ | `total_revenue`, `cancellation_rate`, `avg_lead_time` etc. |

## Respostas às perguntas de negócio

As quatro perguntas da seção 1 do enunciado são respondidas pelas visões Gold.
Os números abaixo vêm da execução sobre o dataset completo (119.209 reservas na
Silver). Valores em euros (€).

### 1. Quanto cada hotel está faturando mês a mês?

Visão: `revenue_by_hotel_month` (52 linhas, uma por hotel × mês).

> **Só 2 hotéis.** Essa é uma limitação do dataset público, não um recorte
> nosso. O enunciado (seção 2.1) diz que as reservas estão "distribuídas em dois
> hotéis em Portugal", e o artigo original (Antonio, Almeida & Nunes, 2019)
> anonimizou os hotéis como `City Hotel` e `Resort Hotel`. O
> `hotel_metadata.csv` tem exatamente essas 2 linhas. O pipeline não depende
> desse número: a visão agrupa por `hotel` e funcionaria igual com as mais de
> 100 mil propriedades da Reservia.

| Hotel | 2015 (jul–dez) | 2016 (ano todo) | 2017 (jan–ago) | Total |
|---|---|---|---|---|
| City Hotel (Lisboa) | €1,89 mi | €6,86 mi | €5,64 mi | **€14,39 mi** |
| Resort Hotel (Algarve) | €2,62 mi | €4,81 mi | €4,17 mi | **€11,60 mi** |

| Destaque | Hotel | Mês | Receita | Diária média (`avg_adr`) |
|---|---|---|---|---|
| Melhor mês | Resort Hotel | ago/2017 | €1,08 mi | €201,68 |
| Pior mês | City Hotel | jul/2015 | €80 mil | €73,91 (67% de cancelamento) |
| Pior mês do Resort | Resort Hotel | jan/2016 | €89 mil | €45,36 |

| Hotel | Taxa média de cancelamento mensal |
|---|---|
| City Hotel | 41,5% |
| Resort Hotel | 27,1% |

O City fatura mais no total, mas cancela bem mais. O Resort tem sazonalidade
forte, com pico no verão europeu e vale no inverno.

### 2. Quais segmentos e canais cancelam mais? Com quanta antecedência reservam?

Visão: `cancellation_by_segment`.

| Segmento | Reservas | Cancelamento | Antecedência média |
|---|---|---|---|
| Groups | 19.790 | **61,1%** | 187 dias |
| Online TA | 56.408 | 36,8% | 83 dias |
| Offline TA/TO | 24.182 | 34,3% | 135 dias |
| Aviation | 235 | 22,1% | 4 dias |
| Corporate | 5.282 | 18,8% | 22 dias |
| Direct | 12.582 | 15,4% | 50 dias |
| Complementary | 728 | 12,2% | 14 dias |

| Canal | Reservas | Cancelamento | Antecedência média |
|---|---|---|---|
| TA/TO | 97.750 | **41,1%** | 115 dias |
| Corporate | 6.651 | 22,1% | 45 dias |
| GDS | 193 | 19,2% | 21 dias |
| Direct | 14.610 | 17,5% | 58 dias |

Quem reserva com mais antecedência cancela mais. A combinação mais crítica é
`Groups` via `TA/TO` com cliente `Transient` (7.573 reservas), que cancela ~98%
com antecedência média de 225 dias.

### 3. De que países vêm os hóspedes mais valiosos?

Visão: `top_countries_by_revenue` (Top 20; abaixo os 10 primeiros).

| # | País | Reservas efetivas | Receita | Ticket médio | Antecedência média |
|---|---|---|---|---|---|
| 1 | Portugal | 20.977 | €5,54 mi | €264 | 50 dias |
| 2 | Reino Unido | 9.667 | €4,11 mi | €425 | 127 dias |
| 3 | França | 8.468 | €3,10 mi | €366 | 78 dias |
| 4 | Espanha | 6.383 | €2,25 mi | €352 | 44 dias |
| 5 | Alemanha | 6.067 | €2,07 mi | €341 | 139 dias |
| 6 | Irlanda | 2.542 | €1,24 mi | **€488** | 112 dias |
| 7 | Itália | 2.428 | €0,87 mi | €357 | 80 dias |
| 8 | Bélgica | 1.868 | €0,76 mi | €406 | 93 dias |
| 9 | Holanda | 1.716 | €0,63 mi | €368 | 75 dias |
| 10 | Suíça | 1.298 | €0,55 mi | €422 | 84 dias |

Portugal lidera pelo volume, mas tem o menor ticket médio do Top 10. Por
reserva, os hóspedes mais valiosos vêm da Irlanda (€488), do Reino Unido (€425)
e da Suíça (€422).

### 4. Como detectar reservas com risco alto de cancelamento?

Visão: `cancellation_by_lead_time`.

| Antecedência | Reservas | Cancelamento | Receita perdida em cancelamentos |
|---|---|---|---|
| 0–7 dias | 19.635 | 9,6% | €0,45 mi |
| 8–30 dias | 18.945 | 27,9% | €2,05 mi |
| 31–90 dias | 29.529 | 37,7% | €4,33 mi |
| 91–180 dias | 26.420 | 44,7% | €5,13 mi |
| 181–365 dias | 21.533 | 55,5% | €4,42 mi |
| > 365 dias | 3.147 | **67,7%** | €0,36 mi |

| Tipo de hóspede | Reservas | Cancelamento |
|---|---|---|
| Novo | 115.455 | 37,8% |
| Recorrente | 3.754 | 14,7% |

| Tipo de depósito | Reservas | Cancelamento |
|---|---|---|
| No Deposit | 104.460 | 28,4% |
| Non Refund | 14.587 | 99,4% ⚠️ |
| Refundable | 162 | 22,2% |

**Perfil de risco alto para campanhas de retenção:** hóspede novo, sem depósito
(`No Deposit`), antecedência acima de 30 dias e segmento `Groups` ou `TA/TO`.
As faixas de 31 a 365 dias concentram **€13,9 mi** em receita perdida com
cancelamentos.

> ⚠️ **Qualidade de dados:** a taxa de 99,4% em `Non Refund` contraria a
> intuição (depósito não reembolsável deveria reduzir cancelamentos). É uma
> anomalia conhecida desse dataset, provavelmente um problema de registro na
> fonte, e deve ser tratada como achado de qualidade de dados, não como regra
> de negócio.

#### Score de risco por reserva

Visões: `booking_cancellation_risk` (uma linha por reserva) e
`cancellation_risk_summary` (validação do score).

Cada reserva recebe um `risk_score`: a **taxa histórica de cancelamento do seu
perfil**. O perfil combina `lead_time_bucket`, `market_segment`, `deposit_type`
e `is_repeated_guest`. Perfis com menos de 30 reservas usam a taxa do perfil
mais amplo (`lead_time_bucket` × `deposit_type`), para evitar taxas instáveis.
O `risk_level` classifica o score: **High** (≥ 50%), **Medium** (30–50%) e
**Low** (< 30%).

| `risk_level` | Reservas | % do total | Cancelamento real | Receita perdida em cancelamentos |
|---|---|---|---|---|
| High | 22.300 | 18,7% | **83,9%** | €5,94 mi |
| Medium | 44.738 | 37,5% | 38,7% | €7,98 mi |
| Low | 52.171 | 43,8% | 15,7% | €2,81 mi |

O score separa bem os grupos: uma reserva High cancela 5 vezes mais que uma Low.
Para retenção, o foco natural é o grupo High e a parte do Medium com maior receita.

| High, por tipo de depósito | Reservas | Cancelamento real |
|---|---|---|
| Non Refund | 14.587 | 99,4% ⚠️ |
| No Deposit | 7.705 | 54,7% |
| Refundable | 8 | 50,0% |

> ⚠️ Dois terços do grupo High vêm da anomalia de `Non Refund` descrita acima.
> Sem ela, o High teria ~7,7 mil reservas, com 55% de cancelamento. Em produção,
> esse ponto teria de ser validado com a área de negócio antes de acionar
> campanhas com base no score.

**Limites:** o score é calculado e avaliado sobre os mesmos dados, então mede
quanto o perfil explica o cancelamento, e não a capacidade de prever reservas
novas. Em produção, as taxas por perfil seriam calculadas sobre o histórico e
aplicadas às reservas futuras, ainda não chegadas. O próximo passo seria um
modelo (por exemplo, regressão logística ou gradient boosting no Spark MLlib)
com separação entre treino e teste, avaliado por AUC.

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

## Checklist – entrega (seção 5)

| Requisito | Status | Onde / como |
|---|---|---|
| Tarefas em PySpark com a **API de DataFrames** (sem SQL direto), em scripts `.py` | ✅ | As 3 etapas usam só a API de DataFrames (`read`, `withColumn`, `filter`, `join`, `groupBy().agg()`), sem `spark.sql()` nem views temporárias. Optamos por scripts `.py` executados via `spark-submit` e não criamos notebook. |
| Scripts PySpark com leitura Bronze, transformações Silver e agregações Gold | ✅ | [`raw_to_bronze.py`](scripts/raw_to_bronze.py), [`bronze_to_silver.py`](scripts/bronze_to_silver.py), [`silver_to_gold.py`](scripts/silver_to_gold.py) |
| Código organizado e legível | ✅ | Uma função por transformação, config externa e módulo comum em [`utils/`](utils/) |
| Estrutura de pastas sugerida | ✅ | `scripts/`, `utils/` (com `config.yaml`), `data/{raw,bronze,silver,gold}/`; o notebook opcional não foi incluído |
| README: visão geral | ✅ | [Visão geral](#visão-geral) |
| README: versões do Python e do PySpark | ✅ | [Requisitos](#requisitos) |
| README: instalação de dependências | ✅ | [Instalação](#instalação) e `requirements.txt` |
| README: como apontar os caminhos dos CSVs | ✅ | [Caminhos dos arquivos](#caminhos-dos-arquivos) |
| README: comandos para rodar o pipeline | ✅ | [Rodando o pipeline](#rodando-o-pipeline) |
| README: recorte realizado | ✅ | [Recorte do dataset](#recorte-do-dataset) |
| README: principais transformações da Silver | ✅ | [Silver – principais transformações](#silver--principais-transformações) |
| README: visões Gold e pergunta de negócio de cada uma | ✅ | [Gold – visões analíticas](#gold--visões-analíticas) |
| Comentário arquitetural (AWS/Databricks, 1–2 parágrafos) | ✅ | [Comentário arquitetural](#comentário-arquitetural--produção-na-aws--databricks) |
| Entrega via repositório Git (preferido) | ✅ | https://github.com/marciohdo/BEFLY_SPARK_MARCIO |
