# Teste Técnico – PySpark + Arquitetura Medalhão (BeFly)

## Resumo do teste

**Nome:**
**Desafio BeFly – Pipeline de Dados em PySpark (Arquitetura Bronze–Silver–Gold)**

**Objetivo principal:**
Implementar um mini-pipeline de dados seguindo a arquitetura **Medalhão** (camadas **Bronze**, **Silver**, **Gold**) usando **PySpark**, processando dados de **reservas de hotéis** (contexto de turismo) para análise de receita, ocupação, cancelamentos e perfil de hóspedes.

**Perfil alvo:**
Engenheiro(a) de Dados **Pleno/Sênior**

**Tempo estimado de execução:**
Até **2 horas**

**Tecnologias esperadas:**

- Python + **PySpark** (API de DataFrames)
- Execução local (ex.: `pyspark`, `spark-submit`, Spark local)
- Leitura/gravação de arquivos **CSV/Parquet** no sistema de arquivos local

**Principais habilidades avaliadas:**

- Ingestão de dados "brutos" com PySpark (**camada Bronze**)
- Limpeza, padronização, enriquecimento e integração de tabelas (**camada Silver**)
- Criação de **visões analíticas agregadas** (**camada Gold**)
- Organização lógica em camadas (pastas `bronze/`, `silver/`, `gold/` e schemas consistentes)
- Raciocínio arquitetural: como o pipeline local se encaixaria em um **Data Lake em nuvem (AWS, S3 + Glue + Athena / Lakehouse)**

---

## Enunciado para o(a) candidato(a)

### 1. Contexto de negócio

Você foi contratado pela **BeFly**, uma holding brasileira de turismo formada por mais de 30 empresas (agências, OTAs, consolidadoras, operadoras, corporate travel e turismo de luxo). Entre as marcas do grupo está a **Reservia**, consolidadora de hotéis com integração a mais de **100 mil propriedades** ao redor do mundo, atendendo agências parceiras.

A BeFly está construindo um **Data Lake em nuvem (AWS)** e um dos primeiros casos de uso priorizados é consolidar e analisar **dados de reservas de hotéis** para responder a perguntas críticas das marcas do grupo:

- Quanto cada hotel está faturando mês a mês?
- Quais segmentos de mercado e canais de distribuição cancelam mais reservas? Quanto antes reservam?
- De que países vêm os hóspedes mais valiosos?
- Como detectar reservas com risco alto de cancelamento para acionar campanhas de retenção?

No desenho de arquitetura da BeFly, os dados são organizados em camadas seguindo o modelo **Medalhão**:

- **Bronze:** dados brutos / quase brutos, espelhando as fontes originais
- **Silver:** dados limpos, padronizados, com tipos corrigidos e integrados
- **Gold:** visões analíticas, normalmente agregadas, para consumo por times de negócio/BI

Seu desafio é **simular esse processo localmente**, usando PySpark e um **dataset público de reservas de hotéis**.

---

### 2. Dataset público utilizado

Usaremos o dataset público **"Hotel Booking Demand"**:

- **Nome do dataset:** Hotel Booking Demand
- **Fonte original:** artigo *Hotel Booking Demand Datasets* (Antonio, Almeida & Nunes, *Data in Brief*, vol. 22, 2019)
- **Link Kaggle (recomendado):** <https://www.kaggle.com/datasets/jessemostipak/hotel-booking-demand>
- **Espelhos públicos no GitHub** (caso prefira não criar conta no Kaggle):
  - <https://github.com/aaqibqadeer/Hotel-booking-demand>
  - <https://github.com/rfordatascience/tidytuesday/tree/master/data/2020/2020-02-11>

O conjunto de dados utilizado neste teste possui **três arquivos** CSV:

#### 2.1. `hotel_bookings.csv` — arquivo principal

Contém **~119.390 reservas** entre 2015 e 2017, distribuídas em dois hotéis em Portugal (um *City Hotel* em Lisboa e um *Resort Hotel* no Algarve). Possui **32 colunas**, dentre elas:

| Coluna | Tipo esperado | Descrição |
|---|---|---|
| `hotel` | string | Tipo do hotel: `Resort Hotel` ou `City Hotel` |
| `is_canceled` | int | `1` se a reserva foi cancelada, `0` caso contrário |
| `lead_time` | int | Dias entre a data da reserva e a data de chegada |
| `arrival_date_year` | int | Ano da chegada (2015–2017) |
| `arrival_date_month` | string | Mês por extenso em inglês (`January`, `February`, …) |
| `arrival_date_week_number` | int | Número da semana ISO da chegada |
| `arrival_date_day_of_month` | int | Dia do mês da chegada (1–31) |
| `stays_in_weekend_nights` | int | Nº de noites em fins de semana (sáb/dom) |
| `stays_in_week_nights` | int | Nº de noites em dias úteis (seg–sex) |
| `adults` | int | Nº de adultos na reserva |
| `children` | int | Nº de crianças na reserva (pode estar nulo / `NA`) |
| `babies` | int | Nº de bebês na reserva |
| `meal` | string | Plano de refeição: `BB` (Bed & Breakfast), `HB` (Half Board), `FB` (Full Board), `SC` (Self-Catering), `Undefined` |
| `country` | string | Código ISO 3 do país de origem do hóspede (pode estar nulo, ex.: `"NULL"`) |
| `market_segment` | string | Segmento de mercado: `Online TA`, `Offline TA/TO`, `Direct`, `Corporate`, `Groups`, `Complementary`, `Aviation`, `Undefined` |
| `distribution_channel` | string | Canal de distribuição: `TA/TO`, `Direct`, `Corporate`, `GDS`, `Undefined` |
| `is_repeated_guest` | int | `1` se o hóspede já reservou antes, `0` caso contrário |
| `previous_cancellations` | int | Cancelamentos anteriores desse hóspede |
| `previous_bookings_not_canceled` | int | Reservas anteriores não canceladas |
| `reserved_room_type` | string | Tipo de quarto reservado (códigos `A`–`L`, anonimizados) |
| `assigned_room_type` | string | Tipo de quarto efetivamente atribuído |
| `booking_changes` | int | Quantidade de alterações feitas na reserva |
| `deposit_type` | string | `No Deposit`, `Refundable`, `Non Refund` |
| `agent` | int | ID do agente que fez a reserva (pode ser nulo) |
| `company` | int | ID da empresa contratante (frequentemente nulo) |
| `days_in_waiting_list` | int | Dias em lista de espera |
| `customer_type` | string | `Transient`, `Contract`, `Transient-Party`, `Group` |
| `adr` | double | **A**verage **D**aily **R**ate — valor médio diário em € |
| `required_car_parking_spaces` | int | Vagas de estacionamento solicitadas |
| `total_of_special_requests` | int | Quantidade de pedidos especiais |
| `reservation_status` | string | `Check-Out`, `Canceled`, `No-Show` |
| `reservation_status_date` | date | Data do último status da reserva |

#### 2.2. `country_metadata.csv` — tabela de referência geográfica

Mapeia os códigos de país para nome e continente (~50 países representados nas reservas).

| Coluna | Tipo | Descrição |
|---|---|---|
| `country_code` | string | Código ISO 3 (ex.: `PRT`, `GBR`, `BRA`) |
| `country_name` | string | Nome por extenso |
| `continent` | string | Continente (`Europe`, `Asia`, `South America`, etc.) |

#### 2.3. `hotel_metadata.csv` — tabela de referência dos hotéis

Caracteriza cada um dos dois hotéis presentes no dataset.

| Coluna | Tipo | Descrição |
|---|---|---|
| `hotel_name` | string | `Resort Hotel` ou `City Hotel` |
| `city` | string | Cidade do hotel |
| `country_code` | string | Código ISO 3 do país |
| `star_rating` | int | Quantidade de estrelas |
| `opened_year` | int | Ano de inauguração |

> **Observação:** os arquivos `country_metadata.csv` e `hotel_metadata.csv`
> são **disponibilizados junto a este teste** (entregues em anexo). Eles
> simulam tabelas de referência mantidas pela BeFly em sistemas internos.

#### Recorte recomendado

O arquivo `hotel_bookings.csv` tem **~119.390 linhas**, o que **roda
confortavelmente em PySpark local em laptops modestos** (cabe em ~200 MB de
RAM). Você pode optar por:

- **Manter o dataset completo** (recomendado se sua máquina tem ≥ 8 GB RAM); ou
- **Recortar** para um subconjunto, por exemplo:
  - apenas um ano: `arrival_date_year = 2016`
  - apenas um hotel: `hotel = 'City Hotel'`
  - amostra aleatória: 30% dos registros

Documente no README qual recorte você escolheu (ou se manteve o dataset completo).

---

### 3. Arquitetura esperada (conceitual)

Você deve simular localmente a arquitetura **Medalhão** usando diretórios:

- `data/bronze/` – dados brutos ou quase brutos
- `data/silver/` – dados limpos e integrados
- `data/gold/` – visões analíticas

**Camada Bronze**

- Recebe os dados *como vieram* das fontes (CSVs).
- Persiste em formato **Parquet** com mínimas modificações (ideal: schema próximo ao original, sem transformações de negócio).
- Objetivo: preservar uma cópia confiável e reprocessável dos dados de origem.
- Exemplo de paths:
  - `data/bronze/bookings/`
  - `data/bronze/countries/`
  - `data/bronze/hotels/`

**Camada Silver**

- Recebe os dados da Bronze.
- Aplica **limpeza**, **conversão de tipos**, tratamento de valores nulos e **enriquecimento** (joins com tabelas de referência).
- Resulta em dados coerentes, com colunas bem definidas e prontas para análise.
- Exemplo de path: `data/silver/bookings_enriched/`

**Camada Gold**

- Recebe os dados da Silver.
- Produz **tabelas agregadas** que respondem a perguntas de negócio.
- Geralmente tem volume bem menor, focada em indicadores.
- Exemplo de paths:
  - `data/gold/revenue_by_hotel_month/`
  - `data/gold/cancellation_by_segment/`
  - `data/gold/top_countries_by_revenue/`

---

### 4. Tarefas em PySpark

Implemente as tarefas abaixo em PySpark usando a **API de DataFrames** (não SQL direto). Você pode usar um ou mais scripts `.py` ou um notebook (`.ipynb`).

#### 4.1 Bronze – ingestão dos dados

1. **Leitura dos arquivos brutos**

   - Ler, em PySpark, os três arquivos CSV:
     - `hotel_bookings.csv`
     - `country_metadata.csv`
     - `hotel_metadata.csv`
   - Configure a leitura com:
     - `header = True`
     - separador `,`
     - inferência de schema (`inferSchema = True`) **ou** schema explícito (esquema explícito é considerado **bônus**).
     - Atenção: o `hotel_bookings.csv` original usa a string literal `"NULL"` para representar valores nulos em algumas colunas (ex.: `agent`, `company`, `country`). Configure a leitura para tratar essa string como nulo.
   - Aplique o **recorte de volume** que julgar adequado (caso decida recortar).

2. **Persistência na camada Bronze**

   - Salve os DataFrames lidos em `data/bronze/` em formato **Parquet** com compressão Snappy.
   - Exemplo de organização:
     - `data/bronze/bookings/`
     - `data/bronze/countries/`
     - `data/bronze/hotels/`
   - **Bônus:** particionar a saída de `bookings` por `arrival_date_year`.

---

#### 4.2 Silver – limpeza, padronização e enriquecimento

A partir dos dados Bronze, crie um DataFrame Silver. Expectativas:

1. **Conversão de tipos**

   - Garantir tipos numéricos adequados:
     - `is_canceled`, `lead_time`, `arrival_date_year`, `arrival_date_week_number`, `arrival_date_day_of_month`, `stays_in_weekend_nights`, `stays_in_week_nights`, `adults`, `children`, `babies`, `is_repeated_guest`, `previous_cancellations`, `previous_bookings_not_canceled`, `booking_changes`, `days_in_waiting_list`, `required_car_parking_spaces`, `total_of_special_requests` → `integer`
     - `adr` → `double`
   - `reservation_status_date` → `date`

2. **Criação de coluna de data de chegada**

   - Criar `arrival_date` (`date`) a partir de `arrival_date_year`, `arrival_date_month` (mês por extenso em inglês — você precisará mapeá-lo para um número 1–12) e `arrival_date_day_of_month`. Use `to_date`, `concat_ws`, `lpad` (e similar).

3. **Tratamento de valores nulos / inconsistentes**

   - `children`: substituir nulos por `0` (são poucos casos).
   - `country`: substituir nulos por `"UNK"` (ou outra estratégia documentada).
   - Filtrar registros claramente inválidos:
     - reservas sem nenhum hóspede (`adults + children + babies = 0`)
     - registros com `adr < 0` (erros de digitação na fonte)
   - Documente quantos registros foram removidos em cada filtro (logs ajudam).

4. **Criação de colunas derivadas (faça pelo menos 3)**

   Exemplos esperados:

   - `total_nights = stays_in_weekend_nights + stays_in_week_nights`
   - `total_guests = adults + children + babies`
   - `is_family`: `1` se `children > 0` OU `babies > 0`, senão `0`
   - `revenue`: `adr * total_nights` para reservas **não canceladas**, `0` caso contrário
   - `booking_status`: legível a partir de `reservation_status` (`Canceled` / `NoShow` / `CheckedOut`)
   - `is_long_stay`: `1` se `total_nights > 7`
   - Outras colunas derivadas são bem-vindas, desde que façam sentido.

5. **Enriquecimento com tabelas de referência**

   - **Junção com `country_metadata`:**
     - `bookings.country == country_metadata.country_code` (LEFT JOIN)
     - traz `country_name` e `continent`
   - **Junção com `hotel_metadata`:**
     - `bookings.hotel == hotel_metadata.hotel_name` (LEFT JOIN)
     - traz `hotel_city`, `hotel_star_rating`, `hotel_opened_year`
   - Certifique-se de que a junção **não duplique** registros (uma linha por reserva).

6. **Persistência da camada Silver**

   - Salvar o DataFrame Silver em formato **Parquet** em `data/silver/bookings_enriched/`.
   - **Bônus:** particionar por `arrival_date_year` + `arrival_date_month_num` (ou apenas `arrival_date_year`) para acelerar queries temporais.

---

#### 4.3 Gold – visões analíticas

A partir dos dados Silver, crie **pelo menos uma** visão Gold (idealmente duas ou três). Sugestões alinhadas a perguntas reais da BeFly — escolha as que preferir, ou proponha as suas:

1. **Receita e ocupação por hotel × mês**
   - Granularidade: `(hotel, ano, mês)`
   - Métricas:
     - `total_bookings`, `effective_bookings`, `cancelled_bookings`
     - `total_revenue` (soma de `revenue`)
     - `avg_adr` (apenas em reservas efetivas)
     - `total_nights_sold`
     - `cancellation_rate = cancelled_bookings / total_bookings`

2. **Cancelamentos por segmento**
   - Granularidade: `(market_segment, customer_type, distribution_channel)` ou um subconjunto dessas dimensões
   - Métricas:
     - `total_bookings`, `cancelled_bookings`, `cancellation_rate`
     - `avg_lead_time`
     - `avg_total_special_requests`

3. **Top países por receita**
   - Granularidade: `(country, country_name, continent)`
   - Métricas:
     - `effective_bookings`, `total_revenue`, `avg_ticket`, `avg_lead_time`
   - Apresentar **Top 20** ordenados por `total_revenue` desc.

4. **Análise de estadia média e perfil de hóspedes**
   - Granularidade: `(hotel, customer_type)` ou `(continent, is_family)`
   - Métricas:
     - `avg_total_nights`, `avg_total_guests`, `pct_long_stay`, `pct_family`

**Requisitos para a camada Gold:**

- A visão Gold deve ser um **DataFrame agregado** (`groupBy` + `agg`).
- O resultado deve ser salvo em `data/gold/` em **Parquet ou CSV**.
- Nome de colunas autoexplicativo (ex.: `total_revenue`, `cancellation_rate`, `avg_lead_time`).

---

### 5. Entrega esperada

O que esperamos receber:

1. **Código fonte**

   - Scripts PySpark (`.py`) e/ou notebook (`.ipynb`) com:
     - Leitura Bronze
     - Transformações Silver
     - Agregações Gold
   - Código organizado e legível: nomes de variáveis claros, separação lógica das etapas, comentários pontuais quando necessário.

2. **Estrutura de pastas do projeto**

   Sugestão (pode adaptar):

   ```
   projeto/
     README.md
     scripts/
       raw_to_bronze.py
       bronze_to_silver.py
       silver_to_gold.py
     utils/
       config.yaml         (opcional — se usar config externa)
     notebooks/
       exploracao_inicial.ipynb   (opcional)
     data/
       raw/
         hotel_bookings.csv
         country_metadata.csv
         hotel_metadata.csv
       bronze/
         bookings/
         countries/
         hotels/
       silver/
         bookings_enriched/
       gold/
         revenue_by_hotel_month/
         cancellation_by_segment/
   ```

3. **README.md**

   Contendo:

   - **Visão geral** breve do que seu código faz.
   - **Instruções de execução**:
     - Versão do Python / PySpark usada.
     - Como instalar dependências.
     - Como apontar para os caminhos dos arquivos CSV originais.
     - Comando(s) para rodar o pipeline (ex.: `spark-submit scripts/raw_to_bronze.py` etc.).
   - Descrição:
     - Do **recorte** realizado (ou se manteve o dataset completo).
     - Das **principais transformações** aplicadas na Silver.
     - Da(s) **visão(ões) Gold** gerada(s) e qual pergunta de negócio cada uma responde.

4. **Comentário arquitetural (curto)**

   No README (ou em seção separada), escreva **1–2 parágrafos** respondendo:

   > Como você adaptaria esse pipeline (Bronze–Silver–Gold) para rodar na nuvem AWS e/ou Databricks em produção?
   > Cite serviços, padrões de armazenamento (S3 layout, formatos), orquestração, controle de qualidade e governança.

---

### 6. Sobre o tempo e a forma de entrega

- Tempo estimado: **até 2 horas**.
- Você pode entregar como repositório Git (preferido) ou como `.zip` por email.
- Não há "resposta certa única": estamos avaliando seu raciocínio de engenharia, sua atenção a detalhes e clareza na comunicação. Boa sorte!
