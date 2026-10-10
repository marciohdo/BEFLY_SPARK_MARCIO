"""Raw -> Bronze: lê os CSVs de origem com schema explícito e persiste em Parquet (Snappy).

Nenhuma regra de negócio é aplicada aqui. A única adição são colunas técnicas de
linhagem (_source_file, _ingested_at), para auditoria e reprocessamento.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from utils.common import get_logger, get_spark, load_config, parse_args

log = get_logger("raw_to_bronze")

# Schema explícito espelhando o CSV original.
# - `children` fica como string: a fonte usa "NA" (e não "NULL") para nulos nessa
#   coluna; a conversão para inteiro é responsabilidade da Silver.
# - `reservation_status_date` fica como string (cru); o cast para date ocorre na Silver.
BOOKINGS_SCHEMA = StructType(
    [
        StructField("hotel", StringType()),
        StructField("is_canceled", IntegerType()),
        StructField("lead_time", IntegerType()),
        StructField("arrival_date_year", IntegerType()),
        StructField("arrival_date_month", StringType()),
        StructField("arrival_date_week_number", IntegerType()),
        StructField("arrival_date_day_of_month", IntegerType()),
        StructField("stays_in_weekend_nights", IntegerType()),
        StructField("stays_in_week_nights", IntegerType()),
        StructField("adults", IntegerType()),
        StructField("children", StringType()),
        StructField("babies", IntegerType()),
        StructField("meal", StringType()),
        StructField("country", StringType()),
        StructField("market_segment", StringType()),
        StructField("distribution_channel", StringType()),
        StructField("is_repeated_guest", IntegerType()),
        StructField("previous_cancellations", IntegerType()),
        StructField("previous_bookings_not_canceled", IntegerType()),
        StructField("reserved_room_type", StringType()),
        StructField("assigned_room_type", StringType()),
        StructField("booking_changes", IntegerType()),
        StructField("deposit_type", StringType()),
        StructField("agent", IntegerType()),
        StructField("company", IntegerType()),
        StructField("days_in_waiting_list", IntegerType()),
        StructField("customer_type", StringType()),
        StructField("adr", DoubleType()),
        StructField("required_car_parking_spaces", IntegerType()),
        StructField("total_of_special_requests", IntegerType()),
        StructField("reservation_status", StringType()),
        StructField("reservation_status_date", StringType()),
    ]
)

COUNTRIES_SCHEMA = StructType(
    [
        StructField("country_code", StringType()),
        StructField("country_name", StringType()),
        StructField("continent", StringType()),
    ]
)

HOTELS_SCHEMA = StructType(
    [
        StructField("hotel_name", StringType()),
        StructField("city", StringType()),
        StructField("country_code", StringType()),
        StructField("star_rating", IntegerType()),
        StructField("opened_year", IntegerType()),
    ]
)


def read_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("sep", ",")
        .option("nullValue", "NULL")  # a fonte usa a string literal "NULL" para nulos
        .option("mode", "FAILFAST")  # schema divergente deve quebrar a ingestão, não virar nulo
        .schema(schema)
        .csv(path)
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_ingested_at", F.current_timestamp())
    )


def main() -> None:
    args = parse_args(__doc__)
    config = load_config(args.config)
    spark = get_spark(config, "raw_to_bronze")

    # Recorte: dataset completo (~119k linhas), por ser pequeno. Ver README.
    sources = [
        ("bookings", BOOKINGS_SCHEMA, "arrival_date_year"),
        ("countries", COUNTRIES_SCHEMA, None),
        ("hotels", HOTELS_SCHEMA, None),
    ]

    for name, schema, partition_col in sources:
        df = read_csv(spark, config["raw"][name], schema)
        writer = df.write.mode("overwrite").option("compression", "snappy")
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.parquet(config["bronze"][name])
        log.info("Bronze %-9s | %7d linhas -> %s", name, df.count(), config["bronze"][name])

    spark.stop()


if __name__ == "__main__":
    main()
