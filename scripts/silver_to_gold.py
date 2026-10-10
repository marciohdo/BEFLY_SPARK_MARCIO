"""Silver -> Gold: visões agregadas para as perguntas de negócio da BeFly."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from utils.common import get_logger, get_spark, load_config, parse_args

log = get_logger("silver_to_gold")


def is_canceled() -> Column:
    return F.col("is_canceled") == 1


def is_effective() -> Column:
    return F.col("is_canceled") == 0


def count_if(condition: Column) -> Column:
    return F.sum(F.when(condition, 1).otherwise(0))


def cancellation_metrics() -> list:
    return [
        F.count(F.lit(1)).alias("total_bookings"),
        count_if(is_canceled()).alias("cancelled_bookings"),
        F.round(F.avg(F.col("is_canceled")), 4).alias("cancellation_rate"),
    ]


def revenue_by_hotel_month(silver: DataFrame) -> DataFrame:
    """Quanto cada hotel fatura mês a mês?"""
    return (
        silver.groupBy(
            "hotel", "hotel_city", "arrival_date_year", "arrival_date_month_num", "arrival_date_month"
        )
        .agg(
            *cancellation_metrics(),
            count_if(is_effective()).alias("effective_bookings"),
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.round(F.avg(F.when(is_effective(), F.col("adr"))), 2).alias("avg_adr"),
            F.sum(F.when(is_effective(), F.col("total_nights")).otherwise(0)).alias("total_nights_sold"),
        )
        .orderBy("hotel", "arrival_date_year", "arrival_date_month_num")
    )


def cancellation_by_segment(silver: DataFrame) -> DataFrame:
    """Quais segmentos/canais cancelam mais? Com quanta antecedência reservam?"""
    return (
        silver.groupBy("market_segment", "distribution_channel", "customer_type")
        .agg(
            *cancellation_metrics(),
            F.round(F.avg("lead_time"), 1).alias("avg_lead_time"),
            F.round(F.avg("total_of_special_requests"), 2).alias("avg_total_special_requests"),
        )
        .orderBy(F.desc("cancellation_rate"), F.desc("total_bookings"))
    )


def cancellation_by_lead_time(silver: DataFrame) -> DataFrame:
    """Que perfis de reserva têm risco alto de cancelamento (insumo para campanhas de retenção)?"""
    return (
        silver.groupBy("lead_time_bucket", "deposit_type", "is_repeated_guest")
        .agg(
            *cancellation_metrics(),
            F.round(F.avg("previous_cancellations"), 3).alias("avg_previous_cancellations"),
            F.round(F.sum(F.when(is_canceled(), F.col("adr") * F.col("total_nights")).otherwise(0)), 2).alias(
                "revenue_at_risk"
            ),
        )
        .orderBy("lead_time_bucket", "deposit_type", "is_repeated_guest")
    )


def top_countries_by_revenue(silver: DataFrame, top_n: int = 20) -> DataFrame:
    """De que países vêm os hóspedes mais valiosos? (país desconhecido excluído)"""
    return (
        silver.filter(is_effective() & (F.col("country") != "UNK"))
        .groupBy("country", "country_name", "continent")
        .agg(
            F.count(F.lit(1)).alias("effective_bookings"),
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.round(F.sum("revenue") / F.count(F.lit(1)), 2).alias("avg_ticket"),
            F.round(F.avg("lead_time"), 1).alias("avg_lead_time"),
        )
        .orderBy(F.desc("total_revenue"))
        .limit(top_n)
    )


def guest_profile_by_hotel(silver: DataFrame) -> DataFrame:
    """Estadia média e perfil de hóspedes por hotel e tipo de cliente (apenas reservas efetivas)."""
    return (
        silver.filter(is_effective())
        .groupBy("hotel", "customer_type")
        .agg(
            F.count(F.lit(1)).alias("effective_bookings"),
            F.round(F.avg("total_nights"), 2).alias("avg_total_nights"),
            F.round(F.avg("total_guests"), 2).alias("avg_total_guests"),
            F.round(F.avg("is_long_stay"), 4).alias("pct_long_stay"),
            F.round(F.avg("is_family"), 4).alias("pct_family"),
        )
        .orderBy("hotel", F.desc("effective_bookings"))
    )


RISK_PROFILE = ["lead_time_bucket", "market_segment", "deposit_type", "is_repeated_guest"]
RISK_FALLBACK = ["lead_time_bucket", "deposit_type"]
RISK_MIN_SUPPORT = 30  # perfis com menos reservas usam a taxa do perfil mais amplo


def booking_cancellation_risk(silver: DataFrame) -> DataFrame:
    """Score de risco por reserva: taxa histórica de cancelamento do perfil da reserva.

    Em produção, as taxas por perfil seriam calculadas sobre o histórico e aplicadas
    às reservas futuras (ainda não chegadas) para acionar campanhas de retenção.
    """
    profile_rates = silver.groupBy(RISK_PROFILE).agg(
        F.count(F.lit(1)).alias("profile_bookings"),
        F.avg("is_canceled").alias("profile_rate"),
    )
    fallback_rates = silver.groupBy(RISK_FALLBACK).agg(F.avg("is_canceled").alias("fallback_rate"))

    risk_score = F.round(
        F.when(F.col("profile_bookings") >= RISK_MIN_SUPPORT, F.col("profile_rate")).otherwise(F.col("fallback_rate")),
        4,
    )
    return (
        silver.join(F.broadcast(profile_rates), RISK_PROFILE)
        .join(F.broadcast(fallback_rates), RISK_FALLBACK)
        .withColumn("risk_score", risk_score)
        .withColumn(
            "risk_level",
            F.when(F.col("risk_score") >= 0.5, "High").when(F.col("risk_score") >= 0.3, "Medium").otherwise("Low"),
        )
        .select(
            "hotel", "arrival_date", "booking_date", "country", "customer_type", "distribution_channel",
            *RISK_PROFILE, "adr", "total_nights", "is_canceled", "risk_score", "risk_level",
        )
    )


def cancellation_risk_summary(risk: DataFrame) -> DataFrame:
    """Validação do score: o cancelamento real deve crescer de Low para High."""
    total = risk.count()
    return (
        risk.groupBy("risk_level")
        .agg(
            *cancellation_metrics(),
            F.round(F.count(F.lit(1)) / F.lit(total), 4).alias("pct_of_bookings"),
            F.round(F.avg("risk_score"), 4).alias("avg_risk_score"),
            F.round(F.sum(F.when(is_canceled(), F.col("adr") * F.col("total_nights")).otherwise(0)), 2).alias(
                "revenue_at_risk"
            ),
        )
        .orderBy(F.desc("avg_risk_score"))
    )


def main() -> None:
    args = parse_args(__doc__)
    config = load_config(args.config)
    spark = get_spark(config, "silver_to_gold")

    silver = spark.read.parquet(config["silver"]["bookings_enriched"]).cache()
    log.info("Silver bookings_enriched: %d linhas", silver.count())

    risk = booking_cancellation_risk(silver).cache()
    views = {
        "revenue_by_hotel_month": revenue_by_hotel_month(silver),
        "cancellation_by_segment": cancellation_by_segment(silver),
        "cancellation_by_lead_time": cancellation_by_lead_time(silver),
        "top_countries_by_revenue": top_countries_by_revenue(silver),
        "guest_profile_by_hotel": guest_profile_by_hotel(silver),
        "booking_cancellation_risk": risk,
        "cancellation_risk_summary": cancellation_risk_summary(risk),
    }

    for name, df in views.items():
        # Volume pequeno: um único arquivo por visão facilita o consumo por BI.
        df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(config["gold"][name])
        log.info("Gold %-26s | %6d linhas -> %s", name, df.count(), config["gold"][name])
        df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
