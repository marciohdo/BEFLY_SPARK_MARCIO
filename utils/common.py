"""Funções compartilhadas pelas etapas do pipeline: config, SparkSession e logging."""

import argparse
import logging
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = PROJECT_ROOT / "utils" / "config.yaml"


def get_logger(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    return logging.getLogger(name)


def parse_args(description: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG),
        help="Caminho do arquivo YAML de configuração (padrão: utils/config.yaml)",
    )
    return parser.parse_args()


def _resolve_paths(node):
    """Converte recursivamente caminhos relativos do YAML em absolutos (a partir da raiz)."""
    if isinstance(node, dict):
        return {k: _resolve_paths(v) for k, v in node.items()}
    if isinstance(node, str) and node.startswith("data/"):
        return str(PROJECT_ROOT / node)
    return node


def load_config(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return _resolve_paths(yaml.safe_load(f))


def get_spark(config: dict, step: str) -> SparkSession:
    spark_cfg = config["spark"]
    spark = (
        SparkSession.builder.appName(f"{spark_cfg['app_name']}-{step}")
        .master(spark_cfg["master"])
        .config("spark.sql.shuffle.partitions", spark_cfg["shuffle_partitions"])
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
