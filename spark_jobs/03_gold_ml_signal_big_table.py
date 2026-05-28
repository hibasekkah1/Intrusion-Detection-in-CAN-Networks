import argparse
import logging
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    hash as spark_hash,
    lit,
    when,
)

from common_bq import (
    ATTACK_TYPES,
    bq_table,
    create_spark,
    delete_sources_rows,
    load_yaml,
    processed_sources,
    read_bq,
    table_exists,
    write_audit_status,
    write_bq,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("gold_ml_signal_big_table_no_pmod")

DOMAIN = "ml"


def add_ml_split(df: DataFrame) -> DataFrame:
    """
    Add deterministic ML split without using pyspark.sql.functions.pmod.

    Dataproc image Spark can miss the Python pmod wrapper. This implementation uses
    Column modulo (%) instead:
      split_bucket = abs(hash(event_id)) % 100
      0-69   -> train
      70-84  -> validation
      85-99  -> test
    """
    if "event_id" not in df.columns:
        raise RuntimeError("Gold ML requires column event_id in Silver ML signal_clean tables")

    bucket = (spark_abs(spark_hash(col("event_id"))) % lit(100)).cast("int")

    return (
        df.withColumn("split_bucket", bucket)
        .withColumn(
            "ml_split",
            when(col("split_bucket") < lit(70), lit("train"))
            .when(col("split_bucket") < lit(85), lit("validation"))
            .otherwise(lit("test")),
        )
        .drop("split_bucket")
    )


def read_silver_signal_tables(spark, cfg) -> DataFrame:
    dfs = []

    for attack_type in ATTACK_TYPES:
        table_name = f"signal_clean_{attack_type}"
        dataset_key = f"silver_{DOMAIN}"

        if not table_exists(cfg, dataset_key, table_name):
            logger.warning("Silver ML table missing, skipping: %s", bq_table(cfg, dataset_key, table_name))
            continue

        logger.info("Reading %s.%s", dataset_key, table_name)
        df = read_bq(spark, cfg, dataset_key, table_name)
        dfs.append(df)

    if not dfs:
        raise RuntimeError("No Silver ML signal_clean_* tables found")

    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), dfs)


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold ML signal big table without pmod import")
    parser.add_argument("--config_path", required=True)
    args = parser.parse_args()

    cfg = load_yaml(args.config_path)
    spark = create_spark("gold_ml_signal_big_table_no_pmod")

    try:
        df = read_silver_signal_tables(spark, cfg)

        all_sources = [row["source_file"] for row in df.select("source_file").distinct().collect()]
        done_sources = processed_sources(cfg, "gold", DOMAIN, "signal")
        new_sources = [source for source in all_sources if source not in done_sources]

        if not new_sources:
            logger.info("No new SIGNAL files for Gold ML")
            return

        logger.info("Gold ML new signal sources=%s", len(new_sources))

        out = add_ml_split(df.filter(col("source_file").isin(new_sources))).persist(StorageLevel.MEMORY_AND_DISK)

        try:
            delete_sources_rows(cfg, f"gold_{DOMAIN}", "signal_big_table", new_sources)
            write_bq(out, cfg, f"gold_{DOMAIN}", "signal_big_table", "append")

            for source in new_sources:
                write_audit_status(cfg, "gold", DOMAIN, "signal", source, "SUCCESS")

            logger.info("Gold ML signal_big_table completed successfully")
        finally:
            out.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
