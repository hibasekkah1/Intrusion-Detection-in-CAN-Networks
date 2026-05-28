"""
02_silver_ml_clean.py
======================
Pipeline : Bronze ML → Silver ML

Stratégie :
  - Mode normal (--attack_type all) :
      1 seul job Spark → lit toutes les attaques en union → dispatch par attack_type
      Même approche que silver_analytics → rapide, 1 seul contexte YARN

  - Mode reprise partielle (--attack_type fuzz) :
      1 attaque uniquement → utile pour relancer une attaque échouée
      sans retraiter l'ensemble du pipeline

  - Mode représentation (--representation raw|signal|all) :
      Filtrer sur raw, signal, ou les deux
"""

import argparse
import logging
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lit, md5, min as spark_min
from pyspark.sql.types import DoubleType, IntegerType, LongType

from common_bq import (
    ATTACK_TYPES,
    bq_table,
    create_spark,
    delete_sources_rows,
    load_yaml,
    processed_sources,
    read_bq,
    read_bq_by_attack,
    table_exists,
    write_audit_status,
    write_bq,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("silver_ml_clean")

DOMAIN = "ml"


# ═══════════════════════════════════════════════════════════════
# TRANSFORMATIONS
# ═══════════════════════════════════════════════════════════════

def transform_raw(df: DataFrame) -> DataFrame:
    """
    Nettoyage RAW :
    - Cast timestamp_us / timestamp → LongType
    - Cast arbitration_id, dlc, label
    - Calcul elapsed_seconds depuis le début de la capture
    - Calcul event_id MD5 incluant le label
    - Déduplication par event_id
    """
    if "timestamp_us" in df.columns:
        df = df.withColumn("timestamp_us", col("timestamp_us").cast(LongType()))
    elif "timestamp" in df.columns:
        df = df.withColumn(
            "timestamp_us",
            (col("timestamp").cast(DoubleType()) * lit(1000000)).cast(LongType()),
        )
    else:
        raise RuntimeError("RAW table must contain timestamp_us or timestamp")

    for column_name, data_type in [
        ("arbitration_id", IntegerType()),
        ("dlc",            IntegerType()),
        ("label",          IntegerType()),
    ]:
        if column_name in df.columns:
            df = df.withColumn(column_name, col(column_name).cast(data_type))

    # Label absent → 0 (benign par défaut)
    if "label" not in df.columns:
        df = df.withColumn("label", lit(0).cast(IntegerType()))

    # elapsed_seconds depuis le début de la capture
    starts = df.groupBy("source_file", "dump_id").agg(
        spark_min("timestamp_us").alias("start_ts_us")
    )
    df = df.join(starts, ["source_file", "dump_id"], "left")

    # Colonnes optionnelles dans event_id
    arb_col = col("arbitration_id").cast("string") if "arbitration_id" in df.columns else lit("")
    dlc_col = col("dlc").cast("string")             if "dlc"            in df.columns else lit("")

    return (
        df
        .withColumn("event_id", md5(concat_ws("|",
            col("source_file"),
            col("timestamp_us").cast("string"),
            arb_col,
            dlc_col,
            col("label").cast("string"),
        )))
        .withColumn("elapsed_seconds", (col("timestamp_us") - col("start_ts_us")) / lit(1000000.0))
        .withColumn("is_attack", col("label") == 1)
        .drop("start_ts_us")
        .dropDuplicates(["event_id"])
    )


def transform_signal(df: DataFrame) -> DataFrame:
    """
    Nettoyage SIGNAL :
    - Cast timestamp → DoubleType
    - Cast label
    - Calcul elapsed_seconds depuis le début de la capture
    - Calcul event_id MD5
    - Déduplication par event_id
    """
    if "timestamp" not in df.columns:
        raise RuntimeError("SIGNAL table must contain timestamp")

    df = df.withColumn("timestamp", col("timestamp").cast(DoubleType()))

    if "label" in df.columns:
        df = df.withColumn("label", col("label").cast(IntegerType()))
    else:
        df = df.withColumn("label", lit(0).cast(IntegerType()))

    starts = df.groupBy("source_file", "dump_id").agg(
        spark_min("timestamp").alias("start_ts")
    )
    df = df.join(starts, ["source_file", "dump_id"], "left")

    return (
        df
        .withColumn("event_id", md5(concat_ws("|",
            col("source_file"),
            col("timestamp").cast("string"),
            col("label").cast("string"),
        )))
        .withColumn("elapsed_seconds", col("timestamp") - col("start_ts"))
        .withColumn("is_attack", col("label") == 1)
        .drop("start_ts")
        .dropDuplicates(["event_id"])
    )


# ═══════════════════════════════════════════════════════════════
# ÉCRITURE PAR ATTACK_TYPE
# ═══════════════════════════════════════════════════════════════

def write_by_attack(df: DataFrame, cfg: dict, rep: str, attacks: list) -> None:
    """
    Dispatch le DataFrame par attack_type et écrit dans les tables Silver ML.
    Même logique que silver_analytics → 1 seul contexte Spark.
    """
    base = "raw_clean" if rep == "raw" else "signal_clean"

    for attack in attacks:
        sub = df.filter(col("attack_type") == attack)
        sources = [r["source_file"] for r in sub.select("source_file").distinct().collect()]

        if not sources:
            logger.info("Aucune source pour attack=%s rep=%s", attack, rep)
            continue

        output_table = f"{base}_{attack}"
        logger.info("Écriture Silver ML %s attack=%s → %s (%s sources)",
                    rep, attack, output_table, len(sources))

        delete_sources_rows(cfg, f"silver_{DOMAIN}", output_table, sources)
        write_bq(sub, cfg, f"silver_{DOMAIN}", output_table, "append")

        for source in sources:
            write_audit_status(cfg, "silver", DOMAIN, rep, source, "SUCCESS")


# ═══════════════════════════════════════════════════════════════
# MODE NORMAL — 1 seul job Spark, toutes attaques
# ═══════════════════════════════════════════════════════════════

def process_all_attacks(spark, cfg: dict, rep: str) -> None:
    """
    Mode rapide : lit toutes les tables Bronze ML en union,
    transforme en 1 seul job Spark, dispatch par attack_type.
    Même architecture que silver_analytics → overhead YARN minimal.
    """
    input_base = "raw_valid" if rep == "raw" else "signal_valid"

    logger.info("[silver_ml] Mode ALL — rep=%s lecture bronze union", rep)

    try:
        df = read_bq_by_attack(spark, cfg, f"bronze_{DOMAIN}", input_base)
    except RuntimeError as e:
        logger.warning("Bronze ML tables absentes pour rep=%s : %s", rep, e)
        return

    # Filtrer les sources déjà traitées
    all_sources = [r["source_file"] for r in df.select("source_file").distinct().collect()]
    done        = processed_sources(cfg, "silver", DOMAIN, rep)
    new_sources = [s for s in all_sources if s not in done]

    if not new_sources:
        logger.info("[silver_ml] Aucune nouvelle source pour rep=%s", rep)
        return

    logger.info("[silver_ml] Nouvelles sources rep=%s : %d", rep, len(new_sources))

    filtered = df.filter(col("source_file").isin(new_sources))
    transform = transform_raw if rep == "raw" else transform_signal
    out = transform(filtered).persist(StorageLevel.MEMORY_AND_DISK)

    try:
        write_by_attack(out, cfg, rep, ATTACK_TYPES)
    finally:
        out.unpersist()


# ═══════════════════════════════════════════════════════════════
# MODE REPRISE PARTIELLE — 1 attaque, 1 représentation
# ═══════════════════════════════════════════════════════════════

def process_one_attack(spark, cfg: dict, rep: str, attack_type: str) -> None:
    """
    Mode reprise partielle : 1 seule attaque.
    Utile pour relancer un attack_type échoué sans tout retraiter.
    """
    input_base   = "raw_valid" if rep == "raw" else "signal_valid"
    output_base  = "raw_clean" if rep == "raw" else "signal_clean"
    input_table  = f"{input_base}_{attack_type}"
    output_table = f"{output_base}_{attack_type}"
    dataset_key  = f"bronze_{DOMAIN}"

    if not table_exists(cfg, dataset_key, input_table):
        logger.warning("Table Bronze absente, skipped: %s",
                       bq_table(cfg, dataset_key, input_table))
        return

    logger.info("[silver_ml] Mode ONE attack=%s rep=%s", attack_type, rep)
    df = read_bq(spark, cfg, dataset_key, input_table)

    all_sources = [r["source_file"] for r in df.select("source_file").distinct().collect()]
    done        = processed_sources(cfg, "silver", DOMAIN, rep)
    new_sources = [s for s in all_sources if s not in done]

    if not new_sources:
        logger.info("Aucune nouvelle source pour attack=%s rep=%s", attack_type, rep)
        return

    logger.info("Silver ML %s attack=%s new_sources=%d", rep, attack_type, len(new_sources))

    filtered = df.filter(col("source_file").isin(new_sources))
    transform = transform_raw if rep == "raw" else transform_signal
    out = transform(filtered).persist(StorageLevel.MEMORY_AND_DISK)

    try:
        delete_sources_rows(cfg, f"silver_{DOMAIN}", output_table, new_sources)
        write_bq(out, cfg, f"silver_{DOMAIN}", output_table, "append")

        for source in new_sources:
            write_audit_status(cfg, "silver", DOMAIN, rep, source, "SUCCESS")

        logger.info("Completed Silver ML %s attack=%s", rep, attack_type)
    finally:
        out.unpersist()


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Silver ML — fast single-job mode + partial recovery"
    )
    parser.add_argument("--config_path",    required=True)
    parser.add_argument("--representation", choices=["raw", "signal", "all"], default="all",
                        help="Représentation à traiter (default: all)")
    parser.add_argument("--attack_type",    choices=ATTACK_TYPES + ["all"],   default="all",
                        help="Type d'attaque à traiter (default: all → mode rapide 1 job)")
    args = parser.parse_args()

    cfg  = load_yaml(args.config_path)
    reps = ["raw", "signal"] if args.representation == "all" else [args.representation]

    logger.info("Silver ML | representation=%s | attack_type=%s",
                args.representation, args.attack_type)

    spark = create_spark("silver_ml_clean")

    try:
        for rep in reps:
            if args.attack_type == "all":
                # ── Mode rapide : 1 job Spark, toutes attaques
                process_all_attacks(spark, cfg, rep)
            else:
                # ── Mode reprise partielle : 1 attaque uniquement
                process_one_attack(spark, cfg, rep, args.attack_type)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()