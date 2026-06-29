
import argparse
import uuid
from datetime import datetime, timezone
from google.cloud import bigquery
from google.cloud import storage
import yaml


def load_yaml(path: str) -> dict:
    if path.startswith("gs://"):
        client = storage.Client()
        parts = path[5:].split("/", 1)
        blob = client.bucket(parts[0]).blob(parts[1])
        return yaml.safe_load(blob.download_as_text())
    with open(path, "r") as f:
        return yaml.safe_load(f)


def bq_table(cfg, dataset_key, table_name):
    project = cfg["bigquery"]["project_id"]
    dataset = cfg["bigquery"]["datasets"][dataset_key]
    return f"{project}.{dataset}.{table_name}"



def write_result(bq_client, cfg, table_name, check_name, status,
                 row_count=None, error_message=None):
    """Ecrit un resultat de controle qualite dans la table d'audit."""
    audit_table = bq_table(cfg, "audit", "data_quality_results")
    row = {
        "check_id": str(uuid.uuid4()),
        "table_name": table_name,
        "check_name": check_name,
        "status": status,
        "row_count": row_count,
        "error_message": error_message,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
    errors = bq_client.insert_rows_json(audit_table, [row])
    if errors:
        print(f"  [AUDIT ERROR] {errors}")
    icon = "[PASS]" if status == "PASS" else "[WARN]" if status == "WARN" else "[FAIL]"
    print(f"  {icon} {check_name} : {status}"
          + (f" — {error_message}" if error_message else ""))
    return status



def check_table_exists(bq_client, cfg, table_ref):
    """Controle 1 — La table existe dans BigQuery."""
    try:
        bq_client.get_table(table_ref)
        return write_result(bq_client, cfg, table_ref, "table_exists", "PASS")
    except Exception:
        write_result(bq_client, cfg, table_ref, "table_exists", "WARN",
                     error_message="Table absente — aucun fichier traite pour ce domaine")
        return "WARN"


def check_not_empty(bq_client, cfg, table_ref):
    """Controle 2 — La table contient au moins une ligne."""
    try:
        result = bq_client.query(
            f"SELECT COUNT(*) as n FROM `{table_ref}`"
        ).result()
        n = list(result)[0]["n"]
        if n == 0:
            return write_result(bq_client, cfg, table_ref, "row_count", "WARN",
                                row_count=0, error_message="Table vide")
        return write_result(bq_client, cfg, table_ref, "row_count", "PASS",
                            row_count=n)
    except Exception as e:
        return write_result(bq_client, cfg, table_ref, "row_count", "WARN",
                            error_message=str(e))


def check_required_columns(bq_client, cfg, table_ref, required_cols):
    """Controle 3 — Les colonnes requises sont presentes."""
    try:
        table = bq_client.get_table(table_ref)
        existing = {field.name for field in table.schema}
        missing = [c for c in required_cols if c not in existing]
        if missing:
            return write_result(bq_client, cfg, table_ref, "required_columns", "FAIL",
                                error_message=f"Colonnes manquantes : {missing}")
        return write_result(bq_client, cfg, table_ref, "required_columns", "PASS")
    except Exception as e:
        return write_result(bq_client, cfg, table_ref, "required_columns", "WARN",
                            error_message=str(e))


def check_null_rate(bq_client, cfg, table_ref, critical_cols, threshold=0.05):
    """
    Controle 4 — Taux de valeurs nulles sur les colonnes critiques.
    Seuil : 5% de nulls maximum sur une colonne critique -> FAIL
    """
    try:
        null_exprs = ", ".join(
            [f"COUNTIF({c} IS NULL) / COUNT(*) as null_rate_{c}"
             for c in critical_cols]
        )
        result = list(bq_client.query(
            f"SELECT {null_exprs} FROM `{table_ref}`"
        ).result())[0]

        violations = []
        for col in critical_cols:
            rate = result[f"null_rate_{col}"]
            if rate is not None and rate > threshold:
                violations.append(f"{col} : {rate:.1%} de nulls")

        if violations:
            return write_result(bq_client, cfg, table_ref, "null_rate", "FAIL",
                                error_message=f"Taux de nulls excessif : {violations}")
        return write_result(bq_client, cfg, table_ref, "null_rate", "PASS")
    except Exception as e:
        return write_result(bq_client, cfg, table_ref, "null_rate", "WARN",
                            error_message=str(e))


def check_categorical_values(bq_client, cfg, table_ref, column, allowed_values):
    """
    Controle 5 — Les valeurs d'une colonne categorique sont dans la liste attendue.
    Ex : attack_type doit etre parmi {benign, fuzz, fabr, masq, susp, repl}
    """
    try:
        allowed_str = ", ".join([f"'{v}'" for v in allowed_values])
        result = list(bq_client.query(f"""
            SELECT COUNT(*) as n
            FROM `{table_ref}`
            WHERE {column} NOT IN ({allowed_str})
              AND {column} IS NOT NULL
        """).result())[0]
        n_invalid = result["n"]
        if n_invalid > 0:
            return write_result(bq_client, cfg, table_ref,
                                f"categorical_values_{column}", "FAIL",
                                error_message=f"{n_invalid} valeurs invalides dans {column}")
        return write_result(bq_client, cfg, table_ref,
                            f"categorical_values_{column}", "PASS")
    except Exception as e:
        return write_result(bq_client, cfg, table_ref,
                            f"categorical_values_{column}", "WARN",
                            error_message=str(e))


def check_value_range(bq_client, cfg, table_ref, column, min_val=None, max_val=None):
    """
    Controle 6 — Les valeurs d'une colonne numerique sont dans la plage attendue.
    Ex : attack_rate_pct doit etre entre 0 et 100
    """
    try:
        conditions = []
        if min_val is not None:
            conditions.append(f"{column} < {min_val}")
        if max_val is not None:
            conditions.append(f"{column} > {max_val}")
        if not conditions:
            return "PASS"

        where = " OR ".join(conditions)
        result = list(bq_client.query(f"""
            SELECT COUNT(*) as n
            FROM `{table_ref}`
            WHERE ({where}) AND {column} IS NOT NULL
        """).result())[0]
        n_invalid = result["n"]
        if n_invalid > 0:
            return write_result(bq_client, cfg, table_ref,
                                f"value_range_{column}", "FAIL",
                                error_message=(
                                    f"{n_invalid} valeurs hors plage [{min_val}, {max_val}] "
                                    f"dans {column}"
                                ))
        return write_result(bq_client, cfg, table_ref, f"value_range_{column}", "PASS")
    except Exception as e:
        return write_result(bq_client, cfg, table_ref, f"value_range_{column}", "WARN",
                            error_message=str(e))


def check_metric_coherence(bq_client, cfg, table_ref, check_name, coherence_query,
                           error_msg):
    """
    Controle 7 — Coherence entre metriques calculees.
    Ex : attack_snapshot_count + normal_snapshot_count = total_snapshot_count
    La query doit retourner le nombre de lignes incoh\u00e9rentes.
    """
    try:
        result = list(bq_client.query(coherence_query).result())[0]
        n_incoherent = result["n"]
        if n_incoherent > 0:
            return write_result(bq_client, cfg, table_ref, check_name, "FAIL",
                                error_message=f"{n_incoherent} lignes : {error_msg}")
        return write_result(bq_client, cfg, table_ref, check_name, "PASS")
    except Exception as e:
        return write_result(bq_client, cfg, table_ref, check_name, "WARN",
                            error_message=str(e))


def check_freshness(bq_client, cfg, table_ref, timestamp_col="ingested_at",
                    max_hours=25):
    """
    Controle 8 — Fraicheur des donnees.
    La derniere donnee doit avoir ete inseree il y a moins de max_hours heures.
    """
    try:
        result = list(bq_client.query(f"""
            SELECT TIMESTAMP_DIFF(
                CURRENT_TIMESTAMP(),
                MAX({timestamp_col}),
                HOUR
            ) as hours_since_last
            FROM `{table_ref}`
        """).result())[0]
        hours = result["hours_since_last"]
        if hours is None:
            return write_result(bq_client, cfg, table_ref, "freshness", "WARN",
                                error_message="Impossible de determiner la fraicheur")
        if hours > max_hours:
            return write_result(bq_client, cfg, table_ref, "freshness", "WARN",
                                error_message=(
                                    f"Donnees vieilles de {hours}h "
                                    f"(seuil : {max_hours}h)"
                                ))
        return write_result(bq_client, cfg, table_ref, "freshness", "PASS")
    except Exception as e:
        return write_result(bq_client, cfg, table_ref, "freshness", "WARN",
                            error_message=str(e))


def check_volume_vs_silver(bq_client, cfg, gold_table, silver_table,
                           min_ratio=0.5):
    """
    Controle 9 — Volumetrie Gold vs Silver.
    La table Gold doit contenir au moins min_ratio * lignes Silver.
    Detecte les pertes anormales de donnees lors des agregations.
    """
    try:
        gold_n = list(bq_client.query(
            f"SELECT COUNT(*) as n FROM `{gold_table}`").result())[0]["n"]
        silver_n = list(bq_client.query(
            f"SELECT COUNT(*) as n FROM `{silver_table}`").result())[0]["n"]

        if silver_n == 0:
            return write_result(bq_client, cfg, gold_table, "volume_vs_silver", "WARN",
                                error_message="Table Silver vide")
        ratio = gold_n / silver_n
        if ratio < min_ratio:
            return write_result(bq_client, cfg, gold_table, "volume_vs_silver", "WARN",
                                row_count=gold_n,
                                error_message=(
                                    f"Gold contient {ratio:.1%} des lignes Silver "
                                    f"(seuil min : {min_ratio:.0%}). "
                                    f"Gold={gold_n}, Silver={silver_n}"
                                ))
        return write_result(bq_client, cfg, gold_table, "volume_vs_silver", "PASS",
                            row_count=gold_n)
    except Exception as e:
        return write_result(bq_client, cfg, gold_table, "volume_vs_silver", "WARN",
                            error_message=str(e))



ATTACK_TYPES = ["benign", "fuzz", "fabr", "masq", "susp", "repl"]
ML_SPLITS = ["train", "validation", "test"]
LABEL_VALUES = [0, 1]


def run_checks_fact_window(bq_client, cfg):
    """Controles sur fact_window_5min_wide."""
    t = bq_table(cfg, "gold_analytics", "fact_window_5min_wide")
    print(f"\n{'='*50}")
    print(f"Controles : {t}")
    print(f"{'='*50}")

    statuses = []

    # Controles structurels
    if check_table_exists(bq_client, cfg, t) == "WARN":
        return
    statuses.append(check_not_empty(bq_client, cfg, t))
    statuses.append(check_required_columns(bq_client, cfg, t, [
        "capture_id", "window_id", "attack_id", "source_file",
        "attack_type", "window_5min", "total_snapshot_count",
        "attack_snapshot_count", "normal_snapshot_count",
        "attack_rate_pct", "is_attack_window"
    ]))

    # Controles sur les nulls
    statuses.append(check_null_rate(bq_client, cfg, t, [
        "capture_id", "window_id", "attack_id", "attack_type",
        "total_snapshot_count", "attack_rate_pct"
    ]))

    # Controles categoriques
    statuses.append(check_categorical_values(
        bq_client, cfg, t, "attack_type", ATTACK_TYPES))

    # Controles de plages
    statuses.append(check_value_range(
        bq_client, cfg, t, "attack_rate_pct", min_val=0, max_val=100))
    statuses.append(check_value_range(
        bq_client, cfg, t, "window_5min", min_val=0))
    statuses.append(check_value_range(
        bq_client, cfg, t, "total_snapshot_count", min_val=0))

    # Coherence : attack + normal = total
    statuses.append(check_metric_coherence(
        bq_client, cfg, t,
        "coherence_snapshot_counts",
        f"""
        SELECT COUNT(*) as n FROM `{t}`
        WHERE ABS(attack_snapshot_count + normal_snapshot_count
                  - total_snapshot_count) > 1
        """,
        "attack_count + normal_count != total_count"
    ))

    # Coherence : window_start < window_end
    statuses.append(check_metric_coherence(
        bq_client, cfg, t,
        "coherence_window_boundaries",
        f"""
        SELECT COUNT(*) as n FROM `{t}`
        WHERE window_start_s >= window_end_s
        """,
        "window_start_s >= window_end_s"
    ))

    if "FAIL" in statuses:
        raise RuntimeError(f"FAIL detecte sur {t} — pipeline interrompu")


def run_checks_dim_attack(bq_client, cfg):
    """Controles sur dim_attack."""
    t = bq_table(cfg, "gold_analytics", "dim_attack")
    print(f"\n{'='*50}")
    print(f"Controles : {t}")
    print(f"{'='*50}")

    if check_table_exists(bq_client, cfg, t) == "WARN":
        return
    check_not_empty(bq_client, cfg, t)
    check_required_columns(bq_client, cfg, t,
                           ["attack_id", "attack_type", "severity"])
    check_categorical_values(bq_client, cfg, t, "attack_type", ATTACK_TYPES)
    check_categorical_values(bq_client, cfg, t, "severity",
                             ["LOW", "MEDIUM", "HIGH", "CRITICAL"])
    check_null_rate(bq_client, cfg, t, ["attack_id", "attack_type"])


def run_checks_dim_capture(bq_client, cfg):
    """Controles sur dim_capture."""
    t = bq_table(cfg, "gold_analytics", "dim_capture")
    print(f"\n{'='*50}")
    print(f"Controles : {t}")
    print(f"{'='*50}")

    if check_table_exists(bq_client, cfg, t) == "WARN":
        return
    check_not_empty(bq_client, cfg, t)
    check_required_columns(bq_client, cfg, t,
                           ["capture_id", "source_file", "duration_seconds",
                            "row_count", "label_0_count", "label_1_count"])
    check_null_rate(bq_client, cfg, t, ["capture_id", "source_file"])
    check_value_range(bq_client, cfg, t, "duration_seconds", min_val=0)
    check_value_range(bq_client, cfg, t, "row_count", min_val=0)

    # Coherence : label_0 + label_1 = row_count
    check_metric_coherence(
        bq_client, cfg, t,
        "coherence_label_counts",
        f"""
        SELECT COUNT(*) as n FROM `{t}`
        WHERE ABS(label_0_count + label_1_count - row_count) > 1
        """,
        "label_0_count + label_1_count != row_count"
    )


def run_checks_dim_window(bq_client, cfg):
    """Controles sur dim_window."""
    t = bq_table(cfg, "gold_analytics", "dim_window")
    print(f"\n{'='*50}")
    print(f"Controles : {t}")
    print(f"{'='*50}")

    if check_table_exists(bq_client, cfg, t) == "WARN":
        return
    check_not_empty(bq_client, cfg, t)
    check_required_columns(bq_client, cfg, t,
                           ["window_id", "window_5min",
                            "window_start_s", "window_end_s", "capture_phase"])
    check_categorical_values(bq_client, cfg, t, "capture_phase",
                             ["pre_attack", "during_attack",
                              "post_attack", "no_attack"])
    check_value_range(bq_client, cfg, t, "window_5min", min_val=0)


def run_checks_signal_big_table(bq_client, cfg):
    """Controles sur signal_big_table."""
    t = bq_table(cfg, "gold_ml", "signal_big_table")
    t_silver = bq_table(cfg, "silver_ml", "signal_clean_benign")
    print(f"\n{'='*50}")
    print(f"Controles : {t}")
    print(f"{'='*50}")

    statuses = []

    if check_table_exists(bq_client, cfg, t) == "WARN":
        return
    statuses.append(check_not_empty(bq_client, cfg, t))
    statuses.append(check_required_columns(bq_client, cfg, t, [
        "event_id", "source_file", "attack_type",
        "label", "is_attack", "elapsed_seconds", "ml_split"
    ]))

    # Nulls sur colonnes critiques
    statuses.append(check_null_rate(bq_client, cfg, t, [
        "event_id", "label", "attack_type", "ml_split"
    ]))

    # Valeurs categoriques
    statuses.append(check_categorical_values(
        bq_client, cfg, t, "attack_type", ATTACK_TYPES))
    statuses.append(check_categorical_values(
        bq_client, cfg, t, "ml_split", ML_SPLITS))

    # Label : uniquement 0 ou 1
    statuses.append(check_metric_coherence(
        bq_client, cfg, t,
        "label_values",
        f"""
        SELECT COUNT(*) as n FROM `{t}`
        WHERE label NOT IN (0, 1)
        """,
        "label contient des valeurs autres que 0 ou 1"
    ))

    # Coherence is_attack = (label == 1)
    statuses.append(check_metric_coherence(
        bq_client, cfg, t,
        "coherence_is_attack_label",
        f"""
        SELECT COUNT(*) as n FROM `{t}`
        WHERE (label = 1 AND is_attack = FALSE)
           OR (label = 0 AND is_attack = TRUE)
        """,
        "incoherence entre label et is_attack"
    ))

    # elapsed_seconds >= 0
    statuses.append(check_value_range(
        bq_client, cfg, t, "elapsed_seconds", min_val=0))

    # Fraicheur
    statuses.append(check_freshness(bq_client, cfg, t,
                                   timestamp_col="ingested_at"))

    if "FAIL" in statuses:
        raise RuntimeError(f"FAIL detecte sur {t} — pipeline interrompu")



def main():
    parser = argparse.ArgumentParser(
        description="Controles qualite complets sur les tables Gold CAN IDS"
    )
    parser.add_argument("--config_path", required=True)
    args = parser.parse_args()

    cfg = load_yaml(args.config_path)
    bq_client = bigquery.Client(project=cfg["bigquery"]["project_id"])

    print("\nDEBUT DES CONTROLES QUALITE DATA MESH")
    print(f"{'='*50}")

    run_checks_fact_window(bq_client, cfg)
    run_checks_dim_attack(bq_client, cfg)
    run_checks_dim_capture(bq_client, cfg)
    run_checks_dim_window(bq_client, cfg)
    run_checks_signal_big_table(bq_client, cfg)

    print(f"\n{'='*50}")
    print("CONTROLES QUALITE TERMINES — Consulter audit.data_quality_results")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()