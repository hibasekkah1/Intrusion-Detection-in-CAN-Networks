"""
setup_dataplex.py — v3
======================
Gestion correcte des zones existantes + rate limit assets.
"""

import argparse
import time
import yaml
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import dataplex_v1
from google.api_core.exceptions import AlreadyExists, NotFound, ResourceExhausted


def load_yaml(path: str) -> dict:
    if path.startswith("gs://"):
        client = storage.Client()
        parts = path[5:].split("/", 1)
        blob = client.bucket(parts[0]).blob(parts[1])
        return yaml.safe_load(blob.download_as_text())
    with open(path, "r") as f:
        return yaml.safe_load(f)


def retry_with_backoff(fn, label="", max_retries=6, base_delay=20):
    for attempt in range(max_retries):
        try:
            return fn()
        except ResourceExhausted:
            if attempt == max_retries - 1:
                print(f"  [FAIL] Rate limit depasse apres {max_retries} tentatives : {label}")
                return None
            delay = base_delay * (2 ** attempt)
            print(f"  [RATE LIMIT] {label} — Attente {delay}s avant retry ({attempt+1}/{max_retries})...")
            time.sleep(delay)
        except Exception as e:
            if "already in use" in str(e) or "already exists" in str(e).lower():
                print(f"  [EXISTS] Deja existant : {label}")
                return "exists"
            print(f"  [ERROR] {label} : {e}")
            return None
    return None


def get_tables_metadata(cfg: dict) -> list:
    project = cfg["bigquery"]["project_id"]
    datasets = cfg["bigquery"]["datasets"]
    tables = []

    for attack in ["benign", "fuzz", "fabr", "masq", "susp", "repl"]:
        for rep in ["raw_valid", "signal_valid"]:
            for domain_key in ["bronze_analytics", "bronze_ml"]:
                domain = "analytics" if "analytics" in domain_key else "ml"
                tables.append({
                    "project": project, "dataset": datasets[domain_key],
                    "table": f"{rep}_{attack}", "layer": "bronze", "domain": domain,
                    "description": f"[BRONZE | {domain.upper()}] Donnees {rep.replace('_',' ')} brutes du type '{attack}'.",
                    "labels": {"layer": "bronze", "domain": domain, "attack-type": attack,
                               "representation": rep.split("_")[0], "owner": "data-engineer"},
                })

    for attack in ["benign", "fuzz", "fabr", "masq", "susp", "repl"]:
        for rep in ["raw_clean", "signal_clean"]:
            for domain_key in ["silver_analytics", "silver_ml"]:
                domain = "analytics" if "analytics" in domain_key else "ml"
                tables.append({
                    "project": project, "dataset": datasets[domain_key],
                    "table": f"{rep}_{attack}", "layer": "silver", "domain": domain,
                    "description": f"[SILVER | {domain.upper()}] Donnees nettoyees du type '{attack}'. event_id, elapsed_seconds, is_attack.",
                    "labels": {"layer": "silver", "domain": domain, "attack-type": attack,
                               "representation": rep.split("_")[0], "owner": "data-engineer"},
                })

    for tbl, desc, mtype, consumer in [
        ("fact_window_5min_wide", "Table de faits Star Schema. Agregations 5 min.", "fact-table", "power-bi"),
        ("dim_attack", "Dimension type d attaque. 6 types X-CANIDS.", "dimension", "power-bi"),
        ("dim_capture", "Dimension fichier source de capture.", "dimension", "power-bi"),
        ("dim_window", "Dimension fenetre temporelle de 5 minutes.", "dimension", "power-bi"),
    ]:
        tables.append({
            "project": project, "dataset": datasets["gold_analytics"],
            "table": tbl, "layer": "gold", "domain": "analytics",
            "description": f"[GOLD | ANALYTICS] {desc}",
            "labels": {"layer": "gold", "domain": "analytics", "model-type": mtype,
                       "consumer": consumer, "owner": "data-analyst"},
        })

    tables.append({
        "project": project, "dataset": datasets["gold_ml"],
        "table": "signal_big_table", "layer": "gold", "domain": "ml",
        "description": "[GOLD | ML] One Big Table. Signaux physiques + label + ml_split.",
        "labels": {"layer": "gold", "domain": "ml", "model-type": "one-big-table",
                   "consumer": "data-scientist", "owner": "data-scientist"},
    })

    for tbl, desc, purpose in [
        ("file_processing_status", "Table de tracabilite principale.", "lineage-traceability"),
        ("data_quality_results", "Resultats des controles qualite (PASS/WARN/FAIL).", "data-quality"),
    ]:
        tables.append({
            "project": project, "dataset": datasets["audit"],
            "table": tbl, "layer": "audit", "domain": "all",
            "description": f"[AUDIT] {desc}",
            "labels": {"layer": "audit", "domain": "all", "purpose": purpose, "owner": "data-engineer"},
        })

    return tables


def apply_bigquery_metadata(cfg: dict):
    project = cfg["bigquery"]["project_id"]
    bq_client = bigquery.Client(project=project)
    tables = get_tables_metadata(cfg)
    print(f"\n[1/4] Application des metadonnees BigQuery natifs ({len(tables)} tables)...")
    success = 0
    for entry in tables:
        table_ref = f"{entry['project']}.{entry['dataset']}.{entry['table']}"
        try:
            table = bq_client.get_table(table_ref)
            table.description = entry["description"]
            clean_labels = {k.lower().replace("_", "-")[:63]: str(v).lower().replace("_", "-")[:63]
                           for k, v in entry["labels"].items()}
            table.labels = clean_labels
            bq_client.update_table(table, ["description", "labels"])
            success += 1
        except NotFound:
            print(f"  [SKIP] {entry['dataset']}.{entry['table']}")
        except Exception as e:
            print(f"  [ERROR] {table_ref} : {e}")
    print(f"  Resultat : {success}/{len(tables)} tables documentees")


def get_or_create_lake(dp_client, project: str, location: str) -> str:
    """Recupere le lake existant ou le cree."""
    lake_id = "can-ids-lake"
    parent = f"projects/{project}/locations/{location}"
    lake_name = f"{parent}/lakes/{lake_id}"

    # Verifier si le lake existe deja
    try:
        lake = dp_client.get_lake(name=lake_name)
        print(f"  [EXISTS] Lake existant : {lake.display_name} [{lake.state.name}]")
        return lake_name
    except NotFound:
        pass

    lake = dataplex_v1.Lake()
    lake.display_name = "CAN IDS Data Lake"
    lake.description = "Pipeline Data Engineering — Architecture Medallion — Data Mesh"
    lake.labels = {"project": "can-ids", "team": "data-engineering"}

    def _create():
        op = dp_client.create_lake(parent=parent, lake_id=lake_id, lake=lake)
        return op.result(timeout=120)

    result = retry_with_backoff(_create, label="Lake can-ids-lake")
    if result and result != "exists":
        print(f"  [OK] Lake cree : {result.name}")
    return lake_name


def get_or_create_zones(dp_client, lake_name: str) -> dict:
    """Recupere les zones existantes ou les cree."""
    zones_config = [
        {"zone_id": "bronze-zone",        "display_name": "Zone Bronze — Donnees Brutes",
         "zone_type": dataplex_v1.Zone.Type.RAW,
         "labels": {"layer": "bronze"}},
        {"zone_id": "silver-zone",        "display_name": "Zone Silver — Donnees Nettoyees",
         "zone_type": dataplex_v1.Zone.Type.CURATED,
         "labels": {"layer": "silver"}},
        {"zone_id": "gold-analytics-zone","display_name": "Zone Gold Analytics — Data Analyst",
         "zone_type": dataplex_v1.Zone.Type.CURATED,
         "labels": {"layer": "gold", "domain": "analytics"}},
        {"zone_id": "gold-ml-zone",       "display_name": "Zone Gold ML — Data Scientist",
         "zone_type": dataplex_v1.Zone.Type.CURATED,
         "labels": {"layer": "gold", "domain": "ml"}},
        {"zone_id": "audit-zone",         "display_name": "Zone Audit — Tracabilite et Qualite",
         "zone_type": dataplex_v1.Zone.Type.CURATED,
         "labels": {"layer": "audit"}},
    ]

    zone_names = {}
    for zc in zones_config:
        zone_name = f"{lake_name}/zones/{zc['zone_id']}"

        # Verifier si la zone existe deja
        try:
            zone = dp_client.get_zone(name=zone_name)
            print(f"  [EXISTS] Zone existante : {zone.display_name} [{zone.state.name}]")
            zone_names[zc["zone_id"]] = zone_name
            continue
        except NotFound:
            pass

        zone = dataplex_v1.Zone()
        zone.display_name = zc["display_name"]
        zone.type_ = zc["zone_type"]
        zone.labels = zc["labels"]
        zone.resource_spec = dataplex_v1.Zone.ResourceSpec(
            location_type=dataplex_v1.Zone.ResourceSpec.LocationType.SINGLE_REGION
        )
        zone.discovery_spec = dataplex_v1.Zone.DiscoverySpec(
            enabled=True, schedule="0 3 * * *"
        )

        def _create_zone(z=zone, zid=zc["zone_id"]):
            op = dp_client.create_zone(parent=lake_name, zone_id=zid, zone=z)
            return op.result(timeout=120)

        result = retry_with_backoff(_create_zone, label=zc["display_name"])
        if result and result != "exists":
            print(f"  [OK] Zone creee : {zc['display_name']}")
        zone_names[zc["zone_id"]] = zone_name
        print(f"  [WAIT] Pause 15s...")
        time.sleep(15)

    return zone_names


def attach_bigquery_assets(dp_client, cfg: dict, zone_names: dict):
    project = cfg["bigquery"]["project_id"]
    datasets = cfg["bigquery"]["datasets"]

    assets_config = [
        {"zone_id": "bronze-zone",        "asset_id": "bronze-analytics-asset",
         "display_name": "Bronze Analytics", "dataset": datasets["bronze_analytics"]},
        {"zone_id": "bronze-zone",        "asset_id": "bronze-ml-asset",
         "display_name": "Bronze ML",        "dataset": datasets["bronze_ml"]},
        {"zone_id": "silver-zone",        "asset_id": "silver-analytics-asset",
         "display_name": "Silver Analytics", "dataset": datasets["silver_analytics"]},
        {"zone_id": "silver-zone",        "asset_id": "silver-ml-asset",
         "display_name": "Silver ML",        "dataset": datasets["silver_ml"]},
        {"zone_id": "gold-analytics-zone","asset_id": "gold-analytics-asset",
         "display_name": "Gold Analytics",   "dataset": datasets["gold_analytics"]},
        {"zone_id": "gold-ml-zone",       "asset_id": "gold-ml-asset",
         "display_name": "Gold ML",          "dataset": datasets["gold_ml"]},
        {"zone_id": "audit-zone",         "asset_id": "audit-asset",
         "display_name": "Audit",            "dataset": datasets["audit"]},
    ]

    for ac in assets_config:
        zone_name = zone_names.get(ac["zone_id"])
        if not zone_name:
            print(f"  [SKIP] Zone non trouvee : {ac['zone_id']}")
            continue

        asset_name = f"{zone_name}/assets/{ac['asset_id']}"

        # Verifier si l'asset existe deja
        try:
            asset = dp_client.get_asset(name=asset_name)
            print(f"  [EXISTS] Asset existant : {asset.display_name} [{asset.state.name}]")
            continue
        except NotFound:
            pass

        asset = dataplex_v1.Asset()
        asset.display_name = ac["display_name"]
        asset.resource_spec = dataplex_v1.Asset.ResourceSpec(
            name=f"projects/{project}/datasets/{ac['dataset']}",
            type_=dataplex_v1.Asset.ResourceSpec.Type.BIGQUERY_DATASET,
        )
        asset.discovery_spec = dataplex_v1.Asset.DiscoverySpec(enabled=True)

        def _create_asset(a=asset, zn=zone_name, aid=ac["asset_id"]):
            op = dp_client.create_asset(parent=zn, asset_id=aid, asset=a)
            return op.result(timeout=120)

        result = retry_with_backoff(_create_asset, label=ac["display_name"],
                                    max_retries=6, base_delay=20)
        if result and result != "exists":
            print(f"  [OK] Asset attache : {ac['display_name']}")

        print(f"  [WAIT] Pause 20s...")
        time.sleep(20)


def show_status(cfg: dict):
    project = cfg["bigquery"]["project_id"]
    location = cfg["bigquery"]["location"]
    dp_client = dataplex_v1.DataplexServiceClient()
    lake_name = f"projects/{project}/locations/{location}/lakes/can-ids-lake"

    print(f"\n{'='*60}")
    print(f"STATUS DATAPLEX — {project}")
    print(f"{'='*60}")

    try:
        lake = dp_client.get_lake(name=lake_name)
        print(f"\n[OK] Lake : {lake.display_name} [{lake.state.name}]")
    except NotFound:
        print(f"\n[MISSING] Lake non trouve — executer --deploy")
        return

    zones = list(dp_client.list_zones(parent=lake_name))
    print(f"\n[OK] Zones ({len(zones)}) :")
    total_assets = 0
    for zone in zones:
        assets = list(dp_client.list_assets(parent=zone.name))
        total_assets += len(assets)
        print(f"     - {zone.display_name} [{zone.state.name}] — {len(assets)} assets")

    print(f"\n[OK] Total assets : {total_assets}/7")

    bq_client = bigquery.Client(project=project)
    datasets = cfg["bigquery"]["datasets"]
    labeled = 0
    for dataset_id in datasets.values():
        try:
            for t in bq_client.list_tables(dataset_id):
                table = bq_client.get_table(f"{project}.{dataset_id}.{t.table_id}")
                if table.labels:
                    labeled += 1
        except Exception:
            pass
    print(f"\n[OK] Tables BigQuery avec labels natifs : {labeled}/55")
    print(f"\nConsole Dataplex :")
    print(f"  https://console.cloud.google.com/dataplex?project={project}")


def delete_dataplex(cfg: dict):
    project = cfg["bigquery"]["project_id"]
    location = cfg["bigquery"]["location"]
    dp_client = dataplex_v1.DataplexServiceClient()
    lake_name = f"projects/{project}/locations/{location}/lakes/can-ids-lake"

    print(f"\n[DELETE] Suppression du Lake Dataplex...")
    try:
        zones = list(dp_client.list_zones(parent=lake_name))
        for zone in zones:
            assets = list(dp_client.list_assets(parent=zone.name))
            for asset in assets:
                op = dp_client.delete_asset(name=asset.name)
                op.result(timeout=120)
                print(f"  [OK] Asset supprime : {asset.display_name}")
                time.sleep(20)
            op = dp_client.delete_zone(name=zone.name)
            op.result(timeout=120)
            print(f"  [OK] Zone supprimee : {zone.display_name}")
            time.sleep(20)
        op = dp_client.delete_lake(name=lake_name)
        op.result(timeout=120)
        print(f"  [OK] Lake supprime")
    except NotFound:
        print(f"  [SKIP] Lake non trouve")
    except Exception as e:
        print(f"  [ERROR] {e}")


def deploy(cfg: dict):
    project = cfg["bigquery"]["project_id"]
    location = cfg["bigquery"]["location"]

    print(f"\n{'='*60}")
    print(f"DEPLOIEMENT DATAPLEX — {project}")
    print(f"NOTE : Pauses automatiques pour respecter le quota Dataplex.")
    print(f"       Duree totale estimee : 5 a 10 minutes.")
    print(f"{'='*60}")

    dp_client = dataplex_v1.DataplexServiceClient()

    # 1. Labels BigQuery
    apply_bigquery_metadata(cfg)
    time.sleep(3)

    # 2. Lake
    print(f"\n[2/4] Lake Dataplex...")
    lake_name = get_or_create_lake(dp_client, project, location)
    time.sleep(10)

    # 3. Zones
    print(f"\n[3/4] Zones Dataplex...")
    zone_names = get_or_create_zones(dp_client, lake_name)
    print(f"  [WAIT] Pause 30s avant les assets...")
    time.sleep(30)

    # 4. Assets
    print(f"\n[4/4] Assets BigQuery...")
    attach_bigquery_assets(dp_client, cfg, zone_names)

    print(f"\n{'='*60}")
    print(f"DATAPLEX DEPLOYE AVEC SUCCES")
    print(f"{'='*60}")
    print(f"\n  https://console.cloud.google.com/dataplex?project={project}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_path", required=True)
    parser.add_argument("--deploy", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--delete", action="store_true")
    args = parser.parse_args()
    cfg = load_yaml(args.config_path)
    if args.status:
        show_status(cfg)
    elif args.delete:
        delete_dataplex(cfg)
    else:
        deploy(cfg)


if __name__ == "__main__":
    main()