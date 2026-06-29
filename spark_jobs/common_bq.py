import os, re, json, logging, tempfile
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
import yaml
from google.cloud import bigquery, storage
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, md5, concat_ws, min as spark_min
from pyspark.sql.types import DoubleType, IntegerType, LongType
try:
    import pyarrow.parquet as pq
except Exception:
    pq = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
ATTACK_TYPES = ["benign", "fuzz", "fabr", "masq", "susp", "repl"]

def now_utc(): return datetime.now(timezone.utc).isoformat()
def parse_gcs_uri(uri: str) -> Tuple[str, str]: return uri.replace("gs://", "", 1).split("/", 1)
def load_yaml(path: str) -> Dict[str, Any]:
    if path.startswith("gs://"):
        b,k=parse_gcs_uri(path); return yaml.safe_load(storage.Client().bucket(b).blob(k).download_as_text())
    with open(path, encoding="utf-8") as f: return yaml.safe_load(f)
def create_spark(name: str) -> SparkSession:
    return (SparkSession.builder.appName(name)
            .config("spark.sql.shuffle.partitions", "120")
            .config("spark.default.parallelism", "120")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.caseSensitive", "true")
            .getOrCreate())
def bq_table(cfg, dataset_key, table_name): return f"{cfg['bigquery']['project_id']}.{cfg['bigquery']['datasets'][dataset_key]}.{table_name}"
def audit_table(cfg, table_name): return f"{cfg['bigquery']['project_id']}.{cfg['bigquery']['datasets']['audit']}.{table_name}"
def table_exists(cfg, dataset_key, table_name):
    try:
        bigquery.Client(project=cfg['bigquery']['project_id']).get_table(bq_table(cfg,dataset_key,table_name)); return True
    except Exception: return False
def read_bq(spark,cfg,dataset_key,table_name): return spark.read.format("bigquery").option("table", bq_table(cfg,dataset_key,table_name)).load()
def read_bq_by_attack(spark,cfg,dataset_key,base_table):
    dfs=[]; log=logging.getLogger('bq_reader')
    for a in ATTACK_TYPES:
        t=f"{base_table}_{a}"
        if table_exists(cfg,dataset_key,t):
            log.info("Reading %s", bq_table(cfg,dataset_key,t)); dfs.append(read_bq(spark,cfg,dataset_key,t))
        else: log.warning("Missing table skipped: %s", bq_table(cfg,dataset_key,t))
    if not dfs: raise RuntimeError(f"No BigQuery attack tables found for {dataset_key}.{base_table}_*")
    out=dfs[0]
    for df in dfs[1:]: out=out.unionByName(df, allowMissingColumns=True)
    return out
def write_bq(df,cfg,dataset_key,table_name,mode="append"):
    logging.getLogger('bq_writer').info("Writing %s", bq_table(cfg,dataset_key,table_name))
    (df.write.format("bigquery").option("table", bq_table(cfg,dataset_key,table_name)).option("writeMethod","direct").mode(mode).save())
def processed_sources(cfg, layer, domain, representation) -> Set[str]:
    client=bigquery.Client(project=cfg['bigquery']['project_id']); table=audit_table(cfg,'file_processing_status')
    q=f"SELECT source_file FROM `{table}` WHERE layer=@layer AND domain=@domain AND representation=@representation AND status='SUCCESS'"
    job=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('layer','STRING',layer),bigquery.ScalarQueryParameter('domain','STRING',domain),bigquery.ScalarQueryParameter('representation','STRING',representation)])
    try: return {r['source_file'] for r in client.query(q, job_config=job).result()}
    except Exception: return set()
def write_audit_status(cfg, layer, domain, representation, source_file, status='SUCCESS', rows_written=-1, error_message=None):
    client=bigquery.Client(project=cfg['bigquery']['project_id']); table=audit_table(cfg,'file_processing_status')
    errors=client.insert_rows_json(table,[{'layer':layer,'domain':domain,'representation':representation,'source_file':source_file,'status':status,'rows_written':rows_written,'error_message':error_message,'processed_at':now_utc()}])
    if errors: raise RuntimeError(errors)
def delete_source_rows(cfg,dataset_key,table_name,source_file):
    if not table_exists(cfg,dataset_key,table_name): return
    client=bigquery.Client(project=cfg['bigquery']['project_id'])
    q=f"DELETE FROM `{bq_table(cfg,dataset_key,table_name)}` WHERE source_file=@source_file"
    client.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('source_file','STRING',source_file)])).result()
def delete_sources_rows(cfg,dataset_key,table_name,sources):
    if not sources or not table_exists(cfg,dataset_key,table_name): return
    client=bigquery.Client(project=cfg['bigquery']['project_id'])
    q=f"DELETE FROM `{bq_table(cfg,dataset_key,table_name)}` WHERE source_file IN UNNEST(@sources)"
    client.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter('sources','STRING',sources)])).result()
def delete_capture_rows(cfg,dataset_key,table_name,capture_ids):
    if not capture_ids or not table_exists(cfg,dataset_key,table_name): return
    client=bigquery.Client(project=cfg['bigquery']['project_id'])
    q=f"DELETE FROM `{bq_table(cfg,dataset_key,table_name)}` WHERE capture_id IN UNNEST(@capture_ids)"
    client.query(q, job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter('capture_ids','STRING',capture_ids)])).result()
def sanitize_bq_column_name(name: str) -> str:
    # Minimal change: add sig_ only if a column starts with a digit; keep names readable for Data Scientists.
    safe = re.sub(r"[^a-zA-Z0-9_]", "_", str(name).strip())
    safe = re.sub(r"_+", "_", safe).strip("_") or "col"
    if safe[0].isdigit(): safe = f"sig_{safe}"
    return safe
def make_unique_columns(cols: List[str]) -> List[str]:
    counts = Counter()
    out = []

    for c in cols:
        safe = sanitize_bq_column_name(c)

        # clé de comparaison insensible à la casse
        key = safe.lower()

        counts[key] += 1

        if counts[key] == 1:
            out.append(safe)
        else:
            out.append(f"{safe}_{counts[key]}")

    return out
def deduplicate_columns(df: DataFrame) -> DataFrame: return df.toDF(*make_unique_columns(df.columns))
def detect_attack(source_file: str) -> str:
    lower=source_file.lower()
    for a in ATTACK_TYPES:
        if a in lower: return a
    return 'benign'
def list_parquet_files(prefix_uri: str):
    b,p=parse_gcs_uri(prefix_uri.rstrip('/')); base=p.rstrip('/')+'/'; out=[]
    for blob in storage.Client().list_blobs(b, prefix=base):
        if blob.name.endswith('.parquet'):
            out.append({'source_file': blob.name[len(base):], 'gcs_uri': f'gs://{b}/{blob.name}'})
    return out
def download_gcs_file(gcs_uri, local_path):
    b,k=parse_gcs_uri(gcs_uri); storage.Client().bucket(b).blob(k).download_to_filename(local_path)
def read_parquet_resilient(spark, gcs_uri):
    try: return deduplicate_columns(spark.read.option('mergeSchema','false').parquet(gcs_uri))
    except Exception as err:
        text=str(err)
        if 'COLUMN_ALREADY_EXISTS' not in text and 'already exists' not in text: raise
        if pq is None: raise RuntimeError('pyarrow required to repair duplicate columns') from err
        with tempfile.TemporaryDirectory() as tmp:
            local=os.path.join(tmp, os.path.basename(gcs_uri)); download_gcs_file(gcs_uri, local)
            tbl=pq.read_table(local); tbl=tbl.rename_columns(make_unique_columns(tbl.schema.names)); pdf=tbl.to_pandas().fillna('')
            return None if pdf.empty else spark.createDataFrame(pdf)
def add_metadata(df, source_file, rep, domain):
    return (df.withColumn('source_file',lit(source_file)).withColumn('domain',lit(domain)).withColumn('representation',lit(rep))
            .withColumn('attack_type',lit(detect_attack(source_file))).withColumn('dump_id',lit(source_file.split('/')[-1].split('.')[0]))
            .withColumn('dataset_type',lit('xcanids')).withColumn('attack_parameter',lit('')).withColumn('target_aid',lit(''))
            .withColumn('fuzz_rate',lit(None).cast('int')).withColumn('replay_start_sec',lit(None).cast('double')).withColumn('replay_end_sec',lit(None).cast('double'))
            .withColumn('ingested_at',current_timestamp()))
def normalize_raw(df):
    if 'timestamp_us' in df.columns: df=df.withColumn('timestamp_us',col('timestamp_us').cast(LongType()))
    elif 'timestamp' in df.columns: df=df.withColumn('timestamp_us',(col('timestamp').cast(DoubleType())*lit(1000000)).cast(LongType()))
    for c,t in [('arbitration_id',IntegerType()),('dlc',IntegerType()),('label',IntegerType())]:
        if c in df.columns: df=df.withColumn(c,col(c).cast(t))
    return df
def normalize_signal(df):
    if 'timestamp' in df.columns: df=df.withColumn('timestamp',col('timestamp').cast(DoubleType()))
    if 'label' in df.columns:
        df=df.withColumn('label', col('label').cast(IntegerType()))
    else:
        df=df.withColumn('label', lit(0).cast(IntegerType()))
    return df
