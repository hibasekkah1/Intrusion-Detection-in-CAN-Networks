import argparse, logging
from datetime import datetime, timezone
import yaml
from google.cloud import bigquery, storage
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
def load_yaml(path):
    if path.startswith('gs://'):
        b,k=path.replace('gs://','',1).split('/',1); return yaml.safe_load(storage.Client().bucket(b).blob(k).download_as_text())
    return yaml.safe_load(open(path,encoding='utf-8'))
def t(cfg,k,n): return f"{cfg['bigquery']['project_id']}.{cfg['bigquery']['datasets'][k]}.{n}"
def audit(cfg): return f"{cfg['bigquery']['project_id']}.{cfg['bigquery']['datasets']['audit']}.data_quality_results"
def check(client,table,cols):
    status='PASS'; err=None; row_count=None
    try:
        row_count=int(list(client.query(f"SELECT COUNT(*) c FROM `{table}`").result())[0]['c'])
        if row_count==0: status='WARN'; err='empty_table'
        for c in cols: list(client.query(f"SELECT `{c}` FROM `{table}` LIMIT 1").result())
    except Exception as e:
        err_str=str(e)
        # Table absente → WARN (pas encore créée) au lieu de FAIL
        if '404' in err_str or 'Not found' in err_str or 'not found' in err_str:
            status='WARN'; err=f'table_not_found: {err_str}'
        else:
            status='FAIL'; err=err_str
    return {'check_id':f"{table}:required:{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",'table_name':table,'check_name':'required_columns_and_count','status':status,'row_count':row_count,'error_message':err,'checked_at':datetime.now(timezone.utc).isoformat()}
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--config_path',required=True); args=ap.parse_args(); cfg=load_yaml(args.config_path); client=bigquery.Client(project=cfg['bigquery']['project_id'])
    checks=[(t(cfg,'gold_ml','signal_big_table'),['event_id','source_file','attack_type','label','ml_split']),(t(cfg,'gold_analytics','fact_window_5min_wide'),['capture_id','window_id','attack_id','source_file','attack_type']),(t(cfg,'gold_analytics','dim_signal'),['signal_id','signal_column_name']),(t(cfg,'gold_analytics','dim_attack'),['attack_id','attack_type']),(t(cfg,'gold_analytics','dim_capture'),['capture_id','source_file']),(t(cfg,'gold_analytics','dim_window'),['window_id','window_5min'])]
    rows=[check(client,table,cols) for table,cols in checks]; errors=client.insert_rows_json(audit(cfg),rows)
    if errors: raise RuntimeError(errors)
    failed=[r for r in rows if r['status']=='FAIL']
    warned=[r for r in rows if r['status']=='WARN']
    if warned:
        for w in warned: logging.warning('WARN: %s — %s', w['table_name'], w['error_message'])
    if failed: raise RuntimeError(failed)
if __name__=='__main__': main()