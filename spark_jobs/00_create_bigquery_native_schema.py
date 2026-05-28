import argparse, logging, yaml
from google.cloud import bigquery, storage
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
def load_yaml(path):
    if path.startswith('gs://'):
        b,k=path.replace('gs://','',1).split('/',1); return yaml.safe_load(storage.Client().bucket(b).blob(k).download_as_text())
    return yaml.safe_load(open(path,encoding='utf-8'))
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--config_path', required=True); args=ap.parse_args(); cfg=load_yaml(args.config_path)
    client=bigquery.Client(project=cfg['bigquery']['project_id']); loc=cfg['bigquery'].get('location','europe-southwest1')
    for ds in cfg['bigquery']['datasets'].values():
        d=bigquery.Dataset(f"{cfg['bigquery']['project_id']}.{ds}"); d.location=loc; client.create_dataset(d, exists_ok=True); logging.info('Dataset ready: %s', d.dataset_id)
    audit=cfg['bigquery']['datasets']['audit']; project=cfg['bigquery']['project_id']
    schemas={
      'file_processing_status':[('layer','STRING'),('domain','STRING'),('representation','STRING'),('source_file','STRING'),('status','STRING'),('rows_written','INT64'),('error_message','STRING'),('processed_at','TIMESTAMP')],
      'data_quality_results':[('check_id','STRING'),('table_name','STRING'),('check_name','STRING'),('status','STRING'),('row_count','INT64'),('error_message','STRING'),('checked_at','TIMESTAMP')]
    }
    for t, fields in schemas.items():
        client.create_table(bigquery.Table(f"{project}.{audit}.{t}", schema=[bigquery.SchemaField(n,tp) for n,tp in fields]), exists_ok=True); logging.info('Audit table ready: %s', t)
if __name__=='__main__': main()
