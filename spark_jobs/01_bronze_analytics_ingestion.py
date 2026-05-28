import argparse, logging
from common_bq import *
logger=logging.getLogger('bronze_analytics')
analytics='analytics'
def process_representation(spark,cfg,rep):
    base='raw_valid' if rep=='raw' else 'signal_valid'; done=processed_sources(cfg,'bronze',analytics,rep)
    files=[f for f in list_parquet_files(cfg['gcs_paths']['landing'][rep]) if f['source_file'] not in done]
    if not files: logger.info('No new %s files for bronze %s', rep, analytics); return
    for f in files:
        src=f['source_file']; attack=detect_attack(src); table=f'{base}_{attack}'
        try:
            df=read_parquet_resilient(spark,f['gcs_uri'])
            if df is None: continue
            df=normalize_raw(df) if rep=='raw' else normalize_signal(df)
            df=add_metadata(df,src,rep,analytics)
            delete_source_rows(cfg,f'bronze_{analytics}',table,src)
            write_bq(df,cfg,f'bronze_{analytics}',table,'append')
            write_audit_status(cfg,'bronze',analytics,rep,src,'SUCCESS')
        except Exception as e:
            write_audit_status(cfg,'bronze',analytics,rep,src,'FAILED',error_message=str(e)); raise
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--config_path',required=True); args=ap.parse_args(); cfg=load_yaml(args.config_path); spark=create_spark(f'bronze_{analytics}')
    try: process_representation(spark,cfg,'raw'); process_representation(spark,cfg,'signal')
    finally: spark.stop()
if __name__=='__main__': main()
