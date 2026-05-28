import argparse, logging
from pyspark import StorageLevel
from common_bq import *
logger=logging.getLogger('silver_analytics')
analytics='analytics'
def transform_raw(df):
    if 'timestamp_us' in df.columns: df=df.withColumn('timestamp_us',col('timestamp_us').cast(LongType()))
    for c,t in [('arbitration_id',IntegerType()),('dlc',IntegerType()),('label',IntegerType())]:
        if c in df.columns: df=df.withColumn(c,col(c).cast(t))
    start=df.groupBy('source_file','dump_id').agg(spark_min('timestamp_us').alias('start_ts')); df=df.join(start,['source_file','dump_id'],'left')
    return df.withColumn('event_id',md5(concat_ws('|',col('source_file'),col('timestamp_us').cast('string'),col('arbitration_id').cast('string'),col('dlc').cast('string')))).withColumn('elapsed_seconds',(col('timestamp_us')-col('start_ts'))/lit(1000000.0)).withColumn('is_attack',col('label')==1).drop('start_ts').dropDuplicates(['event_id'])
def transform_signal(df):
    if 'timestamp' in df.columns: df=df.withColumn('timestamp',col('timestamp').cast(DoubleType()))
    if 'label' in df.columns: df=df.withColumn('label',col('label').cast(IntegerType()))
    start=df.groupBy('source_file','dump_id').agg(spark_min('timestamp').alias('start_ts')); df=df.join(start,['source_file','dump_id'],'left')
    return df.withColumn('event_id',md5(concat_ws('|',col('source_file'),col('timestamp').cast('string'),col('label').cast('string')))).withColumn('elapsed_seconds',col('timestamp')-col('start_ts')).withColumn('is_attack',col('label')==1).drop('start_ts').dropDuplicates(['event_id'])
def write_by_attack(df,cfg,rep):
    base='raw_clean' if rep=='raw' else 'signal_clean'
    for a in ATTACK_TYPES:
        sub=df.filter(col('attack_type')==a); sources=[r['source_file'] for r in sub.select('source_file').distinct().collect()]
        if sources: delete_sources_rows(cfg,f'silver_{analytics}',f'{base}_{a}',sources); write_bq(sub,cfg,f'silver_{analytics}',f'{base}_{a}','append')
def process_representation(spark,cfg,rep):
    inbase='raw_valid' if rep=='raw' else 'signal_valid'; df=read_bq_by_attack(spark,cfg,f'bronze_{analytics}',inbase)
    sources=[r['source_file'] for r in df.select('source_file').distinct().collect()]; done=processed_sources(cfg,'silver',analytics,rep); new=[s for s in sources if s not in done]
    if not new: logger.info('No new %s for silver %s',rep,analytics); return
    out=(transform_raw(df.filter(col('source_file').isin(new))) if rep=='raw' else transform_signal(df.filter(col('source_file').isin(new)))).persist(StorageLevel.MEMORY_AND_DISK)
    write_by_attack(out,cfg,rep)
    for s in new: write_audit_status(cfg,'silver',analytics,rep,s,'SUCCESS')
    out.unpersist()
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--config_path',required=True); args=ap.parse_args(); cfg=load_yaml(args.config_path); spark=create_spark(f'silver_{analytics}')
    try: process_representation(spark,cfg,'raw'); process_representation(spark,cfg,'signal')
    finally: spark.stop()
if __name__=='__main__': main()
