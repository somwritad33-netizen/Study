import dlt
from pyspark.sql.functions import  current_timestamp

vendor_path ='/Volumes/dev_p2p/staging/raw_data/vendor'
primary_key = 'vendor_id'
checkpoint = f'{vendor_path}/checkpoint'

@dlt.view(
     name='bronze_vendor_view'
)

def bronze_raw_stream():
    return(
        spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format','csv')
        .option('header', True)
        .load(vendor_path)
        .withColumn('loadtime', current_timestamp())
    )



dlt.create_streaming_table(
    name='bronze_vendor',
    comment='bronze table for vendor data',
    table_properties={'pipelines.autoOptimize.managed': 'true'},
    partition_cols=['loadtime'])


dlt.apply_changes(
    target='bronze_vendor',
    source='bronze_vendor_view',
    keys=[primary_key],
    sequence_by='loadtime'
)