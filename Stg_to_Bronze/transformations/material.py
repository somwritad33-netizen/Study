import dlt
from pyspark.sql.functions import  current_timestamp

catalog = spark.conf.get("catalog")


material_path =f'/Volumes/{catalog}/staging/raw_data/material'
primary_key = 'material_id'
schema_location = '/Volumes/dev_p2p/staging/raw_data/material_schema'

@dlt.view(
     name='bronze_material_view'
)

def bronze_raw_stream():
    return(
        spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format','csv')
        .option('cloudFiles.schemaLocation', schema_location)
        .option('header', True)
        .load(material_path)
        .withColumn('loadtime', current_timestamp())
    )



dlt.create_streaming_table(
    name='bronze_material',
    comment='bronze table for material data',
    table_properties={'pipelines.autoOptimize.managed': 'true'},
    partition_cols=['loadtime'])


dlt.apply_changes(
    target='bronze_material',
    source='bronze_material_view',
    keys=[primary_key],
    sequence_by='loadtime'
)