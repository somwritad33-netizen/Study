import dlt
from pyspark.sql.functions import col, current_timestamp, expr

# =========================================================
# CONFIG
# =========================================================
gr_path = '/Volumes/dev_p2p/staging/raw_data/good_receipt'  # <-- update to your PDF folder

# =========================================================
# BRONZE: raw PDF files ingested via Auto Loader,
# parsed using Databricks' built-in ai_parse_document function.
# No external library (pypdf, etc.) or environment setup required.
# =========================================================


@dlt.view(
     name='bronze_gr_view'
)

def bronze_invoice_pdf():
    return (
        spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format', 'binaryFile')
        .load(gr_path)
        # ai_parse_document expects real bytes; wrapping content this way avoids
        # a known issue where Spark's binaryFile reader passes a memoryview-like
        # object that the function can't process directly.
        .withColumn('content_bytes', expr("CAST(content AS BINARY)"))
        .withColumn('parsed_document', expr("ai_parse_document(content_bytes)"))
        .withColumn('loadtime', current_timestamp())
        .withColumn('filename', expr("element_at(split(path, '/'), -1)"))
        .drop('content', 'content_bytes')  # drop raw bytes after parsing to save storage
    )


dlt.create_streaming_table(
    name='bronze_gr_pdf1',
    comment='Bronze gr table, deduplicated/upserted by filename via apply_changes',
    table_properties={'pipelines.autoOptimize.managed': 'true'}
)
    
dlt.apply_changes(
    target='bronze_gr_pdf1',
    source='bronze_gr_view',
    keys=['filename'],
    sequence_by='loadtime',
    stored_as_scd_type=1
)