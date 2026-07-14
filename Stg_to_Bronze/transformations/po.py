import dlt
from pyspark.sql.functions import col, current_timestamp, expr

# =========================================================
# CONFIG
# =========================================================
invoice_path = '/Volumes/dev_p2p/staging/raw_data/purchase_order'  # <-- update to your PDF folder

# =========================================================
# BRONZE: raw PDF files ingested via Auto Loader,
# parsed using Databricks' built-in ai_parse_document function.
# No external library (pypdf, etc.) or environment setup required.
# =========================================================
@dlt.table(
    name='bronze_po_pdf',
    comment='Raw po PDF files ingested via Auto Loader, parsed with ai_parse_document',
    table_properties={'pipelines.autoOptimize.managed': 'true'}
)
def bronze_invoice_pdf():
    return (
        spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format', 'binaryFile')
        .load(invoice_path)
        # ai_parse_document expects real bytes; wrapping content this way avoids
        # a known issue where Spark's binaryFile reader passes a memoryview-like
        # object that the function can't process directly.
        .withColumn('content_bytes', expr("CAST(content AS BINARY)"))
        .withColumn('parsed_document', expr("ai_parse_document(content_bytes)"))
        .withColumn('loadtime', current_timestamp())
        .withColumn('filename', expr("element_at(split(path, '/'), -1)"))
        .drop('content', 'content_bytes')  # drop raw bytes after parsing to save storage
    )