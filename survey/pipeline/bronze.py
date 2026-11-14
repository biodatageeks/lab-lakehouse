# Bronze: the survey CSV files exactly as they arrive, loaded incrementally with Auto Loader.
#
# * one streaming table, PARTITIONED BY year; `year` comes from the folder name (year=2024/)
# * every column stays a string, values untouched ("NA" included) — cleaning happens in silver
# * schema evolution: a new year with new columns adds them to the table (addNewColumns)
# * column mapping lets Delta store column names with spaces and quotes (2025 has some)
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("survey.landing")


@dp.table(
    name="survey.bronze.responses",
    comment="Stack Overflow Developer Survey answers, one row per answer sheet, raw strings",
    partition_cols=["year"],
    table_properties={"delta.columnMapping.mode": "name", "quality": "bronze"},
)
def responses():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("multiLine", "true")  # quoted answers may contain line breaks
        .option("escape", '"')  # RFC 4180: a quote inside a value is written ""
        .option("cloudFiles.inferColumnTypes", "false")  # keep everything as strings
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.partitionColumns", "year")
        .option("cloudFiles.schemaHints", "year INT")
        .load(LANDING)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
