# Databricks notebook source
from pyspark.sql.functions import col , split, current_timestamp, element_at, regexp_replace, to_timestamp, concat, substring 

# COMMAND ----------

_spider_name = "get_data_guanabara"
_catalog_name = "genai_bronze_dev"
_data_schema_name = "database"
_bronze_table_name = "tb_offers_bronze"
_schema_location = f"/Volumes/{_catalog_name}/webscraping/webscraping/_schemaLocation/{_catalog_name}/{_bronze_table_name}"
_checkpoint_location = f"{_schema_location}"

_volume_schema_name = "webscraping"
_volume_path = f"/Volumes/{_catalog_name}/webscraping/webscraping/data/{_spider_name}/"


# COMMAND ----------

# Read the JSON file using autoloader with inferSchema
df = ( spark.readStream.format("cloudFiles") 
    .option("cloudFiles.format","json") 
    .option("cloudFiles.schemaLocation", f"{_schema_location}") 
    .option("inferSchema", "true") 
    .load(f"{_volume_path}/*.json")
    # .select("*",col("_metadata.file_name").alias("DATASOURCE")
            # ,to_timestamp(substring(concat(element_at(split(col ("DATASOURCE"), '-'),-2),regexp_replace(element_at(split(col ("DATASOURCE"), '-'),-1), ".json", "")),0,14),'yyyyMMddHHmmss').alias("PROCESSED_TIME"))
    # )
)

# COMMAND ----------

# Auto loader writes data to the bronze table using availableNow=True to process each file in order.
# try:
streamQy = (
    df.writeStream
    .outputMode("append")
    .queryName(f"AutoLoad_{_bronze_table_name.capitalize()}")
    .option("mergeSchema", "false")
    .option("checkpointLocation", _checkpoint_location)
    .trigger(availableNow=True)
    .toTable(f"{_catalog_name}.{_data_schema_name}.{_bronze_table_name}")
).awaitTermination()
# except Exception as error:
#     dbutils.notebook.exit(f'There are ERRORS for file {_bronze_table_name}: {error}')

# COMMAND ----------

# %sql
# DROP TABLE IF EXISTS genai_bronze_dev.database.tb_products_silver ;

# CREATE OR REPLACE TABLE genai_bronze_dev.database.tb_products_silver (
#   product STRING 
# )
 
