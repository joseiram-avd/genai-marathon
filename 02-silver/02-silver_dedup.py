# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

# Automatic generated Bronze-layer delta-table creation
# for extractor -0GL_ACCOUNT_ATTR  (datetime:2023-06-22 07:52:24)
# Set the values for catalog_name and schema_name at runtime
dbutils.widgets.text("catalog_name","")
catalog_name = dbutils.widgets.get("catalog_name")
dbutils.widgets.text("schema_name","")
schema_name = dbutils.widgets.get("schema_name")
if(catalog_name == "" or schema_name == ""): raise Exception("Pass catalog_name and schema_name as arguments for running this notebook.")

_reset = dbutils.widgets.dropdown("_reset", "False",["False","True"])

# COMMAND ----------

# MAGIC %run ../_includes/includes

# COMMAND ----------

table_name_silver = f"{catalog_name}.{schema_name}.tb_products_silver"
key_fields = ["product"]

table_name_bronze = f"{catalog_name}.{schema_name}.tb_offers_bronze"
temp_view_name_bronze = f"{catalog_name}.{schema_name}.tmp_view_{table_name_bronze}"

checkpoints_location = f"abfss://landing@{__storage_account}.dfs.core.windows.net/_checkpoint/{catalog_name}/{schema_name}/{table_name_silver}"


# COMMAND ----------

sql_statement = f"""
	MERGE INTO  {table_name_silver}  as target  
	USING {temp_view_name_bronze} 	 as source 
	ON	target.product = source.product 
    
	WHEN NOT MATCHED THEN 
	INSERT 
 		* 
	 
	;"""   

# COMMAND ----------

if _reset == True:
  reset_silver_table(checkpoints_location, table_name_silver);

# COMMAND ----------

update_silver_table(table_name_silver, table_name_bronze, key_fields, key_fields, sql_statement, temp_view_name_bronze, checkpoints_location)
