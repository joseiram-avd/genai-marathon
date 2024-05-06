# Databricks notebook source
__workspace_name = spark.conf.get("spark.databricks.workspaceUrl").split('.')[0]

__bg_name = 'genai'

#dev is de default environment
__environment = 'dev'
__storage_account = 'rg9east9us29data9lake'

#qa
if __workspace_name == 'adb-xxxxx':
    __environment = 'qa'
    __storage_account = 'rg9east9us29data9lake'

#prod 
if __workspace_name == 'adb-yyyyy':
    __environment = 'prd'
    __storage_account = 'rg9east9us29data9lake'


__catalog_bronze = f'{__bg_name}_bronze_{__environment}'
__catalog_silver = f'{__bg_name}_silver_{__environment}'
__catalog_gold   = f'{__bg_name}_gold_{__environment}' 
__catalog_consumption   = f'{__bg_name}_consumption_{__environment}' 

__schema_bronze_work = 'database'
__schema_bronze_stage = 'database'

__schema_silver_work = 'database'
__schema_silver_stage = 'database'

#create many schema as needed 
__schema_gold_work = 'database'
__schema_gold_stage = 'database'

#under the repo root path 
# __root_path = ""

#under the workspace root path 
__workspace_path = "../../Workspace"
__project_path = "Amauri/Project/edw"


# COMMAND ----------

__workspace_name, __storage_account, __catalog_bronze, __catalog_silver, __catalog_gold, __catalog_consumption, __workspace_path, __project_path

# COMMAND ----------

def __run_notebook(notebook, notebooks_folder, arguments):

    if notebook.startswith("_"): return 
    
    var_return = notebook
    
    try:
        notebook = f"/{__project_path}/{notebooks_folder}/{notebook}"

        var_return = dbutils.notebook.run(notebook,600, arguments = arguments)

        var_return = f"{notebook}-SUCCESS"

    except Exception as e:
        var_return = (f"{notebook}: {e}")

    finally:
        print ( var_return ) 

# COMMAND ----------

def __run_ddl_notebook(notebook, notebooks_folder, catalog, schema):
    
    __run_notebook(notebook, notebooks_folder, arguments = {"catalog_name" : f"{catalog}","schema_name" : f"{schema}"})

# COMMAND ----------

def __run_data_load_notebook(notebook, notebooks_folder, catalog, schema):
    
    __run_notebook(notebook, notebooks_folder, arguments = {"catalog_name" : f"{catalog}","schema_name" : f"{schema}"})


# def run_notebook(notebook ):

#     if notebook.startswith("_"): return 
    
#     table_name = notebook.strip("nb_load_data_")
#     var_return = table_name

#     try:
#         var_return = dbutils.notebook.run("../edw/"+notebook,600, arguments = {"catalog_name" : f"{catalog_name}","schema_name" : f"{schema_name}"}) 
        
#     except Exception as e:
        
#         var_return = f"{table_name} {e}"

#     finally:
#         print(var_return)

# COMMAND ----------

#Table to record whether the flat file exists or not
def create_log_table():
    create_table = f"""
        CREATE TABLE IF NOT EXISTS {__catalog_bronze}.{__schema_bronze_stage}.tb_load_control(
            TABLENAME VARCHAR(25) comment "Table or flat file nam",
            MESSAGE VARCHAR(255) comment "Message"
        ); """

    spark.sql(create_table)
     

# COMMAND ----------

# do and undo logging
def log_auto_loader_ingestion(table_name, message):
    create_table = f"""
        INSERT INTO {__catalog_bronze}.{__schema_bronze_stage}.tb_load_control
        VALUES ('{table_name}', '{message}')
    ;"""
    spark.sql(create_table)

def delete_log_auto_loader_ingestion(table_name):
    create_table = f"""
        DELETE FROM {__catalog_bronze}.{__schema_bronze_stage}.tb_load_control 
        WHERE TABLENAME = '{table_name}'
    ;"""
    spark.sql(create_table)  

# COMMAND ----------

from pyspark.sql import functions as F

class Upsert:
    def __init__(self, sql_query, update_temp):
        self.sql_query = sql_query
        self.update_temp = update_temp

    def upsert_to_delta(self, microBatchDF, batch_ID):
        session = sparkSession().sql()
        
        microBatchDF.createOrReplaceTempView(self.update_temp)
        microBatchDF._jdf.sparkSession().sql(self.sql_query)

def reset_silver_table(_checkpoints_location, _table_name):
    dbutils.fs.rm(f"{_checkpoints_location}", True)
    spark.sql(f"TRUNCATE TABLE {_table_name}")

def update_silver_table(silver_table, bronze_table, key_fields, select_fields, sql_statement, update_temp, checkpoints_location):
    
    streaming_merge = Upsert(sql_statement, update_temp)

    deduped_df = (
        spark.readStream
        .option("ignoreDeletes","true")
        .table(f"{bronze_table}")
        .drop(
            F.col("DI_SEQUENCE_NUMBER"),
            F.col("DI_OPERATION_TYPE"),
            F.col("ODQ_CHANGEMODE"),
            F.col("ODQ_ENTITYCNTR"),
            F.col("_rescued_data"),
            F.col("DATASOURCE"),
        )
        .dropDuplicates(key_fields)
        .select(select_fields)
    )

    query = (
        deduped_df.writeStream.foreachBatch(streaming_merge.upsert_to_delta)
        .option("checkpointLocation", f"{checkpoints_location}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{silver_table}")
    )

    query.awaitTermination()


# COMMAND ----------

# create_log_table()
