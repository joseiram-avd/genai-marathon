# Databricks notebook source
# DBTITLE 1,Install Libraries
#Install libraries
%pip install mlflow==2.10.0 lxml==4.9.3 langchain databricks-vectorsearch==0.22 cloudpickle==2.2.1 databricks-sdk cloudpickle==2.2.1 pydantic transformers databricks-feature-engineering==0.2.0

# COMMAND ----------

# DBTITLE 1,Databricks VectorSearch LangChain
# %pip install --quiet --upgrade databricks-vectorsearch langchain
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Libraries and User Setup
#Import Libraries
from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline
from transformers.utils import logging

# #setup catalog and show widget at top
# dbutils.widgets.text("catalog_name","main")
# catalog_name = dbutils.widgets.get("catalog_name")

# #break user in their own schema
# current_user = spark.sql("SELECT current_user() as username").collect()[0].username
# schema_name = f'genai_workshop_{current_user.split("@")[0].split(".")[0]}'

# #create schema
# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
# print(f"\nUsing catalog + schema: {catalog_name}.{schema_name}")

# COMMAND ----------

# DBTITLE 1,Define Model (DBRX)
import os
from langchain.llms import HuggingFacePipeline
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from langchain.chat_models import ChatDatabricks

#call dbrx, hosted by Databricks Foundation Model API
dbrx_model = ChatDatabricks(endpoint="databricks-dbrx-instruct", max_tokens = 1100) #275 

# COMMAND ----------

# DBTITLE 1,Run LLM Chain
#This just saves us from repeating the steps of creating Prompt Templates and the LLMChain
def run_llm_chain(input_string, template_string, model):
  """
  given an input string, template, and model, execute a langchain chain on the input with a given prompt template

  params
  ==========
  input_string (str): the incoming query from a user to be evaluated
  template_string (str): prompt template append or pre-pend to input_string (required for prompt engineering)
  model (langchain model): the name of the model 
  """
  prompt_template = PromptTemplate(
    input_variables=["input_string"],
    template=template_string,
  )
  model_chain = LLMChain(
    llm=model,
    prompt=prompt_template,
    output_key="Response",
    verbose=False
  )

  return model_chain.run({"input_string": input_string})

# COMMAND ----------

import unity_catalog
import hashlib

def generate_md5(input_string):
    md5_hash = hashlib.md5(input_string.encode())
    return md5_hash.hexdigest()

# Create a new UDF registration
# udf_registration = unity_catalog..UDFRegistration(
#     name="generate_md5",
#     description="Generates an MD5 code from an input string",
#     language="python",
#     code="""
#         import hashlib

#         def generate_md5(input_string):
#             md5_hash = hashlib.md5(input_string.encode())
#             return md5_hash.hexdigest()
#     """
# )

# Register the UDF
# unity_catalog.register_udf(udf_registration)

# COMMAND ----------

import jsonschema 

def validate_dict(item):
    schema = { 
              'item': {'type': 'string'},
              'nome': {'type': 'string'},
              'marca': {'type': 'string'},
              'tamanho': {'type': 'string'},
              'produto_base': {'type': 'string'},
              'categoria': {'type': 'string'},
              'comestivel': {'type': 'string'}
    }

    try:
        jsonschema.validate(instance=item, schema=schema)
        return True
    except jsonschema.exceptions.ValidationError:
        return False
     

# COMMAND ----------

# DBTITLE 1,Creating a Prompt Template to Append to our Inputs
from langchain import PromptTemplate
from langchain.chains import LLMChain
 

intro_template = """
Você é um classificador de produtos em um supermercado. Cada item da lista de produtos representa a descrição completa do produto. Classifique as características de todos os itens da lista de produtos, de acordo com as seguintes regras:
1) item: reproduzir o texto completo para descrição completa do produto.
2) nome: nome do produto.
3) marca: Identificar a marca do produto.
4) tamanho: unidade de medida, ou tamanho descrito na descrição do produto.
5) produto_base: Nome mais genérico possível, com uma ou duas palavras.
6) categoria: Alimento ou Bebida ou Não Alimento.
7) comestivel SIM ou NÃO.
Responda apenas como um arquivo JSON flat, ou seja, sem formatação ou quebra de linha.
User Question: "{input_string}"
"""

# COMMAND ----------

from pyspark.sql.functions import row_number, lit, col, StructType
from pyspark.sql.window import Window
from pyspark.sql.types import StructField, StringType, StructType
import json 

#limit record to fit with max_tokens
limit_records = 10

# Define the schema for the dataframe to be saved into target silver table
schema = StructType([
    StructField("item", StringType(), True),
    StructField("nome", StringType(), True),
    StructField("marca", StringType(), True),
    StructField("tamanho", StringType(), True),
    StructField("produto_base", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("comestivel", StringType(), True),
    StructField("id", StringType(), True)
])

table_name = "genai_bronze_dev.database.tb_products_classified_silver"

# Check if the table exists
if spark.catalog.tableExists(table_name):
    print("Table already exists")
else:
    # Create the table with the defined schema
    spark.createDataFrame([], schema).write.saveAsTable(table_name)
    print("Table created successfully")

# Retrieve the data from the table tb_product_silver in the genai_bronze_dev.database
df = spark.sql(f"""
    SELECT a.product
    FROM genai_bronze_dev.database.tb_products_silver AS a
    LEFT JOIN genai_bronze_dev.database.tb_products_classified_silver AS b
    ON a.product = b.item
    WHERE a.product IS NOT NULL
    AND b.item IS NULL
""")

# Add a column called 'id' that contains row numbers from 1 to n
w = Window().orderBy(lit('product'))
df = df.withColumn('id', row_number().over(w))

# Get the total number of rows in the dataframe
total_rows = df.count()

# Set the number of records to iterate at a time
batch_size = limit_records

# Calculate the number of iterations needed
num_iterations = int(total_rows / batch_size)

# data_dict
data_dict =[]

# Iterate through the dataframe in batches
for i in range(num_iterations):
    # Get the starting index of the batch
    start_index = i * batch_size
    
    # Get the ending index of the batch
    end_index = (i + 1) * batch_size

    # Extract the current batch of records from the dataframe
    product_list = df.select("product").where(col('id').between(start_index + 1, end_index)).rdd.flatMap(lambda x: x).collect()

    # Convert to a string in the format "[product1, product2, product3, ...]"
    user_question = "[" + ", ".join(product_list) + "]"

    # Llm processing request
    llm_chain_response = run_llm_chain({"input_string":user_question}, intro_template, dbrx_model)

    # Convert llm chain response into dict
    for item in json.loads(llm_chain_response):
        if validate_dict(item) == True: 
            data_dict.append( item )
        else:
            print( item )



# COMMAND ----------

# DBTITLE 1,Save LLM Chain Response to Silver
from pyspark.sql.functions import md5

# Save the llm chain response into target silver table
df_append = spark.createDataFrame(data_dict, schema).filter("item is not null")
df_append = df_append.withColumn('id', md5('item'))
df_append.write.format("delta").mode("append").saveAsTable(table_name)


# COMMAND ----------

from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

df = spark.sql("select * from genai_bronze_dev.`database`.tb_products_classified_silver")

fs.create_table(
    name="genai_bronze_dev.database.tb_products_classified_silver_FS",
    primary_keys=["id"],
    df=df,
    description="Products feature table"
)

# fs.publish_table(
#     name="genai_bronze_dev.database.tb_products",
#     online_store=fs.online_store_spec("tb_products_online_store"),
#     mode="merge"
# )

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient(model_registry_uri="databricks-uc")

# first create a table with User Features calculated above 
fe_table_name_users = f"genai_bronze_dev.database.tb_products_classified_silver_FS"

fe.create_table(
    name=fe_table_name_users, # unique table name (in case you re-run the notebook multiple times)
    primary_keys=["id"],
    df=df,
    description="Products Features",
    tags={"team":"analytics"}
)
