# Databricks notebook source
# Install libraries
!pip install azure-identity
!pip install tabulate --upgrade pip
!pip install unidecode

# COMMAND ----------

import os
import re
import pyodbc
import pandas as pd
import numpy as np
import pyspark.pandas as ps
import pyspark
import errno
import unidecode
import concurrent.futures

from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions, generate_account_sas
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeFileClient, ContentSettings
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import HttpResponseError
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import ClientSecretCredential

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, count, when , regexp_replace, udf, initcap, trim, lit, concat, regexp_extract, concat_ws, lower, countDistinct
from pyspark.sql.types import StringType, StructField, StringType, StructType
from tabulate import tabulate
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# ADLS Gen2 Variables 
container_raw = 'raw'
container_process = 'process'
container_enrich = 'enrich'

scope_name = os.getenv("SECRET_SCOPE")
storageAccountName=dbutils.secrets.get(scope="EDH_KV",key="az-adls-storageaccountName")
app_id = dbutils.secrets.get(scope="EDH_KV",key="EDH-SPN-Client")
client_secret = dbutils.secrets.get(scope="EDH_KV",key="EDH-SPN-Secret")
tenant = dbutils.secrets.get(scope="EDH_KV",key="EDH-SPN-Tenant")
account_key = dbutils.secrets.get(scope="EDH_KV",key="az-adls-access-key")

# Configure ADLS connection 
spark.conf.set("fs.azure.account.auth.type.{0}.dfs.core.windows.net".format(storageAccountName), "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.{0}.dfs.core.windows.net".format(storageAccountName), "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.{0}.dfs.core.windows.net".format(storageAccountName),app_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.{0}.dfs.core.windows.net".format(storageAccountName), client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.{0}.dfs.core.windows.net".format(storageAccountName), "https://login.microsoftonline.com/{0}/oauth2/token".format(tenant))

token_credential = ClientSecretCredential(
    tenant_id = dbutils.secrets.get(scope="EDH_KV",key="EDH-SPN-Tenant"),
    client_id = dbutils.secrets.get(scope="EDH_KV",key="EDH-SPN-Client"),
    client_secret = dbutils.secrets.get(scope="EDH_KV",key="EDH-SPN-Secret")
)

account_name = storageAccountName

previous_release_path = "abfss://{0}@{1}.dfs.core.windows.net/AZ_ADLS_PR_SP/SP_MYGOVERNANCE/V01/EBG/CSV/HARMONISATION_CSV/Previous_Release_2/".format(container_process,storageAccountName)
current_release_path = "abfss://{0}@{1}.dfs.core.windows.net/AZ_ADLS_PR_SP/SP_MYGOVERNANCE/V01/EBG/CSV/MAPPING_CSV/Data_Domain_Mapping/Output-2ndIngestion-2/".format(container_process,storageAccountName)
data_steward_validation = "abfss://{0}@{1}.dfs.core.windows.net/AZ_ADLS_ER_SP/SP_MYGOVERNANCE/V01/EBG/CSV/FINAL_OUTPUT_HARMONISATION/".format(container_enrich,storageAccountName)
final_output_path = "abfss://{0}@{1}.dfs.core.windows.net/AZ_ADLS_ER_SP/SP_MYGOVERNANCE/V01/EBG/CSV/FINAL_OUTPUT_OVERALL/".format(container_enrich,storageAccountName)

# COMMAND ----------

def get_dir_content(ls_path):
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isFile():
            yield dir_path.path
        elif dir_path.isDir() and ls_path != dir_path.path:
            yield from get_dir_content(dir_path.path)

# COMMAND ----------

def load_csv_files(csv_files):
    dataframes = []
    spark = SparkSession.builder.appName("CSVLoader").getOrCreate()
    
    # Define the schema for your DataFrame
    schema = StructType([
        StructField("Ref_No", StringType(), True),
        StructField("Title", StringType(), True),
        StructField("Business_Term", StringType(), True),
        StructField("Business_Terms_Definitions", StringType(), True),
        StructField("Abbreviation", StringType(), True),
        StructField("BusinessStreamDetails", StringType(), True),
        StructField("Data_Domain", StringType(), True),
        StructField("Data_Source", StringType(), True)
    ])

    for file_path in csv_files:
        df = spark.read.format("csv")\
            .option("header", "true")\
            .option("delimiter", ",")\
            .option("multiline", "true")\
            .option("quote", "\"")\
            .option("escape", "\"")\
            .schema(schema)\
            .load(file_path)
        dataframes.append(df)

    merged_df = reduce(DataFrame.unionByName, dataframes)
    
    return merged_df

# COMMAND ----------

def load_parquet_files(parquet_files):
    dataframes = []

    for file_path in parquet_files:
        df = spark.read.parquet(file_path)
        dataframes.append(df)

    merged_df = reduce(DataFrame.unionByName, dataframes)
    
    return merged_df

# COMMAND ----------

@udf(StringType())
def clean_and_ascii_substitute(x):
    if x is None:
        return None
    
    # Remove extra whitespaces and strip leading/trailing spaces
    cleaned_string = ' '.join(x.split()).strip()

    # Substitute non-ASCII characters with closest ASCII representation
    cleaned_string = unidecode.unidecode(cleaned_string)

    return cleaned_string

# COMMAND ----------

@udf
def update_data_domain(data_domain, business_stream_details):
    if data_domain == 'Facilities Management' and business_stream_details != 'Corporate':
        return 'Physical Asset Management'
    return data_domain

# COMMAND ----------

@udf
def add_period(text):
    if isinstance(text, str):
        # Split the text into sentences
        sentences = re.split(r'(?<=[.!?])\s+', text)
    
        # Add a period at the end of each sentence if the last character is not a period
        sentences_with_period = [sentence.strip() + '.' if sentence and sentence[-1] != '.' else sentence for sentence in sentences]
    
        # Join the sentences back together
        return ' '.join(sentences_with_period)
    else:
        return text

# COMMAND ----------

pts_mapping = {
    "PTS 00": "General",
    "PTS 11": "Civil/Structure & Pipeline Engineering",
    "PTS 12": "Mechanical Engineering",
    "PTS 13": "Electrical Engineering",
    "PTS 14": "Instrument/Control Engineering",
    "PTS 15": "Materials, Corrosion, and Inspection Engineering",
    "PTS 16": "Process Engineering",
    "PTS 18": "Health, Safety, Security & Environment (HSSE)",
    "PTS 19": "Project Management",
    "PTS 20": "Maritime",
    "PTS 25": "Drilling"
}

ptg_mapping = {
    "PTG 00": "General",
    "PTG 09": "Geoscience",
    "PTG 10": "Petroleum Engineering",
    "PTG 11": "Civil/Structure & Pipeline Engineering",
    "PTG 12": "Mechanical Engineering",
    "PTG 13": "Electrical Engineering",
    "PTG 14": "Instrument/Control Engineering",
    "PTG 15": "Materials, Corrosion, and Inspection Engineering",
    "PTG 16": "Process Engineering",
    "PTG 18": "Health, Safety, Security & Environment (HSSE)",
    "PTG 19": "Project Management",
    "PTG 20": "Maritime",
    "PTG 25": "Drilling"
}

def map_data_domain_based_on_title_prefix(df, pts_mapping, ptg_mapping):
    # Apply the UDF to map Title to Data_Domain based on key mapping
    for mapping in [pts_mapping, ptg_mapping]:
        for title_prefix, data_domain in mapping.items():
            df = df.withColumn("Data_Domain",
                               when(((df["Title"].startswith(title_prefix)) | (df["Ref_No"].startswith(title_prefix))) & ((df["Data_Source"] == "PTS") | (df["Data_Source"] == "PTG")), data_domain)
                               .otherwise(df["Data_Domain"]))

    return df

# COMMAND ----------

@udf(StringType())
def get_data_steward(business_stream_details, data_domain, data_source):
    if data_source in ["PTS", "PTG"]:
        if data_domain in ["General", "Geoscience", "Petroleum Engineering", "Civil/Structure & Pipeline Engineering", "Mechanical Engineering", "Electrical Engineering", "Instrument/Control Engineering", "Materials, Corrosion, and Inspection Engineering", "Process Engineering", "Project Management", "Maritime", "Drilling","Physical Asset Management","Change Management", "Digital", "Finance & Accounts", "Human Resource", "Internal Audit", "Maritime", "New Energy", "Research & Technology", "Risk Management"]:
            return "Shaiful B Khalid"
        elif data_domain == "Health, Safety, Security & Environment (HSSE)":
            return "Norhazlina Bt Mydin"
    elif data_domain == "Project Management":
        return "Ang John Wei"
    elif data_domain in ["Civil/Structure & Pipeline Engineering", "Mechanical Engineering", "Mechanical Engineering ", "Electrical Engineering", "Instrument/Control Engineering", "Instrument /Control Engineering", "Materials, Corrosion, and Inspection Engineering", "Process Engineering", "Physical Asset Management"]:
        return "Shaiful B Khalid"
    elif data_domain == "Research & Technology":
        return "Dr. Mahpuzah Bt Abai"
    elif data_domain == "Finance & Accounts":
        return "Eugene Hendroff"
    elif data_domain == "Procurement":
        return "Johana Bt Jamjan"
    elif data_domain in ["Human Resource","Library","Change Management","Capability Management"]:
        return "Nor'aini Bt Jalaludin"
    elif data_domain in ["Geoscience", "Petroleum Engineering", "Drilling"]:
        return "Dzulkarnain B Azaman"
    elif data_domain == "Legal":
        return "Sh Isadora Alyahya Bt Sy Mohamed"
    elif data_domain == "Business Planning & Development":
        return "Azmaniah Bt A Aziz"
    elif data_domain == "Strategic Communication":
        return "Sareen B Risham"
    elif data_domain == "Risk Management":
        return "Azaharin B Ahmad"
    elif data_domain == "Health, Safety, Security & Environment (HSSE)":
        return "Norhazlina Bt Mydin"
    elif data_domain == "Internal Audit":
        return "Azrul Azman B Ahmad"
    elif data_domain == "Facilities Management":
        return "M Muzaffar Hazril Marzuki"
    elif data_domain == "Upstream":
        return "Dzulkarnain B Azaman"
    elif data_domain == "Marketing & Trading":
        return "Farhan B A Rahim"
    elif data_domain in ["General", "Knowledge Management"]:
        return "Datin Ts. Habsah Bt Nordin"
    elif data_domain == "Digital":
        return "Phuah Aik Chong"
    elif business_stream_details in ["Upstream", "Upstream-Malaysia"]:
        return "Dzulkarnain B Azaman"
    elif business_stream_details in ["Downstream", "Downstream PCGB", "Downstream ENGEN", "Downstream RMT", "Downstream PDB", "Downstream - PCGB", "Downstream - PDB", "Downstream - ENGEN", "Downstream - RMT", "Downstream - PRPC Group"]:
        return "Azureen Azita Bt Abdullah"
    elif business_stream_details in ["Gas Gas Power", "Gas", "Gas - Gas & Power", "Gas LNG Assets", "Gas - LNG Assets"]:
        return "Farhan B A Rahim"
    else:
        return None

# COMMAND ----------

def filter_based_on_hierarchy(df):
    hierarchy_order = {
        'PTS': 1,
        'PTG': 2,
        'MyGovernance': 3,
        'PETRONAS Outlook Activity 2023 - 2025': 4,
        'PETRONAS Data Standards': 5,
        'SKILL': 6,
        'Finance': 7,
        'TAX': 8,
        'ARIS': 9,
        'HRM': 10
    }
    
    # Define a function to get the rank for each data source
    get_rank = F.udf(lambda source: hierarchy_order.get(source, float('inf')))
    
    # Create a new column to assign a numerical rank based on the hierarchy
    df = df.withColumn('Hierarchy_Rank', get_rank(df['Data_Source']))
    
    # Calculate the minimum rank for each Business_Term
    window_spec = Window.partitionBy(df['Business_Term_Lower']).orderBy(df['Hierarchy_Rank'])
    df = df.withColumn('Min_Rank', F.min(df['Hierarchy_Rank']).over(window_spec))
    
    # Filter rows based on hierarchy rank
    filtered_df = df.filter(df['Hierarchy_Rank'] == df['Min_Rank']).drop('Hierarchy_Rank', 'Min_Rank')
    
    return filtered_df

# COMMAND ----------

def select_longest_definition(df):
    # Calculate the length of 'Business_Terms_Definitions'
    df = df.withColumn('Definition_Length', F.length('Business_Terms_Definitions'))
    
    # Create a window partitioned by 'Business_Term' and ordered by 'Definition_Length' in descending order
    window_spec = Window.partitionBy('Business_Term_Lower', 'Data_Source', 'Data_Domain').orderBy(F.desc('Definition_Length'))
    
    # Rank rows within each group based on the length of 'Business_Terms_Definitions'
    df = df.withColumn('rank', F.row_number().over(window_spec))
    
    # Select only the rows with the longest 'Business_Terms_Definitions' for each group
    df_selected = df.filter(df['rank'] == 1).drop('rank', 'Definition_Length')
    
    return df_selected

# COMMAND ----------

process_csv = get_dir_content(current_release_path)
matchers = ['.csv']
process_csv_1 = [s for s in process_csv if any(xs in s for xs in matchers)]
process_csv_1

# COMMAND ----------

process_csv = get_dir_content(previous_release_path)
matchers = ['.csv']
process_csv_2 = [s for s in process_csv if any(xs in s for xs in matchers)]
process_csv_2

# COMMAND ----------

first_df = load_csv_files(process_csv_1)
display(first_df)

# COMMAND ----------

sec_df = load_csv_files(process_csv_2)
display(sec_df)

# COMMAND ----------

# Append sec_df to raw_df
appended_df = first_df.union(sec_df)

# Display the appended DataFrame(consists of all release from release 1-latest release)
display(appended_df)

# COMMAND ----------

df_count1 = appended_df.count()
df_count1
# df_count2 = sec_df.count()
# df_count2

# COMMAND ----------

# Create comment column which contains ref no and title from ref_no and title column
appended_final_df = appended_df.withColumn('Comment', concat_ws(' ', col('Ref_No'), col('Title'))).withColumn("Data_Steward", lit(None)).withColumn("Synonym", lit(None))
display(appended_final_df)

# COMMAND ----------

# Assuming your DataFrame is named original_df
# Select the columns you want to consider for duplicate removal
columns_to_consider = ["Business_Term", "Business_Terms_Definitions", "Abbreviation", "Data_Domain"]

# Drop duplicates based on the selected columns
deduplicated_df = appended_final_df.dropDuplicates(subset=columns_to_consider)

# Show the resulting DataFrame without duplicates
display(deduplicated_df)

# COMMAND ----------

deduplicated_df.count()

# COMMAND ----------

df2 = deduplicated_df.withColumn("Data_Domain", update_data_domain(deduplicated_df["Data_Domain"], deduplicated_df["BusinessStreamDetails"]))
# display(df2)

# Call the function to process your DataFrame
df3 = map_data_domain_based_on_title_prefix(df2, pts_mapping, ptg_mapping)

# Display the final DataFrame
display(df3)

# COMMAND ----------

df4 = df3.withColumn("Data_Steward", get_data_steward(col("BusinessStreamDetails"), col("Data_Domain"), col("Data_Source")))
display(df4)

# COMMAND ----------

df4.count()

# COMMAND ----------

columns_to_transform = df4.columns

df5 = df4.select(*[clean_and_ascii_substitute(col(col_name)).alias(col_name) for col_name in columns_to_transform])
df6 = df5.withColumn("Business_Terms_Definitions", regexp_replace("Business_Terms_Definitions", ' \n', '\n'))
df6 = df6.filter(col("Business_Term").isNotNull() & ~lower(col("Business_Term")).contains("null") & (col("Business_Term") != " "))
display(df6)

# COMMAND ----------

count = df6.count()
count

# COMMAND ----------

df6.repartition(1).write.csv(path=final_output_path, mode="overwrite", header="true")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Steward Validation

# COMMAND ----------

# df_validation = df3.filter(lower(col("Business_Terms_Definitions")).startswith("as per "))
# display(df_validation)

filter_condition = (
    lower(col("Business_Terms_Definitions")).startswith("as per ") |
    lower(col("Business_Terms_Definitions")).startswith("refer to ") |
    lower(col("Business_Terms_Definitions")).startswith("as define ") |
    lower(col("Business_Terms_Definitions")).startswith("refer ") |
    lower(col("Business_Terms_Definitions")).startswith("see ")
)

df_validation = df6.filter(filter_condition)
display(df_validation)

# COMMAND ----------

df7 = df6.withColumn("Business_Terms_Definitions", add_period(col("Business_Terms_Definitions"))) \
         .withColumn("Business_Term_Period", add_period(col("Business_Term")))
display(df7)

# COMMAND ----------

df7.count()

# COMMAND ----------

# Make "Business_Terms_Definitions" and "Business_Term" lowercase
df8 = df7.withColumn("Business_Terms_Definitions_Lower", F.lower(F.col("Business_Terms_Definitions"))) \
         .withColumn("Business_Term_Lower", F.lower(F.col("Business_Term_Period")))

# Drop duplicates based on specified columns
df8 = df8.dropDuplicates(['Business_Term_Lower', 'Business_Terms_Definitions_Lower', 'Data_Domain'])
df8 = df8.drop('Business_Term_Period')

# Filter out rows where "Business_Terms_Definitions" and "Abbreviation" is null
df8 = df8.filter((col("Business_Terms_Definitions").isNotNull()) | (col("Abbreviation").isNotNull()))
df8 = df8.filter(((~lower(col("Business_Terms_Definitions")).contains("null")) | (~lower(col("Abbreviation")).contains("null"))))

# Show the resulting DataFrame
display(df8)

# COMMAND ----------

df8.count()

# COMMAND ----------

# Assuming df is your original DataFrame
filtered_df = filter_based_on_hierarchy(df8)
display(filtered_df)

# COMMAND ----------

filtered_df.count()

# COMMAND ----------

# Apply the function to the filtered DataFrame
result_df = select_longest_definition(filtered_df)
display(result_df)

# COMMAND ----------

result_df.count()

# COMMAND ----------

list_1 = result_df.select("Data_Source").distinct().collect()
df_test = spark.createDataFrame(list_1)
display(df_test)

# COMMAND ----------

df_test_with_count = result_df.groupBy("Data_Source").agg(F.count("*").alias("count"))

# Display the DataFrame with counts
display(df_test_with_count)

# COMMAND ----------

df_test_with_count_2 = loaded_df_2.groupBy("Data_Source").agg(F.count("*").alias("count"))

# Display the DataFrame with counts
display(df_test_with_count_2)

# COMMAND ----------

# result_df.coalesce(1).write.csv(path=data_steward_validation, mode="append", header="true",encoding="UTF-8")
result_df.write.parquet(path=data_steward_validation, mode="overwrite")

# COMMAND ----------

# Reload the saved parquet to check the rows count
process_csv = get_dir_content(data_steward_validation)
matchers = ['.parquet']
process_csv_test = [s for s in process_csv if any(xs in s for xs in matchers)]
process_csv_test

# COMMAND ----------

loaded_df = spark.read.parquet(data_steward_validation)
display(loaded_df)

# COMMAND ----------

result_df.count()

# COMMAND ----------

loaded_df.count()

# COMMAND ----------

loaded_df_2.count()

# COMMAND ----------

def load_parquet_files(parquet_files):
    dataframes = []

    for file_path in parquet_files:
        df = spark.read.parquet(file_path)
        dataframes.append(df)

    merged_df = reduce(lambda a, b: a.unionByName(b), dataframes)

    return merged_df

# COMMAND ----------

loaded_df_2 = load_parquet_files(process_csv_test)
display(loaded_df_2)

# COMMAND ----------

list_2 = df_input.select("Ref_No").distinct().collect()
df_test_2 = spark.createDataFrame(list_2)
display(df_test_2)

# COMMAND ----------

differences = df_test.subtract(df_test_2)
display(differences)
