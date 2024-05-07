# Databricks notebook source
# MAGIC %run "/Users/nursuaidah.rezali@petronas.com/POC/Configurations"

# COMMAND ----------

!pip install azure-identity
!pip install tabulate --upgrade pip

# COMMAND ----------

import os
import re
import datetime
import json
import errno
import unicodedata
import concurrent.futures
import pandas as pd
import pyspark.pandas as ps
import numpy as np
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, udf, when, concat_ws
from datetime import datetime
from tabulate import tabulate
from functools import reduce
from azure.core.credentials import AzureKeyCredential
from azure.core.exceptions import HttpResponseError
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeFileClient, ContentSettings
from azure.core.exceptions import ResourceNotFoundError

# COMMAND ----------

#ADLS Gen2 Variables 
container_schema = 'enigma'

process_csv_parq_1 = "abfss://{0}@{1}.dfs.core.windows.net/POC/Process/Prev_Release/".format(container_schema,storage_acc)
process_csv_parq_2 = "abfss://{0}@{1}.dfs.core.windows.net/POC/Process/Extracted_Output/Raw/".format(container_schema,storage_acc)
output_path_title_mapping = "abfss://{0}@{1}.dfs.core.windows.net/POC/Process/Extracted_Output/Title/".format(container_schema,storage_acc)

# COMMAND ----------

def get_df_initial(tables, schema):
    dfs = []
    for table in tables:
        query = "SELECT * FROM {}.{}".format(schema, table)
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbUrl_config) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option("query", query) \
            .option("user", username_config) \
            .option("password", password_config) \
            .load()
        for col in df.dtypes:
            df = df.withColumn(col[0], df[col[0]].cast('string'))
        df = df.fillna('')
        df = df.dropDuplicates()
        df = df.withColumn("LastUpdated", F.current_date())
        dfs.append(df)
    return dfs

# COMMAND ----------

def get_dir_content(ls_path):
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isFile():
            yield dir_path.path
        elif dir_path.isDir() and ls_path != dir_path.path:
            yield from get_dir_content(dir_path.path)

# COMMAND ----------

def load_parquet_files(parquet_files):
    dataframes = []

    for file_path in parquet_files:
        df = spark.read.parquet(file_path)
        dataframes.append(df)

    merged_df = reduce(DataFrame.unionByName, dataframes)
    
    return merged_df

# COMMAND ----------

def remove_non_printable_chars(string):
    if string is None:
        return ''
    # Replace any non-printable characters with an empty string
    printable_string = ''.join(char for char in string if unicodedata.category(char)[0] != 'C')
    return printable_string

def is_basic_latin(char):
    # Define a condition to check if a character is part of the Basic Latin Unicode block
    return ord(char) < 128

def clean_string(string):
    # Remove non-printable characters first, then filter basic Latin and whitespace
    cleaned_string = ''.join(char for char in remove_non_printable_chars(string) if is_basic_latin(char) or char.isspace())

    # Remove the extra double quotes around abbreviations
    cleaned_string = re.sub(r'\s?""', '', cleaned_string)

    # Remove double quotes at the start and end of the string
    cleaned_string = cleaned_string.strip('"')

    cleaned_string = re.sub(r'\s+', ' ', cleaned_string.strip())
    return cleaned_string

# COMMAND ----------

tables = ["TrsDocRevision", "MstCdBdBusiness", "MstSection","MstOpuDivisionAsset"]
schema = "dbo"

dfs = get_df_initial(tables, schema)
dfs

# COMMAND ----------

df1 = dfs[1].withColumnRenamed("Name","Name_1").withColumnRenamed("IsDeleted","IsDeleted_1").withColumnRenamed("Id","Id_1") # mstcdcdbusiness
display(df1)

# COMMAND ----------

df2 = dfs[0] # trsdocrevision
display(df2)

# COMMAND ----------

df_joined = df2.join(df1, df1.Id_1 == df2.CdBdBusinessId, "fullouter")

df_joined.filter((col("DocumentUrl").like("%.docx%") |
                        col("DocumentUrl").like("%.pdf%") |
                        col("DocumentUrl").like("%.doc%") |
                        col("DocumentUrl").like("%.docm%")) &
                       (col("IsDeleted") == 0) &
                       (col("IsPublished") == 1) &
                       (col("IsRetired") == 0) &
                       (col("ApprovedDate") < '2023-05-13 00:00:00.000'))

df_select = df_joined.select(col("DocumentCode"), col("Title"), col("Name_1").alias("BusinessStreamDetails"))
display(df_select)

# COMMAND ----------

process_parq_1 = get_dir_content(process_csv_parq_1)
matchers = ['.parquet']
process_parq_1 = [s for s in process_parq_1 if any(xs in s for xs in matchers)]
process_parq_1 = load_parquet_files(process_parq_1)
display(process_parq_1)

# COMMAND ----------

process_parq_2 = get_dir_content(process_csv_parq_2)
matchers = ['.parquet']
process_parq_2 = [s for s in process_parq_2 if any(xs in s for xs in matchers)]
process_parq_2 = load_parquet_files(process_parq_2)
display(process_parq_2)

# COMMAND ----------

df_count1 = process_parq_1.count()
print("first dataframe count:", df_count1)
df_count2 = process_parq_2.count()
print("second dataframe count:", df_count2)

# COMMAND ----------

# Append sec_df to raw_df
appended_df = process_parq_1.union(process_parq_2)

# Display the appended DataFrame(consists of all release from release 1-latest release)
display(appended_df)
appended_df.count()

# COMMAND ----------

columns_to_convert = ["Business_Terms_Definitions", "Business_Terms"]

# Spark UDF to remove non-printable characters
remove_non_printable_udf = udf(remove_non_printable_chars, StringType())

# Spark UDF to clean the string
clean_string_udf = udf(clean_string, StringType())

df_modified = appended_df.select(
    *[when(col(column).isNull(), col(column)).otherwise(clean_string_udf(remove_non_printable_udf(col(column)))).alias(column) if column in columns_to_convert else col(column)for column in appended_df.columns])

display(df_modified)
df_modified.count()

# COMMAND ----------

df_merged = df_select.join(df_modified, df_select.DocumentCode == df_modified.Ref_No, "inner")
df_joined = df_merged.select("Ref_No", "Title", "Business_Terms", "Business_Terms_Definitions", "BusinessStreamDetails","Abbreviation").distinct().withColumn('combined', concat_ws(', ', col('Title'), col('Business_Terms')))
display(df_joined)
df_joined.count()

# COMMAND ----------

import datetime

# Get the current date in the desired format
current_date = datetime.datetime.now().strftime("%d/%m/%Y")

# Define the target directory path including the date
target_directory_path = output_path_title_mapping + current_date

# Save the DataFrame to the specified directory
df_joined.repartition(1).write.parquet(path=target_directory_path, mode="overwrite")
print("dataframe has been saved in -->", target_directory_path)

# COMMAND ----------


