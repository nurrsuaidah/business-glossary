# Databricks notebook source
# MAGIC %run "/Users/nursuaidah.rezali@petronas.com/POC/Configurations"

# COMMAND ----------

dbutils.library.restartPython()
!pip install openai --upgrade
!pip install azure-identity

# COMMAND ----------

import os
import io
import re
import pandas as pd
import json
import csv
import string 
import random
import time
import datetime
import openai
from openai import AzureOpenAI
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeFileClient, ContentSettings, FileSystemClient
from azure.storage.blob import BlobServiceClient
from pyspark.sql.functions import col, udf, concat_ws, regexp_replace, lit
from pyspark.sql.types import IntegerType, StringType, ArrayType
from pyspark.sql import DataFrame, SparkSession, functions as F
from functools import reduce
from pyspark.sql.window import Window

# COMMAND ----------

container_schema = 'enigma'

raw_path = "abfss://{0}@{1}.dfs.core.windows.net/POC/Process/Extracted_Output/Title/".format(container_schema,storage_acc)
final_output_path = "abfss://{0}@{1}.dfs.core.windows.net/POC/Process/Extracted_Output/Domain/".format(container_schema,storage_acc)

# openAI
openai.api_type = "azure"
openai.api_base = "https://openailx.openai.azure.com/"
openai.api_version = "2023-09-15-preview"
openai.api_key = "a2deb20b096a47cfa902bc33209226c8"

# COMMAND ----------

def get_dir_content(ls_path, matchers):
    latest_date = None
    latest_folder_path = None
    
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isFile():
            if any(xs in dir_path.path for xs in matchers):
                yield dir_path.path
        elif dir_path.isDir() and ls_path != dir_path.path:
            yield from get_dir_content(dir_path.path, matchers)
            
            # Check and update the latest folder path
            created_time = datetime.datetime.fromtimestamp(dir_path.creationTime / 1000)
            if latest_date is None or created_time > latest_date:
                latest_date = created_time
                latest_folder_path = dir_path.path
    
    # After iterating through all subdirectories, return the latest folder path
    if latest_folder_path:
        yield latest_folder_path

# COMMAND ----------

def load_parquet_files(parquet_files):
    dataframes = []

    for file_path in parquet_files:
        df = spark.read.parquet(file_path)
        dataframes.append(df)

    merged_df = reduce(lambda a, b: a.unionByName(b), dataframes)

    return merged_df

# COMMAND ----------

def convert_column_to_list(df):
    windowSpec = Window.orderBy(F.monotonically_increasing_id())
    combined_list = df.withColumn("row_num", F.row_number().over(windowSpec)) \
                      .withColumn("combined_with_prefix", F.concat(F.lit("Input "), F.col("row_num"), F.lit(": "), F.col("combined"))) \
                      .select("combined_with_prefix") \
                      .rdd.flatMap(lambda x: x) \
                      .collect()
    return combined_list

# COMMAND ----------

def retry_with_exponential_backoff(
    func,
    initial_delay: float = 1,
    exponential_base: float = 2,
    jitter: bool = True,
    max_retries: int = 3,
    max_tokens = 2500,
    errors: tuple = (openai.RateLimitError, openai.APIError, openai.BadRequestError),
):
    """Retry a function with exponential backoff."""
 
    def wrapper(*args, **kwargs):
        # Initialize variables
        num_retries = 0
        delay = initial_delay
        INITIAL_MAX_TOKEN = max_tokens
 
        # Loop until a successful response or max_retries is hit or an exception is raised
        while num_retries < max_retries:
            try:
#               print(f"Attempt {num_retries}/{max_retries} - Requesting {INITIAL_MAX_TOKEN} max tokens")
                return func(max_token=INITIAL_MAX_TOKEN , *args, **kwargs)
 
            # Retry on specific errors
            except openai.BadRequestError:
                INITIAL_MAX_TOKEN -= 500
                num_retries += 1
            
            except errors as e:
                # Increment retries
                num_retries += 1
 
                # Check if max retries has been reached
                if num_retries > max_retries:
                    print(
                        f"Maximum number of retries ({max_retries}) exceeded."
                    )
                    return ''
 
                # Increment the delay
                delay *= exponential_base * (1 + jitter * random.random())
 
                # Sleep for the delay
                time.sleep(delay)
    
    return wrapper

# COMMAND ----------

@retry_with_exponential_backoff
def extract_data_domain(raw_text, max_token=2500):
    output = []
    response = openai.Completion.create(
        engine="text-davinci-003",
        prompt="""
        you must not add anything to the input.\nyou have to classify each input into one of the thirty-one data domain define here.\nyou must classify each input into the most accurate domain based on the description define for each domain here.\nyou must not classify the input into domain that is not define here.\nreturn me the domain specific for each input in the form of input and output only.\n\nDOMAIN:\nDomain 1: General\nDescription 1: Data that is not produced from any process or function i.e. Reference Data\nDomain 2: Digital\nDescription 2: Data related to digital technology and processes that includes data management & engineering, software engineering, data science, architecture, product management, service delivery & operations, cyber security and experience design\nDomain 3: Knowledge Management\nDescription 3: Data related to the process of creating, sharing, using and managing the knowledge and information of an organization\nDomain 4: Finance & Accounts\nDescription 4: Data related to financial performance and health, including the companies and groups' financial statements for statutory and management reporting\nDomain 5: Procurement\nDescription 5: Data related to the process of sourcing and optimizing organizational spending, that includes invoices sourcing, requisitioning, ordering, inspection, and reconciliation\nDomain 6: Business Planning & Development\nDescription 6: Data related to the process of determining a commercial enterprise's objectives, strategies and projected actions to pursue strategic opportunities\nDomain 7: Legal\nDescription 7: Data related to the management of commercial entities by the laws of the company and the regulation of commercial transactions by the laws of contract and related fields\nDomain 8: Human Resource\nDescription 8: Data related to the management of end-to-end talent journey where the employees make reference towards Group HR policies and guidelines that need to be adhered and adopted\nDomain 9: Library\nDescription 9: Data related to the end-to-end library and information resource management as well as librarian consultancy within the organisation\nDomain 10: Change Management\nDescription 10: Data related to the process of business and organisation transformational change through people, process, systems and tools\nDomain 11: Capability Management\nDescription 11: Data related to the activities that build organizational competency and capability in the organization\nDomain 12: Strategic Communication\nDescription 12: Data related to the strategic communication activities to internal and external stakeholders ensuring consistent organisational message and brand\nDomain 13: Geoscience\nDescription 13: Data related to the activities such as geology, geophysics, and geochemistry dealing with physical and chemistry constitution of earth in exploration activities\nDomain 14: Petroleum Engineering\nDescription 14: Data related to field of engineering concerned to the production of hydrocarbon, which can be crude oil or natural gas\nDomain 15: Drilling\nDescription 15: Data related to the process of drilling a hole in the ground for the extraction of crude oil and natural gas, injection of fluid from surface to a subsurface reservoir or for subsurface formations evaluation or monitoring in exploration activities\nDomain 16: New Energy\nDescription 16: Data related to the different forms of energy recovered from new alternative sources (solar, wind, etc.) other than the conventional energy like hydrocarbons and nuclear\nDomain 17: Marketing & Trading\nDescription 17: Data related to the process of business trading of crude oil, gas and petroleum products and marketing efforts by key business entities\nDomain 18: Civil/Structure & Pipeline Engineering\nDescription 18: Data related to the process of designing the framework and structures in capital projects to withstand environmental stresses ensuring safe, stable and secure structures and pipelines thoughout the lifecycle\nDomain 19: Mechanical Engineering\nDescription 19: Data related to the discipline of engineering that design and produce solutions involving mechanical components that supports the operation of equipments, machineries, structures, processes and systems\nDomain 20: Electrical Engineering\nDescription 20: Data related to the discipline of engineering that deals with the application of electricity, electronics, and electromagnetism\nDomain 21: Instrument /Control Engineering\nDescription 21: Data related to the discipline of engineering that studies the measurement and control of process variables, as well as the design and implementation of equipments and systems containing these process variables.\nDomain 22: Materials, Corrosion, and Inspection Engineering\nDescription 22: Data related to the discipline of engineering that studies materials and processes related to material deterioration prevention due to electrochemical and chemical reactions, known as corrosion.\nDomain 23: Process Engineering\nDescription 23: Data related to process technology and engineering that supports plant activities to ensure smooth plant operations.\nDomain 24: Physical Asset Management\nDescription 24: Data related to the process of implementing maintenance activities to sustain the integrity and reliability of company's assets, as well as ensuring the governance to realize the asset value throughout its life cycle.\nDomain 25: Research & Technology\nDescription 25: Data related to the process of innovative activities undertaken by the company in the development of new services or products to improve the existing services and products.\nDomain 26: Project Management\nDescription 26: Data related to the process of leading a team to work at a specific time to achieve specific goals and meet success criteria, especially for capital projects.\nDomain 27: Health, Safety, Security & Environment (HSSE)\nDescription 27: Data related to the process of implementing practical methods to protect and safeguard people, assets, reputation, environment and security by adhering to the standards and operating practices outlined in HSSE governing document.\nDomain 28: Internal Audit\nDescription 28: Data related to internal audit, assurance and consulting activities designed to add value and improve organization's operations.\nDomain 29: Risk Management\nDescription 29: Data related to the identification, evaluation, manage and monitor of risks to reduce the likelihood on impact towards business, financial exposures, operational matters and reputation of the organization.\nDomain 30: Facilities Management\nDescription 30: Data related to the facility services to ensure the functionality, comfort, safety and efficiency are well managed (buildings and sites, infrastructure and real estate).\nDomain 31: Maritime\nDescription 31: Data related to the maritime affairs covering any business activity or object relating to the sea, particularly where it is in connection with marine trading or naval matters.\n\nInput 1: WAH-Working At Heights Procedure, Work at height\nOutput 1: Domain 27: Health, Safety, Security & Environment (HSSE)\n\nInput 2: Trade Facilities, Applicant\nOutput 2: Domain 5: Procurement\n\n
        """ + raw_text,
        temperature=0,
        max_tokens=2500,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0,
        # best_of=1,
        stop=None
    )
    
    choices = response['choices']
    if choices:
        # Extract the domain label from each choice and append to the output list
        for choice in choices:
            output_text = choice['text'].strip()
            # domain_label = output_text.split(": ", 1)[-1].strip()
            domain_label = output_text.split(":")[-1].strip()
            output.append(domain_label)
    
    return output

# COMMAND ----------

matchers = ['.parquet']
# Define the latest folder path
latest_parquet_folder = next(get_dir_content(raw_path, matchers), None)
# Create a list of Parquet files in the latest folder
parquet_files = [latest_parquet_folder]
print("list of parquet files:", parquet_files)
# Call the load_parquet_files function to load the Parquet files
df_input = load_parquet_files(parquet_files)
# Need to define either to use business term or business term definition with title
df_input = df_input.withColumn("Data_Source", lit('MyGovernance')).withColumn('combined', concat_ws(', ', col('Title'), col('Business_Terms_Definitions')))
display(df_input)

# COMMAND ----------

df_input.count()

# COMMAND ----------

result_list = convert_column_to_list(df_input)
print(result_list)

# COMMAND ----------

len(result_list)

# COMMAND ----------

output = []

batch_size = 100  # You can choose your batch size
num_batches = (len(result_list) + batch_size - 1) // batch_size

for batch_index in range(num_batches):
    start_index = batch_index * batch_size
    end_index = min((batch_index + 1) * batch_size, len(result_list))  # Ensure the end_index doesn't go beyond the list length

    batch_result_list = result_list[start_index:end_index]

    batch_output = []
    for i, result_text in enumerate(batch_result_list):
        if result_text:
            result = extract_data_domain(result_text)
            if result is not None:
                batch_output.extend(result)
            else:
                batch_output.append(result)

    output.extend(batch_output)

output_list = [item for pair in zip(result_list, output) for item in pair]
# Print the input and output list
print("output_list:", output_list)

# COMMAND ----------

df_output = spark.createDataFrame(zip(result_list, output), ["input", "output"])
df_output = df.withColumn("input", F.regexp_replace("input", r"Input \d+: ", ""))
display(df_output)

# COMMAND ----------

# Join the DataFrames based on partial match in the "Title" column of df2
df_joined = df_input.join(df_output, df_input["combined"] == df_output["input"], "inner")
columns_to_drop = ["combined", "input"]
df_joined = df_joined.drop(*columns_to_drop).withColumnRenamed("output","Data_Domain")
display(df_joined)

# COMMAND ----------

# Get the current date in the desired format
current_date = datetime.datetime.now().strftime("%d/%m/%Y")
# Define the target directory path including the date
target_directory_path = final_output_path + current_date
# Save the DataFrame to the specified directory
df_joined.repartition(1).write.parquet(path=target_directory_path, mode="append")

# COMMAND ----------


