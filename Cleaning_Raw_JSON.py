# Databricks notebook source
# MAGIC %md
# MAGIC ####Import Libraries

# COMMAND ----------

import re
import os
import pyodbc
import pandas as pd
import numpy as np
import pyspark
from pyspark.sql.functions import col, concat_ws, map_values, flatten, monotonically_increasing_id, udf, when, lit, explode, length, explode_outer, count, lag, to_date, regexp_extract, regexp_replace
from pyspark.sql.types import StringType, BooleanType, DateType, DoubleType, DecimalType, ArrayType, StructType, StructField, MapType
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ####Connection & Configuration to ADLS

# COMMAND ----------

container_process = 'process'
project = 'EBG'

directoryPath_processQ5 = 'AZ_ADLS_PR_SP/SP_MYGOVERNANCE/V01/EBG/JSON/RAW_JSON/2023/08/02/' #L2L3 Documents

urlQ5 = f"abfss://{container_process}@{storageAccountName}.dfs.core.windows.net/{directoryPath_processQ5}"
# urlQ6 = f"abfss://{container_process}@{storageAccountName}.dfs.core.windows.net/{directoryPath_processQ6}"

mount_pointQ5 = f"/mnt/{project}/{container_process}/{directoryPath_processQ5}"
# mount_pointQ6 = f"/mnt/{project}/{container_process}/{directoryPath_processQ6}"

# COMMAND ----------

print("========= ADLS Gen2 Connection Details ==========")
print(f"container_name = {container_process}")
print(f"storage_account_name = {storageAccountName}")
print(f"config = {configs}")

print(f"folder_name_Q5 = {mount_pointQ5}")
print("=================================================")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Function Path within Directory

# COMMAND ----------

def get_dir_content(ls_path):
    for dir_path in dbutils.fs.ls(ls_path):
        if dir_path.isFile():
            yield dir_path.path
        elif dir_path.isDir() and ls_path != dir_path.path:
            yield from get_dir_content(dir_path.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ####File List

# COMMAND ----------

listQ5 = get_dir_content(urlQ5)
# listQ6 = get_dir_content(urlQ6)

matchers = ['.json']

file_listQ5 = [s for s in listQ5 if any(xs in s for xs in matchers)]
# file_listQ6 = [s for s in listQ6 if any(xs in s for xs in matchers)]

# COMMAND ----------

length_Q5 = len(file_listQ5)
# length_Q6 = len(file_listQ6)

combined_length = length_Q5 # + length_Q5  + 

print("Length of Q5:", length_Q5)
# print("Length of Q6:", length_Q6)
print("Combined length:", combined_length)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1) Extract Fields from JSON

# COMMAND ----------

#For Quarter 5 - 2nd Ingestion

raw_df_Q5 = spark.sparkContext.emptyRDD()

raw_df_Q5 = spark.read.option("multiline", "true").json(file_listQ5)

raw_df_Q5.cache()

raw_df_Q5 = raw_df_Q5.select(col("FileName").alias("Ref_No"),
                   col("Field.Abbreviation_Table.FieldContent.RowContent.Abbreviation.FieldValue").alias
                   ("Business_Terms"),
                   col("Field.Abbreviation_Table.FieldContent.RowContent.Description.FieldValue").alias
                   ("Business_Terms_Definitions")
               )
display(raw_df_Q5)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2) Fields Extracted : Ref_No -- Business_Terms -- Business_Terms_Definitions

# COMMAND ----------

# 1st pair : Ref_No -- Business Terms & Defintions Table -- Business Terms & Defintions Table

# explodedDF_Q5 = raw_df_Q5.select(explode(raw_df_Q5.Ref_No).alias("Ref_No"),"Business_Terms","Business_Terms_Definitions")
explodedDF_Q5 = raw_df_Q5.select("Ref_No",explode(raw_df_Q5.Business_Terms).alias("Business_Terms"),"Business_Terms_Definitions")
explodedDF_Q5 = explodedDF_Q5.select("Ref_No","Business_Terms",explode(raw_df_Q5.Business_Terms_Definitions).alias("Business_Terms_Definitions"))
# display(explodedDF_Q5)

explodedBusiness_Terms_DF_Q5 = explodedDF_Q5.select("Ref_No",explode(explodedDF_Q5.Business_Terms).alias("Business_Terms"))
explodedFullDF_Q5 = explodedDF_Q5.select(explode(explodedDF_Q5.Business_Terms_Definitions).alias("Business_Terms_Definitions"))
# display(explodedFullDF_Q5)

df_Q5 = explodedBusiness_Terms_DF_Q5.withColumn("id", monotonically_increasing_id()).join(explodedFullDF_Q5.withColumn("id", monotonically_increasing_id()),"id").select("Ref_No","Business_Terms", "Business_Terms_Definitions")

display(df_Q5)

# COMMAND ----------

df_count = df_Q5.count()
df_count

# COMMAND ----------

# MAGIC %md
# MAGIC ####3) Remove rows that do not have a value in the 'Ref_No' column

# COMMAND ----------

df = df_Q5.filter((df_Q5.Ref_No.isNotNull()) & (df_Q5.Ref_No != ""))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####4) Remove unnecessary values in the 'Ref_No' column

# COMMAND ----------

# Extract the desired part of the string using regexp_extract
# 0 will include everything including what you've specified, while 1 will only take in the condition that you've specified
df1 = df.withColumn("Ref_No_Modified", regexp_extract(col("Ref_No"), r"Document_(.*?)(?:REV|0_)", 1),)
df2 = df1.select("Ref_No_Modified","Ref_No", "Business_Terms", "Business_Terms_Definitions")
df2 = df2.drop("Ref_No").withColumnRenamed("Ref_No_Modified", "Ref_No")
display(df2)

# COMMAND ----------

excluded_values = ["PTS","MY SKG X X M 07 2004 I","MY PMA DYG DYGCPP P L3 0000019184 I","MY SKG PC4 PC4DP-A P 05 2117 I","MY PMA X X M 06 2680 I","MY PMA SP X P 05 2676 I","MY SKG PC4 PC4DP-A P 05 2115 I"]
values_to_delete = ['JCP-manual-signed-ver0-april-202','7-3-19-0007_PECAS Implementation Manual V4.','7-13-20-0012_NET Multi-Tasked (MT) Implementation Manual V1.','SMART-manual-signed-ver0-may-202']

df3 = df2.filter(
    (~col("Ref_No").isin(values_to_delete) | col("Ref_No").isNull()) &
    (~col("Ref_No").isin(excluded_values))
)

display(df3)

# COMMAND ----------

#Double check the unnecessary value in Ref_No column
duplicates_df1 = df3.groupBy("Ref_No").agg(count("*").alias("count")).filter(col("count") > 0)

#Sort based on count
duplicates_df1 = duplicates_df1.sort(col('count').desc())

# Show the duplicate values and their counts
display(duplicates_df1)

# COMMAND ----------

df_count = df3.count()
df_count

# COMMAND ----------

# MAGIC %md
# MAGIC ####5) Extract Business_Terms in the Business_Terms_Definitions column

# COMMAND ----------

df4 = df3.withColumn("Business_Terms", when(col("Business_Terms").isNull(), regexp_extract(col("Business_Terms_Definitions"), r'\b[A-Z][A-Z]+(-[A-Z]+)?|([A-Z]\.[A-Z]\.[A-Z]\.[A-Z]\.[A-Z])|([A-Z]\.[A-Z]\.[A-Z])|\b[A-Z0-9-]+|^(.*?)\s+(?=A positive choke)|\w+\s*\w+\s*\([^)]+\)', 0)).otherwise(col("Business_Terms")))

# Show the modified DataFrame
df4 = df4.withColumn('Business_Terms', F.when(df4['Business_Terms'] == 'null', None).otherwise(df4['Business_Terms']))
display(df4)

# COMMAND ----------

# MAGIC %md
# MAGIC ####6a) Checking unnecessary values from Business_Terms column

# COMMAND ----------

# Filter and count duplicate values in the "Business_Terms" column
duplicates_df2 = df4.groupBy("Business_Terms").agg(count("*").alias("count")).filter(col("count") > 0)

#Sort based on count
duplicates_df2 = duplicates_df2.sort(col('count').desc())

# Show the duplicate values and their counts
display(duplicates_df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####6b) Remove row that have unneccesary values in Business_Terms column

# COMMAND ----------

values_to_delete_terms = ['3.2. CHECKLIST','1','3.2','8', 'Table 1', '3', '2', 'TABLE 1:', 'ABBREVIATIONS: CDOF : CFRB :', 'Description', 'Table 5 :', 'Table 8 :', 'Table 4 :', 'serious consequences (e.g. failure of access control systems)', 'Table 1:', 'Table 6 :', 'Table 7 :', 'Table 2 :', 'Table 3 :', 'No', '0 Calm', 'Scale Description', 'Figure 1:', 'DAR ESD 3.2', '1 Light air', 'Appendix', 'PPE 3.2', '3.2.1. Table 1 :', '50mm Manual Valve', 'No.', 'O Operation', 'M', 'R', 'N', 'V Visual', 'L Lubricate', '3.2.1. Table 1:', 'MY SBO O 05 093, Rev.1 Feb 2013', '3.2. CHECKLIST 300-VB-42011 / 300-VB-42012', 'Valve No', '20-VB-42007 /', '20-VG-42008', 'PG-71001/72001', 'ABV-7101/7201', 'ABV-7102/7202', 'D-71021/72021', 'V-71001/2 & V-72001/2', 'VALVE NO.', 'PD-1001', 'PD-1002', '7. REFERENCES','Figure 1','Figure 2','Figure 3','Figure 4','Figure 1:','What happened?','INFORMATION','PROCESS SAFETY','Emergency OP','Hazards and Effect Register','Statement of Fitness in HSE','PL-0001','8.5.','8.6.','8.4.','8.3.','50VB-','Tag No.','1301','MOV-0402','80VB-5206','Valve Tag No','ITEM','320-11-10015-12480K','0','ITEM NO','APPENDIX 1:','3.1 CHECKLIST','5.0','Appendix A','3.1','1. INTRODUCTION','200-VB-11002','1.4 Distribution','1.7','3.8','250-VB- 04084','200-VB-','250-VB-11004/ 250-VB-11005','1.6','3.4','1.8','3.3','2. DEFINITIONS','SDV-1011','3.5','4.1 Record','BDV-1010','11003','100-VB-04086/ 100-VB-04087','SDV-1010','1.3','3. INSPECTION PLAN','3.6','1.5','100-VB- 04085','63018','2.2','200-VB-11001/','3.7','1.1','150-VB-63015/ 150-VB-63016/','150-VB-63017','3.2.','2.1','1.2','4.','REFERENCE DOCUMENTS','80-VB-','80mm Manual Valve','50mm Manual Valve','3.2. CHECKLIST 80-VB-55051','80-VB-55063','LANGUAGE CONVENTION','6.1.','3.0 SCOPE','6.3. Site Visit Agenda','5.0 METHODOLOGY','6.0','7.0 APPENDICES','2.0 OBJECTIVES','4.0','6.4.','7.1.1.','7.1.','6.2. Team Composition','1.0 INTRODUCTION','UG-0402 PCV- 004 inlet','PW-0001/0022','PW-0002/0023','3.2. CHECKLIST 3.2.1. Table 1 :','CD-69001/2','UA-69001','UA-69101','ABV-6901','TAG NO.','REFERENCED DOCUMENTS','Internal 28/05/2018','Location:','5-1','2.3.2.','2.3.1.','DOC. NO','PSV-6830 A/B','OIL','24/05/2018']

df5 = df4.filter(~df4['Business_Terms'].isin(values_to_delete_terms) | df4['Business_Terms'].isNull())
df5 = df4.filter(~col("Business_Terms").rlike(r'^\d'))
df5 = df5.withColumn("Abbreviation", lit(None).cast(StringType()))

display(df5)

# COMMAND ----------

# MAGIC %md
# MAGIC ####6c) Checking if still have unncessary values in Business_Terms column

# COMMAND ----------

# Filter and count duplicate values in the "Business_Terms" column
duplicates_df3 = df5.groupBy("Business_Terms").agg(count("*").alias("count")).filter(col("count") > 0)

#Sort based on count
duplicates_df3 = duplicates_df3.sort(col('count').desc())

# Show the duplicate values and their counts
display(duplicates_df3)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Save Final DF

# COMMAND ----------

# Define the folder path
directoryPath_outputQ5 = "abfss://{0}@{1}.dfs.core.windows.net/AZ_ADLS_PR_SP/SP_MYGOVERNANCE/V01/EBG/CSV/PROCESS_CSV/2023/08/11/".format(container_process,storageAccountName)

#Create the full file path
output_path = directoryPath_outputQ5 + 'Q5'
print(f"Target folder -> {output_path}")

# COMMAND ----------

df5.repartition(1).write.parquet(output_path, mode="overwrite")

# COMMAND ----------


