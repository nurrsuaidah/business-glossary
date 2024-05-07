# Databricks notebook source
# MAGIC %md
# MAGIC ## Introduction
# MAGIC
# MAGIC This notebook is to extract the key attributes from PDF, output of the extraction would be in JSON and will be stored into EKIH ADLS

# COMMAND ----------

# MAGIC %pip install pycryptodome==3.15.0
# MAGIC %pip install PyPDF2
# MAGIC %pip install azure-ai-formrecognizer==3.3.2
# MAGIC %pip install mysql-connector-python
# MAGIC %pip install pymysql
# MAGIC %pip install sqlalchemy
# MAGIC %pip install pandas
# MAGIC %pip install tqdm
# MAGIC %pip install azure-identity

# COMMAND ----------

import shutil
import os
import re
import time
import datetime
import json
import concurrent.futures
from PyPDF2 import PdfReader
from PyPDF2 import PdfWriter
from PyPDF2 import PdfFileWriter, PdfFileReader
from collections import OrderedDict
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions, generate_account_sas, BlobClient
from azure.storage.filedatalake import DataLakeFileClient, ContentSettings, DataLakeServiceClient
from azure.storage.filedatalake import DataLakeServiceClient, DataLakeFileClient, ContentSettings

# COMMAND ----------

scope_name = os.getenv("SECRET_SCOPE")
storage_acc = dbutils.secrets.get(scope=scope_name, key=os.getenv("AKV_ACC_NAME"))
account_url = "https://{0}.dfs.core.windows.net/".format(storage_acc)
app_id = dbutils.secrets.get(scope=scope_name, key=os.getenv("AKV_SPN_APP_ID"))
client_secret = dbutils.secrets.get(scope=scope_name, key=os.getenv("AKV_SPN_SECRET"))
tenant = dbutils.secrets.get(scope=scope_name, key=os.getenv("AKV_SPN_TENANT_ID"))

# Configure ADLS connection 
spark.conf.set("fs.azure.account.auth.type.{0}.dfs.core.windows.net".format(storage_acc), "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.{0}.dfs.core.windows.net".format(storage_acc), "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.{0}.dfs.core.windows.net".format(storage_acc),app_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.{0}.dfs.core.windows.net".format(storage_acc), client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.{0}.dfs.core.windows.net".format(storage_acc), "https://login.microsoftonline.com/{0}/oauth2/token".format(tenant))

container = 'enigma'

# token credential for ADLS
token_credential = ClientSecretCredential(
    tenant_id = tenant,
    client_id = app_id,
    client_secret = client_secret
)

# Source data of the EBG
input_directory = "POC/Raw/pdf_test/"
output_directory = "POC/Process/Extracted_Output/Raw-1/"

# COMMAND ----------

endpoint = "https://ptsg5edhformrecog01.cognitiveservices.azure.com/"
key = "2fa44aedc42d48b38e1d6a5430dd3042"
credential = AzureKeyCredential(key)
document_analysis_client = DocumentAnalysisClient(endpoint, credential)

# COMMAND ----------

def get_blob(container,path,docFormat):
    service_client = DataLakeServiceClient(account_url = account_url, credential=token_credential)
    file_system_client = service_client.get_file_system_client(file_system=container) 
    paths = file_system_client.get_paths(path)   
    files = [i.name for i in paths]
    file_list = []
    for i in files:
        if docFormat.upper() in i.upper():
            file_list.append(i)
    return file_list

# COMMAND ----------

def extract_image_text_FormRecognizer_Prebuilt(model,file_list,type='content'):
    try:
        for file_name in file_list:
            source_client = DataLakeFileClient(account_url=account_url, file_system_name=container, file_path=file_name, credential=token_credential)
            streamdownloader = source_client.download_file()
            pdf_bytes = streamdownloader.readall()
            poller = document_analysis_client.begin_analyze_document(model, pdf_bytes)
            result = poller.result()

            if type == 'text':
                result = result.content
    except Exception as e:
        print(f"Error processing {file_name}: {e}") # Handle exceptions here
    return result

# COMMAND ----------

def get_key_value_pair_from_page(filename,Page_Name,result_content,model):
    dict = {}
    dict['FileName'] = filename.split('/')[-1]
    dict['Source'] = filename
    dict['Timestamp'] = datetime.datetime.now().isoformat()
    dict['PageName'] = list(Page_Name[0].keys())[0]
    dict['MLModel'] = model
    dict['Datasource'] = 'SharePoint'
    dict_page = []
    print("I am in key val")
    for idx, document in enumerate(result_content.documents):
        outer_dict = {}
        for name, field in document.fields.items():
            inner_dict = {}
            if field.value_type != "list":
                field_value = field.value if field.value else field.content
                if field_value is None:
                    if name[2:].replace('_', ' ').lower() in result_content.content.lower():
                        field_value = ''
                inner_dict["FieldValue"] = field_value
                inner_dict["Confidence"] = field.confidence
                outer_dict[name] = inner_dict
            elif field.value_type == 'list':
                field_value_content = []
                confidence_table = ''
                columns = []
                rows = []
                confidences = []
                for item in field.value:
                    table = item.value
                    if len(columns)==0:
                        keys = [key for key in table.keys()]
                        columns.extend(keys)
                    row_value = [key_value.value for key_value in table.values()]
                    rows.append(row_value)
                for values in rows:
                    field_value_table = {}
                    nested_dict = {}
                    mapped_dict = {k: v for k, v in (zip(columns,values))}
                    for k, v in mapped_dict.items():
                        nested_dict[k] = {}
                        nested_dict[k]['FieldValue'] = v
                    field_value_table['RowContent'] = nested_dict
                    field_value_content.append(field_value_table)
                inner_dict['FieldContent'] = field_value_content
                if 'Table' not in name:
                    name += ' Table'
                outer_dict[name] = inner_dict
        dict['Field'] = [outer_dict]
            
    return dict

# COMMAND ----------

def upload_json(json_output,json_object):
    fileClient = DataLakeFileClient(account_url=account_url, file_system_name=container, file_path=json_output, credential=token_credential)
    fileClient.upload_data(json_object, overwrite=True)
    print("JSON output has been successfully saved into ADLS:", json_output)

# COMMAND ----------

def pdf_to_json(output_dir,input_dir,file_list):
    Pages_to_be_process = [{'EBG_OEMS_MY_3G':['EBG_OEMS_MY_3G']}]
    for file_name in file_list:
        try:
            s1 = file_name.split('/')
            file_name = s1[-1]
            file = os.path.join(input_dir, file_name)
            result = extract_image_text_FormRecognizer_Prebuilt('EBG_OEMS_MY_3G', file_list, type='content')
            ans = get_key_value_pair_from_page(file, Pages_to_be_process, result, model='EBG_OEMS_MY_3G')
            time_fields2 = round(time.time())
            json_file = str(time_fields2) + '_' + file.split('/')[-1].split('.pdf')[0] + '.json'
            json_output = os.path.join(output_dir, json_file)
            json_object = json.dumps(ans, indent = 4)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(upload_json, json_output, json_object)
        except Exception as e:
            print(f"Error processing {file_name}: {e}")

# COMMAND ----------

docFormat = 'PDF'
file_list = get_blob(container,input_directory,docFormat)
pdf_to_json(output_directory,input_directory,file_list)

# COMMAND ----------


