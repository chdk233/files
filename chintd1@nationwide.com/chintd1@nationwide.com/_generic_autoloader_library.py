# Databricks notebook source
# DBTITLE 1,Import Functions
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import *
import datetime
import io
import re
import json
from itertools import groupby
from operator import itemgetter
import io
import zipfile
import pandas as pd
from pyspark.sql.types import *
import datetime
import json
import boto3
import time
import ast
from datetime import datetime as dt
from pyspark.sql import functions as f
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame
import datetime


# COMMAND ----------

def get_configs(config_table_name,job_cd):
  try:
    sqlstring = f"select lower(JOB_CD) as JOB_CD,JOB_NAME,CONFIGNAME,CONFIGVALUE from {config_table_name} where JOB_CD in({job_cd})"
    dfconfig = spark.sql(sqlstring)
    return dfconfig
  except Exception as e:
    exception_type, exception_object, exception_traceback = sys.exc_info()
    print(f"Function get_configs - error {str(exception_object)} occured at line number {exception_traceback.tb_lineno} while reading table {config_table_name}.")
    raise Exception(e)

def job_cd_config_check(dataframe,job_code):
  try:
    error_job_cd_list=[]
    error_value_list=[]
    error_message=''
    not_support_filetype=[]
    for job_cd in job_code.split(','):
      mandatory_fields=['sns_arn','out_of_scope_files_err_table','error_table','file_name']
      compressed_fields=['all_files_validation']
      uncompressed_fields=['raw_table','stage_table','schema_stage_table']
      json_fields=['schema_json']
      if dataframe.filter(col("JOB_CD")==job_cd.strip(" ")).count() ==0: 
        error_job_cd_list.append(job_cd)
        continue
      try:
        file_name=dataframe.filter((col("JOB_CD")==job_cd)&(lower(col("CONFIGNAME"))=="file_name")).first()['CONFIGVALUE'].split('.')[-1].strip(' ').lower() 
#         print(file_name)
      except:
        error_job_cd_list.append(job_cd+':file_name')
        continue
      if file_name not in ['csv','dat','tar','zip','json','txt','cntrl']:
        not_support_filetype.append(file_name)
      for field in mandatory_fields:
        if dataframe.filter((col("JOB_CD")==job_cd.strip(" ")) & (lower(col("CONFIGNAME"))==field)).count()!=1:
          error_value_list.append(job_cd+':'+field)
      if file_name == 'json':
        for field in json_fields:
          if dataframe.filter((col("JOB_CD")==job_cd.strip(" ")) & (lower(col("CONFIGNAME"))==field)).count()!=1:
            error_value_list.append(job_cd+':'+field)
        raw_table_names=dataframe.filter((col("JOB_CD")==job_cd.strip(" "))&(lower(col("CONFIGNAME")).rlike("raw_table.*".lower()))).select("CONFIGVALUE").collect()
        for raw_tb_nm in raw_table_names:
          raw_table=raw_tb_nm['CONFIGVALUE'].strip(' ').lower()
          if dataframe.filter((col("JOB_CD")==job_cd.strip(" ")) & (lower(col("CONFIGNAME"))==f"source_element_to_{raw_table.split('.')[-1]}")).count()!=1:
            error_value_list.append(job_cd+':'+f"source_element_to_{raw_table.split('.')[-1]}")
      if file_name != 'tar' and file_name != 'zip': 
        for field in uncompressed_fields: 
          if field == 'raw_table':
            raw_table_names=dataframe.filter((col("JOB_CD")==job_cd.strip(" "))&(lower(col("CONFIGNAME")).rlike("raw_table.*".lower()))).select("CONFIGVALUE").collect()
            for raw_tb_nm in raw_table_names:
              raw_table=raw_tb_nm['CONFIGVALUE'].strip(' ').lower()
              if dataframe.filter((col("JOB_CD")==job_cd.strip(" ")) & (lower(col("CONFIGNAME"))==f"schema_raw_{raw_table.split('.')[-1]}")).count()!=1:
                error_value_list.append(job_cd+':'+f"schema_raw_{raw_table.split('.')[-1]}")
            if len(raw_table_names)==0:
              error_value_list.append(job_cd+':'+'raw_table')
      if file_name == 'tar' or file_name == 'zip':
        for field in compressed_fields:
          if dataframe.filter((col("JOB_CD")==job_cd.strip(" ")) & (lower(col("CONFIGNAME"))==field)).count()!=1:
            error_value_list.append(job_cd+':'+field)
    if len(error_job_cd_list) !=0:
      error_message=f"Please add the records in the config table for the JOB_CD {','.join(error_job_cd_list)}"
    if len(error_value_list) !=0:
      error_message+=f" Please add the required records {','.join(error_value_list)} in the config table"
    if len(not_support_filetype) !=0:
      error_message+=f" Generic Autoloader dont support the following file types  {','.join(not_support_filetype)}"
    if len(error_job_cd_list) !=0 or len(error_value_list) !=0 :
        raise Exception(f"{error_message}") 
  except Exception as e:
    exception_type, exception_object, exception_traceback = sys.exc_info()
    print(f"Function job_cd_config_check - error {str(exception_object)} occured at line number {exception_traceback.tb_lineno} while Validating the Config Table Records.")
    raise Exception(e)

def GenericAutoloaderInputSTream(
     sourcepath: str,
     fileformat: str,
     maxFiletrigger: int,
     GlobFilter: str = None,
     recursive: bool = True,
     queueURL: str = None,
     region: str = "us-east-1",
     notifications: bool = True,
     inclFiles: bool = False,
     valoptions: bool = True     
) -> DataFrame:
    
      dfFileStream = spark.readStream.format("cloudFiles")\
      .option( "pathGlobFilter", GlobFilter)\
      .option( "recursiveFileLookup", recursive)\
      .option( "cloudFiles.format", fileformat)\
      .option( "cloudFiles.maxFilesPerTrigger", maxFiletrigger)\
      .option( "cloudFiles.queueUrl",queueURL)\
      .option( "cloudFiles.region", region)\
      .option("cloudFiles.useNotifications", notifications)\
      .option( "cloudFiles.includeExistingFiles", inclFiles)\
      .option( "cloudFiles.validateOptions", valoptions)\
      .load(sourcepath)
      return dfFileStream
    
    
def GenericAutoloaderOutputSTream(read_stream,check_point_location,micro_batch,run_type):
  if run_type.lower()=='realtime':
    read_stream\
    .writeStream\
    .queryName("read_stream")\
    .option("checkpointLocation",check_point_location)\
    .foreachBatch(micro_batch)\
    .start()
  else:
    read_stream\
    .writeStream\
    .trigger(once=True)\
    .queryName("read_stream")\
    .option("checkpointLocation",check_point_location)\
    .foreachBatch(micro_batch)\
    .start()


# COMMAND ----------

class ErrorCC:
   def __init__(self,batchid, batchstarttime,audittype, error_type,error_notification,load_date,load_hour):
      self.batch_id=batchid
      self.batch_start_time=batchstarttime
      self.audit_type=audittype
      self.error_type=error_type
      self.error_notification=error_notification
      self.load_date=load_date
      self.load_hour=load_hour

class LogCC:
   def __init__(self,batch_id,job_cd,batch_start_time,read_files_count,error_files_count,raw_count):
      self.batch_id=batch_id
      self.job_cd=job_cd
      self.batch_start_time=batch_start_time
      self.read_files_count=read_files_count
#       self.jobid=jobid
#       self.runid=runid
#       self.clusterid=clusterid
      self.error_files_count=error_files_count
      self.raw_count=raw_count
  
def log_table_load(logcc):
  try:
#   log_table=job_configDF.filter((col("JOB_CD")==logcc.job_cd)&(lower(col("CONFIGNAME"))=="log_table")).first()['CONFIGVALUE']
    proccessed_files_count=logcc.read_files_count-logcc.error_files_count
    if logcc.read_files_count==0:
      status='failed'
    elif logcc.read_files_count ==proccessed_files_count:
      status='success'
    else:
      status='partially success'
    log_df.withColumn('batchid',lit(logcc.batch_id))\
      .withColumn('job_code',lit(logcc.job_cd))\
      .withColumn('status',lit(status))\
      .withColumn('batchstarttime',lit(str(logcc.batch_start_time)))\
      .withColumn('batchendtime',lit(str(datetime.datetime.now())))\
      .withColumn('totalbatchtimemin',lit(str(((datetime.datetime.now()-logcc.batch_start_time).total_seconds())/60)))\
      .withColumn('readfilescount',lit(logcc.read_files_count))\
      .withColumn('processedfilescount',lit(proccessed_files_count))\
      .withColumn('rawcount',lit(logcc.raw_count))\
      .withColumn('jobid',lit(str(jobId)))\
      .withColumn('runid',lit(str(runId)))\
      .withColumn('clusterid',lit(str(clusterId)))\
      .withColumn("db_load_time",current_timestamp())\
      .withColumn("db_load_date",current_date())\
      .selectExpr("cast(batchid as long) batchid",'job_code','status','batchstarttime','batchendtime','totalbatchtimemin',"cast(readfilescount as long) readfilescount",'jobid','runid','clusterid',"cast(processedfilescount as long) processedfilescount",'db_load_time','db_load_date',"cast(rawcount as long) rawcount")\
      .coalesce(1).write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("db_load_date").saveAsTable(log_table)
#     log_df.show(truncate=False)
  except Exception as e:
    exception_type, exception_object, exception_traceback = sys.exc_info()
    print(f"Function log_table_load - error {str(exception_object)} occured at line number {exception_traceback.tb_lineno} while loading stats log table")
    raise Exception(e)
      
def send_mail(sns_arn,error_count,subject,message):
  try:
    aws_accountid='786994105833'
    sts_client = boto3.client("sts",region_name='us-east-1',endpoint_url ='https://sts.us-east-1.amazonaws.com')
    response2 = sts_client.assume_role(RoleArn=f'arn:aws:iam::{aws_accountid}:role/dw-pl-cmt-TelematicS3FileMove-role',RoleSessionName='myDhfsession')
    #sns_client = boto3.Session(region_name='us-east-1').client("sns")
    sns_client = boto3.Session(region_name='us-east-1').client("sns",
                            aws_access_key_id=response2.get('Credentials').get('AccessKeyId'),
                            aws_secret_access_key=response2.get('Credentials').get('SecretAccessKey'),
                            aws_session_token=response2.get('Credentials').get('SessionToken'))
#     sts_client = boto3.client("sts")
#     sns_client = boto3.Session(region_name='us-east-1').client("sns")
    topic_arn=sns_arn
    if error_count>0:
        sns_client.publish(
                  TopicArn = topic_arn,
                  Message = message,
                  Subject = subject)
        print(f"mail sent {subject} due to {error_count} error records")
  except Exception as e:
    exception_type, exception_object, exception_traceback = sys.exc_info()
    print(f"Function send_mail - error {str(exception_object)} occured at line number {exception_traceback.tb_lineno} while Sending Mail Notification.")
    raise Exception(e)
      
def logToErrorTable(invalidDF,job_code,error_config_name,errorcc):
  try:
    config_record=job_configDF.filter((col("JOB_CD")==job_code.split(',')[-1]) & (lower(col("CONFIGNAME"))==error_config_name.lower())).first()
    error_table_name=config_record['CONFIGVALUE']
    try:
      invalidDF.write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("LOAD_DATE").saveAsTable(error_table_name)
    except:
      invalidDF.drop('FILE_DATA').withColumn('FILE_DATA',lit('Unsupported data - Used different file pattern binary decoding')).write.format("delta").mode("append").option("mergeSchema", "true").partitionBy("LOAD_DATE").saveAsTable(error_table_name)
    if errorcc.error_notification=='Yes':
      job_sns_arn=job_configDF.filter((col("JOB_CD")==job_code.split(',')[-1]) & (lower(col("CONFIGNAME"))=='sns_arn')).first()['CONFIGVALUE']
      run_time=datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
      job_name=invalidDF.select('JOB_NAME').first()['JOB_NAME']
#       invalidDF.show(truncate=False)
      try:
        error_count=invalidDF.first()['ERROR_COUNT']
      except:
        error_count=invalidDF.count()
      send_mail(job_sns_arn,error_count,f"{job_name} {errorcc.audit_type} Failure",f"Hi Team,\n\n {job_name} - {errorcc.audit_type} Check Failed with {error_count} records on {run_time},please check. \n \n Regards,\nSupport Team")
  except Exception as e:
    exception_type, exception_object, exception_traceback = sys.exc_info()
    print(f"Function logToErrorTable - error {str(exception_object)} occured at line number {exception_traceback.tb_lineno} while loading error  table")
    raise Exception(e)

def zero_byte_check(dataframe,job_code,errorcc):
  try:
    zero_byte_file_count=dataframe.count()
    job_name=job_configDF.filter((col("JOB_CD")==job_code.split(',')[-1]) & (lower(col("CONFIGNAME"))=="error_table")).first()['JOB_NAME']
    if zero_byte_file_count !=0:
      error_DF=dataframe.withColumn("JOB_CD",lit(job_code))\
      .withColumn("JOB_NAME",lit(job_name))\
	  .withColumn("Error_Cause",lit("Received Zero Byte File"))\
      .withColumn("BATCH_ID",lit(errorcc.batch_id))\
      .withColumn("BATCH_START_TIME",lit(errorcc.batch_start_time))\
      .withColumn("LOAD_DATE",lit(errorcc.load_date))\
      .withColumn("LOAD_HOUR",lit(errorcc.load_hour))\
      .withColumn("ERROR_COUNT",lit(1))\
	  .selectExpr("cast(BATCH_ID as long) BATCH_ID","BATCH_START_TIME","JOB_CD","JOB_NAME","path as FILE_NAME","cast(ERROR_COUNT as long) ERROR_COUNT","Error_Cause","LOAD_DATE","LOAD_HOUR")
#       error_DF.show()
      logToErrorTable(error_DF,job_code,'error_table',errorcc)
  except Exception as e:
    exception_type, exception_object, exception_traceback = sys.exc_info()
    print(f"Function zero_byte_check - error {str(exception_object)} occured at line number {exception_traceback.tb_lineno}.")
    raise Exception(e)
    
def schema_check(config_df,job_code,errorcc):
  try:
    job_code_list=job_code.split(',')
    error_list=[]
    for job_cd in job_code_list:
      job_name=config_df.filter(col("JOB_CD")==job_cd).first()['JOB_NAME']
      raw_table_name=config_df.filter((col("JOB_CD")==job_cd) & (lower(col("CONFIGNAME")).rlike("raw_table.*"))).collect()
      file=config_df.filter((col("JOB_CD")==job_cd) & (lower(col("CONFIGNAME")).rlike("file_name"))).first()['CONFIGVALUE']
      if '.'.join(file.split('.')[-2:]).strip(' ').lower() == 'tar.gz':
        continue
      for raw_table_nm in raw_table_name:
        raw_config_df=config_df.filter((col("JOB_CD")==job_cd) & (lower(col("CONFIGNAME"))==f"schema_raw_{raw_table_nm.CONFIGVALUE.split('.')[-1].lower()}"))
        stage_config_df=config_df.filter((col("JOB_CD")==job_cd) & (lower(col("CONFIGNAME"))=="schema_stage_table"))
        if raw_config_df.count() ==1:
          raw_config_schema=raw_config_df.first()['CONFIGVALUE']
        else: raw_config_schema=""
        if stage_config_df.count() ==1:
          stage_config_schema=stage_config_df.first()['CONFIGVALUE']
        else: stage_config_schema=""
        if raw_config_schema.strip(" ")=="":
          error_list.append((job_cd,job_name,"Raw Schema Not Found"))
        if stage_config_schema.strip(" ")=="":
          error_list.append((job_cd,job_name,"Stage Schema Not Found"))
        print(error_list)
        if len(error_list) >0:
          rdd=spark.sparkContext.parallelize(error_list)
          error_DF=spark.createDataFrame(rdd).toDF("JOB_CD","JOB_NAME","Error_Cause")\
          .withColumn("BATCH_ID",lit(errorcc.batch_id))\
          .withColumn("BATCH_START_TIME",lit(errorcc.batch_start_time))\
          .withColumn("LOAD_DATE",lit(errorcc.load_date))\
          .withColumn("LOAD_HOUR",lit(errorcc.load_hour))\
	  	.withColumn("FILE_NAME",lit(""))\
          .withColumn("ERROR_COUNT",lit(1))\
	      .selectExpr("cast(BATCH_ID as long) BATCH_ID","BATCH_START_TIME","JOB_CD","JOB_NAME","FILE_NAME","cast(ERROR_COUNT as long) ERROR_COUNT","Error_Cause","LOAD_DATE","LOAD_HOUR")
          logToErrorTable(error_DF,job_code,'error_table',errorcc)
  except Exception as e:
    exception_type, exception_object, exception_traceback = sys.exc_info()
    print(f"Function schema_check - error {str(exception_object)} occured at line number {exception_traceback.tb_lineno}.")
    raise Exception(e)
    
def create_empty_rawDF(job_config_df):
  try:
    schema_list=job_config_df.filter(lower(col('CONFIGNAME')).rlike('schema_raw.*')).select("JOB_CD","CONFIGNAME","CONFIGVALUE").collect()
    global log_df
    log_df=spark.sql("select 1 as sno")
    for value in schema_list:
      print(value['CONFIGVALUE'])
      job_cd_emptyDF=value['CONFIGNAME'].replace('schema_raw_','').lower()+f"_{value['JOB_CD']}_emptyDF"
      job_cd_empty_stringDF=value['CONFIGNAME'].replace('schema_raw_','').lower()+f"_{value['JOB_CD']}_empty_stringDF"
      data=spark.sparkContext.emptyRDD()
      globals()[job_cd_emptyDF]=spark.createDataFrame(data,schema=eval(value['CONFIGVALUE']))
      df=eval(job_cd_emptyDF)
      globals()[job_cd_empty_stringDF]=df.select([col(column).cast('string') for column in df.columns])
  except Exception as e:
    exception_type, exception_object, exception_traceback = sys.exc_info()
    print(f"Function create_empty_rawDF - error {str(exception_object)} occured at line number {exception_traceback.tb_lineno} while creating empty dataframe")
    raise Exception(e)
    

def null_check(data_df,config_df,job_code,errorcc,raw_table_name):
  try:
    try:
      primary_key=config_df.filter((col("JOB_CD")==job_code) & (upper(col("CONFIGNAME"))==f"NULL_CHECK_COLS_{raw_table_name}".upper())).first()["CONFIGVALUE"]
    except:
      primary_key=''
    if primary_key.strip(' ')=='':
      raw_condition="1=1"
    else:
      condition=""
      raw_condition=""
      job_name=config_df.filter(col("JOB_CD")==job_code).first()['JOB_NAME']
      for key in primary_key.split(','):
        stripped_key=key.strip(" ")
        if condition =="":
          condition+=f"(trim({stripped_key}) == '' or {stripped_key} is null)"
          raw_condition+=f"(trim({stripped_key}) != '' and {stripped_key} is not null)"
        else:
          condition+=f" or (trim({stripped_key}) == '' or {stripped_key} is null)"
          raw_condition+=f" and (trim({stripped_key}) != '' and {stripped_key} is not null)"
      print(condition)
      error_DF=data_df.filter(condition).groupBy('path').count()\
            .withColumn("BATCH_ID",lit(errorcc.batch_id))\
            .withColumn("BATCH_START_TIME",lit(errorcc.batch_start_time))\
            .withColumn("LOAD_DATE",lit(errorcc.load_date))\
            .withColumn("JOB_CD",lit(job_code))\
            .withColumn("JOB_NAME",lit(job_name))\
            .withColumn("LOAD_HOUR",lit(errorcc.load_hour))\
            .withColumn("FILE_NAME",col('path'))\
            .withColumn("Error_Cause",lit(f"Null Check Failed for Columns {primary_key} in table {raw_table_name}"))\
	        .selectExpr("cast(BATCH_ID as long) BATCH_ID","BATCH_START_TIME","cast(JOB_CD as string) JOB_CD","JOB_NAME","FILE_NAME","cast(count as long) ERROR_COUNT","Error_Cause","LOAD_DATE","LOAD_HOUR")
      if error_DF.count()!=0:
        logToErrorTable(error_DF,job_code,'error_table',errorcc)
    return raw_condition
  except Exception as e:
    exception_type, exception_object, exception_traceback = sys.exc_info()
    print(f"Function null_check - error {str(exception_object)} occured at line number {exception_traceback.tb_lineno} .")
    raise Exception(e)

def stage_table_load(dataframe,config_df,job_code):
  try:
    try:
      partition=config_df.filter((col("JOB_CD")==job_code) & (lower(col("CONFIGNAME"))==f"partition_stage_table")).first()["CONFIGVALUE"]
    except:
      partition=''
    try:
      audit_col=config_df.filter((col("JOB_CD")==job_code) & (lower(col("CONFIGNAME")).rlike(f"audit_raw_.*")) & (trim(col("CONFIGNAME"))!=''  )).first()["CONFIGVALUE"]
    except:
      audit_col=''
      drop_columns=[]
    if audit_col==None:
      select_cols=['*']
      drop_columns=[]
    elif audit_col.strip(' ')=='':
      select_cols=['*']
      drop_columns=[]
    else:
      df_column_add=[]
      drop_columns=[]
      dataframe_cols=[col.lower().strip(' ') for col in dataframe.columns]
      for a_col in audit_col.split(',::'):
        if a_col.strip (' ') =='':
          continue
        a_col_name=a_col.split('::')[0].strip(' ')
        try:
          a_col_val=eval(a_col.split('::')[-1].strip(' '))
        except:
          a_col_val=a_col.split('::')[-1].strip(' ')
        df_column_add.append((a_col_name,a_col_val))
        if a_col_name.lower() in dataframe_cols:
          drop_columns.append(a_col_name)
        select_cols=['*']+[lit(a_col[1]).alias(a_col[0]) for a_col in df_column_add]
    print(job_code)
    stage_table=config_df.filter((col("JOB_CD")==job_code) & (lower(col("CONFIGNAME"))=="stage_table")).first()["CONFIGVALUE"]
    if partition:
      dataframe.drop(*drop_columns).select(select_cols).drop('path').write.format("delta").partitionBy(partition).option("mergeSchema", "true").mode("append").saveAsTable(stage_table)
    else:
      dataframe.drop(*drop_columns).select(select_cols).drop('path').write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable(stage_table)
    print(f"loaded {dataframe.count()} records into the stage table {stage_table}")
  except Exception as e:
    exception_type, exception_object, exception_traceback = sys.exc_info()
    print(f"Function stage_table_load - error {str(exception_object)} occured at line number {exception_traceback.tb_lineno} while loading stage table.")
    raise Exception(e)
  
def raw_table_load(dataframe,config_df,job_code,raw_table,errorCC):
  try:
    partition=config_df.filter((col("JOB_CD")==job_code) & (lower(col("CONFIGNAME"))==f"partition_raw_{raw_table.split('.')[-1].lower()}")).first()["CONFIGVALUE"]
  except:
    partition=''
  try:
    audit_col=config_df.filter((col("JOB_CD")==job_code) & (lower(col("CONFIGNAME"))==f"audit_raw_{raw_table.split('.')[-1].lower()}")).first()["CONFIGVALUE"]
  except:
    audit_col=''
  try:
    if audit_col.strip(' ')=='':
      select_cols=['*']
      drop_columns=[]
    else:
      df_column_add=[]
      drop_columns=[]
      dataframe_cols=[col.lower().strip(' ') for col in dataframe.columns]
      for a_col in audit_col.split(',::'):
        if a_col.strip (' ') =='':
          continue
        a_col_name=a_col.split('::')[0].strip(' ')
        try:
          a_col_val=eval(a_col.split('::')[-1].strip(' '))
        except:
          a_col_val=a_col.split('::')[-1].strip(' ')
        df_column_add.append((a_col_name,a_col_val))
        if a_col_name.lower() in dataframe_cols:
          drop_columns.append(a_col_name)
        select_cols=['*']+[lit(a_col[1]).alias(a_col[0]) for a_col in df_column_add]
    audit_cols_includeDF=dataframe.drop(*drop_columns).select(select_cols)
    condition=null_check(audit_cols_includeDF,config_df,job_code,errorCC,raw_table.split('.')[-1])
#     dataframe.show(truncate=False)
    filtered_rawDf=audit_cols_includeDF.filter(condition)
    if filtered_rawDf.count() >0:
      if partition:
        filtered_rawDf.drop('path').write.format("delta").partitionBy(partition).option("mergeSchema", "false").mode("append").saveAsTable(raw_table)
      else:
        filtered_rawDf.drop('path').write.format("delta").option("mergeSchema", "false").mode("append").saveAsTable(raw_table)
      print(f"loaded {filtered_rawDf.count()} records into the raw table {raw_table}")
    return filtered_rawDf.count()
  except Exception as e:
    exception_type, exception_object, exception_traceback = sys.exc_info()
    print(f"Function raw_table_load - error {str(exception_object)} occured at line number {exception_traceback.tb_lineno} while loading table {raw_table}")
    raise Exception(e)


# COMMAND ----------

#notebookcallerlogic
try:
  l_job_configDF=get_configs(ConfigTableName,JobCode+ExtractionChildJobCode)
  unstructured_flag=False
  compressed_flag=False
  json_flag=False
  if ExtractionJobCode.strip(' ') =='':
    l_JobCode=JobCode
  else:
    l_JobCode=JobCode.strip(',')+','+ExtractionJobCode.strip(',')
  for l_job_cd in l_JobCode.split(','):
    l_file_name=l_job_configDF.filter((col("JOB_CD")==l_job_cd.split(':')[-1].strip(' ')) & (lower(col("CONFIGNAME")).rlike("file_name"))).first()['CONFIGVALUE']
    if '.'.join(l_file_name.split('.')[-2:]).lower() == 'tar.gz' or l_file_name.split('.')[-1].lower().strip(' ')=='zip':
      compressed_flag=True
    elif l_file_name.split('.')[-1].lower().strip(' ')=='json':
      json_flag=True
    elif l_file_name.split('.')[-1].lower().strip(' ')=='csv' or l_file_name.split('.')[-1].lower().strip(' ')=='dat' or l_file_name.split('.')[-1].lower().strip(' ')=='txt':
      unstructured_flag=True
    else:
      print(f"Generic Autoloader can't handle {l_file_name} files")
except Exception as e:
  exception_type, exception_object, exception_traceback = sys.exc_info()
  print(f"Function notebookcallerlogic - error {str(exception_object)} occured at line number {exception_traceback.tb_lineno}.")
  raise Exception(e)

# COMMAND ----------

# MAGIC %run ./_generic_autoloader_Unstructured_library $execution=unstructured_flag

# COMMAND ----------

# MAGIC %run ./_generic_autoloader_json_library $execution=json_flag

# COMMAND ----------

# MAGIC %run ./_generic_autoloader_compressed_library $execution=compressed_flag

# COMMAND ----------

import threading
def handleMicroBatch_load(job_configDF,job_cd,batch_id,intermediate_record_sep):
  try:
    unstructured_record_count,json_error_count,unstructured_error_count,tar_error_count,zip_error_count,zip_record_count,tar_record_count,json_record_count=0,0,0,0,0,0,0,0
    file_name=job_configDF.filter((col("JOB_CD")==job_cd)&(lower(col("CONFIGNAME"))=="file_name")).first()['CONFIGVALUE']
    print(file_name)
    if file_name.split('.')[-1].lower()=='csv' or file_name.split('.')[-1].lower()=='dat' or file_name.split('.')[-1].lower()=='txt':
      column_sep=job_configDF.filter((col("JOB_CD")==job_cd)&(lower(col("CONFIGNAME"))=="column_seperator")).first()['CONFIGVALUE']
      unstructured_count=handleUnstructuredMicroBatch(batch_id,job_configDF,job_cd,intermediate_record_sep,column_sep.strip(' '))
      unstructured_error_count+=int(unstructured_count.split(':')[0])
      unstructured_record_count+=int(unstructured_count.split(':')[1])
    elif file_name.split('.')[-1].lower()=='json':
      json_count=handleJsonMicroBatch(batch_id,job_configDF,job_cd)
      json_error_count+=int(json_count.split(':')[0])
      json_record_count+=int(json_count.split(':')[1])
    elif '.'.join(file_name.split('.')[-2:]).lower()=='tar.gz':
      tar_count=handleTarGzMicroBatch(batch_id,job_configDF,job_cd,'Tar')
      tar_error_count+=int(tar_count.split(':')[0])
      tar_record_count+=int(tar_count.split(':')[1])
    elif file_name.split('.')[-1].lower()=='zip':
      zip_count=handleTarGzMicroBatch(batch_id,job_configDF,job_cd,'Zip')
      zip_error_count+=int(zip_count.split(':')[0])
      zip_record_count+=int(zip_count.split(':')[1])
    return str(unstructured_error_count+json_error_count+tar_error_count+zip_error_count)+':'+str(zip_record_count+tar_record_count+json_record_count+unstructured_record_count)
  except Exception as e:
    exception_type, exception_object, exception_traceback = sys.exc_info()
    print(f"Function handleMicroBatch_load - error {str(exception_object)} occured at line number {exception_traceback.tb_lineno}.")
    raise Exception(e)
    
class JobCodeLoadThread (threading.Thread):
  def __init__(self, **kwargs):
      threading.Thread.__init__(self)
      self.name          = "TableLoadThread-{}".format(
                              str(kwargs.get("thread_number")))
      self.job_configDF   = kwargs.get("job_configDF")
      self.batch_id    =kwargs.get("batch_id")
      self.intermediate_record_sep    =kwargs.get("intermediate_record_sep")
      self.job_cd=kwargs.get("job_cd")
  def run(self):
    try:
      error_load_count=handleMicroBatch_load(self.job_configDF,self.job_cd,self.batch_id,self.intermediate_record_sep)
      self.status=("Thread {} completed successfully on {}"\
              .format(self.name,str(datetime.datetime.now())))
      self.error_load_count=error_load_count
      print(self.status)
    except Exception as e:
      self.error_load_count='0:0'
      self.status=("Error occured for Thread '{}' while processing JobCode {} \nHere is the error :{}"\
                      .format(self.name,self.job_cd,str(e)))
      print(self.status)
      raise Exception(e)

# COMMAND ----------

def convertBytesToString(bytesdata,record_sep,column_sep):
  stream = io.BytesIO(bytesdata)
  if column_sep.strip(" ")=='':
    record_list = [line.replace(b"\n",b"").replace(b"\r",b"").decode("utf-8") for line in stream]
  else:
    record_list = [line.replace(b"\n",b"").replace(b"\r",b"").replace(f"{column_sep}".encode('utf-8'),f"{intermediate_col_sep}".encode('utf-8')).decode("utf-8") for line in stream]
  return record_sep.join(record_list)


def handleMicroBatch(microbatchDF, batch_id):
  try:
    error_count,record_count=0,0
#     global different_pattern_error_count,json_error_count,unstructured_error_count,tar_error_count,zip_error_count
    
    print(f"Running autoloader at {str(datetime.datetime.now())}") 
    #microbatchDF.show(truncate=False)
    batchSize = microbatchDF.count()
#     jobId=dbutils.notebook.getContext.tags.getOrElse("jobId","")
#     runId=dbutils.notebook.getContext.tags.getOrElse("runId","")
#     clusterId=dbutils.notebook.getContext.tags.getOrElse("clusterId","")
    print('batchSize: ',batchSize)
    micro_batch_start_time=datetime.datetime.now()
    if(batchSize > 0):
      batch_start_time=str(datetime.datetime.now())
      load_date=datetime.datetime.now().strftime('%Y-%m-%d')
      load_hour=str(datetime.datetime.now().strftime('%H'))+'00'
      file_path_list = [filename["path"] for filename in microbatchDF.select("path").collect()]
      print(file_path_list)
      
      job_cd_file_path_list=[]
      job_cd_list=JobCode.split(',')
      for jb_cd in job_cd_list:
        print(jb_cd)
        file_name=job_configDF.filter((col("JOB_CD")==jb_cd)&(lower(col("CONFIGNAME"))=="file_name")).first()['CONFIGVALUE']
        job_cd_df='microbatch_DF'+str(jb_cd)
        if file_name[0]=='~':
          globals()[job_cd_df]=microbatchDF.filter(~lower(col("path")).rlike(file_name[1:].lower()+'$'))
          print(f"~lower(col('path')).rlike({file_name[1:].lower()}+'$')")
        else:
          globals()[job_cd_df]=microbatchDF.filter(lower(col("path")).rlike(file_name.lower()+'$'))
        job_cd_file_path_list+=[filename["path"] for filename in eval(job_cd_df).select("path").collect()]
      different_pattern_files=set(file_path_list).difference(job_cd_file_path_list)
      print(different_pattern_files)
      if len(different_pattern_files) !=0:
        different_patternDF=microbatchDF.filter(col('path').isin(list(different_pattern_files)))
        bytesUDF = udf(convertBytesToString)
        try:
          different_patternDF1=different_patternDF.withColumn("path_parsed",regexp_replace(bytesUDF("content",lit("#"),lit(",")),intermediate_col_sep,','))
        except:
          different_patternDF1=different_patternDF.withColumn("path_parsed",'Unsupported data - Used different file pattern binary decoding')
        different_pattern_parsed_df = different_patternDF1.drop("modificationTime","content").withColumn("JOB_CD",lit(JobCode))\
	    .withColumn("Error_Cause",lit("Different Pattern files read"))\
        .withColumn("BATCH_ID",lit(batch_id))\
        .withColumn("BATCH_START_TIME",lit(batch_start_time))\
        .withColumn("LOAD_DATE",lit(load_date))\
        .withColumn("LOAD_HOUR",lit(load_hour))\
        .withColumn("JOB_NAME",lit(JobCode))\
	    .selectExpr("cast(BATCH_ID as long) BATCH_ID","BATCH_START_TIME","JOB_CD","JOB_NAME","path as FILE_NAME","path_parsed as FILE_DATA","Error_Cause","LOAD_DATE","LOAD_HOUR")
        logToErrorTable(different_pattern_parsed_df,JobCode,'out_of_scope_files_err_table',ErrorCC(batch_id,batch_start_time,'Different pattern files','SoftError','Yes',load_date,load_hour))
        different_pattern_error_count=different_patternDF.count()
      else:
        different_pattern_error_count=0
      threads=[]
      thread_status=' '
      for thread_no,job_cd in enumerate(job_cd_list):
        arguments={'thread_number':thread_no,
             'job_configDF':job_configDF,
             'batch_id':batch_id,
             'intermediate_record_sep':intermediate_record_sep,
             'job_cd':job_cd}
        thread = JobCodeLoadThread(**arguments)
        thread.start()
        threads.append(thread)
      for t in threads:
        t.join()
      for t in threads:
        thread_status=thread_status+'@,'+t.status
        error_count+=int(t.error_load_count.split(':')[0]) 
        record_count+=int(t.error_load_count.split(':')[1]) 
      if 'Error occured for Thread' in t.status:
        raise Exception(thread_status)
    else:
      different_pattern_error_count=0
  except Exception as e:
    exception_type, exception_object, exception_traceback = sys.exc_info()
    print(f"Function handleMicroBatch - error {str(exception_object)} occured at line number {exception_traceback.tb_lineno}.")
    raise Exception(e)
  finally:
    log_table_load(LogCC(batch_id,JobCode,micro_batch_start_time,batchSize,error_count+different_pattern_error_count,record_count))