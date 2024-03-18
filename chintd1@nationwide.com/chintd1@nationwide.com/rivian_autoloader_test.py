# Databricks notebook source
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

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE dhf_iot_rivian_stageraw_dev.rivian_stage_trips(load_date Date,path string)
# MAGIC USING delta
# MAGIC PARTITIONED BY (load_date)
# MAGIC LOCATION 's3://pcds-databricks-common-786994105833/iot/delta/stageraw/rivian/dhf_iot_rivian_stageraw_dev/rivian_stage_trips';
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dhf_iot_rivian_stageraw_dev.file_error (
# MAGIC BATCH_ID BIGINT,
# MAGIC BATCH_START_TIME STRING,
# MAGIC JOB_CD STRING,
# MAGIC JOB_NAME STRING,
# MAGIC FILE_NAME STRING,
# MAGIC FILE_DATA STRING,
# MAGIC ERROR_CAUSE STRING,
# MAGIC LOAD_DATE STRING,
# MAGIC LOAD_HOUR STRING)
# MAGIC USING delta
# MAGIC PARTITIONED BY (LOAD_DATE)
# MAGIC LOCATION 's3://pcds-databricks-common-786994105833/iot/delta/raw/rivian/dhf_iot_rivian_stageraw_dev/file_error';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE dhf_iot_rivian_stageraw_dev.job_error (
# MAGIC BATCH_ID BIGINT,
# MAGIC BATCH_START_TIME STRING,
# MAGIC JOB_CD STRING,
# MAGIC JOB_NAME STRING,
# MAGIC FILE_NAME STRING,
# MAGIC ERROR_COUNT BIGINT,
# MAGIC ERROR_CAUSE STRING,
# MAGIC LOAD_DATE STRING,
# MAGIC LOAD_HOUR STRING)
# MAGIC USING delta
# MAGIC PARTITIONED BY (LOAD_DATE)
# MAGIC LOCATION 's3://pcds-databricks-common-786994105833/iot/delta/raw/rivian/dhf_iot_rivian_stageraw_dev/job_error';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE dhf_iot_rivian_stageraw_dev  LOCATION 's3a://pcds-databricks-common-786994105833/iot/delta/raw/rivian/dhf_iot_rivian_stageraw_dev'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_generic_autoloader_dev.job_config where JOB_CD=11003--job_name like '%Smart Miles%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 'db_load_time::current_timestamp(),::load_date::to_date(split(split(col("path"),"/")[5],"=")[1],"yyyyMMdd")'

# COMMAND ----------

config_df=spark.read.table("dhf_iot_generic_autoloader_dev.job_config")
audit_col=config_df.filter((col("JOB_CD")==2001) & (lower(col("CONFIGNAME")).rlike(f"audit_raw_.*")) & (trim(col("CONFIGNAME"))!=''  )).first()["CONFIGVALUE"]
print(audit_col)

# COMMAND ----------

df=spark.read.table("dhf_iot_generic_autoloader_dev.job_config")
df.coalesce(1).write.format("csv").save("s3://pcds-internal-iot-rivian-telematics-786994105833/rivian_raw/tr")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc dhf_iot_generic_autoloader_dev.job_config --where CONFIGVALUE='db_load_time::current_timestamp(),::load_date::to_date(split(split(col("path"),"/")[5],"=")[1])'-- and JOB_NAME='Rivian_trips_summary'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE dhf_iot_generic_autoloader_dev.job_config SET CONFIGVALUE = 'db_load_time::current_timestamp(),::load_date::to_date(split(split(col("path"),"/")[5],"=")[1])' WHERE CONFIGVALUE = 'db_load_time::current_timestamp(),::load_date::to_date(split(split(col("path"),"/")[5],"=")[1],"yyyyMMdd")'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO dhf_iot_generic_autoloader_test.job_config VALUES
# MAGIC     ('2003', 'Rivian_trips_summary', 'date_format_string', 'yyyy-mm-dd');
# MAGIC     
# MAGIC

# COMMAND ----------

 
dhf_iot_rivian_raw_dev.av_aggregate
dhf_iot_rivian_raw_dev.trips
dhf_iot_rivian_raw_dev.trips_summary
dhf_iot_rivian_raw_dev.event_summary

dhf_iot_rivian_stageraw_dev.rivian_stage_av_aggregate
dhf_iot_rivian_stageraw_dev.rivian_stage_trips
dhf_iot_rivian_stageraw_dev.rivian_stage_trips_summary
dhf_iot_rivian_stageraw_dev.rivian_stage_event_summary

arn:aws:sns:us-east-1:786994105833:dw-cmt-pl-iot-telematics-emailgroup

dhf_iot_rivian_stageraw_dev.file_error
dhf_iot_rivian_stageraw_dev.job_error


dhf_iot_generic_autoloader_dev.job_config

rivian_event_summary_.*.csv



# COMMAND ----------

df=spark.read.table("dhf_iot_rivian_raw_dev.av_aggregate")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --Raw tables
# MAGIC --select count(*) from dhf_iot_rivian_raw_dev.av_aggregate;
# MAGIC --select count(*) from dhf_iot_rivian_raw_dev.trips;
# MAGIC --select count(*)  from dhf_iot_rivian_raw_dev.trips_summary;
# MAGIC select count(*)  from dhf_iot_rivian_raw_dev.event_summary;
# MAGIC
# MAGIC --Stage tables
# MAGIC --desc dhf_iot_rivian_stageraw_dev.rivian_stage_av_aggregate;
# MAGIC --select count(*) from dhf_iot_rivian_stageraw_dev.rivian_stage_trips;
# MAGIC --select count(*) from dhf_iot_rivian_stageraw_dev.rivian_stage_trips_summary;
# MAGIC --select count(*) from dhf_iot_rivian_stageraw_dev.rivian_stage_event_summary;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --Raw tables
# MAGIC select *  from dhf_iot_rivian_raw_test.av_aggregate ;
# MAGIC --select * from dhf_iot_rivian_raw_test.trips;
# MAGIC --select *  from dhf_iot_rivian_raw_test.trips_summary;
# MAGIC --select *  from dhf_iot_rivian_raw_test.event_summary;
# MAGIC
# MAGIC --stage tables
# MAGIC --select * from dhf_iot_rivian_stageraw_test.rivian_stage_av_aggregate;
# MAGIC --select * from dhf_iot_rivian_stageraw_test.rivian_stage_trips;
# MAGIC --select * from dhf_iot_rivian_stageraw_test.rivian_stage_trips_summary;
# MAGIC --select * from dhf_iot_rivian_stageraw_test.rivian_stage_event_summary;

# COMMAND ----------



# COMMAND ----------

s3_source_object_key="rivian-test/rivian_av_aggregate_20210817.01.csv"
s3_target_object_key = f"rivian_raw/daily/load_date={s3_source_object_key.rsplit('_',1)[1].split('.')[0]}"
print(s3_target_object_key)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE dhf_iot_rivian_raw_test LOCATION 's3://pcds-databricks-common-168341759447/iot/delta/raw/rivian/dhf_iot_rivian_raw_test';
# MAGIC CREATE DATABASE dhf_iot_rivian_stageraw_test LOCATION 's3://pcds-databricks-common-168341759447/iot/delta/raw/rivian/dhf_iot_rivian_stageraw_test';
# MAGIC  

# COMMAND ----------



spark.sql("""CREATE TABLE dhf_iot_rivian_stageraw_dev.av_aggregate (
vin	string	,
trip_id	string	,
utc_time	string	,
tzo	int	,
speed	double	,
db_load_time	timestamp	,
load_date	date)
USING delta
PARTITIONED BY (load_date)
LOCATION 's3://pcds-databricks-common-786994105833/iot/delta/raw/rivian/dhf_iot_rivian_raw_dev/av_aggregate';""")

spark.sql("""CREATE TABLE dhf_iot_rivian_stageraw_dev.file_error56 (
vin	string	,
av_optin_date	string	,
last_av_optout_date	string	,
av_elapsed_days	int	,
pol_eff_date	string	,
pol_exp_date	string	,
total_distance	double	,
total_duration	double	,
av_total_distance	double	,
av_total_duration	double	,
av_score	double	,
av_discount	string	,
db_load_time	timestamp	,
load_date	date)
USING delta
PARTITIONED BY (load_date)
LOCATION 's3://pcds-databricks-common-786994105833/iot/delta/raw/rivian/dhf_iot_rivian_raw_dev/tes262t';""")


# COMMAND ----------

# MAGIC %sql
# MAGIC --dhf_iot_rivian_raw_dev.av_aggregate
# MAGIC --dhf_iot_rivian_raw_dev.trips
# MAGIC --dhf_iot_rivian_raw_dev.trips_summary
# MAGIC --dhf_iot_rivian_raw_dev.event_summary
# MAGIC select * from  dhf_iot_rivian_raw_dev.trips where load_date='2021-12-21'

# COMMAND ----------

dbutils.fs.rm("s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Krishna/load_date=2021-04-22/", False)

# COMMAND ----------

# MAGIC %scala
# MAGIC
# MAGIC val PATH = "s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Krishna/load_date=2021-04-22/"
# MAGIC dbutils.fs.ls(PATH)
# MAGIC             .map(_.name)
# MAGIC             .foreach((file: String) => dbutils.fs.rm(PATH + file, false))
# MAGIC
# MAGIC