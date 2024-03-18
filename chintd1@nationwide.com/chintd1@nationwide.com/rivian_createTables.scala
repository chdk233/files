// Databricks notebook source

dbutils.widgets.removeAll()
dbutils.widgets.dropdown("environment", "dev", Seq("dev", "test","prod"), "environment")
val environment=dbutils.widgets.get("environment").toString.toLowerCase

println("environment is "+environment)

// COMMAND ----------


val account_id= environment match {
  case "dev"  => "786994105833"
  case "test"  => "168341759447"
  case "prod"  => "785562577411"
}

// COMMAND ----------

spark.sql(s""" CREATE DATABASE  IF NOT EXISTS dhf_iot_rivian_raw_${environment} LOCATION 's3://pcds-databricks-common-${account_id}/iot/delta/harmonized/dhf_iot_rivian_raw_${environment}';""")

spark.sql(s""" CREATE DATABASE  IF NOT EXISTS dhf_iot_rivian_stageraw_${environment} LOCATION 's3://pcds-databricks-common-${account_id}/iot/delta/harmonized/dhf_iot_rivian_stageraw_${environment}';""")

// COMMAND ----------

//Raw tables 

spark.sql(s"""CREATE TABLE dhf_iot_rivian_raw_${environment}.av_aggregate (
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
LOCATION 's3://pcds-databricks-common-${account_id}/iot/delta/raw/rivian/dhf_iot_rivian_raw_${environment}/av_aggregate';""")
//

spark.sql(s"""CREATE TABLE dhf_iot_rivian_raw_${environment}.trips (
vin	string	,
trip_id	string	,
utc_time	string	,
tzo	int	,
speed	double	,
db_load_time	timestamp	,
load_date	date)
USING delta
PARTITIONED BY (load_date)
LOCATION 's3://pcds-databricks-common-${account_id}/iot/delta/raw/rivian/dhf_iot_rivian_raw_${environment}/trips';""")

//

spark.sql(s"""CREATE TABLE dhf_iot_rivian_raw_${environment}.trips_summary (
vin	string	,
trip_id	string	,
trip_start	string	,
trip_end	string	,
tzo	int	,
distance	double	,
duration	double	,
av_distance	double	,
av_duration	double	,
db_load_time	timestamp	,
load_date	date)
USING delta
PARTITIONED BY (load_date)
LOCATION 's3://pcds-databricks-common-${account_id}/iot/delta/raw/rivian/dhf_iot_rivian_raw_${environment}/trips_summary';""")

//

spark.sql(s"""CREATE TABLE dhf_iot_rivian_raw_${environment}.event_summary (
vin	string	,
event_id	string	,
event_name	string	,
event_timestamp	string	,
tzo	int	,
db_load_time	timestamp	,
load_date	date)
USING delta
PARTITIONED BY (load_date)
LOCATION 's3://pcds-databricks-common-${account_id}/iot/delta/raw/rivian/dhf_iot_rivian_raw_${environment}/event_summary';""")

// COMMAND ----------

//Stage tables

spark.sql(f"""CREATE OR REPLACE TABLE dhf_iot_rivian_stageraw_${environment}.rivian_stage_av_aggregate(load_date Date,path string)
USING delta
PARTITIONED BY (load_date)
LOCATION 's3://pcds-databricks-common-${account_id}/iot/delta/raw/rivian/dhf_iot_rivian_stageraw_${environment}/rivian_stage_av_aggregate';""")

//

spark.sql(f"""CREATE OR REPLACE TABLE dhf_iot_rivian_stageraw_${environment}.rivian_stage_trips(load_date Date,path string)
USING delta
PARTITIONED BY (load_date)
LOCATION 's3://pcds-databricks-common-${account_id}/iot/delta/raw/rivian/dhf_iot_rivian_stageraw_${environment}/rivian_stage_trips';""")

//

spark.sql(f"""CREATE OR REPLACE TABLE dhf_iot_rivian_stageraw_${environment}.rivian_stage_trips_summary(load_date Date,path string)
USING delta
PARTITIONED BY (load_date)
LOCATION 's3://pcds-databricks-common-${account_id}/iot/delta/raw/rivian/dhf_iot_rivian_stageraw_${environment}/rivian_stage_trips_summary';""")

//

spark.sql(f"""CREATE OR REPLACE TABLE dhf_iot_rivian_stageraw_${environment}.rivian_stage_event_summary(load_date Date,path string)
USING delta
PARTITIONED BY (load_date)
LOCATION 's3://pcds-databricks-common-${account_id}/iot/delta/raw/rivian/dhf_iot_rivian_stageraw_${environment}/rivian_stage_event_summary';""")

// COMMAND ----------

//Error tables

spark.sql(f"""CREATE TABLE dhf_iot_rivian_stageraw_${environment}.file_error (
BATCH_ID BIGINT,
BATCH_START_TIME STRING,
JOB_CD STRING,
JOB_NAME STRING,
FILE_NAME STRING,
FILE_DATA STRING,
ERROR_CAUSE STRING,
LOAD_DATE STRING,
LOAD_HOUR STRING)
USING delta
PARTITIONED BY (LOAD_DATE)
LOCATION 's3://pcds-databricks-common-${account_id}/iot/delta/raw/rivian/dhf_iot_rivian_stageraw_${environment}/file_error';""")

//

spark.sql(f"""CREATE TABLE dhf_iot_rivian_stageraw_${environment}.job_error (
BATCH_ID BIGINT,
BATCH_START_TIME STRING,
JOB_CD STRING,
JOB_NAME STRING,
FILE_NAME STRING,
ERROR_COUNT BIGINT,
ERROR_CAUSE STRING,
LOAD_DATE STRING,
LOAD_HOUR STRING)
USING delta
PARTITIONED BY (LOAD_DATE)
LOCATION 's3://pcds-databricks-common-${account_id}/iot/delta/raw/rivian/dhf_iot_rivian_stageraw_${environment}/job_error';
""")

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE DATABASE dhf_iot_generic_autoloader_test LOCATION 's3://pcds-databricks-common-168341759447/iot/generic-autoloader/dhf_iot_generic_autoloader_dev/';

// COMMAND ----------

spark.sql(f"""CREATE TABLE dhf_iot_generic_autoloader_test.job_config (
JOB_CD string,
JOB_NAME string,
CONFIGNAME string,
CONFIGVALUE string)
USING delta
LOCATION 's3://pcds-databricks-common-168341759447/iot/generic-autoloader/dhf_iot_generic_autoloader_dev/job_config';""")



// COMMAND ----------

val dd=spark.read.csv("s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Krishna/config/part-00000-tid-7252219712652589508-fa0db4c3-f643-4fd8-8ebf-289853cbe5a9-1300004-1-c000.csv")
display(dd)

// COMMAND ----------

val df= spark.read.table("dhf_iot_generic_autoloader_dev.job_config")
df.createOrReplaceTempView("table")
val fi=spark.sql("""select * from table where JOB_CD in (2001,2002,2003,2004)""")
fi.coalesce(1).write.option("header","True").option("inferSchema","True").format("csv").save("s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Krishna/config1")