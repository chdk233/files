-- Databricks notebook source
-- MAGIC %sql
-- MAGIC show create table dhf_iot_cmtpl_raw_prod.cmt_tripinterval_374607951

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dhf_iot_cmtpl_raw_prod.trip_detail_realtime where load_date='2022-03-18'  order by db_load_time desc

-- COMMAND ----------

show tables in dhf_iot_cmtpl_raw_prod

-- COMMAND ----------

  select count(distinct(tripSummaryId)) from dhf_iot_sm_ims_raw_dev.smart_miles_tripsummary
  ALTER TABLE dhf_iot_sm_ims_raw_dev.smart_miles_tripsummary ADD columns (db_load_date string)



-- COMMAND ----------

  select * from dhf_iot_generic_autoloader_test.job_config

-- COMMAND ----------

Select drive_id,count(*) as count from dhf_iot_cmtpl_raw_dev.trip_detail_realtime group by drive_id order by count asc limit 10



-- COMMAND ----------

select * from dhf_iot_cmtpl_harmonize_dev.ims_trip_summary_test LIMIT 2
--select count(*) from generic_csv_load.smart_miles_tripsummary

-- COMMAND ----------

SELECT * FROM generic_csv_load.smart_miles_telemetryevents LIMIT 2

-- COMMAND ----------

select * from dhf_config.task_group limit where id=4

-- COMMAND ----------

desc dhf_iot_generic_autoloader_dev.job_config

-- COMMAND ----------

select distinct tripSummaryId as ims_tripSummaryId, db_load_time as ims_db_load_time, db_load_date as ims_db_load_date from  generic_csv_load.smart_miles_tripsummary where tripSummaryId is not null and db_load_time is not null and db_load_date is not null

-- COMMAND ----------

desc dhf_config.task_group 

-- COMMAND ----------

select 
	    path,
accelQuality,
avgSpeed,
deviceIdentifier,
deviceIdentifierType,
deviceSerialNumber,
deviceType,
drivingDistance,
maxSpeed,
system,
milStatus,
secondsOfIdling,
timeZoneOffset,
totalTripSeconds,
transportMode,
transportModeReason,
tripSummaryId,
utcEndDateTime,
utcStartDateTime,
detectedVin,
enrolledVin,
hdopAverage,
enterpriseReferenceExtraInfo,
enterpriseReferenceId,
type,
db_load_time,
db_load_date
    from  generic_csv_load.smart_miles_tripsummary tdr where EXISTS (select 1 from generic_csv_load.smart_miles_tripsummary mb where tdr.tripSummaryId  = mb.tripSummaryId and tdr.db_load_date = mb.db_load_date
                                                                      -- tdr.db_load_time  = mb.db_load_time );

-- COMMAND ----------

select 
	    account_id,
		classification,
		deviceid,
		distance_km,
        driveid,
		driving,
		end_lat,
		end_lon,
		end_ts,
		id,
		start_lat,
		start_lon,
		start_ts,
        load_date,
        load_hour,
        db_load_time,
        db_load_date,
        'CMT' As source_system
    from  dhf_iot_cmtpl_raw_dev.trip_summary_realtime tdr where EXISTS (select 1 from dhf_iot_cmtpl_raw_dev.trip_detail_harmonize_batch mb where tdr.driveid = mb.drive_id and tdr.db_load_date = mb.db_load_date
                                                                       and tdr.load_date= mb.load_date and tdr.load_hour = mb.load_hour --and drive_id='0BC8E7E2-72E6-45D2-9673-64BF3D3E4192'
                                                                      );

-- COMMAND ----------

select * from dhf_iot_generic_autoloader_dev.job_config

-- COMMAND ----------

select distinct tripSummaryId as ims_tripSummaryId, db_load_time as ims_db_load_time, db_load_date as ims_db_load_date from generic_csv_load.smart_miles_scoring where tripSummaryId is not null --and db_load_time is not null and db_load_date is not null;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC columns = ["language","users_count"]
-- MAGIC data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]
-- MAGIC rdd = spark.sparkContext.parallelize(data)
-- MAGIC dfFromRDD1 = rdd.toDF(columns)
-- MAGIC dfFromRDD1.show

-- COMMAND ----------

CREATE DATABASE dhf_iot_curated_dev LOCATION 's3a://pcds-databricks-common-786994105833/iot/delta/curated/dhf_iot_curated_dev';

-- COMMAND ----------

CREATE DATABASE dhf_iot_rivian_raw_dev LOCATION 's3a://pcds-databricks-common-786994105833/iot/delta/raw/rivian/dhf_iot_rivian_raw_dev';

-- COMMAND ----------

--Select * from dhf_iot_rivian_raw_dev.rivian_job_run_log;
Delete from dhf_iot_rivian_raw_dev.rivian_job_run_log where job_id=4359987;

-- COMMAND ----------

CREATE OR REPLACE TABLE dhf_iot_rivian_raw_dev.rivian_stage_data(sno string,load_date string)
USING delta
PARTITIONED BY (load_date)
LOCATION 's3://pcds-databricks-common-786994105833/iot/generic-autoloader/dhf_iot_rivian_raw_dev/rivian_stage_data';

-- COMMAND ----------

CREATE TABLE dhf_iot_rivian_raw_dev.file_error (
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
LOCATION 's3://pcds-databricks-common-786994105833/iot/generic-autoloader/dhf_iot_rivian_raw_dev/file_error';

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df=spark.read.option("header","true").option("inferSchema","true").csv("s3://pcds-internal-iot-rivian-telematics-168341759447/rivian-sourcefiles/daily/load_date=2021-11-23/rivian_event_summary_20211123.00.csv")
-- MAGIC df.printSchema
-- MAGIC //df.write.format("delta").mode("append").saveAsTable("dhf_iot_rivian_raw_test.rivian_av_aggregate")

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from datetime import date
-- MAGIC path='s3://pcds-internal-iot-rivian-telematics-168341759447/rivian-sourcefiles/daily/load_date=2021-11-23/rivian_av_aggregate_20211123.00.csv'
-- MAGIC load_date=(split(split(col("path"),"/")[5],"=")[1],"yyyyMMdd")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.table("dhf_iot_rivian_raw_test.rivian_event_summary").printSchema

-- COMMAND ----------

CREATE TABLE dhf_iot_rivian_raw_dev.job_error (
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
LOCATION 's3://pcds-databricks-common-786994105833/iot/generic-autoloader/dhf_iot_rivian_raw_dev/job_error';

-- COMMAND ----------

desc dhf_iot_rivian_raw_test.av_aggregate
--select * from dhf_iot_rivian_raw_test.trips
--select * from dhf_iot_rivian_raw_test.trips_summary
--select * from dhf_iot_rivian_raw_test.event_summary


-- COMMAND ----------

