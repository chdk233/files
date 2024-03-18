// Databricks notebook source
// MAGIC %sql
// MAGIC CREATE TABLE dhf_iot_cmtpl_raw_dev.driver_summary_daily_test0 as SELECT * FROM dhf_iot_cmtpl_raw_dev.driver_summary_daily_tes where load_date in ('2021-07-14','2021-07-15');

// COMMAND ----------

// MAGIC  %python
// MAGIC  len(dbutils.fs.ls('s3://dw-internal-pl-cmt-telematics-785562577411/CMT/Archive/tar/load_date=2023-05-07/'))

// COMMAND ----------

import org.apache.hadoop.fs.Path
val s3Path = new Path("s3://dw-internal-pl-cmt-telematics-785562577411/CMT/Archive/tar/load_date=2023-05-07/")
val contentSummary = s3Path.getFileSystem(sc.hadoopConfiguration).getContentSummary(s3Path)
val nbFiles = contentSummary.getFileCount()

// COMMAND ----------

// MAGIC %scala
// MAGIC import spark.implicits._
// MAGIC import org.apache.spark.sql._
// MAGIC import org.apache.spark.sql.functions._
// MAGIC import org.apache.spark.sql.streaming.Trigger
// MAGIC import org.apache.spark.sql.types._
// MAGIC
// MAGIC
// MAGIC val df=spark.read.table("dhf_iot_cmtpl_raw_dev.trip_s3lag")
// MAGIC df.withColumn("db_load_date", to_date($"batchtimestamp"))
// MAGIC    .write.format("delta")
// MAGIC          .option("path","s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Bala/trip_s3lag/")
// MAGIC          .option("mergeSchema", "true")
// MAGIC          .saveAsTable("dhf_iot_cmtpl_raw_dev.trip_s3lag_test0")

// COMMAND ----------

// MAGIC %scala
// MAGIC val df=spark.read.table("dhf_iot_cmtpl_raw_dev.trip_s3lag")
// MAGIC df.withColumn("db_load_date", to_date($"batchtimestamp"))
// MAGIC    .write.format("delta")
// MAGIC          .mode("overwrite")
// MAGIC          .option("path","s3://pcds-databricks-common-786994105833/delta/dhf_iot_cmtpl_raw_dev/trip_s3lag/")
// MAGIC          .option("overwriteSchema", "true")
// MAGIC          .partitionBy("db_load_date") 
// MAGIC          .saveAsTable("dhf_iot_cmtpl_raw_dev.trip_s3lag")

// COMMAND ----------

df.withColumn("db_load_date1", current_date()).show

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) original_count from dhf_iot_cmtpl_raw_dev.trip_summary_daily;
// MAGIC --select count(*) from dhf_iot_cmtpl_raw_dev.driver_summary_daily_test0;

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from dhf_iot_cmtpl_raw_dev.trip_s3lag;

// COMMAND ----------

// MAGIC %scala
// MAGIC import java.time.LocalDateTime
// MAGIC import spark.sqlContext.implicits._
// MAGIC val df=spark.read.table("dhf_iot_rivian_raw_dev.rivian_stage_data")
// MAGIC df.write.options(Map("header"->"true", "quote"->"\"")).option("escape", "\"").format("delta").mode("overWrite").saveAsTable("dhf_iot_rivian_raw_dev.test")

// COMMAND ----------

import spark.sqlContext.implicits._

//Get current Date & Time
val df = Seq((1)).toDF("seq")

val curDate = df.withColumn("current_date",current_date().as("current_date"))
    .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
curDate.show(false)

// COMMAND ----------


import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

val df=spark.read.format("csv").option("inferSchema","true").option("header","true").load("s3://pcds-internal-iot-rivian-telematics-786994105833/rivian_raw/daily/load_date=20210817/rivian_trips_20210817.01.csv")
df.write.format("delta").option("path","s3://pcds-databricks-common-786994105833/iot/delta/raw/rivian/dhf_iot_rivian_raw_dev/av_aggregate").saveAsTable("dhf_iot_rivian_raw_dev.trips")

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table  dhf_iot_rivian_raw_dev.av_aggregate
// MAGIC --select * from  dhf_iot_rivian_raw_dev.rivian_stage_data
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from  dhf_iot_rivian_raw_dev.rivian_stage_data

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_generic_autoloader_dev.job_config where JOB_CD=2000
// MAGIC job_error 
// MAGIC file_error
// MAGIC