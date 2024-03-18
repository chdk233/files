// Databricks notebook source
// MAGIC %scala
// MAGIC
// MAGIC val splitpath="s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Archive/tar/load_date=2021-06-21/1100/DS_CMT_3e16d383-aac6-4754-8a6d-be0f377d35e0/48CC745D-FB7F-486F-AB02-6564FE30D2C9.tar.gz".split("/").takeRight(4).mkString("/")
// MAGIC "s3://pcds-databricks-common-786994105833/delta/dhf_iot_cmtpl_raw_dev/trip_detail_realtime/".concat(splitpath)
// MAGIC

// COMMAND ----------



// COMMAND ----------

// MAGIC %scala
// MAGIC import spark.implicits._
// MAGIC import org.apache.spark.sql._
// MAGIC import org.apache.spark.sql.functions._
// MAGIC import org.apache.spark.sql.streaming.Trigger
// MAGIC import org.apache.spark.sql.types._
// MAGIC val data = Seq("s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Archive/tar/load_date=2021-06-21/1100/DS_CMT_3e16d383-aac6-4754-8a6d-be0f377d35e0/48CC745D-FB7F-486F-AB02-6564FE30D2C9.tar.gz","s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Archive/tar/load_date=2021-06-21/1100/DS_CMT_3e16d383-aac6-4754-8a6be0f377d35e0/48CC74-FB7F-486F-AB02-6564FE30D2C9.tar.gz","s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Archive/tar/load_date=2021-06-21/1100/DS_CMT_3e16d383-aac6-4754-8a6d-be0f377d35e0/48CC745D-FB7F-486F-AB02-6564FE30D2C9.tar.gz","s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Arcve/tar/load_date=2021-06-21/1100/DS_CMT_3e16d383-aac6-4754-8a6d-be0f377d35e0/48CC745D-FB7F-486F-AB02-6564FE30D2C9.tar.gz")
// MAGIC val targetpath="s3://pcds-databricks-common-786994105833/iot/raw/smartride/cmt-pl/realtime/tar/"
// MAGIC val df = spark.sparkContext.parallelize(data).toDF("path").withColumn("split",split(col("path"),"/",7)(6)).withColumn("finalpath",concat(lit(targetpath),col("split"))).drop("split").limit(1).show(false)
// MAGIC //df.getString(0)
// MAGIC df.foreach(row =>println(row.getString(0)))
// MAGIC

// COMMAND ----------

// MAGIC %scala
// MAGIC import spark.implicits._
// MAGIC import org.apache.spark.sql._
// MAGIC import org.apache.spark.sql.functions._
// MAGIC import org.apache.spark.sql.streaming.Trigger
// MAGIC import org.apache.spark.sql.types._
// MAGIC
// MAGIC
// MAGIC val df=spark.read.table("dhf_iot_cmtpl_raw_dev.driver_summary_daily_test0")
// MAGIC df.write.format("delta")
// MAGIC          .mode("overwrite")
// MAGIC          .option("path","s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Bala/driver_summary_daily_test0/")
// MAGIC          .option("overwriteSchema", "true")
// MAGIC          .partitionBy("load_date") // different column
// MAGIC          .saveAsTable("dhf_iot_cmtpl_raw_dev.driver_summary_daily_test0")

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDateTime

val badge_daily = spark.read.table("dhf_iot_cmtpl_raw_dev.badge_daily")
val driver_profile_daily=spark.read.table("dhf_iot_cmtpl_raw_dev.driver_profile_daily")
val driver_summary_daily=spark.read.table("dhf_iot_cmtpl_raw_dev.driver_summary_daily")
val fraud_daily=spark.read.table("dhf_iot_cmtpl_raw_dev.fraud_daily")
val heartbeat_daily=spark.read.table("dhf_iot_cmtpl_raw_dev.heartbeat_daily")
val trip_labels_daily=spark.read.table("dhf_iot_cmtpl_raw_dev.trip_labels_daily")
val trip_summary_daily=spark.read.table("dhf_iot_cmtpl_raw_dev.trip_summary_daily")

df.write.format("delta")
        .mode("overwrite")
        .option("path","s3://pcds-databricks-common-786994105833/delta/dhf_iot_cmtpl_raw_dev/trip_s3lag/")
        .option("overwriteSchema", "true")
        .partitionBy("load_date") 
        .saveAsTable("dhf_iot_cmtpl_raw_dev.trip_s3lag")
df.write.format("delta")
        .mode("overwrite")
        .option("path","s3://pcds-databricks-common-786994105833/delta/dhf_iot_cmtpl_raw_dev/trip_s3lag/")
        .option("overwriteSchema", "true")
        .partitionBy("load_date") 
        .saveAsTable("dhf_iot_cmtpl_raw_dev.trip_s3lag")
df.write.format("delta")
        .mode("overwrite")
        .option("path","s3://pcds-databricks-common-786994105833/delta/dhf_iot_cmtpl_raw_dev/trip_s3lag/")
        .option("overwriteSchema", "true")
        .partitionBy("load_date") 
        .saveAsTable("dhf_iot_cmtpl_raw_dev.trip_s3lag")
df.write.format("delta")
        .mode("overwrite")
        .option("path","s3://pcds-databricks-common-786994105833/delta/dhf_iot_cmtpl_raw_dev/trip_s3lag/")
        .option("overwriteSchema", "true")
        .partitionBy("load_date") 
        .saveAsTable("dhf_iot_cmtpl_raw_dev.trip_s3lag")
df.write.format("delta")
        .mode("overwrite")
        .option("path","s3://pcds-databricks-common-786994105833/delta/dhf_iot_cmtpl_raw_dev/trip_s3lag/")
        .option("overwriteSchema", "true")
        .partitionBy("load_date") 
        .saveAsTable("dhf_iot_cmtpl_raw_dev.trip_s3lag")
df.write.format("delta")
        .mode("overwrite")
        .option("path","s3://pcds-databricks-common-786994105833/delta/dhf_iot_cmtpl_raw_dev/trip_s3lag/")
        .option("overwriteSchema", "true")
        .partitionBy("load_date") 
        .saveAsTable("dhf_iot_cmtpl_raw_dev.trip_s3lag")
df.write.format("delta")
        .mode("overwrite")
        .option("path","s3://pcds-databricks-common-786994105833/delta/dhf_iot_cmtpl_raw_dev/trip_s3lag/")
        .option("overwriteSchema", "true")
        .partitionBy("load_date") 
        .saveAsTable("dhf_iot_cmtpl_raw_dev.trip_s3lag")



// COMMAND ----------

// MAGIC %scala
// MAGIC import spark.implicits._
// MAGIC import org.apache.spark.sql._
// MAGIC import org.apache.spark.sql.functions._
// MAGIC import org.apache.spark.sql.streaming.Trigger
// MAGIC import org.apache.spark.sql.types._
// MAGIC
// MAGIC
// MAGIC val df=spark.read.table("dhf_iot_cmtpl_raw_dev.trip_s3lag_test1")
// MAGIC val df2=spark.read.table("dhf_iot_cmtpl_raw_dev.driver_summary_daily_test1")
// MAGIC df.withColumn("db_load_date", to_date($"batchtimestamp")).write.format("delta")
// MAGIC          .mode("overwrite")
// MAGIC          //.option("path","s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Bala/driver_summary_daily_test0/")
// MAGIC          .option("overwriteSchema", "true")
// MAGIC          .partitionBy("db_load_date") // different column
// MAGIC          .saveAsTable("dhf_iot_cmtpl_raw_dev.trip_s3lag_test1")
// MAGIC df.withColumn("db_load_date", current_date()).write.format("delta")
// MAGIC          .mode("overwrite")
// MAGIC          //.option("path","s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Bala/driver_summary_daily_test0/")
// MAGIC          .option("overwriteSchema", "true")
// MAGIC          .partitionBy("db_load_date") // different column
// MAGIC          .saveAsTable("dhf_iot_cmtpl_raw_dev.driver_summary_daily_test0")

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDateTime

val delta_table = "dhf_iot_cmtpl_raw_dev.driver_profile_daily_test0"
val s3_path="s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Bala/driver_profile_daily_test0/"
val df = spark.read.table(delta_table)

df.write.format("delta")
        .mode("overwrite")
        .option("path",s3_path)
        .option("overwriteSchema", "true")
        .partitionBy("load_date") 
        .saveAsTable(delta_table)

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct(db_load_date) from dhf_iot_cmtpl_raw_dev.trip_s3lag_test0 limit 10;

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val enviroment="dev"
val database=s"dhf_iot_cmtpl_raw_${enviroment}"

def createBackupTable(table: String)={
  
  val delta_table=s"$database.${table}"
  val backup_table=s"$database.${table}_bkp1"
  val df = spark.read.table(delta_table)

  df.write.format("delta")
          .option("mergeSchema", "true")
          .saveAsTable(backup_table)
}

createBackupTable("badge_daily")
createBackupTable("fraud_daily")
createBackupTable("driver_profile_daily")
createBackupTable("driver_profile_daily")
createBackupTable("heartbeat_daily")
createBackupTable("trip_labels_daily")
createBackupTable("trip_summary_daily")
createBackupTable("trip_s3lag")


// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct(tripSummaryId)) as count from dhf_iot_sm_ims_delta_dev.smart_miles_tripsummary 

// COMMAND ----------

env="test"

if (env.equals("dev")) {
  deltadatabase                  = "dhf_iot_cmtpl_raw_dev"  
  val account_id                     = "786994105833"
  val deltadatabase_path             = s"s3://pcds-databricks-common-${account_id}/delta/${deltadatabase}"
println(account_id)
}
if (env == "test") {
  val deltadatabase                  = "dhf_iot_cmtpl_raw_test"  
  val account_id                     = "786994105833"
  val deltadatabase_path             = s"s3://pcds-databricks-common-${account_id}/delta/${deltadatabase}"
println(account_id)
}
if (env == "prod") {
  val deltadatabase                  = "dhf_iot_cmtpl_raw_prod"  
  val account_id                     = "786994105833"
  val deltadatabase_path             = s"s3://pcds-databricks-common-${account_id}/delta/${deltadatabase}"
}


// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val env="dev"
val deltadatabase= env match {
  case "dev"  => "dhf_iot_cmtpl_raw_dev"
  case "test"  => "dhf_iot_cmtpl_raw_test"
  case "prod"  => "dhf_iot_cmtpl_raw_prod"
}
val account_id= env match {
  case "dev"  => "786994105833"
  case "test"  => "168341759447"
  case "prod"  => "785562577411"
}
def partitionTable(table: String)= {
  val delta_table=s"${deltadatabase}.${table}"
  val s3_path=s"s3://dw-internal-pl-cmt-telematics-${account_id}/CMT/Bala/${deltadatabase}/${table}"
  val df = spark.read.table(delta_table)

  df.write.format("delta")
        .mode("overwrite")
        .option("path",s3_path)
        .option("overwriteSchema", "true")
        .partitionBy("load_date") 
        .saveAsTable(delta_table)

}
partitionTable("fraud_daily_bkp1")
partitionTable("badge_daily_bkp1")



// COMMAND ----------

//load_date=2021-05-10/
//driver_summary_daily_test0
spark.sql("select * from dhf_iot_cmtpl_raw_dev.driver_summary_daily_test0 where load_date='2021-05-11'")