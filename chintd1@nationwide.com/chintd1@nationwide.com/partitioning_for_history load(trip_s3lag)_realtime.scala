// Databricks notebook source
import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDateTime

val env = "dev"

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
  val s3_path=s"s3://pcds-databricks-common-${account_id}/delta/${deltadatabase}/${table}"
  
  val df = spark.read.table(delta_table)
  
  df.withColumn("db_load_date", to_date($"batchtimestamp"))
        .write.format("delta")
        .mode("overwrite")
        .option("path",s3_path)
        .option("overwriteSchema", "true")
        .partitionBy("db_load_date") 
        .saveAsTable(delta_table)
}
partitionTable("trip_s3lag")

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDateTime

val delta_table=s"dhf_iot_rivian_raw_dev.event_summary"
val s3_path=s"s3://pcds-databricks-common-786994105833/iot/delta/raw/rivian/dhf_iot_rivian_raw_dev/event_summary"

val df = spark.read.table(delta_table)
df.write.format("delta")
        .mode("overwrite")
        .option("path",s3_path)
        .option("overwriteSchema", "true")
        .partitionBy("load_date") 
        .saveAsTable(delta_table)