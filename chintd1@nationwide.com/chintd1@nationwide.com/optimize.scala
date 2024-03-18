// Databricks notebook source
import java.time._

val environment="prod"
val rawTablesList=List(
s"dhf_iot_ims_raw_${environment}.tripsummary",
s"dhf_iot_ims_raw_${environment}.telemetrypoints",
s"dhf_iot_ims_raw_${environment}.telemetryevents",
s"dhf_iot_ims_raw_${environment}.nontripevent",
s"dhf_iot_ims_raw_${environment}.scoring",
s"dhf_iot_ims_raw_${environment}.histogramscoringintervals",
s"dhf_iot_tims_raw_${environment}.tripsummary",
s"dhf_iot_tims_raw_${environment}.telemetrypoints",
s"dhf_iot_fmc_raw_${environment}.tripsummary",
s"dhf_iot_fmc_raw_${environment}.telemetrypoints")

// COMMAND ----------

//Raw tables optimization
import java.time._

rawTablesList.foreach( table => {  
val previous_date=LocalDate.now(ZoneOffset.UTC).minusDays(1).toString()
var condition=s"where load_date <='${previous_date}'"
val optimize_str= s"OPTIMIZE ${table} ${condition}"
println(s"OPTIMIZE starts for ${table} ${condition}")
spark.sql(optimize_str)
println(s"OPTIMIZE completes for ${table} ${condition}") 
 
})


// COMMAND ----------

import java.time._
val environment="prod"
val harmonizeTablesList=List(
s"dhf_iot_harmonized_${environment}.trip_summary",
s"dhf_iot_harmonized_${environment}.trip_point",
s"dhf_iot_harmonized_${environment}.trip_event",
s"dhf_iot_harmonized_${environment}.non_trip_event",
s"dhf_iot_harmonized_${environment}.scoring",
s"dhf_iot_harmonized_${environment}.histogram_scoring_interval",
s"dhf_iot_harmonized_${environment}.device",
s"dhf_iot_harmonized_${environment}.vehicle"
)

// COMMAND ----------

//Harmonize tables optimization
harmonizeTablesList.foreach( table => {  
  val previous_date=LocalDate.now(ZoneOffset.UTC).minusDays(1).toString()
// var condition=s"where LOAD_DT >='${previous_date}'"
  val optimize_str= s"OPTIMIZE ${table} "
  val vacuum_str=s"VACUUM ${table} RETAIN 168 HOURS"
  println(s"OPTIMIZE starts for ${table}")
  spark.sql(optimize_str)
  println(s"OPTIMIZE completes for ${table} ") 

  println(s"VACUUM starts for ${table}")  
  spark.sql(vacuum_str)
  println(s"VACUUM completes for ${table} ") 
 
})