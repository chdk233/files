// Databricks notebook source
// MAGIC %sql
// MAGIC select sourcesystem,max(load_hour) from dhf_iot_ims_raw_prod.telemetrypoints where load_hour !='9999-12-31T00:00:00.000+0000' group by sourcesystem;
// MAGIC -- select DISTINCT(load_date) from dhf_iot_Tims_raw_prod.tripSummary where sourcesystem='TIMS_SR' ORDER BY load_date DESC;
// MAGIC --   select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.scoring where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.histogram_scoring_interval where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_summary where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.vehicle where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.device where load_hr_ts !='9999-12-31T00:00:00.000+0000'group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_event where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_point where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.non_trip_event where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC   

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select LOAD_DATE,count(*) from dhf_iot_ims_raw_prod.telemetrypoints where  SOURCESYSTEM='IMS_SM_5X' GROUP BY LOAD_DATE ORDER BY LOAD_DATE DESC--load_hour !='9999-12-31T00:00:00.000+0000' group by sourcesystem;
// MAGIC -- select tripsummaryid,max(load_date) from dhf_iot_ims_raw_prod.tripSummary where  SOURCESYSTEM='IMS_SM_5X' and load_date'2022-08-31'  group by tripsummaryid having count(*) > 1;
// MAGIC -- select tripsummaryid,load_date from dhf_iot_ims_raw_prod.tripSummary where tripsummaryid in (select tripsummaryid  from dhf_iot_ims_raw_prod.tripSummary where  SOURCESYSTEM='IMS_SM_5X' and load_date>='2022-08-31') and  load_date<'2022-08-31'
// MAGIC select count(*) from dhf_iot_harmonized_prod.trip_point 

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/delta/curated/dhf_iot_curated_prod/TRIP_DETAIL",True)

// COMMAND ----------


schema_data=StructType([StructField("accelQuality",BooleanType(),True),StructField("fuelConsumption",StringType(),True),StructField("avgSpeed",StringType(),True),StructField("device",StructType([StructField("deviceIdentifier",StringType(),True),StructField("deviceIdentifierType",StringType(),True),StructField("deviceSerialNumber",StringType(),True),StructField("deviceType",StringType(),True)]),True),StructField("drivingDistance",StringType(),True),StructField("externalReferences",ArrayType(StructType([StructField("enterpriseReferenceExtraInfo",StringType(),True),StructField("enterpriseReferenceId",StringType(),True),StructField("type",StringType(),True)])),True),StructField("hdopAverage",StringType(),True),StructField("histogramScoringIntervals",ArrayType(StructType([StructField("occurrenceUnit",StringType(),True),StructField("occurrences",StringType(),True),StructField("roadType",StringType(),True),StructField("scoringComponent",StringType(),True),StructField("scoringComponentUnit",StringType(),True),StructField("scoringSubComponent",StringType(),True),StructField("thresholdLowerBound",StringType(),True),StructField("thresholdUpperBound",StringType(),True)])),True),StructField("maxSpeed",StringType(),True),StructField("measurementUnit",StructType([StructField("system",StringType(),True)]),True),StructField("milStatus",BooleanType(),True),StructField("scoring",StructType([StructField("individualComponentScores",ArrayType(StructType([StructField("component",StringType(),True),StructField("componentScore",DoubleType(),True)])),True),StructField("overallScore",DoubleType(),True),StructField("scoreAlgorithmProvider",StringType(),True),StructField("scoreUnit",StringType(),True)]),True),StructField("secondsOfIdling",LongType(),True),StructField("telemetryEvents",ArrayType(StructType([StructField("acceleration",StringType(),True),StructField("avgSpeed",StringType(),True),StructField("degreesLatitude",StringType(),True),StructField("degreesLongitude",StringType(),True),StructField("headingDegrees",LongType(),True),StructField("secondsOfDriving",LongType(),True),StructField("speed",StringType(),True),StructField("telemetryEventSeverityLevel",StringType(),True),StructField("telemetryEventType",StringType(),True),StructField("utcDateTime",StringType(),True)])),True),StructField("telemetryPoints",ArrayType(StructType([StructField("acceleration",StringType(),True),StructField("accelerometerData",StringType(),True),StructField("ambientTemperature",StringType(),True),StructField("barometericPressure",StringType(),True),StructField("coolantTemperature",LongType(),True),StructField("degreesLatitude",StringType(),True),StructField("degreesLongitude",StringType(),True),StructField("engineRPM",StringType(),True),StructField("fuelLevel",StringType(),True),StructField("hdop",StringType(),True),StructField("headingDegrees",LongType(),True),StructField("speed",StringType(),True),StructField("utcDateTime",StringType(),True)])),True),StructField("timeZoneOffset",StringType(),True),StructField("totalTripSeconds",LongType(),True),StructField("transportMode",StringType(),True),StructField("transportModeReason",StringType(),True),StructField("tripSummaryId",StringType(),True),StructField("utcEndDateTime",StringType(),True),StructField("utcStartDateTime",StringType(),True),StructField("vehicle",StructType([StructField("detectedVin",StringType(),True),StructField("enrolledVin",StringType(),True)]),True)])

val df=spark.read.format("text").load("dbfs:/mnt/dw-telematics-prod-read/warehouse/telematics_staging_db/smartmiles_staging_json/source_cd=IMS")
display(df)

// COMMAND ----------

val df=spark.read.format("text").load("dbfs:/mnt/dw-telematics-prod-read/warehouse/telematics_staging_db/smartmiles_staging_json/source_cd=FMC")
display(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE dhf_iot_curated_prod.trip_detail (
// MAGIC   TRIP_DETAIL_ID BIGINT NOT NULL,
// MAGIC   ENRLD_VIN_NB STRING NOT NULL,
// MAGIC   DEVC_KEY STRING NOT NULL,
// MAGIC   SRC_SYS_CD STRING NOT NULL,
// MAGIC   PE_STRT_TS TIMESTAMP NOT NULL,
// MAGIC   PE_END_TS TIMESTAMP,
// MAGIC   PE_STRT_LOCAL_TS TIMESTAMP NOT NULL,
// MAGIC   PE_END_LOCAL_TS TIMESTAMP,
// MAGIC   MILE_CT DECIMAL(18,10),
// MAGIC   ADJST_MILE_CT DECIMAL(15,10),
// MAGIC   PLSBL_MILE_CT DECIMAL(15,10),
// MAGIC   KM_CT DECIMAL(18,10),
// MAGIC   NIGHT_TIME_DRVNG_SC_CT INT,
// MAGIC   FAST_ACLRTN_CT INT,
// MAGIC   HARD_BRKE_CT INT,
// MAGIC   DRVNG_SC_CT INT,
// MAGIC   IDLE_SC_CT INT,
// MAGIC   STOP_SC_CT INT,
// MAGIC   PLSBL_DRIV_SC_CT INT,
// MAGIC   PLSBL_IDLE_SC_CT BIGINT,
// MAGIC   IDLE_TIME_RATIO DECIMAL(20,15),
// MAGIC   TRIP_SC_JSON_TT STRING,
// MAGIC   TRIP_SMRY_KEY STRING NOT NULL,
// MAGIC   TIME_INTRV_1_SCR_QTY DECIMAL(15,6),
// MAGIC   TIME_INTRV_2_SCR_QTY DECIMAL(15,6),
// MAGIC   TIME_INTRV_3_SCR_QTY DECIMAL(15,6),
// MAGIC   TIME_INTRV_4_SCR_QTY DECIMAL(15,6),
// MAGIC   TIME_INTRV_5_SCR_QTY DECIMAL(15,6),
// MAGIC   TIME_INTRV_6_SCR_QTY DECIMAL(15,6),
// MAGIC   TIME_INTRV_7_SCR_QTY DECIMAL(15,6),
// MAGIC   TIME_INTRV_8_SCR_QTY DECIMAL(15,6),
// MAGIC   LOAD_HR_TS TIMESTAMP NOT NULL,
// MAGIC   LOAD_DT DATE NOT NULL,
// MAGIC   ETL_ROW_EFF_DTS TIMESTAMP NOT NULL,
// MAGIC   ETL_LAST_UPDT_DTS TIMESTAMP)
// MAGIC USING delta
// MAGIC PARTITIONED BY (SRC_SYS_CD, LOAD_DT, LOAD_HR_TS)
// MAGIC LOCATION 's3://pcds-databricks-common-785562577411/iot/delta/curated/dhf_iot_curated_prod/TRIP_DETAIL'
// MAGIC

// COMMAND ----------

// MAGIC
// MAGIC %sh
// MAGIC lcd='/tmp'
// MAGIC todir='DWCI.SRP'
// MAGIC ftp_file_1='IOTA57.TEST'
// MAGIC length1=14
// MAGIC echo "Connecting to ${ftp_host}."
// MAGIC ftp_host='GM1.ENT.NWIE.NET'
// MAGIC ftp_user='DWCIDFTP'
// MAGIC ftp_pass='V5C6TG87'
// MAGIC #ftp_user='TLMCDB2'
// MAGIC #ftp_pass='VDFNQ0Q4MjEK'
// MAGIC ftp -inv ${ftp_host} <<!END
// MAGIC user ${ftp_user} ${ftp_pass}
// MAGIC lcd ${lcd}
// MAGIC cd ..
// MAGIC cd ${todir}
// MAGIC site lrecl=${length1} recfm=fb
// MAGIC put ${ftp_file_1}
// MAGIC quit
// MAGIC !END
// MAGIC
// MAGIC

// COMMAND ----------



// COMMAND ----------

import sqlContext.implicits._ 
import org.apache.spark.sql.functions._
import io.delta.tables._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel._

def addSurrogateKey_clt(df:DataFrame, target:String ) = {   
  
    val maxSK = spark.table(s"${target}").count 
    val w  = Window.orderBy($"ETL_ROW_EFF_DTS")
    df.withColumn("ETL_TRIP_SUMMARY_ID", row_number.over(w) + maxSK)       
}

def addAuditColumns_clt(df: DataFrame, targetKey: String) = {
       
    val w = Window.partitionBy(targetKey.split(",").map(colName => col(colName)): _*).orderBy($"ETL_ROW_EFF_DTS")
    val auditDF = df.withColumn("ETL_ROW_EXP_DTS", lead($"ETL_ROW_EFF_DTS", 1, "9999-12-31").over(w)).withColumn("ETL_CURR_ROW_FL", lit("N")).withColumn("ETL_ADD_DTS", current_timestamp()).withColumn("ETL_LAST_UPDATE_DTS", current_timestamp()).withColumn("ETL_CURR_ROW_FL", when($"ETL_ROW_EXP_DTS" === "9999-12-31", "Y").otherwise("N"))    
  auditDF
}

def addHashColumn_clt(viewname: String) = {
  var allCols = spark.sql(s"select * from global_temp.${viewname}").schema.fieldNames.toList
  println("allCols: "+ allCols)
  val hash_exclude_cols = List("_topic_","_partition_","_kafkaoffset_","_kafkatimestamp_","md5_hash","ETL_TRIP_SUMMARY_ID") ++ List("ETL_ROW_EFF_DTS","ETL_ROW_EXP_DTS","ETL_CURR_ROW_FL","ETL_ADD_DTS","ETL_LAST_UPDATE_DTS")
  println("hash_exclude_cols: "+ hash_exclude_cols)
  val hash_cols = allCols.map(_.toLowerCase) diff hash_exclude_cols.map(_.toLowerCase)
  println("hash_cols: "+ hash_cols)  
  val cols=hash_cols.mkString(",")
  val qry=s""" select *,hash(${cols}) as md5_hash from global_temp.${viewname} """  
  spark.sql(qry)  
}

def removeDuplicates_clt(df: DataFrame, target: String, targetKey: String) = {
    print("in removeduplicates function2 \n")
 
    val w = Window.partitionBy(targetKey.split(",").map(colName => col(colName)): _*).orderBy($"ETL_ROW_EFF_DTS")
    val firsRowDF = df.withColumn("rownum", row_number.over(w)).where($"rownum" === 1).drop("rownum")   
    print(firsRowDF.schema.fieldNames.toList) 
   
    val latestInTargetDF = firsRowDF.join(DeltaTable.forName(spark, target).toDF.as("targetDF"), targetKey.split(",").toSeq, "right").filter($"targetDF.ETL_CURR_ROW_FL" === 'Y').select($"targetDF.*").select(firsRowDF.schema.fieldNames.toList.map(col): _*)  
    print("\n latestInTargetDF cols")
    print(latestInTargetDF.schema.fieldNames.toList)  
 
    //add target latest rows to microbatch
    val df2 = df.withColumn("isTarget", lit(0)).union(latestInTargetDF.withColumn("isTarget", lit(1)))
    df2.show()
    // remove consecutive duplicates, choose first 
    val w3 = Window.partitionBy(targetKey.split(",").map(colName => col(colName)): _*).orderBy(col("ETL_ROW_EFF_DTS").asc,col("isTarget").desc)
    val df3 = df2.withColumn("dupe",  $"md5_hash" === lag($"md5_hash", 1).over(w3))
            .where($"dupe" === false || $"dupe".isNull ).drop("dupe") 
  
    // If there is only 1 row for a key, its a Target's duplicate with no other new rows. Ignore it.
    val w2 =  Window.partitionBy(targetKey.split(",").map(colName => col(colName)): _*)
    val df4 = df3.withColumn("count",  count($"*").over(w2)).where($"count" =!= 1 || ($"isTarget" =!= 1  && $"count" === 1)).drop("count").drop("isTarget")
  
    df4.show()
    df4    
}

def defaultMerge_clt (df: DataFrame, target: String) = {
  
    DeltaTable.forName(spark, target)
    .as("events")
    .merge(
      df.as("updates"),
      "events.md5_hash = updates.md5_hash AND events.ETL_ROW_EFF_DTS = updates.ETL_ROW_EFF_DTS") 
    .whenMatched
    .updateExpr( 
        Map("ETL_ROW_EXP_DTS" -> "updates.ETL_ROW_EXP_DTS",
            "ETL_CURR_ROW_FL" -> "'N'",
            "ETL_LAST_UPDATE_DTS" -> "current_timestamp()"
           )
      )
    .whenNotMatched
    .insertAll()
    .execute()   
}




// COMMAND ----------

// MAGIC %sql
// MAGIC delete from dhf_iot_cmtpl_raw_prod.job_run_log where job_type not in ('telematics_realtime','telematics_enrollment')

// COMMAND ----------

// MAGIC %sql
// MAGIC delete from dhf_iot_cmtpl_raw_prod.job_run_log where job_type  in ('telematics_realtime')

// COMMAND ----------

val df=spark.read.table("dhf_iot_sm_ims_raw_dev.smart_miles_tripsummary")//.withColumn("ETL_ROW_EFF_DTS",$"db_load_time").where($"tripSummaryId"==="20527a5e-926d-430c-9246-4b5043d6c129")

df.createOrReplaceTempView("table")
val fin= spark.sql(""" SELECT concat('IMS_',tripSummaryId) AS TRIP_SUMMARY_KEY,
        'IMS'    AS SOUCE_SYSTEM,
		concat('IMS_',deviceSerialNumber) AS DEVICE_KEY,
		concat('IMS_',enrolledVin) AS VEH_KEY,
		accelQuality AS ACCELERATION_QUALITY_IND,
		hdopAverage AS AVERAGE_HDOP_QTY,
		avgSpeed AS AVERAGE_SPEED_MPH_RT,
		drivingDistance AS DRIVING_DISTANCE_MILE_QTY,
		fuelConsumption AS FUEL_COMSUMPTION_QTY,
		secondsOfIdling AS IDLING_SECOND_QTY,
		milStatus AS MALFUNCTION_STATUS_IND,
		maxSpeed AS MAX_SPEED_MPH_RT,
		system AS MEASUREMENT_UNIT_CD,
		enterpriseReferenceId AS POL_NB,
		tripSummaryId AS TRIP_SUMMARY_NB,
		timeZoneOffset AS TIME_ZONE_OFFSET_NUMBER,
		totalTripSeconds AS TRIP_SECOND_QTY,
		transportMode AS TRANSPORT_MODE_CD,
		transportModeReason AS TRANSPORT_MODE_REASON_CD,
		utcStartDateTime AS TRIP_START_TS,
		utcEndDateTime AS TRIP_END_TS,
		db_load_time AS ETL_ROW_EFF_DTS from table

		
""")
fin.createOrReplaceGlobalTempView(s"view")
val addhash=addHashColumn_clt("view")

val dedup=removeDuplicates_clt(addhash,"generic_csv_load.test_k_TRIP_SUMMARY_4","TRIP_SUMMARY_NB")
val audit=addAuditColumns_clt(dedup,"TRIP_SUMMARY_NB")
val surr=addSurrogateKey_clt(audit,"generic_csv_load.test_k_TRIP_SUMMARY_4")


surr.write.format("delta").mode("append").saveAsTable("generic_csv_load.test_k_TRIP_SUMMARY_4")
defaultMerge_clt(surr,"generic_csv_load.test_k_TRIP_SUMMARY_4")

// COMMAND ----------

// MAGIC %sql
// MAGIC select *
// MAGIC          
// MAGIC               from  generic_csv_load.test_k_TRIP_SUMMARY_4 WHERE TRIP_SUMMARY_NB  ='00076ae8-a11c-4f0f-9cb0-5abe6a049c00'

// COMMAND ----------

// MAGIC %sql
// MAGIC --select CAST(concat('IMS_',tripSummaryId) AS STRING ) AS TR, md5('tr||tripSummaryId||fuelConsumption') from dhf_iot_cmtpl_raw_dev.ims_harmonize_unitTest  where tripSummaryId="3eee6c82-70ae-442a-a309-3ee0aa83ec3f"
// MAGIC --select count((surrogatekey)) from  dhf_iot_cmtpl_raw_dev.ims_harmonize_unitTest
// MAGIC select md5(SOUCE_SYSTEM||DEVICE_KEY||VEH_KEY||ACCELERATION_QUALITY_IND||AVERAGE_HDOP_QTY||AVERAGE_SPEED_MPH_RT||DRIVING_DISTANCE_MILE_QTY||IDLING_SECOND_QTY||MALFUNCTION_STATUS_IND||MAX_SPEED_MPH_RT||MEASUREMENT_UNIT_CD||POL_NB||TRIP_SUMMARY_NB||TIME_ZONE_OFFSET_NUMBER||TRIP_SECOND_QTY||TRANSPORT_MODE_CD||TRANSPORT_MODE_REASON_CD||TRIP_START_TS||TRIP_END_TS) AS MD5,TRIP_SUMMARY_NB
// MAGIC          
// MAGIC               from  generic_csv_load.test_k_TRIP_SUMMARY_4 where TRIP_SUMMARY_NB is null
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC DESC  generic_csv_load.test_k_TRIP_SUMMARY_4

// COMMAND ----------


import sqlContext.implicits._ 
import org.apache.spark.sql.functions._
import io.delta.tables._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel._
val df= spark.read.table("dhf_iot_cmtpl_raw_dev.ims_harmonize_unitTest").where(col("tripSummaryId")==="00033725-1941-49b0-bc4a-9c607f7c3f9e").drop("fuelConsumption","transportMode")
val df2=df.withColumn("fuelConsumption",lit(1)).withColumn("transportMode",lit("ter"))
val table=
DeltaTable.forName(spark, "dhf_iot_cmtpl_raw_dev.ims_harmonize_unitTest")
      .as("events")
      .merge(
        df.as("updates"),
        s"events.tripSummaryId = updates.tripSummaryId ")
      .whenMatched("events.db_load_date >= date_sub(current_date(),1)")
      .updateExpr( 
        Map("tripSummaryId" -> "updates.tripSummaryId")
      )
      .execute()  

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_cmtpl_raw_dev.ims_harmonize_unitTest where db_load_date in (date_sub(current_date(),1),current_date(),"2021-09-24")

// COMMAND ----------

val df= spark.read.table("dhf_iot_cmtpl_raw_dev.ims_harmonize_unitTest")
//df.withColumn("concat",concat(lit("IMS_"), col("tripSummaryId"))).select(df.col("*"),$"concat".as("co")).show

df.createOrReplaceTempView("view")
val fin= spark.sql(""" SELECT distinct concat('IMS_',tripSummaryId) AS TRIP_SUMMARY_KEY,
        'IMS'    AS SOURCE_SYSTEM,
		concat('IMS_',deviceSerialNumber) AS DEVICE_KEY,
		concat('IMS_',enrolledVin) AS VEH_KEY,
		accelQuality AS ACCELERATION_QUALITY_FL,
		hdopAverage AS AVERAGE_HDOP_QTY,
		avgSpeed AS AVERAGE_SPEED_RT,
		drivingDistance AS DRIVING_DISTANCE_QTY,
		fuelConsumption AS FUEL_CONSUMPTION_QTY,
		secondsOfIdling AS IDLING_SECOND_QTY,
		milStatus AS MALFUNCTION_STATUS_FL,
		maxSpeed AS MAX_SPEED_RT,
		system AS MEASUREMENT_UNIT_CD,
		enterpriseReferenceId AS POL_NB,
		tripSummaryId AS Source_Trip_Id,
		timeZoneOffset AS TIME_ZONE_OFFSET_NB,
		totalTripSeconds AS TRIP_SECOND_QTY,
		transportMode AS TRANSPORT_MODE_CD,
		transportModeReason AS TRANSPORT_MODE_REASON_CD,
		utcStartDateTime AS TRIP_START_TS,
		utcEndDateTime AS TRIP_END_TS,
		db_load_time AS ETL_ROW_EFF_DTS,
		md5('SOURCE_SYSTEM||DEVICE_KEY||VEH_KEY||ACCELERATION_QUALITY_FL||AVERAGE_HDOP_QTY||AVERAGE_SPEED_RT||DRIVING_DISTANCE_QTY||FUEL_CONSUMPTION_QTY||IDLING_SECOND_QTY||MALFUNCTION_STATUS_FL||MAX_SPEED_RT||MEASUREMENT_UNIT_CD||POL_NB||Source_Trip_Id||TIME_ZONE_OFFSET_NB||TRIP_SECOND_QTY||TRANSPORT_MODE_CD||TRANSPORT_MODE_REASON_CD||TRIP_START_TS||TRIP_END_TS')
         from view

	


		
""")
fin.printSchema()
// fin.createOrReplaceGlobalTempView(s"views")
// var allCols = spark.sql(s"select * from global_temp.views").schema.fieldNames.toList
//   //println("allCols: "+ allCols)
//   val hash_exclude_cols = List("_topic_","_partition_","_kafkaoffset_","_kafkatimestamp_","row_hashed","surrogatekey") ++ List("ETL_ROW_EFF_DTS","ETL_ROW_EXP_DTS","ETL_CURR_ROW_FL","ETL_ADD_DTS","ETL_LAST_UPDATE_DTS")
//   println("hash_exclude_cols: "+ hash_exclude_cols)
//   val hash_cols = allCols.map(_.toLowerCase) diff hash_exclude_cols.map(_.toLowerCase)
//   println("hash_cols: "+ hash_cols)  
//   val cols=hash_cols.mkString("||")
// println("cols: "+ cols)
//   val qry=s""" select *,md5(${cols}) as row_hashed from global_temp.views """  
//   spark.sql(qry).select($"TRIP_SUMMARY_KEY",$"row_hashed").show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE dhf_iot_cmtpl_raw_dev.unit_test_harmonize (
// MAGIC 		TRIP_SUMMARY_ID BIGINT  ,
// MAGIC 		TRIP_SUMMARY_KEY STRING  ,
// MAGIC 		ETL_ROW_EFF_DTS	TIMESTAMP,
// MAGIC         ETL_ROW_EXP_DTS	TIMESTAMP,
// MAGIC 		ETL_CURR_ROW_FL STRING ,
// MAGIC 		ETL_ADD_DTS TIMESTAMP,
// MAGIC 		ETL_LAST_UPDATE_DTS TIMESTAMP ,
// MAGIC 		HASH INT,
// MAGIC 		SOURCE_SYSTEM STRING ,
// MAGIC 		VEH_KEY STRING ,
// MAGIC 		DEVICE_KEY STRING,
// MAGIC 		ACCELERATION_QUALITY_FL BOOLEAN ,
// MAGIC 		AVERAGE_HDOP_QTY STRING,
// MAGIC 		AVERAGE_SPEED_RT STRING,
// MAGIC 		DRIVING_DISTANCE_QTY STRING,
// MAGIC 		FUEL_CONSUMPTION_QTY STRING,
// MAGIC 		IDLING_SECOND_QTY BIGINT,
// MAGIC 		MALFUNCTION_STATUS_FL BOOLEAN ,
// MAGIC 		MAX_SPEED_RT STRING,
// MAGIC 		MEASUREMENT_UNIT_CD STRING ,
// MAGIC 		POL_NB STRING,
// MAGIC 		Source_Trip_Id STRING ,
// MAGIC 		TIME_ZONE_OFFSET_NB STRING,
// MAGIC 		TRIP_SECOND_QTY BIGINT,
// MAGIC 		TRANSPORT_MODE_CD STRING ,
// MAGIC 		TRANSPORT_MODE_REASON_CD STRING ,
// MAGIC 		TRIP_START_TS string,
// MAGIC 		TRIP_END_TS string
// MAGIC 		
// MAGIC 	) using delta

// COMMAND ----------

val df =spark.read.table("dhf_iot_cmtpl_raw_dev.ims_harmonize_unitTest")
display(df)

// COMMAND ----------

var maz:Long=0
val df =spark.read.table("dhf_iot_harmonized_dev.backup").count
if (df.equals(0)){
  maz=0
}
else {
  print(2)
  maz=spark.read.table("dhf_iot_harmonized_dev.backup").sort(col("TRIP_SUMMARY_ID").desc).select("TRIP_SUMMARY_ID").first().getLong(0)+1
}

//val df2=df.sort(col("TRIP_SUMMARY_ID").desc).select("TRIP_SUMMARY_ID").first().getLong(0)+1
print(maz)
//display(df)

// COMMAND ----------

val sk=spark.read.table("dhf_iot_cmtpl_raw_dev.ims_harmonize_unitTest").count
val account_id= sk match {
  case 0  => 0
  case _ => spark.read.table("dhf_iot_cmtpl_raw_dev.ims_harmonize_unitTest").sort(col("surrogatekey").desc).select("surrogatekey").first().getLong(0)+1
}


// COMMAND ----------

def addSurrogateKey_cl(df:DataFrame, columnName: String,target:String ) = {   
  
    val maxSK = spark.table(s"${target}").count
    val account_id= sk match {
      case 0  => 0
      case _  => spark.read.table(s"${target}").sort(col(s"${columnName}").desc).select(s"${columnName}").first().getLong(0)+1
 }
    val w  = Window.orderBy($"ETL_ROW_EFF_DTS")
    df.withColumn("ETL_ADD_DTS", current_timestamp()).withColumn((s"${columnName}"), row_number.over(w) + maxSK)       
}

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE dhf_iot_harmonized_dev.DEVICE (
// MAGIC 		DEVICE_ID BIGINT  ,
// MAGIC 		DEVICE_KEY STRING  ,
// MAGIC 		ETL_ROW_EFF_DTS	TIMESTAMP,
// MAGIC 		ETL_ADD_DTS TIMESTAMP,
// MAGIC 		ETL_LAST_UPDATE_DTS TIMESTAMP ,
// MAGIC 		SOURCE_SYSTEM STRING ,
// MAGIC 		DEVICE_IDENTIFIER_NB STRING ,
// MAGIC         DEVICE_IDENTIFIER_TYPE_CD STRING ,
// MAGIC         DEVICE_TYPE_CD STRING ,
// MAGIC         SERIAL_NB STRING  
// MAGIC 		
// MAGIC 	) using delta

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from   dhf_iot_harmonized_dev.trip_summary

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE dhf_iot_harmonized_dev.VEHICLE (
// MAGIC 		VEHICLE_ID BIGINT  ,
// MAGIC 		VEHICLE_KEY STRING  ,
// MAGIC 		ETL_ROW_EFF_DTS	TIMESTAMP,
// MAGIC 		ETL_ADD_DTS TIMESTAMP,
// MAGIC 		SOURCE_SYSTEM STRING ,
// MAGIC 		DETECTED_VIN_NB STRING ,
// MAGIC         ENROLLED_VIN_NB STRING 
// MAGIC 		
// MAGIC 	) using delta

// COMMAND ----------

import scala.reflect.io.Directory
import java.io.File

//val directory = new Directory(new File("/dbfs/user/hive/warehouse/dhf_dev/checkpoint_dir/dev/eventgeneration/14713"))
val directory = new Directory(new File("/dbfs/user/hive/warehouse/dhf_dev/checkpoint_dir/dev/eventharmonizer/14707"))
directory.deleteRecursively()

// COMMAND ----------

spark.sql(s"""CREATE DATABASE IF NOT EXISTS dhf_iot_harmonized_${environment} LOCATION 's3://pcds-databricks-common-${account_id}/iot/delta/harmonized/dhf_iot_harmonized_${environment}';""")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_sm_ims_raw_dev.trip_log;

// COMMAND ----------

// MAGIC %sql
// MAGIC --select count(*) from dhf_iot_sm_ims_raw_dev.sm_ims_non_trip_event;
// MAGIC --delete from dhf_iot_sm_ims_raw_dev.sm_ims_scoring;
// MAGIC --delete from dhf_iot_sm_ims_raw_dev.sm_ims_telemetryevents;
// MAGIC --delete from dhf_iot_sm_ims_raw_dev.sm_ims_histogramscoring;
// MAGIC --delete from dhf_iot_sm_ims_raw_dev.sm_ims_telemetrypoints;
// MAGIC --select count(*) from dhf_iot_sm_ims_raw_dev.sm_ims_tripsummary;
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC --select count(*)  from dhf_iot_harmonized_dev.trip_summary;
// MAGIC --delete from dhf_iot_harmonized_dev.trip_summary;
// MAGIC --delete from dhf_iot_harmonized_dev.trip_event;
// MAGIC --delete from dhf_iot_harmonized_dev.trip_point;
// MAGIC --delete from dhf_iot_harmonized_dev.device;
// MAGIC --delete from dhf_iot_harmonized_dev.vehicle
// MAGIC --delete from dhf_iot_harmonized_dev.non_trip_event;
// MAGIC --select count(*) from dhf_iot_harmonized_dev.non_trip_event;
// MAGIC --select * from dhf_iot_sm_ims_raw_dev.sm_ims_tripSummary--where detectedVin is null
// MAGIC --select count(*) from dhf_iot_sm_ims_raw_dev.sm_ims_non_trip_event;
// MAGIC --select count(*) from dhf_iot_sm_ims_raw_dev.sm_ims_scoring;
// MAGIC desc dhf_iot_sm_ims_raw_dev.sm_ims_tripsummary;
// MAGIC --select count(*) from dhf_iot_sm_ims_raw_dev.sm_ims_histogramscoring;
// MAGIC --select count(*) from dhf_iot_sm_ims_raw_dev.sm_ims_telemetrypoints;
// MAGIC --select count(*) from dhf_iot_sm_ims_raw_dev.sm_ims_tripsummary;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_dev.trip_summary where TRIP_SUMMARY_ID=7700;
// MAGIC --select * from dhf_iot_harmonized_dev.trip_event;
// MAGIC --select * from dhf_iot_harmonized_dev.trip_point;
// MAGIC --select * from dhf_iot_harmonized_dev.device;
// MAGIC --select * from dhf_iot_harmonized_dev.vehicle;
// MAGIC --select * from dhf_iot_harmonized_dev.non_trip_event;
// MAGIC --select * from dhf_iot_sm_ims_raw_dev.sm_ims_tripSummary--where detectedVin is null

// COMMAND ----------

// MAGIC %sql
// MAGIC --desc dhf_iot_sm_ims_raw_dev.sm_ims_non_trip_event
// MAGIC --desc dhf_iot_sm_ims_raw_dev.sm_ims_scoring
// MAGIC desc dhf_iot_sm_ims_raw_dev.sm_ims_telemetryevents
// MAGIC --desc dhf_iot_sm_ims_raw_dev.sm_ims_histogramscoring
// MAGIC --desc dhf_iot_sm_ims_raw_dev.sm_ims_telemetrypoints
// MAGIC --desc dhf_iot_sm_ims_raw_dev.sm_ims_tripsummary