# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import *
import datetime
import io
import re
import json
import zipfile
from pyspark.sql.types import *
import boto3
from datetime import datetime as dt
from pyspark.sql import functions as f

# COMMAND ----------

schema=StructType([StructField("accelQuality",BooleanType(),True),StructField("fuelConsumption",StringType(),True),StructField("avgSpeed",StringType(),True),StructField("device",StructType([StructField("deviceIdentifier",StringType(),True),StructField("deviceIdentifierType",StringType(),True),StructField("deviceSerialNumber",StringType(),True),StructField("deviceType",StringType(),True)]),True),StructField("drivingDistance",StringType(),True),StructField("externalReferences",ArrayType(StructType([StructField("enterpriseReferenceExtraInfo",StringType(),True),StructField("enterpriseReferenceId",StringType(),True),StructField("type",StringType(),True)])),True),StructField("hdopAverage",StringType(),True),StructField("histogramScoringIntervals",ArrayType(StructType([StructField("occurrenceUnit",StringType(),True),StructField("occurrences",StringType(),True),StructField("roadType",StringType(),True),StructField("scoringComponent",StringType(),True),StructField("scoringComponentUnit",StringType(),True),StructField("scoringSubComponent",StringType(),True),StructField("thresholdLowerBound",StringType(),True),StructField("thresholdUpperBound",StringType(),True)])),True),StructField("maxSpeed",StringType(),True),StructField("measurementUnit",StructType([StructField("system",StringType(),True)]),True),StructField("milStatus",BooleanType(),True),StructField("scoring",StructType([StructField("individualComponentScores",ArrayType(StructType([StructField("component",StringType(),True),StructField("componentScore",StringType(),True)])),True),StructField("overallScore",StringType(),True),StructField("scoreAlgorithmProvider",StringType(),True),StructField("scoreUnit",StringType(),True)]),True),StructField("secondsOfIdling",StringType(),True),StructField("telemetryEvents",ArrayType(StructType([StructField("acceleration",StringType(),True),StructField("avgSpeed",StringType(),True),StructField("degreesLatitude",StringType(),True),StructField("degreesLongitude",StringType(),True),StructField("headingDegrees",StringType(),True),StructField("secondsOfDriving",StringType(),True),StructField("speed",StringType(),True),StructField("telemetryEventSeverityLevel",StringType(),True),StructField("telemetryEventType",StringType(),True),StructField("utcDateTime",StringType(),True)])),True),StructField("telemetryPoints",ArrayType(StructType([StructField("acceleration",StringType(),True),StructField("accelerometerData",StringType(),True),StructField("ambientTemperature",StringType(),True),StructField("barometericPressure",StringType(),True),StructField("coolantTemperature",StringType(),True),StructField("degreesLatitude",StringType(),True),StructField("degreesLongitude",StringType(),True),StructField("engineRPM",StringType(),True),StructField("fuelLevel",StringType(),True),StructField("hdop",StringType(),True),StructField("headingDegrees",StringType(),True),StructField("speed",StringType(),True),StructField("utcDateTime",StringType(),True)])),True),StructField("timeZoneOffset",StringType(),True),StructField("totalTripSeconds",StringType(),True),StructField("transportMode",StringType(),True),StructField("transportModeReason",StringType(),True),StructField("tripSummaryId",StringType(),True),StructField("utcEndDateTime",StringType(),True),StructField("utcStartDateTime",StringType(),True),StructField("vehicle",StructType([StructField("detectedVin",StringType(),True),StructField("enrolledVin",StringType(),True)]),True)])

# COMMAND ----------

schema_EVT=StructType([StructField("device",StructType([StructField("deviceIdentifier",StringType(),True),StructField("deviceIdentifierType",StringType(),True),StructField("deviceSerialNumber",StringType(),True),StructField("deviceType",StringType(),True)]),True),StructField("externalReferences",ArrayType(StructType([StructField("enterpriseReferenceExtraInfo",StringType(),True),StructField("enterpriseReferenceId",StringType(),True),StructField("type",StringType(),True)])),True),StructField("telemetryEvents",ArrayType(StructType([StructField("acceleration",StringType(),True),StructField("degreesLatitude",StringType(),True),StructField("degreesLongitude",StringType(),True),StructField("headingDegrees",StringType(),True),StructField("speed",StringType(),True),StructField("telemetryEventSeverityLevel",StringType(),True),StructField("telemetryEventType",StringType(),True),StructField("utcDateTime",StringType(),True)])),True),StructField("telemetrySetId",StringType(),True),StructField("vehicle",StructType([StructField("detectedVin",StringType(),True),StructField("enrolledVin",StringType(),True)]),True)])

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/dw-telematics-prod-read/warehouse/telematics_staging_db/smartmiles_staging_json/

# COMMAND ----------

json_df=spark.read.format("text").load("dbfs:/mnt/dw-telematics-prod-read/warehouse/telematics_staging_db/smartmiles_staging_json/source_cd=IMS/")
parse_df=json_df.withColumn("parse_col",from_json("value",schema=schema)).select("parse_col.*","batch_nb")
parse_evt_df=json_df.withColumn("parse_col",from_json("value",schema=schema_EVT)).select("parse_col.*","batch_nb")
display(json_df.select(min("batch_nb")))



# COMMAND ----------

tripsummary_df=parse_df.withColumn("externalReferences_explode",explode_outer(col("externalReferences"))).select("*","vehicle.*","device.*","measurementUnit.*","externalReferences_explode.*")
tripsummary_df.createOrReplaceTempView("tripsummaryview")


# COMMAND ----------

tripsummary_raw_df=spark.sql("""select cast(tripSummaryId as string),cast(fuelConsumption as string),cast(milStatus as boolean),cast(accelQuality as boolean),cast(transportMode as string),cast(secondsOfIdling as string),cast(transportModeReason as string),cast(hdopAverage as string),cast(utcStartDateTime as string),cast(utcEndDateTime as string),cast(timeZoneOffset as string),cast(drivingDistance as string),cast(maxSpeed as string),cast(avgSpeed as string),cast(totalTripSeconds as bigint),cast(deviceSerialNumber as string),cast(deviceIdentifier as string),cast(deviceIdentifierType as string),cast(deviceType as string),cast(enterpriseReferenceId as string),cast(enterpriseReferenceExtraInfo as string),cast(type as string),cast(enrolledVin as string),cast(detectedVin as string),cast(system as string),"IMS_SM_5X" as SourceSystem,
current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date from tripsummaryview
WHERE tripSummaryId IS NOT NULL and enrolledVin in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")""")

tripsummary_raw_df.write.format("delta").mode("append").saveAsTable("dhf_iot_ims_raw_prod.tripsummary")

# display(tripsummary_raw_df)

# COMMAND ----------

tripsummary_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL)  + coalesce((select max(TRIP_SUMMARY_ID) from dhf_iot_harmonized_prod.trip_summary),0) as BIGINT) as TRIP_SUMMARY_ID, coalesce(tripSummaryId, "NOKEY") AS TRIP_SMRY_KEY,  coalesce(deviceIdentifier,deviceSerialNumber,crc32(detectedvin)) AS DEVC_KEY, cast(accelQuality as STRING) AS ACLRTN_QLTY_FL, cast(hdopAverage as decimal(15,6)) AS AVG_HDOP_QTY, cast(avgSpeed as decimal(18,5)) AS AVG_SPD_MPH_RT, cast(drivingDistance as decimal(15,6)) AS DRVNG_DISTNC_QTY, cast(fuelConsumption as decimal(15,6)) AS FUEL_CNSMPTN_QTY, cast(secondsOfIdling as decimal(15,6)) AS IDLING_SC_QTY, cast(milStatus as STRING) AS MLFNCTN_STTS_FL, cast(maxSpeed as decimal(18,5)) AS MAX_SPD_RT, cast(system as STRING) AS MSURET_UNIT_CD, cast(enterpriseReferenceId as STRING) AS PLCY_NB, cast(timeZoneOffset as decimal(10,2)) as TIME_ZONE_OFFST_NB, cast(totalTripSeconds as decimal(15,6)) AS TRIP_SC_QTY, cast(transportMode as STRING) AS TRNSPRT_MODE_CD, cast(transportModeReason as STRING) AS TRNSPRT_MODE_RSN_CD, coalesce(cast(utcStartDateTime as TIMESTAMP), to_timestamp("9999-12-31")) AS TRIP_START_TS, coalesce(cast(utcEndDateTime as TIMESTAMP), to_timestamp("9999-12-31")) AS TRIP_END_TS, coalesce(enrolledVin,detectedVin) AS VEH_KEY, 
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"IMS_SM_5X" AS SRC_SYS_CD, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS,current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS from tripsummaryview
WHERE tripSummaryId IS NOT NULL and enrolledVin in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")""")

tripsummary_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_summary")
# display(tripsummary_harmonize_df)

# COMMAND ----------

device_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(DEVICE_ID) from dhf_iot_harmonized_prod.device),0) as BIGINT) as DEVICE_ID, coalesce(deviceIdentifier,deviceSerialNumber,crc32(detectedvin), "NOKEY") AS DEVC_KEY, deviceIdentifier as DEVC_ID_NB, deviceIdentifierType as DEVC_ID_TP_CD, deviceType as DEVC_TP_CD, deviceSerialNumber as DEVC_SRL_NB,coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
"IMS_SM_5X" AS SRC_SYS_CD, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from 
(select *,row_number() over (partition by deviceIdentifier,deviceSerialNumber order by batch_nb desc) as row_number from tripsummaryview WHERE tripSummaryId IS NOT NULL and enrolledVin in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")) where row_number=1 
""")

device_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.device")
# display(device_harmonize_df)

# COMMAND ----------

vehicle_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(VEHICLE_ID) from dhf_iot_harmonized_prod.vehicle),0) as BIGINT) as VEHICLE_ID, COALESCE(enrolledVin,detectedVin,"NOKEY") AS VEH_KEY, COALESCE(detectedVin,enrolledVin) as DTCTD_VIN_NB, COALESCE(enrolledVin,detectedVin,"NOKEY") as ENRLD_VIN_NB, coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"IMS_SM_5X" AS SRC_SYS_CD,current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS,  
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from 
(select *,row_number() over (partition by enrolledVin,detectedVin order by batch_nb desc) as row_number from tripsummaryview WHERE tripSummaryId IS NOT NULL and enrolledVin in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")) where row_number=1""")

vehicle_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.vehicle")
# display(vehicle_harmonize_df)

# COMMAND ----------

tripevent_df=parse_df.withColumn("telemetryEvents_explode",explode_outer(col("telemetryEvents"))).select("tripSummaryId","telemetryEvents_explode.*","batch_nb","vehicle.*")
tripevent_df.createOrReplaceTempView("tripevent")


# COMMAND ----------

tripevent_raw_df=spark.sql("""select cast(tripSummaryId as string),cast(degreesLatitude as string),cast(degreesLongitude as string),cast(headingDegrees as string),cast(telemetryEventType as string),cast(utcDateTime as string),cast(speed as string),cast(acceleration as string),cast(secondsOfDriving as string),cast(telemetryEventSeverityLevel as string),cast( tripevent.avgSpeed as string) as telemetryEvents__avgSpeed ,"IMS_SM_5X" as SourceSystem,
current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date from tripevent 
WHERE tripSummaryId IS NOT NULL and enrolledVin in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")""")

tripevent_raw_df.write.format("delta").mode("append").saveAsTable("dhf_iot_ims_raw_prod.telemetryevents")

# display(tripevent_raw_df)

# COMMAND ----------

tripevent_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(TRIP_EVENT_ID) from dhf_iot_harmonized_prod.trip_event),0) as BIGINT) as TRIP_EVENT_ID, cast(acceleration as decimal(18,5)) as ACLRTN_RT, cast(secondsOfDriving as decimal(15,6)) as DRVNG_SC_QTY, cast(telemetryEventSeverityLevel as STRING) as EVNT_SVRTY_CD,coalesce(telemetryEventType, "NOKEY") as EVNT_TP_CD, cast(headingDegrees as decimal(15,6)) as HEADNG_DEG_QTY, cast(speed as decimal(18,10)) as SPD_RT, tripSummaryId AS TRIP_SMRY_KEY, cast(tripevent.avgSpeed  as decimal(18,10)) as AVG_SPD_RT, coalesce(cast(utcDateTime as TIMESTAMP), to_timestamp("9999-12-31")) as UTC_TS, cast(degreesLatitude as decimal(18,10)) as LAT_NB, cast(degreesLongitude as decimal(18,10)) as LNGTD_NB,coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"IMS_SM_5X" AS SRC_SYS_CD, current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS , null as EVNT_STRT_TS, null as EVNT_END_TS, null as EVNT_DRTN_QTY from tripevent 
WHERE tripSummaryId IS NOT NULL and enrolledVin in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")""")

tripevent_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_event")
# display(tripevent_harmonize_df)

# COMMAND ----------

trippoint_df=parse_df.withColumn("telemetryPoints_explode",explode_outer(col("telemetryPoints"))).select("tripSummaryId","telemetryPoints_explode.*","batch_nb","vehicle.*")
trippoint_df.createOrReplaceTempView("trippoint")


# COMMAND ----------

trippoint_raw_df=spark.sql("""select cast(tripSummaryId as string),cast(engineRPM as string),cast(accelerometerData as string),cast(ambientTemperature as string),cast(barometericPressure as string),cast(coolantTemperature as string),cast(fuelLevel as string),cast(hdop as string),cast(utcDateTime as string),cast(degreesLatitude as string),cast(degreesLongitude as string),cast(speed as string),cast(headingdegrees as string),cast(acceleration as string),null as Accel_Longitudinal,null as Accel_Lateral,null as Accel_Vertical,"IMS_SM_5X" as SourceSystem,
current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date
 from trippoint
WHERE tripSummaryId IS NOT NULL and enrolledVin in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")""")

trippoint_raw_df.write.format("delta").mode("append").saveAsTable("dhf_iot_ims_raw_prod.telemetryPoints")
# display(trippoint_raw_df)

# COMMAND ----------

trippoint_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_prod.trip_point),0) as BIGINT) as TRIP_POINT_ID,cast(acceleration as decimal(18,5)) as ACLRTN_RT, cast(ambientTemperature as decimal(15,6)) as AMBNT_TMPRTR_QTY, cast(barometericPressure as decimal(15,6)) as BRMTRC_PRESSR_QTY, cast(coolantTemperature as decimal(15,6)) as COOLNT_TMPRTR_QTY, cast(engineRPM as decimal(18,5)) as ENGIN_RPM_RT, cast(fuelLevel as decimal(15,6)) as FUEL_LVL_QTY, cast(headingDegrees as decimal(15,6)) as HEADNG_DEG_QTY, coalesce(cast(utcDateTime as TIMESTAMP), to_timestamp("9999-12-31")) as UTC_TS, cast(speed as decimal(18,10)) as SPD_RT, cast(accelerometerData as string) as ACLRTMTR_DATA_RT, cast(degreesLatitude as decimal(18,10)) as LAT_NB, cast(degreesLongitude as decimal(18,10)) as LNGTD_NB,  tripSummaryId AS TRIP_SMRY_KEY, cast(hdop as decimal(18,10)) as HDOP_QTY, coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"IMS_SM_5X" AS SRC_SYS_CD, current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from trippoint
WHERE tripSummaryId IS NOT NULL and enrolledVin in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")""")

trippoint_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_point")
# display(trippoint_harmonize_df)

# COMMAND ----------

scoring_main_df=parse_df.select("tripSummaryId", "scoring.*","batch_nb","vehicle.*").withColumn("individualComponentScores_explode",explode_outer(col("individualComponentScores")))

scoring_df=scoring_main_df.select("*","batch_nb","individualComponentScores_explode.*")
scoring_df.createOrReplaceTempView("scoringview")


# COMMAND ----------

scoring_raw_df=spark.sql("""select DISTINCT cast(tripSummaryId as string),cast(scoreAlgorithmProvider as string),cast(scoreUnit as string),cast(overallScore as string),cast(component as string),cast(componentScore as string),"IMS_SM_5X" as SourceSystem,
current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date from scoringview where component is not null and componentScore is not null
and tripSummaryId IS NOT NULL and enrolledVin in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")""")

scoring_raw_df.write.format("delta").mode("append").saveAsTable("dhf_iot_ims_raw_prod.scoring")
# display(scoring_raw_df)

# COMMAND ----------

scoring_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(SCORING_ID) from dhf_iot_harmonized_prod.SCORING),0) as BIGINT) as SCORING_ID,tripSummaryId AS TRIP_SMRY_KEY, scoreAlgorithmProvider as SCR_ALGRM_PRVDR_NM, scoreUnit as SCR_UNIT_CD, cast(overallScore as decimal(18,10))as OVRL_SCR_QTY, coalesce(component, "NOKEY") as INDV_CMPNT_SET_TP_VAL, cast(coalesce(componentScore ,0) as decimal(18,10)) as INDV_CMPNT_SET_SCR_QTY,coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"IMS_SM_5X" AS SRC_SYS_CD, current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS  from scoringview
where component is not null and componentScore is not null
and tripSummaryId IS NOT NULL and enrolledVin in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")""")

scoring_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.SCORING")
# display(scoring_harmonize_df)

# COMMAND ----------

histogramScoringIntervals_df=parse_df.withColumn("histogramScoringIntervals_explode",explode_outer(col("histogramScoringIntervals"))).select("tripSummaryId", "histogramScoringIntervals_explode.*","batch_nb","vehicle.*")


histogramScoringIntervals_df.createOrReplaceTempView("histogramScoringIntervalsview")


# COMMAND ----------

histogramScoringIntervals_raw_df=spark.sql("""select cast(tripSummaryId as string),cast(scoringComponent as string),cast(scoringSubComponent as string),cast(roadType as string),cast(thresholdLowerBound as string),cast(thresholdUpperBound as string),cast(scoringComponentUnit as string),cast(occurrences as string),cast(occurrenceUnit as string),"IMS_SM_5X" as SourceSystem,
current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date from histogramScoringIntervalsview where scoringComponent is not null
and tripSummaryId IS NOT NULL and enrolledVin in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")""")

histogramScoringIntervals_raw_df.write.format("delta").mode("append").saveAsTable("dhf_iot_ims_raw_prod.histogramScoringIntervals")
# display(histogramScoringIntervals_raw_df)

# COMMAND ----------

histogramScoringIntervals_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL) +3000+ coalesce((select max(HISTOGRAM_SCORING_INTERVAL_ID) from dhf_iot_harmonized_prod.HISTOGRAM_SCORING_INTERVAL),0) as BIGINT) as HISTOGRAM_SCORING_INTERVAL_ID, tripSummaryId AS TRIP_SMRY_KEY, coalesce(scoringComponent, "NOKEY") as SCRG_CMPNT_TP_CD, scoringSubComponent as SCRG_SUB_CMPNT_TP_CD, roadType as ROAD_TP_DSC, cast(thresholdLowerBound as decimal(18,10)) as LOWER_BND_THRSHLD_QTY, cast(thresholdUpperBound as decimal(18,10)) as UPPER_BND_THRSHLD_QTY, scoringComponentUnit as SCRG_CMPNT_UNIT_CD, cast(occurrences as int) as OCRNC_CT, occurrenceUnit as OCRNC_UNIT_CD,coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT,"IMS_SM_5X" AS SRC_SYS_CD, current_timestamp() AS ETL_ROW_EFF_DTS,current_timestamp() as ETL_LAST_UPDT_DTS,coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS  from histogramScoringIntervalsview where scoringComponent is not null and tripSummaryId IS NOT NULL and enrolledVin in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")""")

histogramScoringIntervals_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.HISTOGRAM_SCORING_INTERVAL")
# display(histogramScoringIntervals_harmonize_df)

# COMMAND ----------

nonTripEvent_df=parse_evt_df.withColumn("externalReferences_explode",explode_outer(col("externalReferences"))).withColumn("telemetryEvents_explode",explode_outer(col("telemetryEvents"))).select("telemetrySetId","device.*","vehicle.*","externalReferences_explode.*","telemetryEvents_explode.*","batch_nb")


nonTripEvent_df.createOrReplaceTempView("nonTripEventview")


# COMMAND ----------

nonTripEvent_raw_df=spark.sql("""select cast(telemetrySetId as string),cast(degreesLatitude as string),cast(degreesLongitude as string),cast(headingDegrees as string),cast(telemetryEventType as string),cast(utcDateTime as string),cast(speed as string),cast(acceleration as string),cast(telemetryEventSeverityLevel as string),cast(enterpriseReferenceId as string),cast(enterpriseReferenceExtraInfo as string),cast(type as string),cast(deviceSerialNumber as string),cast(deviceIdentifier as string),cast(deviceIdentifierType as string),cast(deviceType as string),cast(enrolledVin as string),cast(detectedVin as string),"IMS_SM_5X" as SourceSystem,
current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date from nonTripEventview
WHERE telemetrySetId IS NOT NULL and enrolledVin in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")""")

nonTripEvent_raw_df.write.format("delta").mode("append").saveAsTable("dhf_iot_ims_raw_prod.nontripevent")
# display(nonTripEvent_raw_df)

# COMMAND ----------

nonTripEvent_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(NON_TRIP_EVENT_ID) from dhf_iot_harmonized_prod.non_trip_event),0) as BIGINT) as NON_TRIP_EVENT_ID, coalesce(telemetrySetId, "NOKEY") as NON_TRIP_EVNT_KEY,case when coalesce(deviceIdentifier,deviceSerialNumber,crc32(detectedvin)) is null then "NOKEY" ELSE coalesce(deviceIdentifier,deviceSerialNumber,crc32(detectedvin)) END DEVC_KEY, cast(acceleration as decimal(18,5)) as ACLRTN_RT, cast(telemetryEventSeverityLevel as STRING) as EVNT_SVRTY_CD, coalesce(telemetryEventType, "NOKEY") AS EVNT_TP_CD, cast(headingDegrees as decimal(15,6)) as HEADNG_DEG_QTY, cast(speed as decimal(18,5)) as SPD_MPH_RT, coalesce(cast(utcDateTime as TIMESTAMP), to_timestamp("9999-12-31")) as UTC_TS, coalesce(enrolledVin,detectedVin) AS VEH_KEY, cast(degreesLatitude as decimal(18,10)) as LAT_NB, cast(degreesLongitude as decimal(18,10)) as LNGTD_NB,coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"IMS_SM_5X" AS SRC_SYS_CD, current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from nonTripEventview
WHERE telemetrySetId IS NOT NULL and enrolledVin in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")""")

nonTripEvent_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.non_trip_event")
# display(nonTripEvent_harmonize_df)