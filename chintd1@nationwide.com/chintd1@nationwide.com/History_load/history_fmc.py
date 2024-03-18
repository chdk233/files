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

schema=StructType([StructField("comment",StringType(),True),StructField("sourceCode",StringType(),True),StructField("telemetryPoints",ArrayType(StructType([StructField("accelerometerDataLat",StringType(),True),StructField("accelerometerDataLong",StringType(),True),StructField("accelerometerDataZ",StringType(),True),StructField("altitude",StringType(),True),StructField("degreesLatitude",StringType(),True),StructField("degreesLongitude",StringType(),True),StructField("headingDegrees",StringType(),True),StructField("ignitionEvent",StringType(),True),StructField("ignitionStatus",StringType(),True),StructField("odometer",StringType(),True),StructField("received",LongType(),True),StructField("speed",StringType(),True),StructField("tripFuelConsumed",StringType(),True),StructField("tripFuelConsumedIdle",StringType(),True),StructField("tripMaxSpeed",StringType(),True),StructField("tripTotalEngineTime",StringType(),True),StructField("tripTotalEngineTimeIdle",StringType(),True),StructField("tripDistanceAccumulated",StringType(),True),StructField("utcDateTime",StringType(),True)])),True),StructField("timeZoneOffset",StringType(),True),StructField("tripSummaryId",StringType(),True),StructField("utcEndDateTime",StringType(),True),StructField("utcStartDateTime",StringType(),True),StructField("vehicle",StructType([StructField("detectedVin",StringType(),True)]),True)])

schema2=StructType([StructField("comment",StringType(),True),StructField("sourcecode",StringType(),True),StructField("telemetrypoints",ArrayType(StructType([StructField("accelerometerdatalat",StringType(),True),StructField("accelerometerdatalong",StringType(),True),StructField("accelerometerdataz",StringType(),True),StructField("altitude",StringType(),True),StructField("degreeslatitude",StringType(),True),StructField("degreeslongitude",StringType(),True),StructField("headingdegrees",StringType(),True),StructField("ignitionevent",StringType(),True),StructField("ignitionstatus",StringType(),True),StructField("odometer",StringType(),True),StructField("received",LongType(),True),StructField("speed",StringType(),True),StructField("tripfuelconsumed",StringType(),True),StructField("tripfuelconsumedidle",StringType(),True),StructField("tripmaxspeed",StringType(),True),StructField("triptotalenginetime",StringType(),True),StructField("triptotalenginetimeidle",StringType(),True),StructField("tripdistanceaccumulated",StringType(),True),StructField("utcdatetime",StringType(),True)])),True),StructField("timezoneoffset",StringType(),True),StructField("tripsummaryid",StringType(),True),StructField("utcenddatetime",StringType(),True),StructField("utcstartdatetime",StringType(),True),StructField("vehicle",StructType([StructField("detectedvin",StringType(),True)]),True)])

# COMMAND ----------

json_df=spark.read.format("text").load("dbfs:/mnt/dw-telematics-prod-read/warehouse/telematics_staging_db/smartmiles_staging_json/source_cd=FMC/")
parse_df=json_df.withColumn("parse_col",from_json("value",schema=schema)).select("parse_col.*","batch_nb")
parse_df2=json_df.withColumn("parse_col",from_json("value",schema=schema2)).select("parse_col.*","batch_nb")

# (json_df.filter("value  not like '%tripSummaryId%'").count())


# COMMAND ----------

tripsummary_df=parse_df.withColumn("telemetryPoints_explode",explode_outer(col("telemetryPoints"))).select("*","vehicle.*","telemetryPoints_explode.*")
tripsummary_df.createOrReplaceTempView("tripsummary_view")
tripsummary_df2=parse_df2.withColumn("telemetryPoints_explode",explode_outer(col("telemetrypoints"))).select("*","vehicle.*","telemetryPoints_explode.*")
tripsummary_df2.createOrReplaceTempView("tripsummary_view2")





# COMMAND ----------

tripsummary_raw_df=spark.sql("""select cast(tripSummaryId as string),cast(tripFuelConsumed as string),cast(tripTotalEngineTimeIdle as string),cast(utcStartDateTime as string),cast(utcEndDateTime as string),cast(timeZoneOffset as string),cast(tripDistanceAccumulated as string),cast(tripMaxSpeed as string),cast(tripTotalEngineTime as string),cast(detectedVin as string),sourceCode,comment,cast(tripFuelConsumedIdle as string),"FMC_SR" as sourcesystem,current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date from (select *,row_number() over (partition by tripSummaryId order by tripMaxSpeed desc) as rn from tripsummary_view) where rn=1 and tripSummaryId is not null
 
""")

tripsummary_raw_df.write.format("delta").mode("append").saveAsTable("dhf_iot_fmc_raw_prod.tripsummary")

# display(tripsummary_raw_df.filter("tripSummaryId is null"))

# COMMAND ----------

tripsummary_raw_df2=spark.sql("""select cast(tripSummaryId as string),cast(tripFuelConsumed as string),cast(tripTotalEngineTimeIdle as string),cast(utcStartDateTime as string),cast(utcEndDateTime as string),cast(timeZoneOffset as string),cast(tripDistanceAccumulated as string),cast(tripMaxSpeed as string),cast(tripTotalEngineTime as string),cast(detectedVin as string),sourceCode,comment,cast(tripFuelConsumedIdle as string),"FMC_SR" as sourcesystem,current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date from (select *,row_number() over (partition by tripSummaryId order by tripMaxSpeed desc) as rn from tripsummary_view2) where rn=1 and tripSummaryId is not null
 
""")

tripsummary_raw_df2.write.format("delta").mode("append").saveAsTable("dhf_iot_fmc_raw_prod.tripsummary")
# display(tripsummary_raw_df2)

# COMMAND ----------

tripsummary_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL)  + coalesce((select max(TRIP_SUMMARY_ID) from dhf_iot_harmonized_prod.trip_summary),0) as BIGINT) as TRIP_SUMMARY_ID, coalesce(tripSummaryId, 'NOKEY') AS TRIP_SMRY_KEY,  cast(crc32(detectedvin) as string) AS DEVC_KEY, cast(timeZoneOffset as decimal(10,2)) AS TIME_ZONE_OFFST_NB, comment as CMNT_TT, cast(tripDistanceAccumulated as decimal(15,6)) as DRVNG_DISTNC_QTY, cast(tripMaxSpeed as decimal(18,5)) as MAX_SPD_RT,cast(tripTotalEngineTime as decimal(15,6)) as TRIP_SC_QTY, cast(tripFuelConsumed as decimal(15,6)) as FUEL_CNSMPTN_QTY, cast(tripTotalEngineTimeIdle as decimal(15,6)) as IDLING_SC_QTY,cast(tripFuelConsumedIdle as decimal(35,15)) as TRIP_FUEL_CNSMD_IDL_QTY,coalesce(to_timestamp(utcStartDateTime, "yyyy-MM-dd'T'HH:mm:ss.000+000")) AS TRIP_START_TS, coalesce(to_timestamp(utcEndDateTime,"yyyy-MM-dd'T'HH:mm:ss.000+000")) AS TRIP_END_TS, detectedVin AS VEH_KEY, coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"FMC_SR" AS SRC_SYS_CD,current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from (select *,row_number() over (partition by tripSummaryId order by tripMaxSpeed desc) as rn from tripsummary_view) where rn=1
and tripSummaryId is not null
""")

tripsummary_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_summary")
# display(tripsummary_harmonize_df)

# COMMAND ----------

tripsummary_harmonize_df2=spark.sql("""select distinct cast(row_number() over(order by NULL)  + coalesce((select max(TRIP_SUMMARY_ID) from dhf_iot_harmonized_prod.trip_summary),0) as BIGINT) as TRIP_SUMMARY_ID, coalesce(tripSummaryId, 'NOKEY') AS TRIP_SMRY_KEY,  cast(crc32(detectedvin) as string) AS DEVC_KEY, cast(timeZoneOffset as decimal(10,2)) AS TIME_ZONE_OFFST_NB, comment as CMNT_TT, cast(tripDistanceAccumulated as decimal(15,6)) as DRVNG_DISTNC_QTY, cast(tripMaxSpeed as decimal(18,5)) as MAX_SPD_RT,cast(tripTotalEngineTime as decimal(15,6)) as TRIP_SC_QTY, cast(tripFuelConsumed as decimal(15,6)) as FUEL_CNSMPTN_QTY, cast(tripTotalEngineTimeIdle as decimal(15,6)) as IDLING_SC_QTY,cast(tripFuelConsumedIdle as decimal(35,15)) as TRIP_FUEL_CNSMD_IDL_QTY,coalesce(to_timestamp(utcStartDateTime, "yyyy-MM-dd'T'HH:mm:ss.000+000")) AS TRIP_START_TS, coalesce(to_timestamp(utcEndDateTime,"yyyy-MM-dd'T'HH:mm:ss.000+000")) AS TRIP_END_TS, detectedVin AS VEH_KEY, coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"FMC_SR" AS SRC_SYS_CD,current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from (select *,row_number() over (partition by tripSummaryId order by tripMaxSpeed desc) as rn from tripsummary_view2) where rn=1 and tripSummaryId is not null
""")

tripsummary_harmonize_df2.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_summary")
# display(tripsummary_harmonize_df2)

# COMMAND ----------

device_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(DEVICE_ID) from dhf_iot_harmonized_prod.device),0) as BIGINT) as DEVICE_ID, coalesce(crc32(detectedvin), "NOKEY") AS DEVC_KEY,coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"FMC_SR" AS SRC_SYS_CD, current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from 
(select *,row_number() over (partition by detectedvin order by batch_nb desc) as row_number from tripsummary_view where detectedvin is not null) where row_number=1 
""")

device_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.device")
# display(device_harmonize_df)

# COMMAND ----------

vehicle_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(VEHICLE_ID) from dhf_iot_harmonized_prod.vehicle),0) as BIGINT) as VEHICLE_ID, COALESCE(detectedVin,"NOKEY") AS VEH_KEY, detectedVin as DTCTD_VIN_NB, COALESCE(detectedVin,"NOKEY") as ENRLD_VIN_NB, coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"FMC_SR" AS SRC_SYS_CD, current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from 
(select *,row_number() over (partition by detectedVin order by batch_nb desc) as row_number from tripsummary_view where detectedVin is not null) where row_number=1""")

vehicle_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.vehicle")
# display(vehicle_harmonize_df)

# COMMAND ----------

trippoint_df=parse_df.withColumn("telemetryPoints_explode",explode_outer(col("telemetryPoints"))).select("tripSummaryId","telemetryPoints_explode.*","batch_nb")
trippoint_df.createOrReplaceTempView("trippointview")
trippoint_df2=parse_df2.withColumn("telemetryPoints_explode",explode_outer(col("telemetrypoints"))).select("tripsummaryid","telemetryPoints_explode.*","batch_nb")
trippoint_df2.createOrReplaceTempView("trippointview2")



# COMMAND ----------

trippoint_raw_df=spark.sql("""select cast(tripSummaryId as string),cast(received as bigint),cast(utcDateTime as string),cast(degreesLatitude as string),cast(degreesLongitude as string),cast(speed as string),cast(headingdegrees as string),cast(accelerometerDataLong as string),cast(accelerometerDataLat as string),cast(accelerometerDataZ as string),cast(altitude as string),ignitionStatus,cast(odometer as string),ignitionEvent,"FMC_SR" as SourceSystem,
current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date
 from trippointview where tripSummaryId is not null
""")

trippoint_raw_df.write.format("delta").mode("append").saveAsTable("dhf_iot_fmc_raw_prod.telemetryPoints")
# display(trippoint_raw_df)

# COMMAND ----------

trippoint_raw_df2=spark.sql("""select cast(tripSummaryId as string),cast(received as bigint),cast(utcDateTime as string),cast(degreesLatitude as string),cast(degreesLongitude as string),cast(speed as string),cast(headingdegrees as string),cast(accelerometerDataLong as string),cast(accelerometerDataLat as string),cast(accelerometerDataZ as string),cast(altitude as string),ignitionStatus,cast(odometer as string),ignitionEvent,"FMC_SR" as SourceSystem,
current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date
 from trippointview2 where tripSummaryId is not null
""")

trippoint_raw_df2.write.format("delta").mode("append").saveAsTable("dhf_iot_fmc_raw_prod.telemetryPoints")
# display(trippoint_raw_df2)

# COMMAND ----------

trippoint_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_prod.trip_point),0) as BIGINT) as TRIP_POINT_ID, coalesce(cast(concat(utcDateTime, "0") as TIMESTAMP), to_timestamp("9999-12-31")) AS UTC_TS, tripSummaryId as TRIP_SMRY_KEY, cast(degreesLatitude as DECIMAL(18,10)) as LAT_NB, cast(degreesLongitude as DECIMAL(18,10)) as LNGTD_NB, cast(speed as DECIMAL(18,10)) as SPD_RT, ignitionEvent as IGNTN_EVNT_CD, ignitionStatus as IGNTN_STTS_CD, cast(odometer as DECIMAL(35,15)) as ODMTR_QTY, cast(altitude as DECIMAL(35,15)) as ALTITUDE_QTY, cast(accelerometerDataZ as DECIMAL(18,10)) as VRTCL_ACCLRTN_RT, cast(accelerometerDataLat as DECIMAL(18,10)) as LATRL_ACCLRTN_RT, cast(accelerometerDataLong as DECIMAL(18,10)) as LNGTDNL_ACCLRTN_RT, received as RCV_TS, coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"FMC_SR" AS SRC_SYS_CD, current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from trippointview where tripSummaryId is not null
""")

trippoint_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_point")
# display(trippoint_harmonize_df)

# COMMAND ----------

trippoint_harmonize_df2=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_prod.trip_point),0) as BIGINT) as TRIP_POINT_ID, coalesce(cast(concat(utcDateTime, "0") as TIMESTAMP), to_timestamp("9999-12-31")) AS UTC_TS, tripSummaryId as TRIP_SMRY_KEY, cast(degreesLatitude as DECIMAL(18,10)) as LAT_NB, cast(degreesLongitude as DECIMAL(18,10)) as LNGTD_NB, cast(speed as DECIMAL(18,10)) as SPD_RT, ignitionEvent as IGNTN_EVNT_CD, ignitionStatus as IGNTN_STTS_CD, cast(odometer as DECIMAL(35,15)) as ODMTR_QTY, cast(altitude as DECIMAL(35,15)) as ALTITUDE_QTY, cast(accelerometerDataZ as DECIMAL(18,10)) as VRTCL_ACCLRTN_RT, cast(accelerometerDataLat as DECIMAL(18,10)) as LATRL_ACCLRTN_RT, cast(accelerometerDataLong as DECIMAL(18,10)) as LNGTDNL_ACCLRTN_RT, received as RCV_TS, coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"FMC_SR" AS SRC_SYS_CD, current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from trippointview2 where tripSummaryId is not null
""")

trippoint_harmonize_df2.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_point")
# display(trippoint_harmonize_df2)