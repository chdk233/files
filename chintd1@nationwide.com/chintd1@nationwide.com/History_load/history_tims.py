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

schema=StructType([StructField("sourceCode",StringType(),True),StructField("telemetryPoints",ArrayType(StructType([StructField("accelerometerDataLat",StringType(),True),StructField("accelerometerDataLong",StringType(),True),StructField("degreesLatitude",StringType(),True),StructField("degreesLongitude",StringType(),True),StructField("engineRPM",StringType(),True),StructField("hdop",StringType(),True),StructField("headingdegrees",StringType(),True),StructField("speed",StringType(),True),StructField("utcDateTime",StringType(),True)])),True),StructField("timeZoneOffset",StringType(),True),StructField("tripSummaryId",StringType(),True),StructField("utcEndDateTime",StringType(),True),StructField("utcStartDateTime",StringType(),True),StructField("vehicle",StructType([StructField("detectedVin",StringType(),True)]),True)])

schema2=StructType([StructField("sourcecode",StringType(),True),StructField("telemetrypoints",ArrayType(StructType([StructField("accelerometerdatalat",StringType(),True),StructField("accelerometerdatalong",StringType(),True),StructField("degreeslatitude",StringType(),True),StructField("degreeslongitude",StringType(),True),StructField("enginerpm",StringType(),True),StructField("hdop",StringType(),True),StructField("headingdegrees",StringType(),True),StructField("speed",StringType(),True),StructField("utcdatetime",StringType(),True)])),True),StructField("timezoneoffset",StringType(),True),StructField("tripsummaryid",StringType(),True),StructField("utcenddatetime",StringType(),True),StructField("utcstartdatetime",StringType(),True),StructField("vehicle",StructType([StructField("detectedvin",StringType(),True)]),True)])

# COMMAND ----------

json_df=spark.read.format("text").load("dbfs:/mnt/dw-telematics-prod-read/warehouse/telematics_staging_db/smartmiles_staging_json/source_cd=TIMS/")
parse_df=json_df.withColumn("parse_col",from_json("value",schema=schema)).select("parse_col.*","batch_nb").filter("tripSummaryId is not null")
parse_df2=json_df.withColumn("parse_col",from_json("value",schema=schema2)).select("parse_col.*","batch_nb").filter("tripsummaryid is not null")
# parse_df=parse_df1.union(parse_df2)
# parse_df.cache()
# display(json_df.filter("value   like '%tripsummaryid%'"))
# display(parse_df1)



# COMMAND ----------

tripsummary_df=parse_df.select("*","vehicle.*")
tripsummary_df.createOrReplaceTempView("tripsummary")
tripsummary_df2=parse_df2.select("*","vehicle.*")
tripsummary_df2.createOrReplaceTempView("tripsummary2")



# COMMAND ----------

tripsummary_raw_df=spark.sql("""select cast(tripSummaryId as string),cast(utcStartDateTime as string),cast(utcEndDateTime as string),cast(timeZoneOffset as string),cast(detectedVin as string),sourceCode,"TIMS_SR" as sourcesystem,current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date from tripsummary
""")

tripsummary_raw_df.write.format("delta").mode("append").saveAsTable("dhf_iot_tims_raw_prod.tripsummary")
# display(tripsummary_raw_df.filter("tripSummaryId is null"))

# COMMAND ----------

tripsummary_raw_df2=spark.sql("""select cast(tripSummaryid as string),cast(utcStartDateTime as string),cast(utcEndDateTime as string),cast(timeZoneOffset as string),cast(detectedvin as string),sourceCode,"TIMS_SR" as sourcesystem,current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date from tripsummary2 
""")

tripsummary_raw_df2.write.format("delta").mode("append").saveAsTable("dhf_iot_tims_raw_prod.tripsummary")
# display(tripsummary_raw_df2)

# COMMAND ----------

tripsummary_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL) +1500+ coalesce((select max(TRIP_SUMMARY_ID) from dhf_iot_harmonized_prod.trip_summary),0) as BIGINT) as TRIP_SUMMARY_ID, coalesce(tripSummaryId, "NOKEY") AS TRIP_SMRY_KEY, cast(crc32(detectedvin) as string) AS DEVC_KEY, cast(timeZoneOffset as decimal(10,2)) AS TIME_ZONE_OFFST_NB, coalesce(cast(utcStartDateTime as TIMESTAMP), to_timestamp("9999-12-31")) AS TRIP_START_TS, coalesce(cast(utcEndDateTime as TIMESTAMP), to_timestamp("9999-12-31")) AS TRIP_END_TS, detectedVin AS VEH_KEY, coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"TIMS_SR" AS SRC_SYS_CD, current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from tripsummary WHERE tripSummaryId IS not NULL
""")

tripsummary_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_summary")
# display(tripsummary_harmonize_df)

# COMMAND ----------

tripsummary_harmonize_df2=spark.sql("""select distinct cast(row_number() over(order by NULL) +1500+ coalesce((select max(TRIP_SUMMARY_ID) from dhf_iot_harmonized_prod.trip_summary),0) as BIGINT) as TRIP_SUMMARY_ID, coalesce(tripSummaryId, "NOKEY") AS TRIP_SMRY_KEY, cast(crc32(detectedvin) as string) AS DEVC_KEY, cast(timeZoneOffset as decimal(10,2)) AS TIME_ZONE_OFFST_NB, coalesce(cast(utcStartDateTime as TIMESTAMP), to_timestamp("9999-12-31")) AS TRIP_START_TS, coalesce(cast(utcEndDateTime as TIMESTAMP), to_timestamp("9999-12-31")) AS TRIP_END_TS, detectedVin AS VEH_KEY, coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"TIMS_SR" AS SRC_SYS_CD, current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from tripsummary2 
""")

tripsummary_harmonize_df2.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_summary")
# display(tripsummary_harmonize_df)

# COMMAND ----------

device_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(DEVICE_ID) from dhf_iot_harmonized_prod.device),0) as BIGINT) as DEVICE_ID, coalesce(crc32(detectedvin), "NOKEY") AS DEVC_KEY,coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"TIMS_SR" AS SRC_SYS_CD, current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from 
(select *,row_number() over (partition by detectedvin order by batch_nb desc) as row_number from tripSummary where detectedvin is not null) where row_number=1 
""")

device_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.device")
# display(device_harmonize_df)

# COMMAND ----------

vehicle_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(VEHICLE_ID) from dhf_iot_harmonized_prod.vehicle),0) as BIGINT) as VEHICLE_ID, COALESCE(detectedVin,"NOKEY") AS VEH_KEY, detectedVin as DTCTD_VIN_NB, COALESCE(detectedVin,"NOKEY") as ENRLD_VIN_NB, coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"TIMS_SR" AS SRC_SYS_CD, current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from 
(select *,row_number() over (partition by detectedVin order by batch_nb desc) as row_number from tripsummary where detectedVin is not null) where row_number=1""")

vehicle_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.vehicle")
# display(vehicle_harmonize_df)

# COMMAND ----------

trippoint_df=parse_df.withColumn("telemetryPoints_explode",explode_outer(col("telemetryPoints"))).select("tripSummaryId","telemetryPoints_explode.*","batch_nb")
trippoint_df.createOrReplaceTempView("trippoint")
trippoint_df2=parse_df2.withColumn("telemetryPoints_explode",explode_outer(col("telemetrypoints"))).select("tripsummaryid","telemetryPoints_explode.*","batch_nb")
trippoint_df2.createOrReplaceTempView("trippoint2")



# COMMAND ----------

trippoint_raw_df=spark.sql("""select cast(tripSummaryId as string),cast(engineRPM as string),cast(hdop as string),cast(utcDateTime as string),cast(degreesLatitude as string),cast(degreesLongitude as string),cast(speed as string),cast(headingdegrees as string),cast(accelerometerDataLong as string),cast(accelerometerDataLat as string),"TIMS_SR" as SourceSystem,
current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date
 from trippoint WHERE tripSummaryId IS not NULL
""")

trippoint_raw_df.write.format("delta").mode("append").saveAsTable("dhf_iot_tims_raw_prod.telemetryPoints")
# display(trippoint_raw_df)

# COMMAND ----------

trippoint_raw_df2=spark.sql("""select cast(tripSummaryId as string),cast(engineRPM as string),cast(hdop as string),cast(utcDateTime as string),cast(degreesLatitude as string),cast(degreesLongitude as string),cast(speed as string),cast(headingdegrees as string),cast(accelerometerDataLong as string),cast(accelerometerDataLat as string),"TIMS_SR" as SourceSystem,
current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date
 from trippoint2
""")

trippoint_raw_df2.write.format("delta").mode("append").saveAsTable("dhf_iot_tims_raw_prod.telemetryPoints")
# display(trippoint_raw_df2)

# COMMAND ----------

trippoint_harmonize_df=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_prod.trip_point),0) as BIGINT) as TRIP_POINT_ID, cast(engineRPM as decimal(18,5)) as ENGIN_RPM_RT, cast(hdop as decimal(18,10)) as HDOP_QTY, tripSummaryId AS TRIP_SMRY_KEY, coalesce(cast(utcDateTime as TIMESTAMP), to_timestamp("9999-12-31")) AS UTC_TS, cast(degreesLatitude as decimal(18,10)) as LAT_NB, cast(degreesLongitude as decimal(18,10)) as LNGTD_NB, cast(speed as decimal(18,10)) as SPD_RT, cast(headingDegrees as decimal(15,6)) as HEADNG_DEG_QTY, cast(accelerometerDataLong as decimal(18,10)) as LNGTDNL_ACCLRTN_RT, cast(accelerometerDataLat as decimal(18,10)) as LATRL_ACCLRTN_RT, coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"TIMS_SR" AS SRC_SYS_CD, current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from trippoint where tripSummaryId is not null
""")

trippoint_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_point")
# display(trippoint_harmonize_df)

# COMMAND ----------

trippoint_harmonize_df2=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_prod.trip_point),0) as BIGINT) as TRIP_POINT_ID, cast(engineRPM as decimal(18,5)) as ENGIN_RPM_RT, cast(hdop as decimal(18,10)) as HDOP_QTY, tripSummaryId AS TRIP_SMRY_KEY, coalesce(cast(utcDateTime as TIMESTAMP), to_timestamp("9999-12-31")) AS UTC_TS, cast(degreesLatitude as decimal(18,10)) as LAT_NB, cast(degreesLongitude as decimal(18,10)) as LNGTD_NB, cast(speed as decimal(18,10)) as SPD_RT, cast(headingDegrees as decimal(15,6)) as HEADNG_DEG_QTY, cast(accelerometerDataLong as decimal(18,10)) as LNGTDNL_ACCLRTN_RT, cast(accelerometerDataLat as decimal(18,10)) as LATRL_ACCLRTN_RT, coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"TIMS_SR" AS SRC_SYS_CD,current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from trippoint2
""")

trippoint_harmonize_df2.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_point")
# display(trippoint_harmonize_df2)