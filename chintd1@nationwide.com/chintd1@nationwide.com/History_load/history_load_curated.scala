// Databricks notebook source
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import java.time.LocalDateTime
import org.apache.spark.sql.streaming.Trigger
import java.time.temporal.ChronoUnit
import java.text.SimpleDateFormat
import java.util.Date
import java.util.TimeZone

// COMMAND ----------

//smartmiles_trip_detail_second

val df=spark.read.format("delta").load("dbfs:/mnt/nw-pcdm-iot-dev-785562577411-read/smartmiles_trip_detail_second/")

df.createOrReplaceTempView("smartmiles_trip_detail_second")

val df2=spark.sql("""select 
cast(row_number() over(order by NULL) + coalesce((select max(TRIP_DETAIL_SECONDS_ID) from default.trip_detail_seconds_hist_test),0) as BIGINT) as TRIP_DETAIL_SECONDS_ID,
cast(enrolled_vin_nb	as	string)	as	ENRLD_VIN_NB,
cast(trip_summary_id	as	string)	as	TRIP_SMRY_KEY,
cast(device_id	as	string)	as	DEVC_KEY,
cast(position_ts	as	timestamp)	as	PSTN_TS,
cast(position_offset_ts	as	timestamp)	as	PSTN_OFFST_TS,
cast(time_zone_offset_nb	as	decimal(15,6))	as	TIME_ZONE_OFFST_NUM,
cast(speed_mph_rt	as	decimal(18,10))	as	SPD_MPH_RT,
cast(speed_kph_rt	as	decimal(18,10))	as	SPD_KPH_RT,
cast(engine_rpm_rt	as	decimal(18,5))	as	ENGIN_RPM_RT,
cast(mile_cn	as	decimal(18,10))	as	DISTNC_MPH,
cast(kilometer_cn	as	decimal(18,10))	as	DISTNC_KPH,
cast(fast_acceleration_cn	as	int)	as	FAST_ACLRTN_EVNT_COUNTR,
cast(hard_brake_cn	as	int)	as	HARD_BRKE_COUNTR,
cast(driving_second_cn	as	int)	as	DRVNG_SC_CNT,
cast(idle_second_cn	as	int)	as	IDLE_SC_CNT,
cast(stop_second_cn	as	int)	as	STOP_SC_CNT,
cast(night_time_driving_second_cn	as	int)	as	NIGHT_TIME_DRVNG_SEC_CNT,
cast(plausible_second_cn	as	int)	as	PLSBL_SC_CNT,
cast(centroid_nb	as	int)	as	CNTRD_NUM,
cast(scrubbed_field_nb	as	string)	as	SCRBD_FLD_DESC,
cast(latitude_nb	as	decimal(24,16))	as	LAT_NB,
cast(longitude_nb	as	decimal(24,16))	as	LNGTD_NB,
cast(source_cd	as	string)	as	SRC_SYS_CD,
0 as MSSNG_TOO_MANY_SEC_FLAG,
current_timestamp() as ETL_ROW_EFF_DTS,
current_timestamp() as ETL_LAST_UPDT_DTS,
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) AS LOAD_HR_TS,
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) AS LOAD_DT from smartmiles_trip_detail_second 
where enrolled_vin_nb in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")""")
df2.count()
//df2.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_detail_seconds")

// COMMAND ----------

//smartride_trip_detail_second

val df3=spark.read.format("parquet").load("dbfs:/mnt/nw-pcdm-iot-dev-785562577411-read/smartride_tripdetail_seconds/")

df3.createOrReplaceTempView("smartride_trip_detail_second")

val df4=spark.sql("""select 
cast(row_number() over(order by NULL) + coalesce((select max(TRIP_DETAIL_SECONDS_ID) from default.trip_detail_seconds_hist_test),0) as BIGINT) as TRIP_DETAIL_SECONDS_ID,
cast(dataload_dt	as	timestamp) as	ETL_ROW_EFF_DTS,
cast(trip_nb	as	string) as	TRIP_SMRY_KEY,
cast(position_ts	as	timestamp) as	PSTN_TS,
cast(position_offset_ts	as	timestamp) as	PSTN_OFFST_TS,
cast(speed_mph	as	decimal(18,10)) as	SPD_MPH_RT,
cast(speed_kph	as	decimal(18,10)) as	SPD_KPH_RT,
cast(distance	as	decimal(18,10)) as	DISTNC_MPH,
cast(distance_kph	as	decimal(18,10)) as	DISTNC_KPH,
cast(nighttime_driving_ind	as	int) as	NIGHT_TIME_DRVNG_SEC_CNT,
cast(eventcounter_fa	as	int) as	FAST_ACLRTN_EVNT_COUNTR,
cast(eventcounter_hb	as	int) as	HARD_BRKE_COUNTR,
cast(deviceserial_nb	as	string) as	DEVC_KEY,
coalesce(enrolledvin_nb,detectedvin_nb) as	ENRLD_VIN_NB,
cast(tripzoneoffset_am	as	decimal(15,6)) as	TIME_ZONE_OFFST_NUM,
cast(enginerpm_qt	as	decimal(18,5)) as	ENGIN_RPM_RT,
cast(time_driving_ind	as	int) as	DRVNG_SC_CNT,
cast(time_idle_ind	as	int) as	IDLE_SC_CNT,
cast(plausible_ind	as	int) as	PLSBL_SC_CNT,
cast(centroid_nb	as	int) as	CNTRD_NUM,
cast(scrubbed_fields	as	string) as	SCRBD_FLD_DESC,
cast(latitude_it	as	decimal(24,16)) as	LAT_NB,
cast(longitude_it	as	decimal(24,16)) as	LNGTD_NB,
cast(eventcounter_br	as	int) as	STOP_SC_CNT,
cast(source_cd	as	string) as	SRC_SYS_CD,
1 as	MSSNG_TOO_MANY_SEC_FLAG,
current_timestamp() as ETL_LAST_UPDT_DTS,
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) AS LOAD_HR_TS,
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) AS LOAD_DT from smartride_trip_detail_second
where enrolledvin_nb in ("19XFA1E55BE037816",
"19XFA16519E050992",
"WDBHA28E0TF335125",
"1GYEE437680147839",
"3HGGK5H42KM733276")""")
//display(df4.select("SRC_SYS_CD").distinct())
df4.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_detail_seconds")

// COMMAND ----------

//smartride_garbage_tripdetail_second

val df=spark.read.format("delta").load("dbfs:/mnt/nw-pcdm-iot-dev-785562577411-read/smartride_garbage_tripdetail_second/")

df.createOrReplaceTempView("smartride_garbage_tripdetail_second")

val df2=spark.sql("""select 
cast(row_number() over(order by NULL) + coalesce((select max(TRIP_DETAIL_SECONDS_ID) from ),0) as BIGINT) as TRIP_DETAIL_SECONDS_ID,
cast(dataload_dt	as	timestamp) as	ETL_ROW_EFF_DTS,
cast(trip_nb	as	string) as	TRIP_SMRY_KEY,
cast(position_ts	as	timestamp) as	PSTN_TS,
cast(position_offset_ts	as	timestamp) as	PSTN_OFFST_TS,
cast(speed_mph	as	decimal(18,10)) as	SPD_MPH_RT,
cast(speed_kph	as	decimal(18,10)) as	SPD_KPH_RT,
cast(distance	as	decimal(18,10)) as	DISTNC_MPH,
cast(distance_kph	as	decimal(18,10)) as	DISTNC_KPH,
cast(nighttime_driving_ind	as	int) as	NIGHT_TIME_DRVNG_SEC_CNT,
cast(eventcounter_fa	as	int) as	FAST_ACLRTN_EVNT_COUNTR,
cast(eventcounter_hb	as	int) as	HARD_BRKE_COUNTR,
cast(deviceserial_nb	as	string) as	DEVC_KEY,
coelasce(enrolledvin_nb,detectedvin_nb) as	ENRLD_VIN_NB,
cast(tripzoneoffset_am	as	decimal(15,6)) as	TIME_ZONE_OFFST_NUM,
cast(enginerpm_qt	as	decimal(18,5)) as	ENGIN_RPM_RT,
cast(time_driving_ind	as	int) as	DRVNG_SC_CNT,
cast(time_idle_ind	as	int) as	IDLE_SC_CNT,
cast(plausible_ind	as	int) as	PLSBL_SC_CNT,
cast(centroid_nb	as	int) as	CNTRD_NUM,
cast(scrubbed_fields	as	string) as	SCRBD_FLD_DESC,
cast(latitude_it	as	decimal(24,16)) as	LAT_NB,
cast(longitude_it	as	decimal(24,16)) as	LNGTD_NB,
cast(eventcounter_br	as	int) as	STOP_SC_CNT,
cast(source_cd	as	string) as	SRC_SYS_CD,
0 as	MSSNG_TOO_MANY_SEC_FLAG,
current_timestamp() as ETL_LAST_UPDT_DTS,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)) AS LOAD_HR_TS,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)) AS LOAD_DT from smartride_trip_detail_second""")

df2.write.format("delta").mode("append").saveAsTable("")

// COMMAND ----------

//smartmiles_garbage_trip_detail_second

val df=spark.read.format("delta").load("dbfs:/mnt/nw-pcdm-iot-dev-785562577411-read/smartmiles_garbage_trip_detail_second/").limit(100)

df.createOrReplaceTempView("smartmiles_trip_detail_second")

val df2=spark.sql("""select 
cast(row_number() over(order by NULL) + coalesce((select max(TRIP_DETAIL_SECONDS_ID) from default.trip_detail_seconds_hist_test),0) as BIGINT) as TRIP_DETAIL_SECONDS_ID,
cast(enrolled_vin_nb	as	string)	as	ENRLD_VIN_NB,
cast(trip_summary_id	as	string)	as	TRIP_SMRY_KEY,
cast(device_id	as	string)	as	DEVC_KEY,
cast(position_ts	as	timestamp)	as	PSTN_TS,
cast(position_offset_ts	as	timestamp)	as	PSTN_OFFST_TS,
cast(time_zone_offset_nb	as	decimal(15,6))	as	TIME_ZONE_OFFST_NUM,
cast(speed_mph_rt	as	decimal(18,10))	as	SPD_MPH_RT,
cast(speed_kph_rt	as	decimal(18,10))	as	SPD_KPH_RT,
cast(engine_rpm_rt	as	decimal(18,5))	as	ENGIN_RPM_RT,
cast(mile_cn	as	decimal(18,10))	as	DISTNC_MPH,
cast(kilometer_cn	as	decimal(18,10))	as	DISTNC_KPH,
cast(fast_acceleration_cn	as	int)	as	FAST_ACLRTN_EVNT_COUNTR,
cast(hard_brake_cn	as	int)	as	HARD_BRKE_COUNTR,
cast(driving_second_cn	as	int)	as	DRVNG_SC_CNT,
cast(idle_second_cn	as	int)	as	IDLE_SC_CNT,
cast(stop_second_cn	as	int)	as	STOP_SC_CNT,
cast(night_time_driving_second_cn	as	int)	as	NIGHT_TIME_DRVNG_SEC_CNT,
cast(plausible_second_cn	as	int)	as	PLSBL_SC_CNT,
cast(centroid_nb	as	int)	as	CNTRD_NUM,
cast(scrubbed_field_nb	as	string)	as	SCRBD_FLD_DESC,
cast(latitude_nb	as	decimal(24,16))	as	LAT_NB,
cast(longitude_nb	as	decimal(24,16))	as	LNGTD_NB,
cast(source_cd	as	string)	as	SRC_SYS_CD,
1 as MSSNG_TOO_MANY_SEC_FLAG,
current_timestamp() as ETL_ROW_EFF_DTS,
current_timestamp() as ETL_LAST_UPDT_DTS,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)) AS LOAD_HR_TS,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)) AS LOAD_DT from smartmiles_garbage_trip_detail_second""")

df2.write.format("delta").mode("append").saveAsTable("")

// COMMAND ----------

//smartmiles_device_status

val df=spark.read.format("delta").load("dbfs:/user/hive/warehouse/smartmiles_device_status")

df.createOrReplaceTempView("smartmiles_device_status")

val df2=spark.sql("""select 
cast(row_number() over(order by NULL) + coalesce((select max(DEVICE_STATUS_ID) from dhf_iot_curated_prod.device_status),0) as BIGINT) as DEVICE_STATUS_ID,
cast(device_id	as	STRING) as	DEVC_KEY,
cast(sr_pgm_instnc_id	as	BIGINT) as	PRGRM_INSTC_ID,
cast(enrolled_vin_nb	as	STRING) as	ENRLD_VIN_NB,
cast(connected_status_in	as	STRING) as	CNCTD_STTS_FLAG,
cast(status_start_ts	as	TIMESTAMP) as	STTS_EFCTV_TS,
cast(status_end_ts	as	TIMESTAMP) as	STTS_EXPRTN_TS,
cast(last_device_activity_ts	as	TIMESTAMP) as	LAST_DEVC_ACTVTY_TS,
cast(device_unavailable_in	as	STRING) as	DEVC_UNAVLBL_FLAG,
"SMARTMILES_HISTORY" as SRC_SYS_CD,
current_timestamp() as ETL_LAST_UPDT_DTS,
current_timestamp() as ETL_ROW_EFF_DTS,
date_trunc('hour', last_device_activity_ts) as LOAD_HR_TS,
TO_DATE(last_device_activity_ts) AS LOAD_DT from smartmiles_device_status
where enrolled_vin_nb in ("KNAFX4A61E5055032",
"4S4BRBJC6A3326528",
"3GNAXNEX0JL262177",
"1N4AA6AP3HC456378",
"1G1ZC5EB7AF187072")""")


display(df2)
//df2.write.format("delta").mode("append").saveAsTable("dhf_iot_curated_prod.device_status")

// COMMAND ----------

//smartride_devicestatus

val df=spark.read.format("delta").load("dbfs:/user/hive/warehouse/smartride_device_status")

df.createOrReplaceTempView("smartride_devicestatus")

val df2=spark.sql("""select 
cast(row_number() over(order by NULL) + coalesce((select max(DEVICE_STATUS_ID) from dhf_iot_curated_prod.device_status),0) as BIGINT) as DEVICE_STATUS_ID,
cast(sr_pgm_instnc_id	as	BIGINT) as	PRGRM_INSTC_ID,
cast(deviceserial_id	as	STRING) as	DEVC_KEY,
cast(enrolledvin_nb	as	STRING) as	ENRLD_VIN_NB,
cast(status	as	STRING) as	CNCTD_STTS_FLAG,
cast(statusstart_ts	as	TIMESTAMP) as	STTS_EFCTV_TS,
cast(statusend_ts	as	TIMESTAMP) as	STTS_EXPRTN_TS,
cast(lastactivity_ts	as	TIMESTAMP) as	LAST_DEVC_ACTVTY_TS,
cast(loststatus_flag	as	STRING) as	DEVC_UNAVLBL_FLAG,
"SMARTRIDE_HISTORY" as SRC_SYS_CD,
current_timestamp() as ETL_LAST_UPDT_DTS,
current_timestamp() as ETL_ROW_EFF_DTS,
DATE_TRUNC("HOUR",lastactivity_ts) AS LOAD_HR_TS,
TO_DATE(lastactivity_ts) AS LOAD_DT from smartride_devicestatus """)

display(df2)
// df2.write.format("delta").mode("append").saveAsTable("dhf_iot_curated_prod.device_status")