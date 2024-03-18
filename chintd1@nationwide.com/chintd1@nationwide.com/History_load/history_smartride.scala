// Databricks notebook source
//smartride_ims_tripsummary

val sr_tripsummarydf=spark.read.format("delta").load("dbfs:/user/hive/warehouse/smartride_ims_tripsummary/")

sr_tripsummarydf.createOrReplaceTempView("smartride_ims_tripsummary_view")


// COMMAND ----------

//smartride_ims_tripsummary

val sr_tripsummarydf2=spark.sql("""select 
cast(trip_nb	as	STRING) as	tripSummaryId,
cast(deviceserial_nb	as	STRING) as	deviceSerialNumber,
cast(enrolledvin_nb	as	STRING) as	enrolledVin,
cast(detectedvin_nb	as	STRING) as	detectedVin,
cast(tripstart_ts	as	STRING) as	utcStartDateTime,
cast(tripend_ts	as	STRING) as	utcEndDateTime,
cast(tripzoneoffset_am	as	STRING) as	timeZoneOffset,
cast(tripdistance_qt	as	STRING) as	drivingDistance,
cast(averagespeed_qt	as	STRING) as	avgSpeed,
cast(maximumspeed_qt	as	STRING) as	maxSpeed,
cast(fuelconsumption_qt	as	STRING) as	fuelConsumption,
cast(milstatus_cd	as	BOOLEAN) as	milStatus,
cast(idletime_ts	as	STRING) as	secondsOfIdling,
cast(trippositionalquality_qt	as	STRING) as	hdopAverage,
cast(accelerometerquality_qt	as	BOOLEAN) as	accelQuality,
"IMS_SR_4X" as SourceSystem,
current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date from smartride_ims_tripsummary_view
where enrolledvin_nb in ("19XFA1E55BE037816",
"19XFA16519E050992",
"WDBHA28E0TF335125",
"1GYEE437680147839",
"3HGGK5H42KM733276")""")

sr_tripsummarydf2.write.format("delta").mode("append").saveAsTable("dhf_iot_ims_raw_prod.tripsummary")
// display(sr_tripsummarydf2)

// COMMAND ----------

//smartride_ims_tripsummary



val har_sr_tripsummarydf2=spark.sql("""select 
cast(row_number() over(order by NULL) + coalesce((select max(TRIP_SUMMARY_ID) from dhf_iot_harmonized_prod.trip_summary),0) as BIGINT) as TRIP_SUMMARY_ID,
coalesce(trip_nb, "NOKEY") AS TRIP_SMRY_KEY,
current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS,
coalesce(deviceserial_nb,crc32(detectedvin_nb)) AS DEVC_KEY,
cast(accelerometerquality_qt as STRING) AS ACLRTN_QLTY_FL,
cast(trippositionalquality_qt as decimal(15,6)) AS AVG_HDOP_QTY,
cast(averagespeed_qt as decimal(18,5)) AS AVG_SPD_MPH_RT,
cast(tripdistance_qt as decimal(15,6)) AS DRVNG_DISTNC_QTY,
cast(fuelconsumption_qt as decimal(15,6)) AS FUEL_CNSMPTN_QTY,
cast(idletime_ts as decimal(15,6)) AS IDLING_SC_QTY,
cast(milstatus_cd as STRING) AS MLFNCTN_STTS_FL,
cast(maximumspeed_qt as decimal(18,5)) AS MAX_SPD_RT,
cast(tripzoneoffset_am as decimal(10,2)) as TIME_ZONE_OFFST_NB,
coalesce(cast(tripstart_ts as TIMESTAMP), to_timestamp("9999-12-31")) AS TRIP_START_TS,
coalesce(cast(tripend_ts as TIMESTAMP), to_timestamp("9999-12-31")) AS TRIP_END_TS,
coalesce(enrolledvin_nb,detectedvin_nb) AS VEH_KEY, 
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"IMS_SR_4X" AS SRC_SYS_CD, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from smartride_ims_tripsummary_view
where enrolledvin_nb in ("19XFA1E55BE037816",
"19XFA16519E050992",
"WDBHA28E0TF335125",
"1GYEE437680147839",
"3HGGK5H42KM733276")""")

har_sr_tripsummarydf2.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_summary")
// display(har_sr_tripsummarydf2)

// COMMAND ----------

//smartride_ims_device



val har_sr_devicedf2=spark.sql("""select 
cast(row_number() over(order by NULL) + coalesce((select max(DEVICE_ID) from dhf_iot_harmonized_prod.device),0) as BIGINT) as DEVICE_ID,
current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS,
coalesce(deviceserial_nb,crc32(detectedvin_nb),"NOKEY") AS DEVC_KEY,
deviceserial_nb as DEVC_SRL_NB,
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"IMS_SR_4X" AS SRC_SYS_CD, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from 
(select *,row_number() over (partition by deviceserial_nb order by batch desc) as row_number from smartride_ims_tripsummary_view where enrolledvin_nb in ("19XFA1E55BE037816",
"19XFA16519E050992",
"WDBHA28E0TF335125",
"1GYEE437680147839",
"3HGGK5H42KM733276")) where row_number=1""")

har_sr_devicedf2.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.device")
// display(har_sr_devicedf2)

// COMMAND ----------

//smartride_ims_vehicle



val har_sr_vehicledf2=spark.sql("""select 
cast(row_number() over(order by NULL) + coalesce((select max(VEHICLE_ID) from dhf_iot_harmonized_prod.vehicle),0) as BIGINT) as VEHICLE_ID, 
current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS,  
coalesce(enrolledvin_nb,detectedvin_nb,"NOKEY") AS VEH_KEY, 
COALESCE(detectedvin_nb,enrolledvin_nb) as DTCTD_VIN_NB, 
COALESCE(enrolledvin_nb,detectedvin_nb,"NOKEY") as ENRLD_VIN_NB, 
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"IMS_SR_4X" AS SRC_SYS_CD, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from 
(select *,row_number() over (partition by enrolledvin_nb,detectedvin_nb order by batch desc) as row_number from smartride_ims_tripsummary_view where enrolledvin_nb in ("19XFA1E55BE037816",
"19XFA16519E050992",
"WDBHA28E0TF335125",
"1GYEE437680147839",
"3HGGK5H42KM733276")) where row_number=1""")

har_sr_vehicledf2.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.vehicle")
// display(har_sr_vehicledf2)

// COMMAND ----------

//smartride_ims_trippoint

val sr_trippointdf=spark.read.format("delta").load("dbfs:/user/hive/warehouse/smartride_ims_trippoint/")

sr_trippointdf.createOrReplaceTempView("smartride_ims_trippoint_view")

// COMMAND ----------

//smartride_ims_trippoint

val sr_trippointdf2=spark.sql("""select 
cast(trip_nb	as	STRING) as	tripSummaryId,
cast(position_ts	as	STRING) as	utcDateTime,
cast(latitude_it	as	STRING) as	degreesLatitude,
cast(longitude_it	as	STRING) as	degreesLongitude,
cast(gpsheading_nb	as	STRING) as	headingdegrees,
cast(vssspeed_am	as	STRING) as	speed,
cast(vssacceleration_pc	as	STRING) as	acceleration,
cast(enginerpm_am	as	STRING) as	engineRPM,
cast(positionquality_nb	as	STRING) as	hdop,
cast(throttleposition_pc	as	STRING) as	throttlePosition,
cast(accelerationlongitudinal_nb	as	STRING) as	Accel_Longitudinal,
cast(accelerationlateral_nb	as	STRING) as	Accel_Lateral,
cast(accelerationvertical_nb	as	STRING) as	Accel_Vertical,
"IMS_SR_4X" as SourceSystem,
current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date from smartride_ims_trippoint_view
where trip_nb in (select tripSummaryId from dhf_iot_ims_raw_prod.tripsummary where  enrolledVin in ("19XFA1E55BE037816",
"19XFA16519E050992",
"WDBHA28E0TF335125",
"1GYEE437680147839",
"3HGGK5H42KM733276"))""")

sr_trippointdf2.write.format("delta").mode("append").saveAsTable("dhf_iot_ims_raw_prod.telemetrypoints")
// display(sr_trippointdf2)

// COMMAND ----------

//smartride_ims_trippoint



val har_sr_trippointdf2=spark.sql("""select 
cast(row_number() over(order by NULL) + coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_prod.trip_point),0) as BIGINT) as TRIP_POINT_ID,
current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS,
cast(vssacceleration_pc as decimal(18,5)) as ACLRTN_RT,
cast(enginerpm_am as decimal(18,5)) as ENGIN_RPM_RT,
cast(gpsheading_nb as decimal(15,6)) as HEADNG_DEG_QTY, 
coalesce(cast(position_ts as TIMESTAMP), to_timestamp("9999-12-31")) as UTC_TS,
cast(vssspeed_am as decimal(18,10)) as SPD_RT,
cast(latitude_it as decimal(18,10)) as LAT_NB,
cast(longitude_it as decimal(18,10)) as LNGTD_NB,
cast(throttleposition_pc as decimal(15,6)) as THRTL_PSTN_NB, 
trip_nb AS TRIP_SMRY_KEY,
cast(positionquality_nb as decimal(18,10)) as HDOP_QTY,
cast(accelerationlongitudinal_nb as decimal(18,10)) as LNGTDNL_ACCLRTN_RT,
cast(accelerationlateral_nb as decimal(18,10)) as LATRL_ACCLRTN_RT,
cast(accelerationvertical_nb as decimal(18,10)) as VRTCL_ACCLRTN_RT, 
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"IMS_SR_4X" AS SRC_SYS_CD, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS
 from smartride_ims_trippoint_view where trip_nb in (select tripSummaryId from dhf_iot_ims_raw_prod.tripsummary where  enrolledVin in ("19XFA1E55BE037816",
"19XFA16519E050992",
"WDBHA28E0TF335125",
"1GYEE437680147839",
"3HGGK5H42KM733276"))""")

har_sr_trippointdf2.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_point")
// display(har_sr_trippointdf2)

// COMMAND ----------

//smartride_ims_tripevent

val sr_tripeventdf=spark.read.format("delta").load("dbfs:/user/hive/warehouse/smartride_ims_tripevent/")

sr_tripeventdf.createOrReplaceTempView("smartride_ims_tripevent_view")

// COMMAND ----------

//smartride_ims_tripevent


val sr_tripeventdf2=spark.sql("""select 
cast(trip_nb	as	STRING) as	tripSummaryId,
cast(event_ts as	STRING) as	utcDateTime,
cast(latitude_it	as	STRING) as	degreesLatitude,
cast(longitude_it	as	STRING) as	degreesLongitude,
cast(eventtimezoneoffset_nb	as	STRING) as	Event_Timezone_Offset,
cast(eventtype_cd	as	STRING) as	telemetryEventType,
cast(eventreferencevalue_ds	as	STRING) as	Event_Reference_Value,
"IMS_SR_4X" as SourceSystem,
current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date from smartride_ims_tripevent_view  where detectedvin_nb is not null or latitude_it is not null or longitude_it is not null or eventreferencevalue_ds is not null AND enrolledvin_nb in ("19XFA1E55BE037816",
"19XFA16519E050992",
"WDBHA28E0TF335125",
"1GYEE437680147839",
"3HGGK5H42KM733276")""")

sr_tripeventdf2.write.format("delta").mode("append").saveAsTable("dhf_iot_ims_raw_prod.telemetryevents")
// display(sr_tripeventdf2)

// COMMAND ----------

//smartride_ims_tripevent



val har_sr_tripeventdf2=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(TRIP_EVENT_ID) from dhf_iot_harmonized_prod.trip_event),0) as BIGINT) as TRIP_EVENT_ID, 
current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS,
coalesce(b.code, "NOKEY") as EVNT_TP_CD,
coalesce(tdr.trip_nb, "NOKEY")  AS TRIP_SMRY_KEY,
coalesce(cast(tdr.event_ts as TIMESTAMP), to_timestamp("9999-12-31")) as UTC_TS,
cast(tdr.eventreferencevalue_ds as decimal(35,15)) as EVT_RFRNC_VAL,
cast(tdr.eventtimezoneoffset_nb as int) as EVT_TMZN_OFFST_NUM,
cast(tdr.latitude_it as decimal(18,10)) as LAT_NB,
cast(tdr.longitude_it as decimal(18,10)) as LNGTD_NB,
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(tdr.batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"IMS_SR_4X" AS SRC_SYS_CD, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(tdr.batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from smartride_ims_tripevent_view tdr left join dhf_iot_harmonized_prod.event_code b on b.source="IMS_SR_4X" and tdr.eventtype_cd=b.code where  tdr.detectedvin_nb is not null or tdr.latitude_it is not null or tdr.longitude_it is not null or tdr.eventreferencevalue_ds is not null AND tdr.enrolledvin_nb in ("19XFA1E55BE037816",
"19XFA16519E050992",
"WDBHA28E0TF335125",
"1GYEE437680147839",
"3HGGK5H42KM733276") 
""")

har_sr_tripeventdf2.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_event")
// display(har_sr_tripeventdf2)


// COMMAND ----------

//smartride_ims_nontripevent

val sr_nontripeventdf2=spark.sql("""select 
cast(trip_nb	as	STRING) as	telemetrySetId,
cast(enrolledvin_nb	as	STRING) as	enrolledVin,
cast(detectedvin_nb	as	STRING) as	detectedVin,
cast(event_ts	as	STRING) as	utcDateTime,
cast(eventtimezoneoffset_nb	as	STRING) as	Event_Timezone_Offset,
cast(latitude_it	as	STRING) as	degreesLatitude,
cast(longitude_it	as	STRING) as	degreesLongitude,
cast(eventtype_cd	as	STRING) as	telemetryEventType,
cast(eventreferencevalue_ds	as	STRING) as	Event_Reference_Value,
"IMS_SR_4X" as SourceSystem,
current_timestamp() as db_load_time,
current_date() as db_load_date,
TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_hour,
TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)) AS load_date from smartride_ims_tripevent_view
where detectedvin_nb is null and latitude_it is null and longitude_it is null and eventreferencevalue_ds is null AND enrolledvin_nb in ("19XFA1E55BE037816",
"19XFA16519E050992",
"WDBHA28E0TF335125",
"1GYEE437680147839",
"3HGGK5H42KM733276")""")

sr_nontripeventdf2.write.format("delta").mode("append").saveAsTable("dhf_iot_ims_raw_prod.nontripevent")
// display(sr_nontripeventdf2)

// COMMAND ----------

//smartride_ims_nontripevent


val har_sr_nontripeventdf2=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(NON_TRIP_EVENT_ID) from dhf_iot_harmonized_prod.non_trip_event),0) as BIGINT) as NON_TRIP_EVENT_ID, 
current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS,
coalesce(tdr.eventtype_cd, "NOKEY") as EVNT_TP_CD,
coalesce(tdr.trip_nb, "NOKEY")  AS NON_TRIP_EVNT_KEY,
coalesce(cast(tdr.event_ts as TIMESTAMP), to_timestamp("9999-12-31")) as UTC_TS,
cast(tdr.eventreferencevalue_ds as decimal(35,15)) as EVT_RFRNC_VAL,
cast(tdr.eventtimezoneoffset_nb as int) as EVT_TMZN_OFFST_NUM,
cast(tdr.latitude_it as decimal(18,10)) as LAT_NB,
cast(tdr.longitude_it as decimal(18,10)) as LNGTD_NB,
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(tdr.batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"IMS_SR_4X" AS SRC_SYS_CD, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(tdr.batch as string), 'yyyyMMddHHmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from smartride_ims_tripevent_view tdr left join dhf_iot_harmonized_prod.event_code b on b.source="IMS_SR_4X" and tdr.eventtype_cd=b.code
where tdr.detectedvin_nb is null and tdr.latitude_it is null and tdr.longitude_it is null and tdr.eventreferencevalue_ds is null AND tdr.enrolledvin_nb in ("19XFA1E55BE037816",
"19XFA16519E050992",
"WDBHA28E0TF335125",
"1GYEE437680147839",
"3HGGK5H42KM733276")""")

har_sr_nontripeventdf2.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.non_trip_event")
// display(har_sr_nontripeventdf2)