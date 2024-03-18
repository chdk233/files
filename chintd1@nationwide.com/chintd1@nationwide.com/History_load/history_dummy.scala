// Databricks notebook source
//smartride_ims_tripsummary

val har_sr_tripsummary_df=spark.read.format("delta").load("dbfs:/mnt/nw-pcdm-iot-dev-785562577411-read/smartride_ims_tripsummary/")

har_sr_tripsummarydf.createOrReplaceTempView("har_smartride_ims_tripsummary")

val har_sr_tripsummarydf2=spark.sql("""select 
cast(row_number() over(order by NULL) + coalesce((select max(TRIP_SUMMARY_ID) from dhf_iot_harmonized_test.trip_summary),0) as BIGINT) as TRIP_SUMMARY_ID,
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
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"SMARTRIDE_HISTORY" AS SRC_SYS_CD, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from har_smartride_ims_tripsummary""")

har_sr_tripsummarydf2.write.format("delta").mode("append").saveAsTable("")

// COMMAND ----------

//smartride_ims_device

val har_sr_device_df=spark.read.format("delta").load("dbfs:/mnt/nw-pcdm-iot-dev-785562577411-read/smartride_ims_tripsummary/").selectExpr("detectedvin_nb","deviceserial_nb","batch_nb")

har_sr_devicedf.createOrReplaceTempView("har_smartride_ims_device")

val har_sr_devicedf2=spark.sql("""select 
cast(row_number() over(order by NULL) + coalesce((select max(DEVICE_ID) from dhf_iot_harmonized_test.device),0) as BIGINT) as DEVICE_ID,
current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS,
coalesce(deviceserial_nb,crc32(detectedvin_nb),"NOKEY") AS DEVC_KEY,
deviceserial_nb as DEVC_SRL_NB,
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"SMARTRIDE_HISTORY" AS SRC_SYS_CD, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from 
(select *,rank() over (partition by deviceserial_nb order by batch_nb desc) as rank har_smartride_ims_device) where rank=1""")

har_sr_devicedf2.write.format("delta").mode("append").saveAsTable("")

// COMMAND ----------

//smartride_ims_vehicle

val har_sr_vehicle_df=spark.read.format("delta").load("dbfs:/mnt/nw-pcdm-iot-dev-785562577411-read/smartride_ims_tripsummary/").selectExpr("enrolledvin_nb","detectedvin_nb","batch_nb")

har_sr_vehicledf.createOrReplaceTempView("har_smartride_ims_vehicle")

val har_sr_vehicledf2=spark.sql("""select 
cast(row_number() over(order by NULL) + coalesce((select max(VEHICLE_ID) from dhf_iot_harmonized_test.vehicle),0) as BIGINT) as VEHICLE_ID, 
current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS,  
coalesce(enrolledvin_nb,detectedvin_nb,"NOKEY") AS VEH_KEY, 
COALESCE(detectedvin_nb,enrolled_vin_nb) as DTCTD_VIN_NB, 
COALESCE(enrolled_vin_nb,detectedvin_nb,"NOKEY") as ENRLD_VIN_NB, 
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"SMARTRIDE_HISTORY" AS SRC_SYS_CD, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from 
(select *,rank() over (partition by enrolledvin_nb,detectedvin_nb order by batch_nb desc) as rank from har_smartride_ims_vehicle) where rank=1""")

har_sr_vehicledf2.write.format("delta").mode("append").saveAsTable("")

// COMMAND ----------

//smartmiles_trip_point

val har_sm_trip_pointdf=spark.read.format("delta").load("dbfs:/mnt/nw-pcdm-iot-dev-785562577411-read/smartmiles_trip_point/").withColumn("explodePoints",explode_outer($"telemetrypoints")).selectExpr("*","telemetryPoints.*").drop("explodePoints","telemetryPoints")

har_sm_trip_pointdf.createOrReplaceTempView("har_smartmiles_trip_point")

val har_sm_trip_pointdf2=spark.sql("""select 
cast(row_number() over(order by NULL) + coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_test.trip_point),0) as BIGINT) as TRIP_POINT_ID,
current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS,
cast(acceleration_rt as decimal(18,5)) as ACLRTN_RT,
cast(ambient_temperature_cn as decimal(15,6)) as AMBNT_TMPRTR_QTY,
cast(barometric_pressure_cn as decimal(15,6)) as BRMTRC_PRESSR_QTY,
cast(coolant_temperature_cn as decimal(15,6)) as COOLNT_TMPRTR_QTY,
cast(engine_rpm_rt as decimal(18,5)) as ENGIN_RPM_RT,
cast(fuel_level_qt as decimal(15,6))as FUEL_LVL_QTY,
cast(heading_degree_nb as decimal(15,6)) as HEADNG_DEG_QTY,
coalesce(cast(position_ts as TIMESTAMP), to_timestamp("9999-12-31")) as UTC_TS,
cast(speed_rt as decimal(18,10)) as SPD_RT,
cast(accelerometer_data_tt as string) as ACLRTMTR_DATA_RT,
cast(horizontal_accuracy_nb as decimal(15,6)) as HRZNTL_ACCRCY_QTY,
cast(latitude_nb as decimal(18,10)) as LAT_NB,
cast(longitude_nb as decimal(18,10)) as LNGTD_NB,
cast(throttle_position_nb as decimal(15,6)) as THRTL_PSTN_NB,
cast(vertical_accuracy_nb as decimal(15,6)) as VRTCL_ACCRCY_QTY, tripSummaryId AS TRIP_SMRY_KEY,
cast(average_hdop_nb as decimal(18,10)) as HDOP_QTY,
cast(accelerometer_data_long_nb as decimal(18,10)) as LNGTDNL_ACCLRTN_RT,
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
coalesce(source_cd,"NOKEY") AS SRC_SYS_CD,
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from har_smartmiles_trip_point""")

har_sm_trip_pointdf2.write.format("delta").mode("append").saveAsTable("")

// COMMAND ----------

//smartride_ims_trippoint

val har_sr_trippointdf=spark.read.format("delta").load("dbfs:/mnt/nw-pcdm-iot-dev-785562577411-read/smartride_ims_trippoint/")

har_sr_trippointdf.createOrReplaceTempView("har_smartride_ims_trippoint")

val har_sr_trippointdf2=spark.sql("""select 
cast(row_number() over(order by NULL) + coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_test.trip_point),0) as BIGINT) as TRIP_POINT_ID,
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
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"SMARTRIDE_HISTORY" AS SRC_SYS_CD, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS
 from har_smartride_ims_trippoint""")

har_sr_trippointdf2.write.format("delta").mode("append").saveAsTable("")

// COMMAND ----------

//smartride_ims_tripevent

val har_sr_tripeventdf=spark.read.format("delta").load("dbfs:/mnt/nw-pcdm-iot-dev-785562577411-read/smartride_ims_tripevent/")

har_sr_tripeventdf.createOrReplaceTempView("har_smartride_ims_tripevent")

val har_sr_tripeventdf2=spark.sql("""select distinct cast(row_number() over(order by NULL) + coalesce((select max(TRIP_EVENT_ID) from dhf_iot_harmonized_test.trip_event),0) as BIGINT) as TRIP_EVENT_ID, 
current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS,
coalesce(eventtype_cd, "NOKEY") as EVNT_TP_CD,
trip_nb AS TRIP_SMRY_KEY,
coalesce(cast(event_ts as TIMESTAMP), to_timestamp("9999-12-31")) as UTC_TS,
cast(eventreferencevalue_ds as decimal(35,15)) as EVT_RFRNC_VAL,
cast(eventtimezoneoffset_nb as int) as EVT_TMZN_OFFST_NUM,
cast(latitude_it as decimal(18,10)) as LAT_NB,
cast(longitude_it as decimal(18,10)) as LNGTD_NB,
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"SMARTRIDE_HISTORY" AS SRC_SYS_CD, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from har_smartride_ims_tripevent""")

har_sr_tripeventdf2.write.format("delta").mode("append").saveAsTable("")

// COMMAND ----------

//smartride_ims_nontripevent

val har_sr_nontripeventdf=spark.read.format("delta").load("dbfs:/mnt/nw-pcdm-iot-dev-785562577411-read/smartride_ims_tripevent/")

har_sr_nontripeventdf.createOrReplaceTempView("har_smartride_ims_nontripevent")

val har_sr_nontripeventdf2=spark.sql("""select distinct cast(row_number() over(order by NULL) +2000+ coalesce((select max(NON_TRIP_EVENT_ID) from dhf_iot_harmonized_test.non_trip_event),0) as BIGINT) as NON_TRIP_EVENT_ID, 
current_timestamp() AS ETL_ROW_EFF_DTS, 
current_timestamp() as ETL_LAST_UPDT_DTS,
coalesce(eventtype_cd, "NOKEY") as EVNT_TP_CD,
trip_nb AS TRIP_SMRY_KEY,
coalesce(cast(event_ts as TIMESTAMP), to_timestamp("9999-12-31")) as UTC_TS,
cast(eventreferencevalue_ds as decimal(35,15)) as EVT_RFRNC_VAL,
cast(eventtimezoneoffset_nb as int) as EVT_TMZN_OFFST_NUM,
cast(latitude_it as decimal(18,10)) as LAT_NB,
cast(longitude_it as decimal(18,10)) as LNGTD_NB,
coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)), to_date("9999-12-31")) as LOAD_DT, 
"SMARTRIDE_HISTORY" AS SRC_SYS_CD, 
coalesce(TO_timestamp(CAST(UNIX_TIMESTAMP(cast(batch_nb as string), 'yyyyMMddhhmm') AS TIMESTAMP)), to_timestamp("9999-12-31")) as LOAD_HR_TS from har_smartride_ims_nontripevent""")

har_sr_nontripeventdf2.write.format("delta").mode("append").saveAsTable("")