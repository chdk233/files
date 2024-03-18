// Databricks notebook source
// MAGIC %sql
// MAGIC --raw table
// MAGIC select * from dhf_iot_ims_raw_prod.tripsummary where sourcesystem='IMS_SM_5X' and load_date>'2023-02-25' and tripSummaryId in 
// MAGIC ('05fbc44a-39dd-4f05-bbeb-5694113e1755',
// MAGIC '6dd565bd-9bb3-4e89-a3c2-bf8273f8b349',
// MAGIC '502cc4b8-d2f1-45b0-b9ed-fcfa8296999d')

// COMMAND ----------

// MAGIC %sql
// MAGIC select *  from  dhf_iot_ims_stageraw_prod.trip_SUMMARY_missing_trips where etl_load_date=current_date and sourcesystem='IMS_SM_5X'

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct cast(row_number() over(order by NULL) + 500 + coalesce((select max(TRIP_SUMMARY_ID) from dhf_iot_harmonized_prod.trip_summary),0) as BIGINT) as TRIP_SUMMARY_ID, coalesce(tripSummaryId, 'NOKEY') AS TRIP_SMRY_KEY, coalesce(db_load_time, to_timestamp('9999-12-31')) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, coalesce(deviceIdentifier,deviceSerialNumber,crc32(detectedvin)) AS DEVC_KEY, cast(accelQuality as STRING) AS ACLRTN_QLTY_FL, cast(hdopAverage as decimal(15,6)) AS AVG_HDOP_QTY, cast(avgSpeed as decimal(18,5)) AS AVG_SPD_MPH_RT, cast(drivingDistance as decimal(15,6)) AS DRVNG_DISTNC_QTY, cast(fuelConsumption as decimal(15,6)) AS FUEL_CNSMPTN_QTY, cast(secondsOfIdling as decimal(15,6)) AS IDLING_SC_QTY, cast(milStatus as STRING) AS MLFNCTN_STTS_FL, cast(maxSpeed as decimal(18,5)) AS MAX_SPD_RT, cast(system as STRING) AS MSURET_UNIT_CD, cast(enterpriseReferenceId as STRING) AS PLCY_NB, cast(timeZoneOffset as decimal(10,2)) as TIME_ZONE_OFFST_NB, cast(totalTripSeconds as decimal(15,6)) AS TRIP_SC_QTY, cast(transportMode as STRING) AS TRNSPRT_MODE_CD, cast(transportModeReason as STRING) AS TRNSPRT_MODE_RSN_CD, coalesce(cast(utcStartDateTime as TIMESTAMP), to_timestamp('9999-12-31')) AS TRIP_START_TS, coalesce(cast(utcEndDateTime as TIMESTAMP), to_timestamp('9999-12-31')) AS TRIP_END_TS, coalesce(enrolledVin,detectedVin) AS VEH_KEY, coalesce(load_date, to_date('9999-12-31')) as LOAD_DT, coalesce(sourcesystem,'NOKEY') AS SRC_SYS_CD, coalesce(load_hour, to_timestamp('9999-12-31')) as LOAD_HR_TS,null as CMNT_TT,null as TRIP_FUEL_CNSMD_IDL_QTY,null as VCHR_NB,null as DISTNC_MTRWY_QTY,null as DISTNC_URBAN_AREAS_QTY,null as DISTNC_ON_ROAD_QTY,null as DISTNC_UNKNOWN_QTY,null as TIME_MTRWY_QTY,null as TIME_URBAN_AREAS_QTY,null as TIME_ON_ROAD_QTY,null as TIME_UNKNOWN_QTY ,null as SPDNG_DISTNC_MTRWY_QTY,null as SPDNG_DISTNC_URBAN_AREAS_QTY,null as SPDNG_DISTNC_ON_ROAD_QTY,null as SPDNG_DISTNC_UNKNOWN_QTY,null as SPDNG_TIME_MTRWY_QTY,null as SPDNG_TIME_URBAN_AREAS_QTY,null as SPDNG_TIME_ON_ROAD_QTY,null as SPDNG_TIME_UNKNOWN_ROAD_QTY,null as DRIV_STTS_SRC_IN,null as DRIV_STTS_DERV_IN,null as ACCT_ID,null as TRIP_START_LAT_NB,null as TRIP_START_LNGTD_NB,null as TRIP_END_LAT_NB,null as TRIP_END_LNGTD_NB,null as NIGHT_TIME_DRVNG_SEC_CNT,null as PLCY_ST_CD,null as PRGRM_CD,null as RSKPT_ACCEL_RT,null as RSKPT_BRK_RT,null as RSKPT_TRN_RT,null as RSKPT_SPDNG,null as RSKPT_PHN_MTN,null as STAR_RTNG,null as STAR_RTNG_ACCEL,null as STAR_RTNG_BRK,null as STAR_RTN_TRN,null as STAR_RTNG_SPDNG,null as STAR_RTNG_PHN_MTN,null as DRIVER_KEY,null as TAG_MAC_ADDR_NM,null as TAG_TRIP_NB,null as SHRT_VEH_ID,null as OBD_MLG_QTY,null as GPS_MLG_QTY,null as HARD_BRKE_QTY,null as HARSH_ACCLRTN_QTY,null as OVER_SPD_QTY,null as DVC_VRSN_NB from dhf_iot_ims_stageraw_prod.trip_summary_missing_trips where  timestampdiff(hour,cast(utcStartDateTime as TIMESTAMP), cast(utcEndDateTime as TIMESTAMP) )<12  and etl_load_date=current_date 
// MAGIC
// MAGIC # tims_trip_point_missing_trips=spark.sql("""select  a.tripsummaryid,a.utcDateTime,a.sourcesystem,a.load_date,a.load_hour from 
// MAGIC # (select * from dhf_iot_tims_raw_prod.telemetrypoints where load_date=current_date-1) a left anti join
// MAGIC # (select * from dhf_iot_harmonized_prod.trip_point where load_dt>=current_date-7 and src_sys_cd in ("TIMS_SR") ) b 
// MAGIC # on   a.tripsummaryid=b.trip_smry_key  and a.utcDateTime=b.UTC_TS and a.sourcesystem=b.src_sys_cd where utcDateTime is not null""")
// MAGIC
// MAGIC # fmc_trip_point_missing_trips=spark.sql("""select  a.tripsummaryid,a.utcDateTime,a.sourcesystem,a.load_date,a.load_hour from 
// MAGIC # (select * from dhf_iot_fmc_raw_prod.telemetrypoints where load_date=current_date-1) a left anti join
// MAGIC # (select * from dhf_iot_harmonized_prod.trip_point where load_dt>=current_date-7 and src_sys_cd in ("FMC_SR","FMC_SM")) b 
// MAGIC # on   a.tripsummaryid=b.trip_smry_key  and a.utcDateTime=b.UTC_TS and a.sourcesystem=b.src_sys_cd where utcDateTime is not null""")

// COMMAND ----------

// MAGIC %sql
// MAGIC --Harmonize table
// MAGIC select * from dhf_iot_harmonized_prod.trip_summary where src_sys_cd='IMS_SM_5X' and load_dt>'2023-02-25' and trip_smry_key in 
// MAGIC ('05fbc44a-39dd-4f05-bbeb-5694113e1755',
// MAGIC '6dd565bd-9bb3-4e89-a3c2-bf8273f8b349',
// MAGIC '502cc4b8-d2f1-45b0-b9ed-fcfa8296999d')

// COMMAND ----------

// MAGIC  %sql
// MAGIC  -- query to load into harmonize table from raw table
// MAGIC  select distinct cast(row_number() over(order by NULL) + 500 + coalesce((select max(TRIP_SUMMARY_ID) from dhf_iot_harmonized_prod.trip_summary),0) as BIGINT) as TRIP_SUMMARY_ID, coalesce(tripSummaryId, 'NOKEY') AS TRIP_SMRY_KEY, coalesce(db_load_time, to_timestamp('9999-12-31')) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, coalesce(deviceIdentifier,deviceSerialNumber,crc32(detectedvin)) AS DEVC_KEY, cast(accelQuality as STRING) AS ACLRTN_QLTY_FL, cast(hdopAverage as decimal(15,6)) AS AVG_HDOP_QTY, cast(avgSpeed as decimal(18,5)) AS AVG_SPD_MPH_RT, cast(drivingDistance as decimal(15,6)) AS DRVNG_DISTNC_QTY, cast(fuelConsumption as decimal(15,6)) AS FUEL_CNSMPTN_QTY, cast(secondsOfIdling as decimal(15,6)) AS IDLING_SC_QTY, cast(milStatus as STRING) AS MLFNCTN_STTS_FL, cast(maxSpeed as decimal(18,5)) AS MAX_SPD_RT, cast(system as STRING) AS MSURET_UNIT_CD, cast(enterpriseReferenceId as STRING) AS PLCY_NB, cast(timeZoneOffset as decimal(10,2)) as TIME_ZONE_OFFST_NB, cast(totalTripSeconds as decimal(15,6)) AS TRIP_SC_QTY, cast(transportMode as STRING) AS TRNSPRT_MODE_CD, cast(transportModeReason as STRING) AS TRNSPRT_MODE_RSN_CD, coalesce(cast(utcStartDateTime as TIMESTAMP), to_timestamp('9999-12-31')) AS TRIP_START_TS, coalesce(cast(utcEndDateTime as TIMESTAMP), to_timestamp('9999-12-31')) AS TRIP_END_TS, coalesce(enrolledVin,detectedVin) AS VEH_KEY, coalesce(load_date, to_date('9999-12-31')) as LOAD_DT, coalesce(sourcesystem,'NOKEY') AS SRC_SYS_CD, coalesce(load_hour, to_timestamp('9999-12-31')) as LOAD_HR_TS,null as CMNT_TT,null as TRIP_FUEL_CNSMD_IDL_QTY,null as VCHR_NB,null as DISTNC_MTRWY_QTY,null as DISTNC_URBAN_AREAS_QTY,null as DISTNC_ON_ROAD_QTY,null as DISTNC_UNKNOWN_QTY,null as TIME_MTRWY_QTY,null as TIME_URBAN_AREAS_QTY,null as TIME_ON_ROAD_QTY,null as TIME_UNKNOWN_QTY ,null as SPDNG_DISTNC_MTRWY_QTY,null as SPDNG_DISTNC_URBAN_AREAS_QTY,null as SPDNG_DISTNC_ON_ROAD_QTY,null as SPDNG_DISTNC_UNKNOWN_QTY,null as SPDNG_TIME_MTRWY_QTY,null as SPDNG_TIME_URBAN_AREAS_QTY,null as SPDNG_TIME_ON_ROAD_QTY,null as SPDNG_TIME_UNKNOWN_ROAD_QTY,null as DRIV_STTS_SRC_IN,null as DRIV_STTS_DERV_IN,null as ACCT_ID,null as TRIP_START_LAT_NB,null as TRIP_START_LNGTD_NB,null as TRIP_END_LAT_NB,null as TRIP_END_LNGTD_NB,null as NIGHT_TIME_DRVNG_SEC_CNT,null as PLCY_ST_CD,null as PRGRM_CD,null as RSKPT_ACCEL_RT,null as RSKPT_BRK_RT,null as RSKPT_TRN_RT,null as RSKPT_SPDNG,null as RSKPT_PHN_MTN,null as STAR_RTNG,null as STAR_RTNG_ACCEL,null as STAR_RTNG_BRK,null as STAR_RTN_TRN,null as STAR_RTNG_SPDNG,null as STAR_RTNG_PHN_MTN,null as DRIVER_KEY,null as TAG_MAC_ADDR_NM,null as TAG_TRIP_NB,null as SHRT_VEH_ID,null as OBD_MLG_QTY,null as GPS_MLG_QTY,null as HARD_BRKE_QTY,null as HARSH_ACCLRTN_QTY,null as OVER_SPD_QTY,null as DVC_VRSN_NB from dhf_iot_ims_raw_prod.tripSummary where sourcesystem='IMS_SM_5X'  and load_date>'2023-02-25' and 
// MAGIC tripSummaryId in 
// MAGIC ('05fbc44a-39dd-4f05-bbeb-5694113e1755',
// MAGIC '6dd565bd-9bb3-4e89-a3c2-bf8273f8b349',
// MAGIC '502cc4b8-d2f1-45b0-b9ed-fcfa8296999d')

// COMMAND ----------

// MAGIC %sql
// MAGIC --trip_summary_id's which are  missing in trip_summary harmonize table
// MAGIC select  DISTINCT a.* from 
// MAGIC (select * from dhf_iot_ims_raw_prod.tripsummary where sourcesystem='IMS_SM_5X' and load_date='2023-03-09' ) a left anti join 
// MAGIC (select * from dhf_iot_harmonized_prod.trip_summary where src_sys_cd='IMS_SM_5X' and   load_dt='2023-03-09') b 
// MAGIC on   a.tripsummaryid=b.trip_smry_key and a.sourcesystem=b.src_sys_cd;

// COMMAND ----------

// MAGIC %sql
// MAGIC --trip_summary_id's which are  missing in trip_point harmonize table
// MAGIC select  DISTINCT a.* from 
// MAGIC (select * from dhf_iot_ims_raw_prod.telemetrypoints where sourcesystem='IMS_SM_5X' and load_date>='2023-02-01' and load_date<='2023-03-01') a left anti join 
// MAGIC (select * from dhf_iot_harmonized_prod.trip_point where src_sys_cd='IMS_SM_5X' and   load_dt>='2023-02-01' and load_dt<='2023-03-01') b
// MAGIC on   a.tripsummaryid=b.trip_smry_key and a.sourcesystem=b.src_sys_cd;

// COMMAND ----------

// MAGIC %sql
// MAGIC --tripsummaryid,timeZoneOffset,enrolledVin fields not matching between raw and harmonize trip_summary table
// MAGIC select   a.tripsummaryid,b.trip_smry_key , a.offset , a.enrolledVin,b.veh_key from 
// MAGIC (select *,cast(timeZoneOffset as 
// MAGIC decimal(10,2)) as offset from dhf_iot_ims_raw_prod.tripsummary where sourcesystem='IMS_SM_5X' and load_date>='2023-02-01' and load_date<='2023-03-01' ) a inner join 
// MAGIC (select * from dhf_iot_harmonized_prod.trip_summary where src_sys_cd='IMS_SM_5X' and   load_dt>='2023-02-01' and load_dt<='2023-03-01') b 
// MAGIC on   a.tripsummaryid=b.trip_smry_key and (a.offset!=b.TIME_ZONE_OFFST_NB or a.enrolledVin!=b.veh_key)

// COMMAND ----------

// MAGIC %sql
// MAGIC -- tripsummaryid , offset, enrolledVin,speed,, engineRPM,, degreesLatitude , degreesLongitude fields not matching between raw and harmonize trip_point table
// MAGIC select   a.tripsummaryid,b.trip_smry_key , a.offset,b.TIME_ZONE_OFFST_NB , a.enrolledVin,b.ENRLD_VIN_NB, a.speed,b.SPD_RT , a.engineRPM,b.ENGIN_RPM_RT , a.degreesLatitude,b.LAT_NB , a.degreesLongitude,b.LNGTD_NB  from 
// MAGIC (select *,cast(timeZoneOffset as 
// MAGIC decimal(10,2)) as offset from dhf_iot_ims_raw_prod.telemetrypoints where sourcesystem='IMS_SM_5X' and   load_date='2023-02-01' ) a inner join 
// MAGIC (select * from dhf_iot_harmonized_prod.trip_point where src_sys_cd='IMS_SM_5X' and   load_dt='2023-02-01') b 
// MAGIC on   a.tripsummaryid=b.trip_smry_key and a.utcDateTime=b.UTC_TS and (a.offset!=b.TIME_ZONE_OFFST_NB or a.enrolledVin!=b.ENRLD_VIN_NB or a.speed!=b.SPD_RT  or a.engineRPM!=b.ENGIN_RPM_RT  or a.degreesLatitude!=b.LAT_NB  or a.degreesLongitude!=b.LNGTD_NB  )
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC --tripsummaryid,timeZoneOffset,enrolledVin fields not matching between raw and harmonize table
// MAGIC select   a.tripsummaryid,b.trip_smry_key , a.offset , a.enrolledVin,b.ENRLD_VIN_NB from (select *,cast(timeZoneOffset as 
// MAGIC decimal(10,2)) as offset from dhf_iot_ims_raw_prod.telemetrypoints where sourcesystem='IMS_SM_5X' and load_date>='2023-02-01' and load_date<='2023-02-07' ) a inner join (select * from dhf_iot_harmonized_prod.trip_point where src_sys_cd='IMS_SM_5X' and   load_dt>='2023-02-01' and load_dt<='2023-02-07') b on   a.tripsummaryid=b.trip_smry_key and (a.offset!=b.TIME_ZONE_OFFST_NB or a.enrolledVin!=b.ENRLD_VIN_NB)

// COMMAND ----------

// MAGIC %sql
// MAGIC  select count(*) from dhf_iot_ims_raw_prod.telemetrypoints where sourcesystem='IMS_SM_5X' and   load_date='2023-02-01'

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select  a.tripsummaryid,b.trip_smry_key , a.offset,b.TIME_ZONE_OFFST_NB , a.enrolledVin,b.ENRLD_VIN_NB, a.speed,b.SPD_RT , a.engineRPM,b.ENGIN_RPM_RT , a.degreesLatitude,b.LAT_NB , a.degreesLongitude,b.LNGTD_NB from 
// MAGIC (select *,cast(timeZoneOffset as 
// MAGIC decimal(10,2)) as offset from dhf_iot_ims_raw_prod.telemetrypoints where sourcesystem='IMS_SM_5X' and load_date>'2023-02-25' and 
// MAGIC tripSummaryId in 
// MAGIC ('05fbc44a-39dd-4f05-bbeb-5694113e1755',
// MAGIC '6dd565bd-9bb3-4e89-a3c2-bf8273f8b349',
// MAGIC '502cc4b8-d2f1-45b0-b9ed-fcfa8296999d')) a inner join
// MAGIC (select * from dhf_iot_harmonized_prod.trip_point where src_sys_cd='IMS_SM_5X'  and load_dt>'2023-02-25' and trip_smry_key in 
// MAGIC ('05fbc44a-39dd-4f05-bbeb-5694113e1755',
// MAGIC '6dd565bd-9bb3-4e89-a3c2-bf8273f8b349',
// MAGIC '502cc4b8-d2f1-45b0-b9ed-fcfa8296999d')) b 
// MAGIC on   a.tripsummaryid=b.trip_smry_key  and a.utcDateTime=b.UTC_TS
// MAGIC
// MAGIC
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC --trip_summary_id not matching between raw and harmonize table
// MAGIC select  a.tripsummaryid,b.trip_smry_key , a.offset,b.TIME_ZONE_OFFST_NB , a.enrolledVin,b.veh_key from 
// MAGIC (select *,cast(timeZoneOffset as 
// MAGIC decimal(10,2)) as offset from dhf_iot_ims_raw_prod.tripsummary where sourcesystem='IMS_SM_5X' and load_date='2023-03-01') a inner join
// MAGIC (select * from dhf_iot_harmonized_prod.trip_summary where src_sys_cd='IMS_SM_5X'  and load_dt='2023-03-01' ) b 
// MAGIC on   a.tripsummaryid=b.trip_smry_key 
// MAGIC
// MAGIC
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select * from dhf_iot_harmonized_prod.trip_summary_missing where sourcesystem='IMS_SM_5X' and load_date='2022-12-19'
// MAGIC select load_date,count(*) from dhf_iot_harmonized_prod.trip_summary_missing where sourcesystem='IMS_SM_5X' group by  load_date

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from  dhf_iot_ims_raw_prod.tripsummary where sourcesystem='IMS_SM_5X' and load_date='2023-02-28' and db_load_time='2023-02-28T15:47:57.911+0000'

// COMMAND ----------

// MAGIC %sql
// MAGIC show tables in dhf_iot_harmonized_prod