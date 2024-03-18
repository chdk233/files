-- Databricks notebook source
-- MAGIC  %python
-- MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventcuration/18201",True)
-- MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration_curation/18201",True)
-- MAGIC
-- MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventcuration/18601",True)
-- MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration_curation/18601",True)
-- MAGIC
-- MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventcuration/18202",True)
-- MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration_curation/18202",True)
-- MAGIC
-- MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventcuration/18611",True)
-- MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration_curation/182611",True)
-- MAGIC
-- MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventcuration/18612",True)
-- MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration_curation/18612",True)
-- MAGIC
-- MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventcuration/18613",True)
-- MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration_curation/18613",True)

-- COMMAND ----------

-- %sql 
-- delete from dhf_iot_curated_prod.trip_detail_chlg;
-- delete from dhf_iot_curated_prod.device_status_chlg;
-- -- delete from dhf_iot_curated_prod.device_summary_chlg;
-- delete from dhf_iot_curated_prod.program_summary_chlg;
-- -- delete from dhf_iot_curated_prod.outbound_score_elements_chlg;
-- delete from dhf_iot_harmonized_prod.trip_detail_seconds_chlg;
-- delete from dhf_iot_curated_prod.daily_mileage_chlg;
-- delete from dhf_iot_curated_prod.ca_annual_mileage_scoring_chlg;


-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- describe history dhf_iot_curated_prod.device_status_intsr4x 
-- MAGIC restore dhf_iot_curated_prod.device_status_intsr4x to version as of 9

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC merge into  dhf_iot_curated_prod.device_status_intsr4x  a using dhf_iot_harmonized_prod.ds_max b on a.ENRLD_VIN_NB=b.ENRLD_VIN_NB and a.STTS_EFCTV_TS>b.STTS_EFCTV_TS when matched then delete
-- MAGIC -- select count(distinct enrld_vin_nb)from dhf_iot_curated_prod.device_status_intsr4x--423847

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC merge into dhf_iot_curated_prod.device_status_intsr4x a using (select a.ENRLD_VIN_NB,a.max_STTS_EFCTV,b.STTS_EFCTV_TS from (select ENRLD_VIN_NB,max(STTS_EFCTV_TS) as max_STTS_EFCTV from dhf_iot_curated_prod.device_status_intsr4x  group by ENRLD_VIN_NB  ) a inner join dhf_iot_harmonized_prod.ds_max b on a.ENRLD_VIN_NB=b.ENRLD_VIN_NB ) b on  a.ENRLD_VIN_NB=b.ENRLD_VIN_NB and  a.STTS_EFCTV_TS=b.max_STTS_EFCTV when matched then update set a.STTS_EXPRTN_TS=b.STTS_EFCTV_TS

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC import org.apache.spark.sql.types._
-- MAGIC spark.conf.set("spark.sql.shuffle.partitions", "5000")
-- MAGIC val df2=spark.sql("select * from dhf_iot_curated_prod.device_status_intsr4x where  load_dt<'2017-01-01'")
-- MAGIC val df=df2.drop("DEVICE_STATUS_ID").withColumn("DEVICE_STATUS_ID",monotonically_increasing_id().cast(LongType)).withColumn("PRGRM_INSTC_ID",col("prog_inst_id").cast(LongType)).drop("prog_inst_id").withColumn("DEVC_KEY",coalesce(col("DEVC_KEY"),lit("NOKEY")))
-- MAGIC df.repartition(5000).write.format("delta").mode("append").saveAsTable("dhf_iot_curated_prod.device_status")
-- MAGIC // display(df)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dhf_iot_curated_prod.device_status_intsr4x

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from ((select ENRLD_VIN_NB,max(STTS_EXPRTN_TS) as max_STTS_EXPRTN,max(STTS_EFCTV_TS) as max_STTS_EFCTV from dhf_iot_curated_prod.device_status_intsr4x  group by ENRLD_VIN_NB  ) a inner join dhf_iot_harmonized_prod.ds_max b on a.ENRLD_VIN_NB=b.ENRLD_VIN_NB) where max_STTS_EFCTV>STTS_EFCTV_TS or max_STTS_EXPRTN>STTS_EFCTV_TS

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC spark.sql("""select ENRLD_VIN_NB,max(STTS_EXPRTN_TS) as max_STTS_EXPRTN_TS,min(STTS_EXPRTN_TS) as min_STTS_EXPRTN_TS,max(STTS_EFCTV_TS) as max_STTS_EFCTV_TS,min(STTS_EFCTV_TS) as STTS_EFCTV_TS from (select a.* from dhf_iot_curated_prod.DEVICE_STATUS a inner join dhf_iot_curated_prod.device_status_intsr4x b on  a.ENRLD_VIN_NB=trim(b.ENRLD_VIN_NB)  and a.src_sys_cd='IMS_SR_4X') group by ENRLD_VIN_NB""").write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.ds_max")

-- COMMAND ----------

MERGE into dhf_iot_harmonized_prod.vehicle as target
USING (
      with t as(
      select *,row_number() over (partition by veh_key order by load_dt desc ) as dup from  dhf_iot_harmonized_prod.vehicle  where src_sys_cd in ('OCTO_SR','IMS_SR_4X')
      )
      select * from t where dup > 1
 
) 
as source
ON source.veh_key=target.veh_key and source.VEHICLE_ID=target.VEHICLE_ID and source.SRC_SYS_CD=target.SRC_SYS_CD  and target.SRC_SYS_CD in ('OCTO_SR','IMS_SR_4X')
WHEN MATCHED THEN DELETE

-- COMMAND ----------

MERGE into dhf_iot_harmonized_prod.device as target
USING (
      with t as(
      select *,row_number() over (partition by devc_key order by load_dt desc ) as dup from  dhf_iot_harmonized_prod.device  where src_sys_cd in ('OCTO_SR','IMS_SR_4X')
      )
      select * from t where dup > 1
 
) 
as source
ON source.devc_key=target.devc_key and source.DEVICE_ID=target.DEVICE_ID and source.SRC_SYS_CD=target.SRC_SYS_CD and target.SRC_SYS_CD in ('OCTO_SR','IMS_SR_4X')
WHEN MATCHED THEN DELETE

-- COMMAND ----------

set spark.sql.autoBroadcastJoinThreshold=-1

-- COMMAND ----------

set spark.sql.autoBroadcastJoinThreshold=-1;
MERGE into dhf_iot_harmonized_prod.TRIP_SUMMARY as target
USING (
      with t as(
      select *,row_number() over (partition by TRIP_SMRY_key order by load_dt desc ) as dup from  dhf_iot_harmonized_prod.TRIP_SUMMARY where src_sys_cd in ('IMS_SR_4X')
      )
      select * from t where dup > 1
 
) 
as source
ON source.TRIP_SMRY_key=target.TRIP_SMRY_key and source.TRIP_SUMMARY_ID=target.TRIP_SUMMARY_ID and target.SRC_SYS_CD='IMS_SR_4X'  
WHEN MATCHED THEN DELETE;

-- COMMAND ----------

MERGE into dhf_iot_harmonized_prod.TRIP_EVENT as target
USING (
      with t as(
      select *,row_number() over (partition by TRIP_SMRY_KEY,EVNT_TP_CD,UTC_TS order by load_dt desc ) as dup from  dhf_iot_harmonized_prod.TRIP_EVENT  where src_sys_cd in ('OCTO_SR') 
      )
      select * from t where dup > 1
 
) 
as source
ON source.TRIP_SMRY_key=target.TRIP_SMRY_key and source.EVNT_TP_CD=target.EVNT_TP_CD and source.UTC_TS=target.UTC_TS and source.TRIP_EVENT_ID=target.TRIP_EVENT_ID and  target.SRC_SYS_CD='OCTO_SR'  
WHEN MATCHED THEN DELETE

-- COMMAND ----------

MERGE into dhf_iot_harmonized_prod.TRIP_EVENT as target
USING (
      with t as(
      select *,row_number() over (partition by TRIP_SMRY_KEY,EVNT_TP_CD,UTC_TS order by load_dt desc ) as dup from  dhf_iot_harmonized_prod.TRIP_EVENT  where src_sys_cd in ('IMS_SR_4X') 
      )
      select * from t where dup > 1
 
) 
as source
ON source.TRIP_SMRY_key=target.TRIP_SMRY_key and source.EVNT_TP_CD=target.EVNT_TP_CD and source.UTC_TS=target.UTC_TS and source.TRIP_EVENT_ID=target.TRIP_EVENT_ID and  target.SRC_SYS_CD='IMS_SR_4X'  
WHEN MATCHED THEN DELETE

-- COMMAND ----------

MERGE into dhf_iot_harmonized_prod.TRIP_POINT as target
USING (
      with t as(
      select *,row_number() over (partition by TRIP_SMRY_KEY,UTC_TS order by load_dt desc ) as dup from  dhf_iot_harmonized_prod.TRIP_POINT  where src_sys_cd='IMS_SR_4X' 
      )
      select * from t where dup > 1
 
) 
as source
ON source.TRIP_SMRY_key=target.TRIP_SMRY_key and source.UTC_TS=target.UTC_TS and source.TRIP_POINT_ID=target.TRIP_POINT_ID and source.SRC_SYS_CD=target.SRC_SYS_CD and source.load_dt=target.load_dt
WHEN MATCHED THEN DELETE

-- COMMAND ----------

MERGE into dhf_iot_harmonized_prod.TRIP_POINT as target
USING (
      with t as(
      select *,row_number() over (partition by TRIP_SMRY_KEY,UTC_TS order by load_dt desc ) as dup from  dhf_iot_harmonized_prod.TRIP_POINT  where src_sys_cd in ('OCTO_SR') 
      )
      select * from t where dup > 1
 
) 
as source
ON source.TRIP_SMRY_key=target.TRIP_SMRY_key and source.UTC_TS=target.UTC_TS and source.TRIP_POINT_ID=target.TRIP_POINT_ID and target.SRC_SYS_CD='OCTO_SR' 
WHEN MATCHED THEN DELETE

-- COMMAND ----------

-- MERGE into dhf_iot_harmonized_prod.TRIP_POINT as target
-- USING (
--       with t as(
--       select *,row_number() over (partition by TRIP_SMRY_KEY,UTC_TS order by load_dt desc ) as dup from  dhf_iot_harmonized_prod.TRIP_POINT  where src_sys_cd='FMC_SR'
--       )
--       select * from t where dup > 1
 
-- ) 
-- as source
-- ON source.TRIP_SMRY_key=target.TRIP_SMRY_key and source.UTC_TS=target.UTC_TS and source.TRIP_POINT_ID=target.TRIP_POINT_ID and source.SRC_SYS_CD=target.SRC_SYS_CD and target.SRC_SYS_CD='FMC_SR'  
-- WHEN MATCHED THEN DELETE

-- COMMAND ----------

MERGE into dhf_iot_harmonized_prod.NON_TRIP_EVENT as target
USING (
      with t as(
      select *,row_number() over (partition by NON_TRIP_EVNT_KEY,EVNT_TP_CD,UTC_TS order by load_dt desc ) as dup from  dhf_iot_harmonized_prod.NON_TRIP_EVENT where src_sys_cd in ('OCTO_SR') 
      )
      select * from t where dup > 1
 
) 
as source
ON source.NON_TRIP_EVNT_KEY=target.NON_TRIP_EVNT_KEY and source.EVNT_TP_CD=target.EVNT_TP_CD and source.UTC_TS=target.UTC_TS and source.NON_TRIP_EVENT_ID=target.NON_TRIP_EVENT_ID and target.SRC_SYS_CD='OCTO_SR' 
WHEN MATCHED THEN DELETE

-- COMMAND ----------

MERGE into dhf_iot_harmonized_prod.NON_TRIP_EVENT as target
USING (
      with t as(
      select *,row_number() over (partition by NON_TRIP_EVNT_KEY,EVNT_TP_CD,UTC_TS order by load_dt desc ) as dup from  dhf_iot_harmonized_prod.NON_TRIP_EVENT where src_sys_cd in ('IMS_SR_4X') 
      )
      select * from t where dup > 1
 
) 
as source
ON source.NON_TRIP_EVNT_KEY=target.NON_TRIP_EVNT_KEY and source.EVNT_TP_CD=target.EVNT_TP_CD and source.UTC_TS=target.UTC_TS and source.NON_TRIP_EVENT_ID=target.NON_TRIP_EVENT_ID and target.SRC_SYS_CD='IMS_SR_4X' 
WHEN MATCHED THEN DELETE

-- COMMAND ----------

-- MERGE into dhf_iot_harmonized_prod.SCORING as target
-- USING (
--       with t as(
--       select *,row_number() over (partition by TRIP_SMRY_KEY,INDV_CMPNT_SET_TP_VAL order by load_dt desc ) as dup from  dhf_iot_harmonized_prod.SCORING  where  load_dt >='2022-07-20' and load_dt<'2022-08-11'
--       )
--       select * from t where dup > 1
 
-- ) 
-- as source
-- ON source.TRIP_SMRY_key=target.TRIP_SMRY_key and source.INDV_CMPNT_SET_TP_VAL=target.INDV_CMPNT_SET_TP_VAL and source.SCORING_ID=target.SCORING_ID and target.SRC_SYS_CD='IMS_SM_5X'  and target.load_dt>='2022-07-20' and target.load_dt<'2022-08-11'
-- WHEN MATCHED THEN DELETE

-- COMMAND ----------

-- MERGE into dhf_iot_harmonized_prod.histogram_scoring_interval as target
-- USING (
--       with t as(
--       select *,row_number() over (partition by TRIP_SMRY_KEY,SCRG_CMPNT_TP_CD,SCRG_SUB_CMPNT_TP_CD,ROAD_TP_DSC,LOWER_BND_THRSHLD_QTY,UPPER_BND_THRSHLD_QTY order by load_dt desc ) as dup from  dhf_iot_harmonized_prod.histogram_scoring_interval  where   load_dt >='2022-07-20' and load_dt<'2022-08-11'
--       )
--       select * from t where dup > 1
 
-- ) 
-- as source
-- ON source.TRIP_SMRY_key=target.TRIP_SMRY_key and  source.SCRG_CMPNT_TP_CD=target.SCRG_CMPNT_TP_CD and source.SCRG_SUB_CMPNT_TP_CD=target.SCRG_SUB_CMPNT_TP_CD and source.ROAD_TP_DSC=target.ROAD_TP_DSC and source.LOWER_BND_THRSHLD_QTY=target.LOWER_BND_THRSHLD_QTY and source.UPPER_BND_THRSHLD_QTY=target.UPPER_BND_THRSHLD_QTY and source.histogram_scoring_interval_ID=target.histogram_scoring_interval_ID and target.SRC_SYS_CD='IMS_SM_5X'  and target.load_dt>='2022-07-20' and target.load_dt<'2022-08-11'
-- WHEN MATCHED THEN DELETE

-- COMMAND ----------

-- MAGIC %python
-- MAGIC device_harmonize_df=spark.sql("""select distinct cast(monotonically_increasing_id() +1 + coalesce((select max(DEVICE_ID) from dhf_iot_harmonized_prod.device),0) as BIGINT) as DEVICE_ID, coalesce(deviceIdentifier,deviceSerialNumber,crc32(detectedvin), "NOKEY") AS DEVC_KEY, deviceIdentifier as DEVC_ID_NB, deviceIdentifierType as DEVC_ID_TP_CD, deviceType as DEVC_TP_CD, deviceSerialNumber as DEVC_SRL_NB, db_load_time AS ETL_ROW_EFF_DTS, 
-- MAGIC current_timestamp() as ETL_LAST_UPDT_DTS, 
-- MAGIC  load_date as LOAD_DT, 
-- MAGIC sourcesystem AS SRC_SYS_CD, 
-- MAGIC load_hour as LOAD_HR_TS from 
-- MAGIC (select *,row_number() over (partition by deviceIdentifier,deviceSerialNumber order by load_hour desc) as row_number from dhf_iot_ims_raw_prod.nontripevent
-- MAGIC    WHERE sourcesystem in ('IMS_SM_5X') and  load_date>='2022-07-20' and load_date<'2022-08-11' and coalesce(deviceIdentifier,deviceSerialNumber,crc32(detectedvin), "NOKEY") not in (select DEVC_KEY from dhf_iot_harmonized_prod.device)) where row_number=1 
-- MAGIC """)
-- MAGIC
-- MAGIC device_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.device")
-- MAGIC # display(device_harmonize_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC device_harmonize_df=spark.sql("""select distinct cast(monotonically_increasing_id() +1 + coalesce((select max(DEVICE_ID) from dhf_iot_harmonized_prod.device),0) as BIGINT) as DEVICE_ID, coalesce(deviceIdentifier,deviceSerialNumber,crc32(detectedvin), "NOKEY") AS DEVC_KEY, deviceIdentifier as DEVC_ID_NB, deviceIdentifierType as DEVC_ID_TP_CD, deviceType as DEVC_TP_CD, deviceSerialNumber as DEVC_SRL_NB, db_load_time AS ETL_ROW_EFF_DTS, 
-- MAGIC current_timestamp() as ETL_LAST_UPDT_DTS, 
-- MAGIC  load_date as LOAD_DT, 
-- MAGIC sourcesystem AS SRC_SYS_CD, 
-- MAGIC load_hour as LOAD_HR_TS from 
-- MAGIC (select *,row_number() over (partition by deviceIdentifier,deviceSerialNumber order by load_hour desc) as row_number from dhf_iot_ims_raw_prod.tripsummary
-- MAGIC   WHERE sourcesystem in ('IMS_SM_5X') and  load_date>='2022-07-20' and load_date<'2022-08-11' and coalesce(deviceIdentifier,deviceSerialNumber,crc32(detectedvin), "NOKEY") not in (select DEVC_KEY from dhf_iot_harmonized_prod.device)) where row_number=1 
-- MAGIC """)
-- MAGIC
-- MAGIC device_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.device")
-- MAGIC # display(device_harmonize_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC vehicle_harmonize_df=spark.sql("""select distinct cast(monotonically_increasing_id() +1 + coalesce((select max(VEHICLE_ID) from dhf_iot_harmonized_prod.vehicle),0) as BIGINT) as VEHICLE_ID, COALESCE(enrolledVin,detectedVin,"NOKEY") AS VEH_KEY, COALESCE(detectedVin,enrolledVin) as DTCTD_VIN_NB, COALESCE(enrolledVin,detectedVin,"NOKEY") as ENRLD_VIN_NB, load_date as LOAD_DT, 
-- MAGIC sourcesystem AS SRC_SYS_CD, 
-- MAGIC load_hour as LOAD_HR_TS,db_load_time AS ETL_ROW_EFF_DTS, 
-- MAGIC current_timestamp() as ETL_LAST_UPDT_DTS from 
-- MAGIC (select *,row_number() over (partition by enrolledVin,detectedVin order by load_hour desc) as row_number from dhf_iot_ims_raw_prod.nontripevent WHERE sourcesystem in ('IMS_SM_5X') and  load_date>='2022-07-20' and load_date<'2022-08-11' and COALESCE(enrolledVin,detectedVin,"NOKEY") not in (select VEH_KEY from dhf_iot_harmonized_prod.vehicle)) where row_number=1""")
-- MAGIC
-- MAGIC vehicle_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.vehicle")
-- MAGIC # display(vehicle_harmonize_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC vehicle_harmonize_df=spark.sql("""select distinct cast(monotonically_increasing_id() +1 + coalesce((select max(VEHICLE_ID) from dhf_iot_harmonized_prod.vehicle),0) as BIGINT) as VEHICLE_ID, COALESCE(enrolledVin,detectedVin,"NOKEY") AS VEH_KEY, COALESCE(detectedVin,enrolledVin) as DTCTD_VIN_NB, COALESCE(enrolledVin,detectedVin,"NOKEY") as ENRLD_VIN_NB, load_date as LOAD_DT, 
-- MAGIC sourcesystem AS SRC_SYS_CD, 
-- MAGIC load_hour as LOAD_HR_TS,db_load_time AS ETL_ROW_EFF_DTS, 
-- MAGIC current_timestamp() as ETL_LAST_UPDT_DTS from 
-- MAGIC (select *,row_number() over (partition by enrolledVin,detectedVin order by load_hour desc) as row_number from dhf_iot_ims_raw_prod.tripsummary WHERE sourcesystem in ('IMS_SM_5X') and  load_date>='2022-07-20' and load_date<'2022-08-11' and COALESCE(enrolledVin,detectedVin,"NOKEY") not in (select VEH_KEY from dhf_iot_harmonized_prod.vehicle)) where row_number=1""")
-- MAGIC
-- MAGIC vehicle_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.vehicle")
-- MAGIC # display(vehicle_harmonize_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC device_harmonize_df=spark.sql("""select DISTINCT cast(monotonically_increasing_id() +1  + coalesce((select max(DEVICE_ID) from dhf_iot_harmonized_prod.device),0) as BIGINT) as DEVICE_ID, coalesce(crc32(detectedvin), "NOKEY") AS DEVC_KEY,load_date as LOAD_DT, 
-- MAGIC "TIMS_SR" AS SRC_SYS_CD, db_load_time AS ETL_ROW_EFF_DTS, 
-- MAGIC current_timestamp() as ETL_LAST_UPDT_DTS, 
-- MAGIC load_hour as LOAD_HR_TS from 
-- MAGIC (select *,row_number() over (partition by detectedvin order by load_hour desc) as row_number from dhf_iot_tims_raw_prod.tripsummary
-- MAGIC   WHERE sourcesystem in ('TIMS_SR') and  load_date>='2022-07-04' and load_date<'2022-08-11' and coalesce(crc32(detectedvin), "NOKEY") not in (select DEVC_KEY from dhf_iot_harmonized_prod.device)) where row_number=1 
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC device_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.device")
-- MAGIC # display(device_harmonize_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC vehicle_harmonize_df=spark.sql("""select  DISTINCT cast(monotonically_increasing_id() +1 + coalesce((select max(VEHICLE_ID) from dhf_iot_harmonized_prod.vehicle),0) as BIGINT) as VEHICLE_ID, COALESCE(detectedVin,"NOKEY") AS VEH_KEY, detectedVin as DTCTD_VIN_NB, COALESCE(detectedVin,"NOKEY") as ENRLD_VIN_NB, LOAD_DATE as LOAD_DT, 
-- MAGIC "TIMS_SR" AS SRC_SYS_CD, db_load_time AS ETL_ROW_EFF_DTS, 
-- MAGIC current_timestamp() as ETL_LAST_UPDT_DTS, 
-- MAGIC LOAD_HOUR as LOAD_HR_TS from 
-- MAGIC (select *,row_number() over (partition by detectedVin order by load_hour desc) as row_number from dhf_iot_ims_raw_prod.tripsummary WHERE sourcesystem in ('TIMS_SR') and  load_date>='2022-07-04' and load_date<'2022-08-11' and detectedVin not in (select VEH_KEY from dhf_iot_harmonized_prod.vehicle)) where row_number=1""")
-- MAGIC
-- MAGIC vehicle_harmonize_df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.vehicle")
-- MAGIC # display(vehicle_harmonize_df)

-- COMMAND ----------

MERGE into dhf_iot_curated_test.device_status; as target
USING (
      
      select *,cast(cast(devc_key as bigint) as string) as new_devc_key from dhf_iot_curated_test.device_status; 
 
) 
as source
ON source.enrld_vin_nb=target.enrld_vin_nb and source.devc_key=target.devc_key and source.device_status_id=target.device_status_id and source.SRC_SYS_CD=target.SRC_SYS_CD   
WHEN MATCHED THEN UPDATE set target.devc_key=source.new_devc_key

-- COMMAND ----------

-- MAGIC                                  
-- MAGIC %scala
-- MAGIC val program_enrollmentDF=spark.sql(s"""select distinct VNDR_ACCT_ID,data_clctn_id,
-- MAGIC CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
-- MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
-- MAGIC     ELSE PRGRM_TP_CD
-- MAGIC END PRGRM_TP_CD,cASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
-- MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
-- MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
-- MAGIC    END src_sys_cd_pe,EVNT_SYS_TS,plcy_nb,
-- MAGIC prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,data_clctn_stts_cd,
-- MAGIC devc_stts_dt,entrp_cust_nb,sbjt_id,sbjt_tp_cd,vndr_cd,
-- MAGIC devc_id,devc_stts_cd,instl_grc_end_dt,plcy_st_cd, last_rqst_dt
-- MAGIC from (select
-- MAGIC distinct VNDR_ACCT_ID,data_clctn_id,PRGRM_TP_CD,EVNT_SYS_TS,
-- MAGIC plcy_nb,prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,
-- MAGIC coalesce(DATA_CLCTN_STTS,'') as data_clctn_stts_cd,
-- MAGIC coalesce(devc_stts_dt,' ') devc_stts_dt,
-- MAGIC coalesce(ENTRP_CUST_NB,' ') entrp_cust_nb,
-- MAGIC coalesce(sbjt_id,' ') as sbjt_id,
-- MAGIC coalesce(sbjt_tp_cd,' ') as sbjt_tp_cd,
-- MAGIC coalesce(vndr_cd,' ') as vndr_cd,
-- MAGIC coalesce(devc_id,' ') as devc_id,
-- MAGIC coalesce(devc_stts_cd,' ') as devc_stts_cd,
-- MAGIC coalesce(instl_grc_end_dt,' ') as instl_grc_end_dt,
-- MAGIC coalesce(plcy_st_cd,' ') as plcy_st_cd,
-- MAGIC coalesce(last_rqst_dt,' ') as last_rqst_dt,
-- MAGIC PROGRAM_ENROLLMENT_ID ,
-- MAGIC LOAD_HR_TS,
-- MAGIC ETL_LAST_UPDT_DTS 
-- MAGIC ,row_number() over (partition by DATA_CLCTN_ID order by EVNT_SYS_TS desc) as rnk
-- MAGIC from dhf_iot_harmonized_prod.program_enrollment
-- MAGIC where upper(VNDR_CD)= 'CMT'
-- MAGIC )where rnk = 1""").select("DATA_CLCTN_ID","PRGRM_TP_CD","src_sys_cd_pe","plcy_st_cd").distinct()
-- MAGIC val trip_src=spark.sql("select a.* from (select distinct driveid,account_id,load_dt,src_sys_cd from dhf_iot_cmt_raw_prod.trip_summary_realtime where load_dt <'2021-06-12')a inner join (select distinct drive_id,load_dt from dhf_iot_cmt_raw_prod.trip_detail_realtime where src_sys_cd is null and load_dt <'2021-06-12')b on a.driveid=b.drive_id and a.load_dt=b.load_dt")
-- MAGIC val trip2=trip_src.join(program_enrollmentDF,trip_src("account_id") ===  program_enrollmentDF("DATA_CLCTN_ID"),"left")
-- MAGIC trip2.createOrReplaceTempView("tab")
-- MAGIC spark.sql("merge into dhf_iot_cmt_raw_prod.trip_detail_realtime a using tab b on a.drive_id=b.driveid and a.load_dt=b.load_dt and a.load_dt<'2021-06-12' and a.src_sys_cd is null when matched then update set a.src_sys_cd=b.src_sys_cd_pe ")

-- COMMAND ----------

-- MAGIC                                  
-- MAGIC %scala
-- MAGIC spark.sql("set spark.sql.shuffle.partitions=auto")
-- MAGIC val program_enrollmentDF=spark.sql(s"""select distinct VNDR_ACCT_ID,data_clctn_id,
-- MAGIC CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
-- MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
-- MAGIC     ELSE PRGRM_TP_CD
-- MAGIC END PRGRM_TP_CD,cASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
-- MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
-- MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
-- MAGIC    END src_sys_cd_pe,EVNT_SYS_TS,plcy_nb,
-- MAGIC prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,data_clctn_stts_cd,
-- MAGIC devc_stts_dt,entrp_cust_nb,sbjt_id,sbjt_tp_cd,vndr_cd,
-- MAGIC devc_id,devc_stts_cd,instl_grc_end_dt,plcy_st_cd, last_rqst_dt
-- MAGIC from (select
-- MAGIC distinct VNDR_ACCT_ID,data_clctn_id,PRGRM_TP_CD,EVNT_SYS_TS,
-- MAGIC plcy_nb,prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,
-- MAGIC coalesce(DATA_CLCTN_STTS,'') as data_clctn_stts_cd,
-- MAGIC coalesce(devc_stts_dt,' ') devc_stts_dt,
-- MAGIC coalesce(ENTRP_CUST_NB,' ') entrp_cust_nb,
-- MAGIC coalesce(sbjt_id,' ') as sbjt_id,
-- MAGIC coalesce(sbjt_tp_cd,' ') as sbjt_tp_cd,
-- MAGIC coalesce(vndr_cd,' ') as vndr_cd,
-- MAGIC coalesce(devc_id,' ') as devc_id,
-- MAGIC coalesce(devc_stts_cd,' ') as devc_stts_cd,
-- MAGIC coalesce(instl_grc_end_dt,' ') as instl_grc_end_dt,
-- MAGIC coalesce(plcy_st_cd,' ') as plcy_st_cd,
-- MAGIC coalesce(last_rqst_dt,' ') as last_rqst_dt,
-- MAGIC PROGRAM_ENROLLMENT_ID ,
-- MAGIC LOAD_HR_TS,
-- MAGIC ETL_LAST_UPDT_DTS 
-- MAGIC ,row_number() over (partition by DATA_CLCTN_ID order by EVNT_SYS_TS desc) as rnk
-- MAGIC from dhf_iot_harmonized_prod.program_enrollment
-- MAGIC where upper(VNDR_CD)= 'CMT'
-- MAGIC )where rnk = 1""").select("DATA_CLCTN_ID","PRGRM_TP_CD","src_sys_cd_pe","plcy_st_cd").distinct()
-- MAGIC val trip_src=spark.sql("select distinct driveid,account_id,load_dt,src_sys_cd from dhf_iot_cmt_raw_prod.trip_summary_realtime where src_sys_cd is null and load_dt <'2021-06-12'")
-- MAGIC val trip2=trip_src.join(program_enrollmentDF,trip_src("account_id") ===  program_enrollmentDF("DATA_CLCTN_ID"),"left")
-- MAGIC trip2.createOrReplaceTempView("tab2")
-- MAGIC spark.sql("merge into dhf_iot_cmt_raw_prod.trip_summary_realtime a using tab2 b on a.driveid=b.driveid and a.load_dt=b.load_dt and a.load_dt<'2021-06-12' and a.src_sys_cd is null when matched then update set a.src_sys_cd=b.src_sys_cd_pe , a.prgrm_cd=b.PRGRM_TP_CD , a.user_st_cd=b.plcy_st_cd ")

-- COMMAND ----------

-- MAGIC                                  
-- MAGIC %scala
-- MAGIC val program_enrollmentDF=spark.sql(s"""select distinct VNDR_ACCT_ID,data_clctn_id,
-- MAGIC CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
-- MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
-- MAGIC     ELSE PRGRM_TP_CD
-- MAGIC END PRGRM_TP_CD,cASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
-- MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
-- MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
-- MAGIC    END src_sys_cd_pe,EVNT_SYS_TS,plcy_nb,
-- MAGIC prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,data_clctn_stts_cd,
-- MAGIC devc_stts_dt,entrp_cust_nb,sbjt_id,sbjt_tp_cd,vndr_cd,
-- MAGIC devc_id,devc_stts_cd,instl_grc_end_dt,plcy_st_cd, last_rqst_dt
-- MAGIC from (select
-- MAGIC distinct VNDR_ACCT_ID,data_clctn_id,PRGRM_TP_CD,EVNT_SYS_TS,
-- MAGIC plcy_nb,prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,
-- MAGIC coalesce(DATA_CLCTN_STTS,'') as data_clctn_stts_cd,
-- MAGIC coalesce(devc_stts_dt,' ') devc_stts_dt,
-- MAGIC coalesce(ENTRP_CUST_NB,' ') entrp_cust_nb,
-- MAGIC coalesce(sbjt_id,' ') as sbjt_id,
-- MAGIC coalesce(sbjt_tp_cd,' ') as sbjt_tp_cd,
-- MAGIC coalesce(vndr_cd,' ') as vndr_cd,
-- MAGIC coalesce(devc_id,' ') as devc_id,
-- MAGIC coalesce(devc_stts_cd,' ') as devc_stts_cd,
-- MAGIC coalesce(instl_grc_end_dt,' ') as instl_grc_end_dt,
-- MAGIC coalesce(plcy_st_cd,' ') as plcy_st_cd,
-- MAGIC coalesce(last_rqst_dt,' ') as last_rqst_dt,
-- MAGIC PROGRAM_ENROLLMENT_ID ,
-- MAGIC LOAD_HR_TS,
-- MAGIC ETL_LAST_UPDT_DTS 
-- MAGIC ,row_number() over (partition by DATA_CLCTN_ID order by EVNT_SYS_TS desc) as rnk
-- MAGIC from dhf_iot_harmonized_prod.program_enrollment
-- MAGIC where upper(VNDR_CD)= 'CMT'
-- MAGIC )where rnk = 1""").select("DATA_CLCTN_ID","PRGRM_TP_CD","src_sys_cd_pe","plcy_st_cd").distinct()
-- MAGIC val trip_src=spark.sql("select distinct driveid,account_id,load_dt,src_sys_cd from dhf_iot_cmt_raw_prod.trip_summary_realtime where src_sys_cd is null and load_dt <'2021-06-12'")
-- MAGIC val trip2=trip_src.join(program_enrollmentDF,trip_src("account_id") ===  program_enrollmentDF("DATA_CLCTN_ID"),"left")
-- MAGIC trip2.createOrReplaceTempView("tab2")
-- MAGIC spark.sql("merge into dhf_iot_cmt_raw_prod.events_realtime a using tab2 b on a.driveid=b.driveid and a.load_dt=b.load_dt and a.load_dt<'2021-06-12' and a.src_sys_cd is null when matched then update set a.src_sys_cd=b.src_sys_cd_pe , a.prgrm_cd=b.PRGRM_TP_CD , a.user_st_cd=b.plcy_st_cd ")

-- COMMAND ----------

-- MAGIC                                  
-- MAGIC %scala
-- MAGIC val program_enrollmentDF=spark.sql(s"""select distinct VNDR_ACCT_ID,data_clctn_id,
-- MAGIC CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
-- MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
-- MAGIC     ELSE PRGRM_TP_CD
-- MAGIC END PRGRM_TP_CD,cASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
-- MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
-- MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
-- MAGIC    END src_sys_cd_pe,EVNT_SYS_TS,plcy_nb,
-- MAGIC prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,data_clctn_stts_cd,
-- MAGIC devc_stts_dt,entrp_cust_nb,sbjt_id,sbjt_tp_cd,vndr_cd,
-- MAGIC devc_id,devc_stts_cd,instl_grc_end_dt,plcy_st_cd, last_rqst_dt
-- MAGIC from (select
-- MAGIC distinct VNDR_ACCT_ID,data_clctn_id,PRGRM_TP_CD,EVNT_SYS_TS,
-- MAGIC plcy_nb,prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,
-- MAGIC coalesce(DATA_CLCTN_STTS,'') as data_clctn_stts_cd,
-- MAGIC coalesce(devc_stts_dt,' ') devc_stts_dt,
-- MAGIC coalesce(ENTRP_CUST_NB,' ') entrp_cust_nb,
-- MAGIC coalesce(sbjt_id,' ') as sbjt_id,
-- MAGIC coalesce(sbjt_tp_cd,' ') as sbjt_tp_cd,
-- MAGIC coalesce(vndr_cd,' ') as vndr_cd,
-- MAGIC coalesce(devc_id,' ') as devc_id,
-- MAGIC coalesce(devc_stts_cd,' ') as devc_stts_cd,
-- MAGIC coalesce(instl_grc_end_dt,' ') as instl_grc_end_dt,
-- MAGIC coalesce(plcy_st_cd,' ') as plcy_st_cd,
-- MAGIC coalesce(last_rqst_dt,' ') as last_rqst_dt,
-- MAGIC PROGRAM_ENROLLMENT_ID ,
-- MAGIC LOAD_HR_TS,
-- MAGIC ETL_LAST_UPDT_DTS 
-- MAGIC ,row_number() over (partition by DATA_CLCTN_ID order by EVNT_SYS_TS desc) as rnk
-- MAGIC from dhf_iot_harmonized_prod.program_enrollment
-- MAGIC where upper(VNDR_CD)= 'CMT'
-- MAGIC )where rnk = 1""").select("DATA_CLCTN_ID","PRGRM_TP_CD","src_sys_cd_pe","plcy_st_cd").distinct()
-- MAGIC val trip_src=spark.sql("select distinct driveid,account_id,load_dt,src_sys_cd from dhf_iot_cmt_raw_prod.trip_summary_realtime where src_sys_cd is null and load_dt <'2021-06-12'")
-- MAGIC val trip2=trip_src.join(program_enrollmentDF,trip_src("account_id") ===  program_enrollmentDF("DATA_CLCTN_ID"),"left")
-- MAGIC trip2.createOrReplaceTempView("tab2")
-- MAGIC spark.sql("merge into dhf_iot_cmt_raw_prod.waypoints_realtime a using tab2 b on a.driveid=b.driveid and a.load_dt=b.load_dt and a.load_dt<'2021-06-12' and a.src_sys_cd is null when matched then update set a.src_sys_cd=b.src_sys_cd_pe , a.prgrm_cd=b.PRGRM_TP_CD , a.user_st_cd=b.plcy_st_cd ")