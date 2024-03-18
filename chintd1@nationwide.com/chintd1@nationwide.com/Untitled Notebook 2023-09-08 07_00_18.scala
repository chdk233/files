// Databricks notebook source
// MAGIC %sql
// MAGIC select a.* from dhf_iot_raw_prod.program_enrollment a  left anti join  dhf_iot_harmonized_prod.program_enrollment b on a.dataCollectionId=b.DATA_CLCTN_ID and a.db_load_time=b.LOAD_HR_TS where  coalesce(programTermBeginDate,"1000-01-01")<=coalesce(programTermEndDate,"9999-01-01")

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct a.db_load_date,count(*) from dhf_iot_raw_prod.program_enrollment a  left anti join  dhf_iot_harmonized_prod.program_enrollment b on a.dataCollectionId=b.DATA_CLCTN_ID and a.db_load_time=b.LOAD_HR_TS where coalesce(programTermBeginDate,"1000-01-01")<=coalesce(programTermEndDate,"9999-01-01")  group by a.db_load_date
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history dhf_iot_harmonized_prod.program_enrollment

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.program_enrollment as of version 10035)

// COMMAND ----------

val df=spark.sql("""select a.* from dhf_iot_raw_prod.program_enrollment a  left anti join  (select * from dhf_iot_harmonized_prod.program_enrollment version as of  10035) b on a.dataCollectionId=b.DATA_CLCTN_ID and a.db_load_time=b.LOAD_HR_TS where coalesce(programTermBeginDate,"1000-01-01")<=coalesce(programTermEndDate,"9999-01-01") and  a.db_load_date in ('2023-08-21','2023-08-23','2023-08-23')""")
df.write.format("delta").mode("append").saveAsTable("dhf_iot_raw_prod.missing_enrollments")


// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct a.db_load_date,count(*) from dhf_iot_raw_prod.program_enrollment a  left anti join  (select * from dhf_iot_harmonized_prod.program_enrollment version as of  10035) b on a.dataCollectionId=b.DATA_CLCTN_ID and a.db_load_time=b.LOAD_HR_TS where coalesce(programTermBeginDate,"1000-01-01")<=coalesce(programTermEndDate,"9999-01-01")  group by a.db_load_date
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_raw_prod.program_enrollment where ((programTermBeginDate is  null and programTermEndDate is  null) or programTermBeginDate<=programTermEndDate) and dataCollectionId='18c2f838-d31a-43d9-a97e-45191dcf8376'

// COMMAND ----------

f=spark.sql(f"""select *,case when DQI_CHECK like '%daily%' then concat('CMT/Archive/zip/DS_CMT__',date(load_hr_ts),'__',date(load_hr_ts)+1,'.zip') 
            when DQI_CHECK like '%realtime%' and src_sys_cd='CMT_PL_SRM' then concat('CMT/Archive/tar/',folder2,folder3,concat('DS_CMT_SRM_',trip_smry_key,'.tar.gz')) 
            when DQI_CHECK like '%realtime%' and src_sys_cd='CMT_PL_SRMCM' then concat('CMT/Archive/tar/',folder2,folder3,concat('DS_CMT_SRMCM_',trip_smry_key,'.tar.gz'))
            when DQI_CHECK like '%realtime%' and src_sys_cd='CMT_PL_FDR' then concat('FDR/Archive/tar/',folder2,folder3,concat('DS_CMT_FDR_',trip_smry_key,'.tar.gz')) end filename from 
              (select concat('load_date=',date(load_hr_ts),'/',LPAD(TRIM(CAST(hour(LOAD_HR_TS) AS VARCHAR(2))),2,'0'),'00/') as folder2,
              concat('DS_CMT_',ACCT_ID,'/') as folder3,SRC_SYS_CD,
              DQI_CHECK,TRIP_SMRY_KEY,LOAD_HR_TS FROM {DRM_DB}.{DRM_Table})""").selectExpr(f"'dw-internal-pl-cmt-telematics-{account_id}' as bucket","filename as key")
    files_list=cmtpl_rawfile_df.distinct().collect()

// COMMAND ----------

// MAGIC %py
// MAGIC account_id='785562577411'
// MAGIC files_list=spark.sql(f"""select 'dw-internal-pl-cmt-telematics-{account_id}' as bucket,concat(folder1, folder2,folder3,folder4) as key from (select 'FDR/Archive/tar/' as folder1,concat('load_date=',date(load_hr_ts),'/',LPAD(TRIM(CAST(hour(LOAD_HR_TS) AS VARCHAR(2))),2,'0'),'00/') as folder2,
// MAGIC               concat('DS_CMT_',account_id,'/') as folder3,concat('DS_CMT_SRM_',driveid,'.tar.gz') as folder4 from dhf_iot_cmt_raw_prod.trip_summary_realtime a inner join (select distinct dataCollectionId from dhf_iot_raw_prod.missing_enrollments) b on a.account_id=b.dataCollectionId where a.src_sys_cd='CMT_PL_FDR' and a.load_dt in ('2023-08-21','2023-08-23','2023-08-23'))""").collect()
// MAGIC for row in files_list:
// MAGIC     bucket=row[0]
// MAGIC     key=row[1]
// MAGIC     print(f"copying file: bucket={bucket} key={key}")

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct db_load_date from dhf_iot_raw_prod.program_enrollment where coalesce(programTermBeginDate,"1000-01-01")<=coalesce(programTermEndDate,"9999-01-01") and dataCollectionId='8c6a6444-1a54-4402-9c66-2340ec1bc34b'

// COMMAND ----------

// MAGIC %sql
// MAGIC desc dhf_iot_raw_prod.program_enrollment

// COMMAND ----------

# Databricks notebook source
    load_date_filter = spark.sql("select date(date_filter) from dhf_iot_datareplay_test.dhf_job_xref where task_id = '100455'").collect()[0]['date_filter']
    query = f"""select distinct cast(row_number() over(order by NULL) +1+ coalesce((select max(TRIP_DRIVER_SCORE_ID) from dhf_iot_harmonized_test.trip_driver_score),0) as BIGINT) as TRIP_DRIVER_SCORE_ID,
 cast(analytic_unit_type	as	STRING) as	ANLYTC_TP_CD,cast(data_clctn_id	as	STRING) as	DATA_CLCTN_ID,cast(driveid	as	STRING) as	TRIP_SMRY_KEY,cast(plcy_nb	as	STRING) as	POLICY_KEY,cast(trip_instance_id	as	STRING) as	TRIP_INSTC_ID,cast(vndr_acct_id	as	STRING) as	DRIVR_KEY,cast(distance_km	as	DECIMAL(15,6)) as	DRVNG_DISTNC_QTY,cast(handheld_count	as	BIGINT) as	HNDHLD_CT,cast(phone_handling_count	as	BIGINT) as	PHN_HNDLNG_CT,cast(phone_motion_count	as	BIGINT) as	PHN_MTN_CT,cast(tapping_count	as	BIGINT) as	TAP_CT,cast(business_event	as	STRING) as	TRNSCTN_TP_CD,cast(classification	as	STRING) as	TRNSPRT_MODE_CD,cast(distance_mi	as	DECIMAL(35,15)) as	DRVNG_DISTNC_MPH_QTY,cast(end_ts	as	TIMESTAMP) as	TRIP_END_TS,cast(number_of_driver_trips	as	BIGINT) as	TRIP_CT,cast(operational_indicator	as	STRING) as	OPRTN_SCR_IN,cast(trnsctn_id	as	STRING) as	TRNSCTN_ID,cast(user_lbl_dt	as	STRING) as	USER_LBL_DT,cast((replace(SUBSTRING_INDEX(utc_offset,':', 2),':','')) as decimal(10,2))as TIME_ZONE_OFFST_NB,cast(model_acronym	as	STRING) as	MODL_ACRNYM_TT,cast(model_name	as	STRING) as	MODL_NM,cast(model_version	as	STRING) as	MODL_VRSN_TT,cast(weighted_distraction_count_per_100km_model_output_type	as	STRING) as	WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_TP_CD,cast(weighted_distraction_count_per_100km_model_output_value	as	DECIMAL(35,15)) as	WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY,cast(weighted_distraction_count_per_100km_model_output_timestamp	as	TIMESTAMP) as	 WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS,cast(handheld_count_per_100km_score_model_output_type	as	STRING) as	HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_TP_CD,cast(handheld_count_per_100km_model_output_value	as	DECIMAL(35,15)) as	HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY,cast(handheld_count_per_100km_model_output_timestamp	as	TIMESTAMP) as	HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS,cast(phone_handling_count_per_100km_model_output_type	as	STRING) as	PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_CD,cast(phone_handling_count_per_100km_model_output_value	as	DECIMAL(35,15)) as	PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY,cast(phone_handling_count_per_100km_model_output_timestamp	as	TIMESTAMP) as	PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS,cast(data_clctn_stts_cd	as	STRING) as	DATA_CLCTN_STTS_DSC,event_source	as EVNT_SRC,cast(devc_id	as	STRING) as	DEVC_ID,cast(devc_stts_cd	as	STRING) as	DEVC_STTS_CD,cast(devc_stts_dt	as	DATE) as	DEVC_STTS_DT,cast(entrp_cust_nb	as	STRING) as	ENTRP_CUST_NB,cast(instl_grc_end_dt	as	DATE) as	INSTL_GRC_END_DT,cast(last_rqst_dt	as	DATE) as	LAST_RQST_DT,cast(plcy_st_cd	as	STRING) as	PLCY_ST_CD,cast(prgrm_cd	as	STRING) as	PGRM_CD,cast(prgrm_stts_cd	as	STRING) as	PRGRM_STTS_DSC,cast(prgrm_term_beg_dt	as	STRING) as	PRGRM_TERM_BEG_DT,cast(prgrm_term_end_dt	as	STRING) as	PRGRM_TERM_END_DT,cast(prgrm_tp_cd	as	STRING) as	PRGRM_TP_CD,cast(sbjt_id	as	STRING) as	SBJT_ID,cast(sbjt_tp_cd	as	STRING) as	SBJCT_TP_CD,cast(src_sys_cd	as	STRING) as	SRC_SYS_CD,cast(vndr_cd	as	STRING) as	VNDR_CD,cast(prgrm_term_beg_dt_used	as	DATE) as	PRGRM_TERM_BEG_DT_USED,cast(prgrm_term_end_dt_used	as	DATE) as	PRGRM_TERM_END_DT_USED,coalesce(LOAD_HOUR, to_timestamp("9999-12-31")) AS LOAD_HR_TS,coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS,coalesce(LOAD_DATE, to_date("9999-12-31")) AS LOAD_DT, CURRENT_TIMESTAMP() as	ETL_LAST_UPDATE_DTS
 from global_temp.fdr_trip_driver_score where load_date>='{load_date_filter}'"""


// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.program_enrollment where DATA_CLCTN_ID='18c2f838-d31a-43d9-a97e-45191dcf8376'

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.program_enrollment_chlg where DATA_CLCTN_ID='18c2f838-d31a-43d9-a97e-45191dcf8376'

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_raw_prod.program_enrollment where dataCollectionIzd='c1a16f53-af15-47a9-9c58-8f382606b796'

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_raw_prod.program_enrollment where dataCollectionId='c1a16f53-af15-47a9-9c58-8f382606b796'

// COMMAND ----------

val df=spark.sql("""select cast( dataCollectionId as string ) as DATA_CLCTN_ID ,
cast( dataCollectionStatus as string ) as DATA_CLCTN_STTS ,
cast( coalesce(deviceId,"0") as string ) as DEVC_ID ,
cast( deviceStatus as string ) as DEVC_STTS_CD ,
cast( deviceStatusDate as string ) as DEVC_STTS_DT ,
cast( program_discountPercent as double ) as DISC_PCT ,
CASE WHEN subjectType="VEHICLE" then cast( coalesce(firstName,"") as string ) else firstName end as DRIVR_FRST_NM,
CASE WHEN subjectType="VEHICLE" then cast( coalesce(lastName,"") as string ) else lastName end as DRIVR_LAST_NM,
CASE WHEN subjectType="VEHICLE" then cast( coalesce(MiddleName,"") as string ) else MiddleName end as DRIVR_MID_NM,
CASE WHEN subjectType="VEHICLE" then cast( coalesce(suffix,"") as string ) else suffix end as DRIVR_SFX,
cast( enrollmentEffectiveDate as string ) as ENRLMNT_EFCTV_DT ,
cast( enrollmentId as string ) as ENRLMNT_ID ,
cast( enrollmentProcessDate as string ) as ENRLMNT_PRCS_DT ,
cast( ecn as string ) as ENTRP_CUST_NB ,
cast( current_timestamp() as timestamp ) as ETL_LAST_UPDT_DTS ,
cast( eventId as string ) as EVNT_ID ,
cast( eventsource as string ) as EVNT_SRC_CD ,
cast( eventtime as bigint ) as EVNT_SYS_TS ,
cast( eventtype as string ) as EVNT_TP_CD ,
cast( installGraceEndDate as string ) as INSTL_GRC_END_DT ,
cast( key as string ) as KFK_KEY ,
cast( topic as string ) as KFK_TOPIC_NM ,
cast( timestamp as bigint ) as KFK_TS ,
cast( lastRequestDate as string ) as LAST_RQST_DT ,
cast( db_load_date as string ) as LOAD_DT ,
cast( db_load_time as timestamp ) as LOAD_HR_TS ,
cast( offset as bigint ) as OFFST_NB ,
cast( policy_discountPercent as double ) as PLCY_DISC_PCT ,
cast( policyNumber as string ) as PLCY_NB ,
cast( policy_scoreDate as string ) as PLCY_SCR_DT ,
cast( policy_scoreModel as string ) as PLCY_SCR_MODL_CD ,
cast( policy_Score as string ) as PLCY_SCR_QTY ,
cast( policy_scoreType as string ) as PLCY_SCR_TP_CD ,
cast( policyState as string ) as PLCY_ST_CD ,
cast( programEndDate as string ) as PRGRM_END_DT ,
cast( programStatus as string ) as PRGRM_STTS_CD ,
cast( coalesce(programTermBeginDate,"1000-01-01") as string ) as PRGRM_TERM_BEG_DT ,
cast( coalesce(programTermEndDate,"9999-01-01") as string ) as PRGRM_TERM_END_DT ,
cast( programType as string ) as PRGRM_TP_CD ,
cast(row_number() over(order by NULL) +90000+ coalesce((select max(PROGRAM_ENROLLMENT_ID) from dhf_iot_harmonized_prod.program_enrollment),0) as BIGINT) as PROGRAM_ENROLLMENT_ID,
cast( partition as int ) as PRTN_NB ,
cast( subjectId as double ) as SBJT_ID ,
cast( subjectType as string ) as SBJT_TP_CD ,
cast( program_ScoreDate as string ) as SCR_DT ,
cast( program_scoreModel as string ) as SCR_MODL_CD ,
cast( program_score as string ) as SCR_QTY ,
cast( program_scoreType as string ) as SCR_TP_CD ,
cast( transactionType as string ) as TRNSCTN_TP_CD ,
cast( timestamp_type as int ) as TS_TP_CD ,
CASE WHEN subjectType="DRIVER" then cast( coalesce(make,"") as string ) else make end as VHCL_MAKE_CD,
CASE WHEN subjectType="DRIVER" then cast( coalesce(model,"") as string ) else model end as VHCL_MODL_CD,
CASE WHEN subjectType="DRIVER" then cast( coalesce(year,"") as string ) else year end as VHCL_YR,
cast( coalesce(vin,"0") as string ) as VIN_NB ,
cast( vendorAccountBeginDate as string ) as VNDR_ACCT_BEG_DT ,
cast( vendorAccountEndDate as string ) as VNDR_ACCT_END_DT ,
cast( vendorAccountId as string ) as VNDR_ACCT_ID ,
cast( Vendor as string ) as VNDR_CD,
case when vendor not in ('CMT', 'LN') then coalesce(cast(policyNumber as string), 'NOKEY') else 'NOKEY' end as POLICY_KEY,
case when vendor not in ('CMT', 'LN') then coalesce(XXHASH64(cast(policyNumber as string)), XXHASH64('NOKEY')) else XXHASH64('NOKEY') end as POLICY_KEY_ID
from (select a.* from dhf_iot_raw_prod.program_enrollment a  left anti join  dhf_iot_harmonized_prod.program_enrollment b on a.dataCollectionId=b.DATA_CLCTN_ID and a.db_load_time=b.LOAD_HR_TS where  coalesce(programTermBeginDate,"1000-01-01")<=coalesce(programTermEndDate,"9999-01-01")) where not (`partition`=0 and `offset` <=2285577)  and coalesce(programTermBeginDate,"1000-01-01")<=coalesce(programTermEndDate,"9999-01-01")""")
df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.program_enrollment_chlg")