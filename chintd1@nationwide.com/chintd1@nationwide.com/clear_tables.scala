// Databricks notebook source
// MAGIC %python
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration_curation/",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventcuration/",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/",True)

// COMMAND ----------

// MAGIC %py
// MAGIC final_df = spark.sql(f"""SELECT distinct CONCAT(
// MAGIC RPAD(TRIM(IF(tb1.DEVC_KEY IS NULL,' ', CAST(cast(tb1.DEVC_KEY as bigint) AS VARCHAR(25)))),25,' ')
// MAGIC ,'                    '
// MAGIC ,RPAD(TRIM(IF(tb1.ENRLD_VIN_NB IS NULL,' ', CAST(tb1.ENRLD_VIN_NB AS VARCHAR(17)))),17,' ')
// MAGIC ,LPAD(TRIM(IF(tb1.PRGRM_INSTC_ID IS NULL,' ', CAST(tb1.PRGRM_INSTC_ID AS VARCHAR(36)))),36,'0')
// MAGIC ,RPAD(REGEXP_REPLACE(IF(tb1.MODL_OTPT_TS IS NULL,' ', CAST(tb1.MODL_OTPT_TS AS VARCHAR(10))),'-',''),8,' ')
// MAGIC ,RPAD(REGEXP_REPLACE(IF(tb1.SCR_STRT_DT IS NULL,' ', CAST(tb1.SCR_STRT_DT AS VARCHAR(10))),'-',''),8,' ')
// MAGIC ,RPAD(REGEXP_REPLACE(IF(tb1.SCR_END_DT IS NULL,' ', CAST(tb1.SCR_END_DT AS VARCHAR(10))),'-',''),8,' ')
// MAGIC ,LPAD(CAST(IF(tb1.SCR_DAYS_CT IS NULL,0,CAST(tb1.SCR_DAYS_CT AS INT)) AS VARCHAR(5)),3,'0')
// MAGIC ,RPAD(TRIM(CAST(tb1.score_model_1 AS VARCHAR(4))),4,' ')
// MAGIC ,LPAD(TRIM(CAST(tb1.PURE_SCR_1_QTY AS VARCHAR(3))),3,'0')
// MAGIC ,LPAD(TRIM(IF((CASE when (tb1.INSTL_PCTG < 95) then 998 when (tb1.PLSBL_DRIV_PCTG < 97 or tb1.PLSBL_DRIV_PCTG is null ) then 997 else (case when (tb1.PURE_SCR_1_QTY < 1) then 1 when (tb1.PURE_SCR_1_QTY > 995) then 995 else tb1.PURE_SCR_1_QTY end) end) IS NULL,' ', CAST((CASE when (tb1.INSTL_PCTG < 95) then 998 when (tb1.PLSBL_DRIV_PCTG < 97 or tb1.PLSBL_DRIV_PCTG is null ) then 997 else (case when (tb1.PURE_SCR_1_QTY < 1) then 1 when (tb1.PURE_SCR_1_QTY > 995) then 995 else tb1.PURE_SCR_1_QTY end) end) AS VARCHAR(3)))),3,'0')
// MAGIC ,'          '
// MAGIC ,RPAD(TRIM(CAST(tb1.score_model_2 AS VARCHAR(4))),4,' ')
// MAGIC ,LPAD(TRIM(IF(tb1.PURE_SCR_2_QTY IS NULL,' ', CAST(tb1.PURE_SCR_2_QTY AS VARCHAR(3)))),3,'0')
// MAGIC ,LPAD(TRIM(IF((CASE when (tb1.INSTL_PCTG < 95) then 998 when (tb1.PLSBL_DRIV_PCTG < 97 or tb1.PLSBL_DRIV_PCTG is null ) then 997 else (case when (tb1.PURE_SCR_2_QTY < 1) then 1 when (tb1.PURE_SCR_2_QTY > 995) then 995 else tb1.PURE_SCR_2_QTY end) end) IS NULL,' ', CAST((CASE when (tb1.INSTL_PCTG < 95) then 998 when (tb1.PLSBL_DRIV_PCTG < 97 or tb1.PLSBL_DRIV_PCTG is null ) then 997 else (case when (tb1.PURE_SCR_2_QTY < 1) then 1 when (tb1.PURE_SCR_2_QTY > 995) then 995 else tb1.PURE_SCR_2_QTY end) end) AS VARCHAR(3)))),3,'0')
// MAGIC ,'          '
// MAGIC ,'    '
// MAGIC ,'000'
// MAGIC ,'000'
// MAGIC ,'          '
// MAGIC ,'    '
// MAGIC ,'000'
// MAGIC ,'000'
// MAGIC ,'          '
// MAGIC ,'    '
// MAGIC ,'000'
// MAGIC ,'000'
// MAGIC ,'          '
// MAGIC ,'    '
// MAGIC ,'000'
// MAGIC ,'000'
// MAGIC ,'          '
// MAGIC ,'    '
// MAGIC ,'000'
// MAGIC ,'000'
// MAGIC ,'          '
// MAGIC ,'    '
// MAGIC ,'000'
// MAGIC ,'000'
// MAGIC ,'          '
// MAGIC ,'    '
// MAGIC ,'000'
// MAGIC ,'000'
// MAGIC ,'          '
// MAGIC ,'    '
// MAGIC ,'000'
// MAGIC ,'000'
// MAGIC ,'          '
// MAGIC ,LPAD(CAST(IF(tb1.DSCNCTD_TIME_PCTG IS NULL,0,CAST(tb1.DSCNCTD_TIME_PCTG*100 AS INT)) AS VARCHAR(5)),5,'0')
// MAGIC ,LPAD(TRIM(IF(tb1.ANNL_MILG_QTY IS NULL,' ', CAST(tb1.ANNL_MILG_QTY AS VARCHAR(6)))),6,'0')
// MAGIC ,RPAD(TRIM(IF(tb1.CNCT_DSCNCT_CT IS NULL,' ', CAST(tb1.CNCT_DSCNCT_CT AS VARCHAR(7)))),7,' ')
// MAGIC ,CONCAT(CONCAT(CONCAT(CONCAT (LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(CAST(tb1.UNINSTL_DAYS_CT/86400 AS INT) AS VARCHAR(3)))),3,'0') , ':'),
// MAGIC LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(CAST((tb1.UNINSTL_DAYS_CT%86400)/3600 AS INT) AS VARCHAR(3)))),2,'0') , ':'),
// MAGIC LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(CAST(((tb1.UNINSTL_DAYS_CT%86400)%3600)/60 AS INT)AS VARCHAR(3)))),2,'0') , ':'),
// MAGIC LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(((tb1.UNINSTL_DAYS_CT%86400)%3600)%60 AS VARCHAR(3)))),2,'0'))
// MAGIC ,'                                             ')
// MAGIC FROM 
// MAGIC (
// MAGIC Select 
// MAGIC outbound_score_elements_id,
// MAGIC DEVC_KEY,
// MAGIC ENRLD_VIN_NB,
// MAGIC case when SRC_SYS_CD LIKE ('%SM%') THEN DATA_CLCTN_ID
// MAGIC      when SRC_SYS_CD LIKE ('%SR%') THEN PRGRM_INSTC_ID
// MAGIC    END AS PRGRM_INSTC_ID,
// MAGIC MODL_OTPT_TS,
// MAGIC SCR_STRT_DT,
// MAGIC SCR_END_DT,
// MAGIC SCR_DAYS_CT,
// MAGIC 'ND1' As score_model_1,
// MAGIC CASE WHEN MODL_ACRNYM_TT='ND1' THEN MODL_OTPT_QTY ELSE 0 END AS PURE_SCR_1_QTY,
// MAGIC 'SM1' As score_model_2,
// MAGIC CASE WHEN MODL_ACRNYM_TT2='SM1' THEN MODL_OTPT_QTY2 ELSE 0 END AS PURE_SCR_2_QTY,
// MAGIC cast(DSCNCTD_TIME_PCTG as double),
// MAGIC cast(ANNL_MILG_QTY as double) as ANNL_MILG_QTY,
// MAGIC CNCT_DSCNCT_CT,
// MAGIC UNINSTL_DAYS_CT,
// MAGIC cast(INSTL_PCTG as double) as INSTL_PCTG,
// MAGIC cast(PLSBL_DRIV_PCTG as double) as PLSBL_DRIV_PCTG,
// MAGIC LOAD_DT
// MAGIC from (select distinct a.*,b.MODL_ACRNYM_TT as MODL_ACRNYM_TT2,b.MODL_OTPT_QTY as MODL_OTPT_QTY2 from (select *,row_number() over (partition by ENRLD_VIN_NB,SRC_SYS_CD,MODL_NM,MODL_ACRNYM_TT order by MAX_PE_END_TS desc) as row_nb from (select * from dhf_iot_curated_prod.inbound_score_elements where date(MODL_OTPT_TS)=current_date-1  or (date(MODL_OTPT_TS) in ('2023-08-19','2023-08-20') and ENRLD_VIN_NB in (select distinct vin from dhf_iot_curated_prod.daily_scoring_replay))) )  a inner join dhf_iot_curated_prod.inbound_score_elements b on a.enrld_vin_nb=b.enrld_vin_nb  and a.outbound_score_elements_id=b.outbound_score_elements_id and a.MODL_ACRNYM_TT="ND1" AND b.MODL_ACRNYM_TT="SM1" where row_nb=1)) tb1 """).drop("row_nb")
// MAGIC
// MAGIC # (select distinct a.*,b.MODL_ACRNYM_TT as MODL_ACRNYM_TT2,b.MODL_OTPT_QTY as MODL_OTPT_QTY2 from (select *,row_number() over (partition by ENRLD_VIN_NB,SRC_SYS_CD,MODL_NM,MODL_ACRNYM_TT order by MAX_PE_END_TS desc) as row_nb from (select * from dhf_iot_curated_prod.inbound_score_elements where date(MODL_OTPT_TS)=current_date-1 )  a inner join dhf_iot_curated_prod.inbound_score_elements b on a.enrld_vin_nb=b.enrld_vin_nb  and a.outbound_score_elements_id=b.outbound_score_elements_id and a.MODL_ACRNYM_TT="ND1" AND b.MODL_ACRNYM_TT="SM1" where row_nb=1)) tb1 """).drop("row_nb")
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/delta/curated/dhf_iot_curated_prod",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/delta/harmonized/dhf_iot_harmonized_prod/",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/delta/rawims/",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/delta/stageraw/",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/delta/raw/fmc/",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/delta/raw/ims/",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/delta/raw/octo/",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/delta/raw/tims/",True)
// MAGIC

// COMMAND ----------

spark.sql("select a.driveid,a.src_sys_cd,a.db_load_time as db_load_date,a.load_dt,count from dhf_iot_cmt_raw_prod.trip_summary_realtime a inner join (select  driveid,load_dt,count(*) as count from dhf_iot_cmt_raw_prod.trip_summary_realtime group by driveid,load_dt having count(*)>1) b on  a.driveid=b.driveid and a.load_dt=b.load_dt ").withColumnRenamed("load_dt","lo")
// .write.format("delta").mode("overWrite").option("overWriteSchema","true").saveAsTable("dhf_iot_cmt_raw_prod.trip_summary_dups")

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_cmt_raw_prod.trip_summary_realtime x using (select * from (select *,rank() over (partition by driveid order by db_load_date desc) as rnk from dhf_iot_cmt_raw_prod.trip_summary_dups ) where rnk>1) y on x.driveid=y.driveid and x.load_dt=y.load_dt and x.src_sys_cd=y.src_sys_cd and x.db_load_time=y.db_load_date when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_cmt_raw_prod.trip_summary_realtime x inner join (select * from (select *,rank() over (partition by driveid order by db_load_date desc) as rnk from dhf_iot_cmt_raw_prod.trip_summary_dups ) where rnk>1) y on x.driveid=y.driveid and x.load_dt=y.load_dt and x.src_sys_cd=y.src_sys_cd and x.db_load_date=y.db_load_date --when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from (select *,rank() over (partition by driveid order by db_load_date desc) as rnk from dhf_iot_cmt_raw_prod.trip_summary_dups ) where rnk>1

// COMMAND ----------

// MAGIC %sql
// MAGIC select a.driveid,a.src_sys_cd,a.db_load_date,a.load_dt,counts from dhf_iot_cmt_raw_prod.trip_summary_realtime a inner join (select  driveid,load_dt,count(*) as counts from dhf_iot_cmt_raw_prod.trip_summary_realtime group by driveid,load_dt having count(*)>1) b on on a.driveid=b.driveid and a.load_dt=b.load_dt and a.src_sys_cd=b.src_sys_cd

// COMMAND ----------

// MAGIC %sql
// MAGIC select *  from dhf_iot_cmt_raw_prod.trip_summary_realtime where load_dt='2021-03-23' and src_sys_cd like '%CMT_PL%'

// COMMAND ----------

// MAGIC %sql
// MAGIC delete from dhf_iot_cmt_raw_prod.events_realtime where events_ts is null and events_lat is null and events_lon is null and event_type is null 

// COMMAND ----------

// MAGIC %sql
// MAGIC delete from dhf_iot_harmonized_prod.trip_event where SRC_SYS_CD like '%CMT_PL%' and uTC_TS='9999-12-31T00:00:00.000+0000' and LAT_NB is null and LNGTD_NB is null 

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_cmt_raw_prod.trip_summary_dups

// COMMAND ----------

// MAGIC                                  
// MAGIC %scala
// MAGIC val program_enrollmentDF=spark.sql(s"""select distinct VNDR_ACCT_ID,data_clctn_id,
// MAGIC CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
// MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
// MAGIC     ELSE PRGRM_TP_CD
// MAGIC END PRGRM_TP_CD,cASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
// MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
// MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
// MAGIC    END src_sys_cd_pe,EVNT_SYS_TS,plcy_nb,
// MAGIC prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,data_clctn_stts_cd,
// MAGIC devc_stts_dt,entrp_cust_nb,sbjt_id,sbjt_tp_cd,vndr_cd,
// MAGIC devc_id,devc_stts_cd,instl_grc_end_dt,plcy_st_cd, last_rqst_dt
// MAGIC from (select
// MAGIC distinct VNDR_ACCT_ID,data_clctn_id,PRGRM_TP_CD,EVNT_SYS_TS,
// MAGIC plcy_nb,prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,
// MAGIC coalesce(DATA_CLCTN_STTS,'') as data_clctn_stts_cd,
// MAGIC coalesce(devc_stts_dt,' ') devc_stts_dt,
// MAGIC coalesce(ENTRP_CUST_NB,' ') entrp_cust_nb,
// MAGIC coalesce(sbjt_id,' ') as sbjt_id,
// MAGIC coalesce(sbjt_tp_cd,' ') as sbjt_tp_cd,
// MAGIC coalesce(vndr_cd,' ') as vndr_cd,
// MAGIC coalesce(devc_id,' ') as devc_id,
// MAGIC coalesce(devc_stts_cd,' ') as devc_stts_cd,
// MAGIC coalesce(instl_grc_end_dt,' ') as instl_grc_end_dt,
// MAGIC coalesce(plcy_st_cd,' ') as plcy_st_cd,
// MAGIC coalesce(last_rqst_dt,' ') as last_rqst_dt,
// MAGIC PROGRAM_ENROLLMENT_ID ,
// MAGIC LOAD_HR_TS,
// MAGIC ETL_LAST_UPDT_DTS 
// MAGIC ,row_number() over (partition by DATA_CLCTN_ID order by EVNT_SYS_TS desc) as rnk
// MAGIC from dhf_iot_harmonized_prod.program_enrollment
// MAGIC where upper(VNDR_CD)= 'CMT'
// MAGIC )where rnk = 1""").select("DATA_CLCTN_ID","PRGRM_TP_CD","src_sys_cd_pe","plcy_st_cd").distinct()
// MAGIC val trip_src=spark.sql("select a.* from (select distinct driveid,accountid,load_dt,src_sys_cd from dhf_iot_cmt_raw_prod.trip_detail_realtime where load_dt <'2021-06-12')a inner join (select distinct drive_id,load_dt from dhf_iot_cmt_raw_prod.trip_detail_realtime where src_sys_cd is null and load_dt <'2021-06-12') on a.driveid=b.drive_id and a.load_dt=b.load_dt")
// MAGIC val trip2=trip_src.join(program_enrollmentDF,trip_src("account_id") ===  program_enrollmentDF("DATA_CLCTN_ID"),"left")
// MAGIC trip2.createOrReplaceTempView("tab")
// MAGIC spark.sql("merge into dhf_iot_cmt_raw_prod.trip_detail_realtime a using tab b on a.drive_id=b.driveid and a.load_dt=b.load_dt and a.load_dt<'2021-06-12' and a.src_sys_cd is null when matched then update set a.src_sys_cd=b.src_sys_cd_pe ")

// COMMAND ----------

// MAGIC                                  
// MAGIC %scala
// MAGIC spark.sql("set spark.sql.shuffle.partitions=auto")
// MAGIC val program_enrollmentDF=spark.sql(s"""select distinct VNDR_ACCT_ID,data_clctn_id,
// MAGIC CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
// MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
// MAGIC     ELSE PRGRM_TP_CD
// MAGIC END PRGRM_TP_CD,cASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
// MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
// MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
// MAGIC    END src_sys_cd_pe,EVNT_SYS_TS,plcy_nb,
// MAGIC prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,data_clctn_stts_cd,
// MAGIC devc_stts_dt,entrp_cust_nb,sbjt_id,sbjt_tp_cd,vndr_cd,
// MAGIC devc_id,devc_stts_cd,instl_grc_end_dt,plcy_st_cd, last_rqst_dt
// MAGIC from (select
// MAGIC distinct VNDR_ACCT_ID,data_clctn_id,PRGRM_TP_CD,EVNT_SYS_TS,
// MAGIC plcy_nb,prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,
// MAGIC coalesce(DATA_CLCTN_STTS,'') as data_clctn_stts_cd,
// MAGIC coalesce(devc_stts_dt,' ') devc_stts_dt,
// MAGIC coalesce(ENTRP_CUST_NB,' ') entrp_cust_nb,
// MAGIC coalesce(sbjt_id,' ') as sbjt_id,
// MAGIC coalesce(sbjt_tp_cd,' ') as sbjt_tp_cd,
// MAGIC coalesce(vndr_cd,' ') as vndr_cd,
// MAGIC coalesce(devc_id,' ') as devc_id,
// MAGIC coalesce(devc_stts_cd,' ') as devc_stts_cd,
// MAGIC coalesce(instl_grc_end_dt,' ') as instl_grc_end_dt,
// MAGIC coalesce(plcy_st_cd,' ') as plcy_st_cd,
// MAGIC coalesce(last_rqst_dt,' ') as last_rqst_dt,
// MAGIC PROGRAM_ENROLLMENT_ID ,
// MAGIC LOAD_HR_TS,
// MAGIC ETL_LAST_UPDT_DTS 
// MAGIC ,row_number() over (partition by DATA_CLCTN_ID order by EVNT_SYS_TS desc) as rnk
// MAGIC from dhf_iot_harmonized_prod.program_enrollment
// MAGIC where upper(VNDR_CD)= 'CMT'
// MAGIC )where rnk = 1""").select("DATA_CLCTN_ID","PRGRM_TP_CD","src_sys_cd_pe","plcy_st_cd").distinct()
// MAGIC val trip_src=spark.sql("select distinct driveid,account_id,load_dt,src_sys_cd from dhf_iot_cmt_raw_prod.trip_summary_realtime where src_sys_cd is null and load_dt <'2021-06-12'")
// MAGIC val trip2=trip_src.join(program_enrollmentDF,trip_src("account_id") ===  program_enrollmentDF("DATA_CLCTN_ID"),"left")
// MAGIC trip2.createOrReplaceTempView("tab2")
// MAGIC spark.sql("merge into dhf_iot_cmt_raw_prod.trip_summary_realtime a using tab2 b on a.driveid=b.driveid and a.load_dt=b.load_dt and a.load_dt<'2021-06-12' and a.src_sys_cd is null when matched then update set a.src_sys_cd=b.src_sys_cd_pe , a.prgrm_cd=b.PRGRM_TP_CD , a.user_st_cd=b.plcy_st_cd ")

// COMMAND ----------

// MAGIC %python
// MAGIC spark.sql("set spark.sql.shuffle.partitions=auto")
// MAGIC days_start_count=31
// MAGIC days_end_count=39
// MAGIC cmt_pl_trip_detail_realtime_missing=spark.sql(f"""select  a.* from 
// MAGIC (select cast(monotonically_increasing_id()+65000+ coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_prod.trip_point),0) as BIGINT) as TRIP_POINT_ID,
// MAGIC drive_id as TRIP_SMRY_KEY, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, time as TM_EPCH,
// MAGIC timestamp_millis(time) as UTC_TS,cast(mm_lat as decimal(18,10)) as LAT_NB,cast(mm_lon as decimal(18,10)) as LNGTD_NB,
// MAGIC cast(mm_dist_km as decimal(35,15)) as DISTNC_TRVL_QTY,
// MAGIC cast(gps_speed as decimal(35,15)) as GPS_PNT_SPD_QTY,cast(gps_heading as decimal(15,6)) as HEADNG_DEG_QTY,gps_valid as GPS_VALID_STTS_SRC_IN,
// MAGIC case when gps_valid = 1 THEN 'Y' when gps_valid=0 THEN 'N' When gps_valid IS NULL THEN " " else '@' end as GPS_VALID_STTS_DERV_IN,
// MAGIC cast(accel_lon_smoothed as decimal(18,10)) as LNGTDNL_ACCLRTN_RT,cast(accel_lat_smoothed as decimal(18,10)) as LATRL_ACCLRTN_RT,accel_valid as ACLRTN_VALID_STTS_SRC_IN,
// MAGIC case when accel_valid=1 THEN 'Y' when accel_valid=0 THEN 'N' when accel_valid IS NULL THEN " " else '@' end as ACLRTN_VALID_STTS_DERV_IN,cast(roll_rate as decimal(18,10)) as ROLL_RT,
// MAGIC cast(pitch_rate as decimal(18,10)) as PITCH_RT,cast(yaw_rate as decimal(18,10)) as YAW_RT,
// MAGIC distraction as DISTRCTN_STTS_SRC_IN,case when distraction=1 THEN 'Y' when distraction=0 THEN 'N' When distraction IS NULL THEN " " Else '@' end as DISTRCTN_STTS_DERV_IN,
// MAGIC mm_state as MM_ST_CD,coalesce(src_sys_cd ,"NOKEY")as SRC_SYS_CD,coalesce(cast(LOAD_DT as DATE), to_date("9999-12-31")) as LOAD_DT,coalesce(LOAD_HR_TS ,to_timestamp("9999-12-31")) as LOAD_HR_TS 
// MAGIC  from dhf_iot_cmt_raw_prod.trip_detail_realtime where src_sys_cd in("CMT_PL_SRM","CMT_PL_SRMCM","CMT_PL_FDR") and load_dt>=current_date-{days_end_count} and LOAD_DT<=current_date-{days_start_count}) a left anti join
// MAGIC (select  * from dhf_iot_harmonized_prod.trip_point where load_dt>=current_date-{days_end_count} and LOAD_DT<=current_date-{days_start_count} and src_sys_cd like '%CMT_PL%') b 
// MAGIC on   a.TRIP_SMRY_KEY=b.TRIP_SMRY_KEY  AND a.UTC_TS=b.UTC_TS AND a.SRC_SYS_CD = b.SRC_SYS_CD where TRIP_SMRY_KEY is not null""")
// MAGIC cmt_pl_trip_detail_realtime_missing.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_point_chlg")

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*)  from dhf_iot_cmt_raw_prod.trip_summary_realtime where load_dt<'2023-05-18'  --188511751   137125041 137455487 164489524

// COMMAND ----------

// MAGIC %sql
// MAGIC select (*)  from dhf_iot_cmt_raw_prod.trip_summary_realtime where driveid='53AD453D-4E79-48FD-ADCC-2BB83B130FEC' and load_dt='2021-06-18'  --188511751   137125041  164489524

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_cmt_raw_prod.events_realtime x using (select a.* from (select  driveid,load_dt,src_sys_cd from dhf_iot_cmt_raw_prod.events_realtime where src_sys_cd ='@' and load_dt>='2021-06-11'and load_dt<='2021-06-21') a inner join (select  driveid,load_dt from dhf_iot_cmt_raw_prod.events_realtime where src_sys_cd !='@' and load_dt>='2021-06-11'and load_dt<='2021-06-21') b on a.driveid=b.driveid and a.load_dt=b.load_dt) y on x.driveid=y.driveid and x.load_dt=y.load_dt and x.load_dt>='2021-06-11'and x.load_dt<='2021-06-21' and x.src_sys_cd=y.src_sys_cd and x.src_sys_cd='@' when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from dhf_iot_cmt_raw_prod.events_realtime   --1,394,162,070  1,202,632,983

// COMMAND ----------

// MAGIC %sql
// MAGIC select  (*),waypoints_lat,waypoints_lon,waypoints_ts,avg_moving_speed_kmh,max_speed_kmh,speed_limit_kmh,link_id,display_code,prepended,driveid,src_sys_cd  from dhf_iot_cmt_raw_prod.waypoints_realtime where load_dt='2021-06-15'  and driveid='275B9252-F6AF-491A-8216-9BD2EE3068B2'

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_cmt_raw_prod.waypoints_realtime a using (select  * from  (select *,row_number() over (partition by driveid,waypoints_lat,waypoints_lon,waypoints_ts,avg_moving_speed_kmh,max_speed_kmh,speed_limit_kmh,link_id,display_code,prepended,src_sys_cd  order by db_load_time desc) as row from dhf_iot_cmt_raw_prod.waypoints_realtime where  load_dt<'2021-01-01' and  load_dt>='2020-01-01') where row>1) b on a.driveid=b.driveid and a.waypoints_lat=b.waypoints_lat and a.waypoints_lon=b.waypoints_lon and a.waypoints_ts=b.waypoints_ts and a.avg_moving_speed_kmh=b.avg_moving_speed_kmh and a.max_speed_kmh=b.max_speed_kmh and a.speed_limit_kmh=b.speed_limit_kmh and a.link_id=b.link_id and a.display_code=b.display_code and a.prepended=b.prepended and a.src_sys_cd=b.src_sys_cd and a.load_dt=b.load_dt and a.db_load_time=b.db_load_time and a.load_dt<'2021-01-01' and  a.load_dt>='2020-01-01' when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_cmt_raw_prod.events_realtime a using (select  * from  (select *,row_number() over (partition by driveid,events_ts,event_type,src_sys_cd order by db_load_time desc) as row from dhf_iot_cmt_raw_prod.events_realtime where  load_dt<'2023-05-16' and  load_dt>='2023-01-01') where row>1) b on a.driveid=b.driveid and a.events_ts=b.events_ts and a.event_type=b.event_type and a.src_sys_cd=b.src_sys_cd and a.load_dt=b.load_dt and a.db_load_time=b.db_load_time and a.load_dt<'2023-05-16' and  a.load_dt>='2023-01-01' when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC select  count(even)  from dhf_iot_harmonized_prod.trip_event where  load_dt='2021-06-15'

// COMMAND ----------

// MAGIC %sql
// MAGIC select  (*),waypoints_lat,waypoints_lon,waypoints_ts,avg_moving_speed_kmh,max_speed_kmh,speed_limit_kmh,link_id,display_code,prepended,driveid,src_sys_cd  from dhf_iot_cmt_raw_prod.waypoints_realtime where load_dt='2021-06-15'  and driveid='275B9252-F6AF-491A-8216-9BD2EE3068B2'

// COMMAND ----------

// MAGIC %sql
// MAGIC select  driveid,events_ts,count(*)  from dhf_iot_cmt_raw_prod.events_realtime where load_dt>'2021-06-15'  group by driveid,events_ts,event_type,src_sys_cd having count(*) >1  --1,385,361  87,885

// COMMAND ----------

val df=spark.sql("select distinct a.* from (select  driveid,load_dt,src_sys_cd from dhf_iot_cmt_raw_prod.waypoints_realtime where src_sys_cd ='@' and load_dt>='2021-06-11'and load_dt<='2022-07-21') a inner join (select  driveid,load_dt from dhf_iot_cmt_raw_prod.waypoints_realtime where src_sys_cd !='@' and load_dt>='2021-06-11'and load_dt<='2022-07-21') b on a.driveid=b.driveid and a.load_dt=b.load_dt")
df.write.format("delta").mode("append").saveAsTable("dhf_iot_cmt_raw_prod.waypoints_dups")

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_cmt_raw_prod.events_realtime x using (select * from dhf_iot_cmt_raw_prod.event_dups) y on x.driveid=y.driveid and x.load_dt=y.load_dt and x.src_sys_cd=y.src_sys_cd  and x.load_dt>='2021-06-11'and y.load_dt<='2022-06-21' and x.src_sys_cd='@' when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_cmt_raw_prod.waypoints_realtime x using (select * from dhf_iot_cmt_raw_prod.waypoints_dups) y on x.driveid=y.driveid and x.load_dt=y.load_dt and x.src_sys_cd=y.src_sys_cd  and x.load_dt>='2021-06-11'and y.load_dt<='2022-06-21' and x.src_sys_cd='@' when matched then delete

// COMMAND ----------

val df=spark.sql("select distinct load_dt from dhf_iot_cmt_raw_prod.waypoints_realtime order by load_dt").filter("load_dt>='2023-01-02' and load_dt<='2023-01-22'")

// COMMAND ----------

// %sql
//  delete from dhf_iot_harmonized_prod.trip_summary;
//  delete from dhf_iot_harmonized_prod.trip_event;
// delete from dhf_iot_harmonized_prod.trip_point;
//  delete from dhf_iot_harmonized_prod.device;
// delete from dhf_iot_harmonized_prod.vehicle;
// delete from dhf_iot_harmonized_prod.non_trip_event;
// delete from dhf_iot_harmonized_prod.scoring;
// delete from dhf_iot_harmonized_prod.histogram_scoring_interval;
// delete from dhf_iot_harmonized_prod.trip_summary_chlg;
//  delete from dhf_iot_harmonized_prod.trip_event_chlg;
// delete from dhf_iot_harmonized_prod.trip_point_chlg;
//  delete from dhf_iot_harmonized_prod.device_chlg;
// delete from dhf_iot_harmonized_prod.vehicle_chlg;
// delete from dhf_iot_harmonized_prod.non_trip_event_chlg;
// delete from dhf_iot_harmonized_prod.scoring_chlg;
// delete from dhf_iot_harmonized_prod.histogram_scoring_interval_chlg;

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC --tims
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='select cast(tripSummaryId as string),cast(engineRPM as string),cast(hdop as string),cast(utcDateTime as string),cast(degreesLatitude as string),cast(degreesLongitude as string),cast(speed as string),cast(headingdegrees as string),cast(accelerometerDataLong as string),cast(accelerometerDataLat as string),path,load_date,load_hour,db_load_date,db_load_time,SourceSystem from  {raw_view}' where  job_cd in (9006) and configname like '%sql_raw_te%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='select cast(tripSummaryId as string),cast(utcStartDateTime as string),cast(utcEndDateTime as string),cast(timeZoneOffset as string),cast(detectedVin as string),sourceCode,path,load_date,load_hour,db_load_date,db_load_time,SourceSystem from {raw_view}' where  job_cd in (9006) and configname like '%sql_raw_tr%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='StructType([StructField("tripSummaryId",StringType(),True),StructField("engineRPM",StringType(),True),StructField("utcDateTime",StringType(),True),StructField("degreesLatitude",StringType(),True),StructField("degreesLongitude",StringType(),True),StructField("speed",StringType(),True),StructField("SourceSystem",StringType(),True),StructField("accelerometerDataLong",StringType(),True),StructField("accelerometerDataLat",StringType(),True),StructField("hdop",StringType(),True),StructField("headingdegrees",StringType(),True),StructField("load_date",DateType(),True),StructField("load_hour",TimestampType(),True),StructField("db_load_time",TimestampType(),True),StructField("db_load_date",DateType(),True)])' where  job_cd in (9006) and configname like '%schema_raw_te%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='StructType([StructField("tripSummaryId",StringType(),True),StructField("utcStartDateTime",StringType(),True),StructField("utcEndDateTime",StringType(),True),StructField("timeZoneOffset",StringType(),True),StructField("detectedVin",StringType(),True),StructField("sourceCode",StringType(),True),StructField("SourceSystem",StringType(),True),StructField("load_date",DateType(),True),StructField("load_hour",TimestampType(),True),StructField("db_load_time",TimestampType(),True),StructField("db_load_date",DateType(),True)])' where  job_cd in (9006) and configname like '%schema_raw_tr%';
// MAGIC
// MAGIC --octo
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='StructType([StructField("type_cd",LongType(),True),StructField("contract_nb",StringType(),True),StructField("voucher_nb",LongType(),True),StructField("enrolledvin_nb",StringType(),True),StructField("trip_nb",StringType(),True),StructField("detectedvin_nb",StringType(),True),StructField("event_ts",StringType(),True),StructField("eventtimezoneoffset_nb",StringType(),True),StructField("latitude_it",StringType(),True),StructField("longitude_it",StringType(),True),StructField("eventtype_cd",StringType(),True),StructField("eventreferencevalue_ds",StringType(),True),StructField("eventstart_ts",StringType(),True),StructField("eventend_ts",StringType(),True),StructField("eventduration_am",StringType(),True),StructField("loadevent",LongType(),True),StructField("batch",StringType(),True),StructField("SourceSystem",StringType(),True),StructField("load_date",DateType(),True),StructField("load_hour",TimestampType(),True),StructField("db_load_time",TimestampType(),True),StructField("db_load_date",DateType(),True)])' where job_cd=7003 and configname like '%schema_raw_%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='StructType([StructField("type_cd",LongType(),True),StructField("fullpolicy_nb",StringType(),True),StructField("voucher_nb",LongType(),True),StructField("deviceserial_nb",LongType(),True),StructField("enrolledvin_nb",StringType(),True),StructField("trip_nb",StringType(),True),StructField("detectedvin_nb",StringType(),True),StructField("tripstart_ts",StringType(),True),StructField("tripend_ts",StringType(),True),StructField("tripdistance_qt",StringType(),True),StructField("averagespeed_qt",StringType(),True),StructField("maximumspeed_qt",StringType(),True),StructField("fuelconsumption_qt",StringType(),True),StructField("milstatus_cd",StringType(),True),StructField("tripzoneoffset_am",StringType(),True),StructField("idletime_ts",StringType(),True),StructField("trippositionalquality_qt",StringType(),True),StructField("accelerometerquality_qt",StringType(),True),StructField("distancemotorways_qt",StringType(),True),StructField("distanceurbanareas_qt",StringType(),True),StructField("distanceotherroad_qt",StringType(),True),StructField("distanceunknownroad_qt",StringType(),True),StructField("timetravelled_qt",StringType(),True),StructField("timemotorways_qt",StringType(),True),StructField("timeurbanareas_qt",StringType(),True),StructField("timeotherroad_qt",StringType(),True),StructField("timeunknownroad_qt",StringType(),True),StructField("speedingdistancemotorways_qt",StringType(),True),StructField("speedingdistanceurbanareas_qt",StringType(),True),StructField("speedingdistanceotherroad_qt",StringType(),True),StructField("speedingdistanceunknownroad_qt",StringType(),True),StructField("speedingtimemotorways_qt",StringType(),True),StructField("speedingtimeurbanareas_qt",StringType(),True),StructField("speedingtimeotherroad_qt",StringType(),True),StructField("speedingtimeunknownroad_qt",StringType(),True),StructField("loadevent",LongType(),True),StructField("batch",StringType(),True),StructField("SourceSystem",StringType(),True),StructField("load_date",DateType(),True),StructField("load_hour",TimestampType(),True),StructField("db_load_time",TimestampType(),True),StructField("db_load_date",DateType(),True)])' where job_cd=7002 and configname like '%schema_raw_%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='StructType([StructField("type_cd",LongType(),True),StructField("voucher_nb",LongType(),True),StructField("deviceserial_nb",LongType(),True),StructField("enrolledvin_nb",StringType(),True),StructField("cumulativetrips_qt",StringType(),True),StructField("cumulativedistance_qt",StringType(),True),StructField("cumulativeengineon_qt",StringType(),True),StructField("firsttripstart_ts",StringType(),True),StructField("lasttripend_ts",StringType(),True),StructField("lastprocessing_ts",StringType(),True),StructField("loadevent",LongType(),True),StructField("batch",StringType(),True),StructField("SourceSystem",StringType(),True),StructField("load_date",DateType(),True),StructField("load_hour",TimestampType(),True),StructField("db_load_time",TimestampType(),True),StructField("db_load_date",DateType(),True)])' where job_cd=7006 and configname like '%schema_raw_%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='StructType([StructField("type_cd",LongType(),True),StructField("fullpolicy_nb",StringType(),True),StructField("voucher_nb",LongType(),True),StructField("trip_nb",StringType(),True),StructField("position_ts",StringType(),True),StructField("latitude_it",StringType(),True),StructField("longitude_it",StringType(),True),StructField("vssspeed_am",StringType(),True),StructField("vssacceleration_pc",StringType(),True),StructField("enginerpm_am",IntegerType(),True),StructField("throttleposition_pc",StringType(),True),StructField("gpsheading_nb",ShortType(),True),StructField("positionquality_nb",StringType(),True),StructField("accelerationvertical_nb",StringType(),True),StructField("accelerationlongitudinal_nb",StringType(),True),StructField("accelerationlateral_nb",StringType(),True),StructField("odometerreading_qt",StringType(),True),StructField("eventaveragespeed_qt",StringType(),True),StructField("eventaverageacceleration_qt",StringType(),True),StructField("distancetravelled_qt",StringType(),True),StructField("timeelapsed_qt",StringType(),True),StructField("gpspointspeed_qt",StringType(),True),StructField("road_tp",StringType(),True),StructField("loadevent",LongType(),True),StructField("batch",StringType(),True),StructField("SourceSystem",StringType(),True),StructField("load_date",DateType(),True),StructField("load_hour",TimestampType(),True),StructField("db_load_time",TimestampType(),True),StructField("db_load_date",DateType(),True)])' where job_cd=7004 and configname like '%schema_raw_%';
// MAGIC
// MAGIC --fmc
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='StructType([StructField("tripSummaryId",StringType(),True),StructField("utcStartDateTime",StringType(),True),StructField("utcEndDateTime",StringType(),True),StructField("timeZoneOffset",StringType(),True),StructField("comment",StringType(),True),StructField("detectedVin",StringType(),True),StructField("load_hour",TimestampType(),True),StructField("load_date",DateType(),True),StructField("tripDistanceAccumulated",StringType(),True),StructField("tripMaxSpeed",StringType(),True),StructField("tripTotalEngineTime",StringType(),True),StructField("tripFuelConsumed",StringType(),True),StructField("tripTotalEngineTimeIdle",StringType(),True),StructField("tripFuelConsumedIdle",StringType(),True),StructField("sourceCode",StringType(),True),StructField("SourceSystem",StringType(),True),StructField("db_load_time",TimestampType(),True),StructField("db_load_date",DateType(),True)])' where job_cd=9004 and configname like '%schema_raw_tr%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='StructType([StructField("tripSummaryId",StringType(),True),StructField("received",LongType(),True),StructField("utcDateTime",StringType(),True),StructField("degreesLatitude",StringType(),True),StructField("degreesLongitude",StringType(),True),StructField("speed",StringType(),True),StructField("ignitionEvent",StringType(),True),StructField("ignitionStatus",StringType(),True),StructField("SourceSystem",StringType(),True),StructField("odometer",StringType(),True),StructField("altitude",StringType(),True),StructField("accelerometerDataZ",StringType(),True),StructField("accelerometerDataLat",StringType(),True),StructField("accelerometerDataLong",StringType(),True),StructField("headingdegrees",StringType(),True),StructField("load_date",DateType(),True),StructField("load_hour",TimestampType(),True),StructField("db_load_time",TimestampType(),True),StructField("db_load_date",DateType(),True)])' where job_cd=9004 and configname like '%schema_raw_te%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='select cast(tripSummaryId as string),cast(received as bigint),cast(utcDateTime as string),cast(degreesLatitude as string),cast(degreesLongitude as string),cast(speed as string),cast(headingdegrees as string),cast(accelerometerDataLong as string),cast(accelerometerDataLat as string),cast(accelerometerDataZ as string),cast(altitude as string),ignitionStatus,cast(odometer as string),ignitionEvent,path,load_date,load_hour,db_load_date,db_load_time,SourceSystem from {raw_view}' where job_cd=9004 and configname like '%sql_raw_te%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='select cast(tripSummaryId as string),cast(tripFuelConsumed as string),cast(tripTotalEngineTimeIdle as string),cast(utcStartDateTime as string),cast(utcEndDateTime as string),cast(timeZoneOffset as string),cast(tripDistanceAccumulated as string),cast(tripMaxSpeed as string),cast(tripTotalEngineTime as string),cast(detectedVin as string),sourceCode,comment,cast(tripFuelConsumedIdle as string),path,load_date,load_hour,db_load_date,db_load_time,SourceSystem from (select *,row_number() over (partition by tripSummaryId order by tripMaxSpeed desc) as rn from {raw_view}) where rn=1' where job_cd=9004 and configname like '%sql_raw_tr%';
// MAGIC
// MAGIC --ims
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='StructType([StructField("tripSummaryId",StringType(),True),StructField("degreesLatitude",StringType(),True),StructField("degreesLongitude",StringType(),True),StructField("headingDegrees",StringType(),True),StructField("telemetryEventType",StringType(),True),StructField("utcDateTime",StringType(),True),StructField("speed",StringType(),True),StructField("acceleration",StringType(),True),StructField("secondsOfDriving",StringType(),True),StructField("telemetryEventSeverityLevel",StringType(),True),StructField("telemetryEvents__avgSpeed",StringType(),True),StructField("SourceSystem",StringType(),True),StructField("Event_Reference_Value",StringType(),True),StructField("Event_Timezone_Offset",StringType(),True),StructField("load_date",DateType(),True),StructField("load_hour",TimestampType(),True),StructField("db_load_time",TimestampType(),True),StructField("db_load_date",DateType(),True)])' where job_cd not in (9004,9006) and configname like '%schema_raw_telemetryevents%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='StructType([StructField("tripSummaryId",StringType(),True),StructField("engineRPM",StringType(),True),StructField("accelerometerData",StringType(),True),StructField("ambientTemperature",StringType(),True),StructField("barometericPressure",StringType(),True),StructField("coolantTemperature",StringType(),True),StructField("fuelLevel",StringType(),True),StructField("hdop",StringType(),True),StructField("utcDateTime",StringType(),True),StructField("degreesLatitude",StringType(),True),StructField("degreesLongitude",StringType(),True),StructField("speed",StringType(),True),StructField("headingdegrees",StringType(),True),StructField("acceleration",StringType(),True),StructField("SourceSystem",StringType(),True),StructField("horizontalAccuracy",StringType(),True),StructField("throttlePosition",StringType(),True),StructField("verticalAccuracy",StringType(),True),StructField("Accel_Longitudinal",StringType(),True),StructField("Accel_Lateral",StringType(),True),StructField("Accel_Vertical",StringType(),True),StructField("load_date",DateType(),True),StructField("load_hour",TimestampType(),True),StructField("db_load_time",TimestampType(),True),StructField("db_load_date",DateType(),True)])' where job_cd not in (9004,9006) and configname like '%schema_raw_telemetrypoints%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='StructType([StructField("tripSummaryId",StringType(),True),StructField("fuelConsumption",StringType(),True),StructField("milStatus",BooleanType(),True),StructField("accelQuality",BooleanType(),True),StructField("transportMode",StringType(),True),StructField("secondsOfIdling",StringType(),True),StructField("transportModeReason",StringType(),True),StructField("hdopAverage",StringType(),True),StructField("utcStartDateTime",StringType(),True),StructField("utcEndDateTime",StringType(),True),StructField("timeZoneOffset",StringType(),True),StructField("drivingDistance",StringType(),True),StructField("maxSpeed",StringType(),True),StructField("avgSpeed",StringType(),True),StructField("totalTripSeconds",LongType(),True),StructField("deviceSerialNumber",StringType(),True),StructField("deviceIdentifier",StringType(),True),StructField("deviceIdentifierType",StringType(),True),StructField("deviceType",StringType(),True),StructField("enterpriseReferenceId",StringType(),True),StructField("enterpriseReferenceExtraInfo",StringType(),True),StructField("type",StringType(),True),StructField("enrolledVin",StringType(),True),StructField("detectedVin",StringType(),True),StructField("system",StringType(),True),StructField("SourceSystem",StringType(),True),StructField("load_date",DateType(),True),StructField("load_hour",TimestampType(),True),StructField("db_load_time",TimestampType(),True),StructField("db_load_date",DateType(),True)])' where job_cd not in (9004,9006) and configname like '%schema_raw_tripsummary%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='StructType([StructField("telemetrySetId",StringType(),True),StructField("degreesLatitude",StringType(),True),StructField("degreesLongitude",StringType(),True),StructField("headingDegrees",StringType(),True),StructField("telemetryEventType",StringType(),True),StructField("utcDateTime",StringType(),True),StructField("speed",StringType(),True),StructField("acceleration",StringType(),True),StructField("telemetryEventSeverityLevel",StringType(),True),StructField("enterpriseReferenceId",StringType(),True),StructField("enterpriseReferenceExtraInfo",StringType(),True),StructField("type",StringType(),True),StructField("deviceSerialNumber",StringType(),True),StructField("deviceIdentifier",StringType(),True),StructField("deviceIdentifierType",StringType(),True),StructField("deviceType",StringType(),True),StructField("enrolledVin",StringType(),True),StructField("detectedVin",StringType(),True),StructField("SourceSystem",StringType(),True),StructField("Event_Reference_Value",StringType(),True),StructField("Event_Timezone_Offset",StringType(),True),StructField("load_date",DateType(),True),StructField("load_hour",TimestampType(),True),StructField("db_load_time",TimestampType(),True),StructField("db_load_date",DateType(),True)])' where job_cd not in (9004,9006) and configname like '%schema_raw_nontripevent%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='StructType([StructField("tripSummaryId",StringType(),True),StructField("scoringComponent",StringType(),True),StructField("scoringSubComponent",StringType(),True),StructField("roadType",StringType(),True),StructField("thresholdLowerBound",StringType(),True),StructField("thresholdUpperBound",StringType(),True),StructField("scoringComponentUnit",StringType(),True),StructField("occurrences",StringType(),True),StructField("occurrenceUnit",StringType(),True),StructField("SourceSystem",StringType(),True),StructField("load_date",DateType(),True),StructField("load_hour",TimestampType(),True),StructField("db_load_time",TimestampType(),True),StructField("db_load_date",DateType(),True)])' where job_cd not in (9004,9006) and configname like '%schema_raw_histogramscoringintervals%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='StructType([StructField("tripSummaryId",StringType(),True),StructField("scoreAlgorithmProvider",StringType(),True),StructField("scoreUnit",StringType(),True),StructField("overallScore",StringType(),True),StructField("component",StringType(),True),StructField("componentScore",StringType(),True),StructField("SourceSystem",StringType(),True),StructField("load_date",DateType(),True),StructField("load_hour",TimestampType(),True),StructField("db_load_time",TimestampType(),True),StructField("db_load_date",DateType(),True)])' where job_cd not in (9004,9006) and configname like '%schema_raw_scoring%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='select DISTINCT cast(tripSummaryId as string),cast(scoreAlgorithmProvider as string),cast(scoreUnit as string),cast(overallScore as string),cast(component as string),cast(componentScore as string),path,load_date,load_hour,db_load_date,db_load_time,SourceSystem from {raw_view} where component is not null and componentScore is not null' where job_cd not in (9004,9006) and configname like '%sql_raw_scoring%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='select cast(tripSummaryId as string),cast(scoringComponent as string),cast(scoringSubComponent as string),cast(roadType as string),cast(thresholdLowerBound as string),cast(thresholdUpperBound as string),cast(scoringComponentUnit as string),cast(occurrences as string),cast(occurrenceUnit as string),path,load_date,load_hour,db_load_date,db_load_time,SourceSystem from {raw_view} where scoringComponent is not null' where job_cd not in (9004,9006) and configname like '%sql_raw_histogramscoringintervals%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='select cast(telemetrySetId as string),cast(degreesLatitude as string),cast(degreesLongitude as string),cast(headingDegrees as string),cast(telemetryEventType as string),cast(utcDateTime as string),cast(speed as string),cast(acceleration as string),cast(telemetryEventSeverityLevel as string),cast(enterpriseReferenceId as string),cast(enterpriseReferenceExtraInfo as string),cast(type as string),cast(deviceSerialNumber as string),cast(deviceIdentifier as string),cast(deviceIdentifierType as string),cast(deviceType as string),cast(enrolledVin as string),cast(detectedVin as string),cast(Event_Reference_Value as string),cast(Event_Timezone_Offset as string),path,load_date,load_hour,db_load_date,db_load_time,SourceSystem from  {raw_view}' where job_cd  in (6003,9003) and configname like '%sql_raw_nontripevent%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='select cast(tripSummaryId as string),cast(degreesLatitude as string),cast(degreesLongitude as string),cast(headingDegrees as string),cast(telemetryEventType as string),cast(utcDateTime as string),cast(speed as string),cast(acceleration as string),cast(secondsOfDriving as string),cast(telemetryEventSeverityLevel as string),cast(telemetryEvents__avgSpeed as string),cast(Event_Reference_Value as string),cast(Event_Timezone_Offset as string),path,load_date,load_hour,db_load_date,db_load_time,SourceSystem from  {raw_view}' where job_cd  in (6001,9001) and configname like '%sql_raw_telemetryevents%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='select cast(tripSummaryId as string),cast(engineRPM as string),cast(accelerometerData as string),cast(ambientTemperature as string),cast(barometericPressure as string),cast(coolantTemperature as string),cast(fuelLevel as string),cast(hdop as string),cast(utcDateTime as string),cast(degreesLatitude as string),cast(degreesLongitude as string),cast(speed as string),cast(headingdegrees as string),cast(acceleration as string),cast(horizontalAccuracy as string),cast(throttlePosition as string),cast(verticalAccuracy as string),null as Accel_Longitudinal,null as Accel_Lateral,null as Accel_Vertical,path,load_date,load_hour,db_load_date,db_load_time,SourceSystem from {raw_view}' where job_cd  in (6001,9001) and configname like '%sql_raw_telemetrypoints%';
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='select cast(tripSummaryId as string),cast(fuelConsumption as string),cast(milStatus as boolean),cast(accelQuality as boolean),cast(transportMode as string),cast(secondsOfIdling as string),cast(transportModeReason as string),cast(hdopAverage as string),cast(utcStartDateTime as string),cast(utcEndDateTime as string),cast(timeZoneOffset as string),cast(drivingDistance as string),cast(maxSpeed as string),cast(avgSpeed as string),cast(totalTripSeconds as bigint),cast(deviceSerialNumber as string),cast(deviceIdentifier as string),cast(deviceIdentifierType as string),cast(deviceType as string),cast(enterpriseReferenceId as string),cast(enterpriseReferenceExtraInfo as string),cast(type as string),cast(enrolledVin as string),cast(detectedVin as string),cast(system as string),path,load_date,load_hour,db_load_date,db_load_time,SourceSystem from {raw_view}' where job_cd  in (6001,9001) and configname like '%sql_raw_tripsummary%';
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC Select DRIVER_KEY, SRC_SYS_CD, LOAD_DT from dhf_iot_harmonized_prod.driver_profile_daily
// MAGIC
// MAGIC where DRIVER_KEY in
// MAGIC
// MAGIC (Select DRIVER_KEY from
// MAGIC
// MAGIC (SELECT Count(*) as Harm_DupeCount, DRIVER_KEY, substr(SRC_SYS_CD,1,6) as SRC_SYS_CD
// MAGIC
// MAGIC FROM dhf_iot_harmonized_prod.driver_profile_daily
// MAGIC
// MAGIC where SRC_SYS_CD in ('CMT_PL', 'CMT_PL_SRM', 'CMT_PL_SRMCM')
// MAGIC
// MAGIC group by 2,3
// MAGIC
// MAGIC having count(*) > 1))
// MAGIC
// MAGIC order by 1,2

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.driver_profile_daily where DRIVER_KEY =10911340

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct min from (SELECT Count(*) as Harm_DupeCount, DRIVER_KEY,min(LOAD_DT) as min,max(LOAD_DT) as max
// MAGIC
// MAGIC FROM dhf_iot_harmonized_prod.driver_profile_daily
// MAGIC
// MAGIC where SRC_SYS_CD in ('CMT_PL', 'CMT_PL_SRM', 'CMT_PL_SRMCM')
// MAGIC
// MAGIC group by 2
// MAGIC
// MAGIC having count(*) > 1)