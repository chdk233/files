-- Databricks notebook source
-- MAGIC %sql
-- MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.badge_daily group by src_sys_cd
-- MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.driver_profile_daily group by src_sys_cd
-- MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.driver_summary_daily group by src_sys_cd
-- MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.portal_user_activity_report_daily group by src_sys_cd
-- MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.heartbeat_daily group by src_sys_cd
-- MAGIC -- select src_sys_cd,max(load_dt) from dhf_iot_harmonized_prod.USER_PERMISSIONS_DAILY group by src_sys_cd
-- MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_labels_daily group by src_sys_cd
-- MAGIC select count(*) from dhf_iot_harmonized_prod.trip_summary_chlg --group by src_sys_cd
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- select sourcesystem,max(load_hour) from dhf_iot_ims_raw_prod.tripSummary where load_hour !='9999-12-31T00:00:00.000+0000' group by sourcesystem;
-- MAGIC -- select sourcesystem,max(load_hour),min(load_date) from dhf_iot_ims_raw_prod.telemetrypoints where load_hour !='9999-12-31T00:00:00.000+0000' group by sourcesystem;
-- MAGIC -- select * from dhf_iot_Tims_raw_prod.telemetrypoints where sourcesystem='TIMS_SR' ORDER BY load_date DESC;
-- MAGIC   -- select src_sys_cd,max(load_hr_ts) from dhf_iot_cmt_raw_prod.trip_detail_realtime group by src_sys_cd;
-- MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.histogram_scoring_interval_chlg group by src_sys_cd;
-- MAGIC select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_summary_chlg group by src_sys_cd;
-- MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.vehicle_chlg group by src_sys_cd;
-- MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.device_chlg group by src_sys_cd;
-- MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_event_chlg group by src_sys_cd;
-- MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_point_chlg group by src_sys_cd;
-- MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.non_trip_event_chlg group by src_sys_cd;
-- MAGIC   

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- -- select sourcesystem,max(load_hour) from dhf_iot_fmc_raw_prod.tripSummary group by sourcesystem;
-- MAGIC -- select DISTINCT(load_date) from dhf_iot_Tims_raw_prod.telemetrypoints where sourcesystem='TIMS_SR' ORDER BY load_date DESC;
-- MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.scoring where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
-- MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.histogram_scoring_interval where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
-- MAGIC  select src_sys_cd,max(LOAD_HR_TS) from dhf_iot_harmonized_prod.trip_summary where load_hr_ts <'9999-01-01T00:00:00.000+0000' group by src_sys_cd;
-- MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.vehicle where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
-- MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.device where load_hr_ts !='9999-12-31T00:00:00.000+0000'group by src_sys_cd;
-- MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_event where load_hr_ts <'8999-12-31T00:00:00.000+0000' group by src_sys_cd;
-- MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_point where load_hr_ts <'8999-12-31T00:00:00.000+0000' group by src_sys_cd;
-- MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.non_trip_event where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
-- MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_gps_waypoints where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
-- MAGIC   

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dhf_iot_cmt_raw_prod.outbound_mobile_score_elements where TRIP_SMRY_KEY like '%055431C1-CFB1-48BC-9FEC-F852EF241081%' and src_sys_cd ='CMT_PL_FDR'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dhf_iot_cmt_raw_prod.trip_summary_realtime where src_sys_cd='CMT_PL_FDR' and load_dt='2023-06-25'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dhf_iot_harmonized_prod.trip_summary_chlg where src_sys_cd='CMT_PL_FDR' and load_dt='2023-06-25'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dhf_iot_harmonized_prod.trip_driver_score_chlg where TRIP_SMRY_KEY like '%055431C1-CFB1-48BC-9FEC-F852EF241081%' and src_sys_cd ='CMT_PL_FDR'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dhf_iot_curated_prod.trip_digest_chlg where TRIP_SMRY_KEY like '%055431C1-CFB1-48BC-9FEC-F852EF241081%'

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select distinct src_sys_cd from dhf_iot_cmt_raw_prod.outbound_mobile_score_elements

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dhf_iot_raw_prod.program_enrollment where vendorAccountId='82383754'

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df=spark.sql("""select distinct cast(monotonically_increasing_id() +65000 as BIGINT) as TRIP_POINT_ID,
-- MAGIC drive_id as TRIP_SMRY_KEY, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, time as TM_EPCH,
-- MAGIC timestamp_millis(time) as UTC_TS,cast(mm_lat as decimal(18,10)) as LAT_NB,cast(mm_lon as decimal(18,10)) as LNGTD_NB,
-- MAGIC cast(mm_dist_km as decimal(35,15)) as DISTNC_TRVL_QTY,
-- MAGIC cast(gps_speed as decimal(35,15)) as GPS_PNT_SPD_QTY,cast(gps_heading as decimal(15,6)) as HEADNG_DEG_QTY,gps_valid as GPS_VALID_STTS_SRC_IN,
-- MAGIC case when gps_valid = 1 THEN 'Y' when gps_valid=0 THEN 'N' When gps_valid IS NULL THEN " " else '@' end as GPS_VALID_STTS_DERV_IN,
-- MAGIC cast(accel_lon_smoothed as decimal(18,10)) as LNGTDNL_ACCLRTN_RT,cast(accel_lat_smoothed as decimal(18,10)) as LATRL_ACCLRTN_RT,accel_valid as ACLRTN_VALID_STTS_SRC_IN,
-- MAGIC case when accel_valid=1 THEN 'Y' when accel_valid=0 THEN 'N' when accel_valid IS NULL THEN " " else '@' end as ACLRTN_VALID_STTS_DERV_IN,cast(roll_rate as decimal(18,10)) as ROLL_RT,
-- MAGIC cast(pitch_rate as decimal(18,10)) as PITCH_RT,cast(yaw_rate as decimal(18,10)) as YAW_RT,
-- MAGIC distraction as DISTRCTN_STTS_SRC_IN,case when distraction=1 THEN 'Y' when distraction=0 THEN 'N' When distraction IS NULL THEN " " Else '@' end as DISTRCTN_STTS_DERV_IN,
-- MAGIC mm_state as MM_ST_CD,coalesce(src_sys_cd ,"NOKEY")as SRC_SYS_CD,coalesce(cast(LOAD_DT as DATE), to_date("9999-12-31")) as LOAD_DT,coalesce(LOAD_HR_TS ,to_timestamp("9999-12-31")) as LOAD_HR_TS 
-- MAGIC  from dhf_iot_cmt_raw_prod.trip_detail_realtime where src_sys_cd in("CMT_PL")""")
-- MAGIC df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_point_chlg")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df=spark.sql("""select distinct cast(monotonically_increasing_id() +65000 as BIGINT) as TRIP_SUMMARY_ID,
-- MAGIC coalesce(driveid, "NOKEY") AS TRIP_SMRY_KEY, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS,account_id as ACCT_ID, deviceid AS DEVC_KEY,cast(distance_km as decimal(15,6)) AS DRVNG_DISTNC_QTY,cast(driving as STRING) as DRIV_STTS_SRC_IN, case
-- MAGIC     when driving = 'true' THEN 'Y'
-- MAGIC     when driving = 'false' THEN 'N'
-- MAGIC     when driving IS NULL then " "
-- MAGIC     else '@' end as DRIV_STTS_DERV_IN, cast((replace(SUBSTRING_INDEX(utc_offset,':', 2),':','.')) as decimal(10,2))as TIME_ZONE_OFFST_NB, classification AS TRNSPRT_MODE_CD,coalesce(start_ts,to_timestamp("9999-12-31")) AS TRIP_START_TS,cast(start_lat as decimal(35,15)) as TRIP_START_LAT_NB,
-- MAGIC cast(start_lon as decimal(35,15)) as TRIP_START_LNGTD_NB,coalesce(end_ts,to_timestamp("9999-12-31")) AS TRIP_END_TS, cast(end_lat as decimal(35,15)) as TRIP_END_LAT_NB,cast(end_lon as decimal(35,15)) as TRIP_END_LNGTD_NB,nightdriving_sec as NIGHT_TIME_DRVNG_SEC_CNT, cast(idle_sec as decimal(15,6)) as IDLING_SC_QTY,cast(id as string) as DRIVER_KEY,user_st_cd as PLCY_ST_CD,prgrm_cd as PRGRM_CD, coalesce(cast(load_dt as DATE), to_date("9999-12-31")) as LOAD_DT, coalesce(src_sys_cd,"NOKEY") AS SRC_SYS_CD, coalesce (LOAD_HR_TS ,to_timestamp("9999-12-31")) as LOAD_HR_TS, "NOKEY" as POLICY_KEY, XXHASH64("NOKEY") as POLICY_KEY_ID from  dhf_iot_cmt_raw_prod.trip_summary_realtime where src_sys_cd in("CMT_PL")""")
-- MAGIC df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_summary_chlg")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df=spark.sql("""select distinct cast(monotonically_increasing_id() +65000 as BIGINT) as TRIP_EVENT_ID,
-- MAGIC coalesce(tdr.db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS,
-- MAGIC case when tdr.src_sys_cd in ("CMT_PL_SRM","CMT_PL_SRMCM","CMT_PL_FDR") THEN coalesce(b.Description, "NOKEY") else coalesce(cast(tdr.event_type as STRING),"NOKEY") end as EVNT_TP_CD , 
-- MAGIC coalesce(tdr.events_ts, to_timestamp("9999-12-31")) as UTC_TS,cast(tdr.events_lat as decimal(18,10)) as LAT_NB,tdr.sub_type as EVNT_SUB_TP_CD, 
-- MAGIC cast(tdr.events_lon as decimal(18,10)) as LNGTD_NB, cast(tdr.severe as STRING) as EVNT_SVRTY_SRC_IN,
-- MAGIC case when tdr.severe='true' THEN 'Y' when tdr.severe='false' THEN 'N' when tdr.severe IS NULL then " " else '@' end as EVNT_SVRTY_DERV_IN,
-- MAGIC cast(tdr.value as decimal(15,6)) as EVT_VAL,tdr.driveid AS TRIP_SMRY_KEY, cast(tdr.speed_kmh as decimal(18,10)) as AVG_SPD_RT,tdr.turn_dps as TURN_DEG_PER_SC_QTY,
-- MAGIC cast(tdr.max_mss as DECIMAL(35,15)) as MAX_MSS_QTY,cast(tdr.risk as decimal(18,10)) as RISK_QTY, tdr.displayed as EVNT_DSPLY_TP_CD,cast(tdr.speed_delta_kmh as DECIMAL(18,10)) as SPD_DLT_KMH_QTY, 
-- MAGIC coalesce(cast(tdr.load_dt as DATE), to_date("9999-12-31")) as LOAD_DT,cast(tdr.duration_for_speed_delta_sec as DECIMAL(18,10)) as SPD_DLT_DRTN_SC_QTY,
-- MAGIC null as ACLRTN_RT ,
-- MAGIC null as DRVNG_SC_QTY ,
-- MAGIC null as EVNT_SVRTY_CD ,
-- MAGIC null as HEADNG_DEG_QTY ,
-- MAGIC null as SPD_RT ,
-- MAGIC null as EVT_RFRNC_VAL ,
-- MAGIC null as EVT_TMZN_OFFST_NUM ,
-- MAGIC null as EVNT_STRT_TS ,
-- MAGIC null as EVNT_END_TS ,
-- MAGIC null as TAG_MAC_ADDR_NM ,
-- MAGIC null as TAG_TRIP_NB ,
-- MAGIC null as SHRT_VEH_ID ,coalesce(tdr.src_sys_cd, "NOKEY") AS SRC_SYS_CD, coalesce(tdr.LOAD_HR_TS , to_timestamp("9999-12-31")) as LOAD_HR_TS , tdr.user_st_cd as PLCY_ST_CD,tdr.prgrm_cd as PRGRM_CD,
-- MAGIC cast(tdr.duration_sec as int) as EVNT_DRTN_QTY from dhf_iot_cmt_raw_prod.events_realtime tdr left join dhf_iot_harmonized_prod.event_code b on tdr.src_sys_cd = b.source and tdr.event_type=b.code where 
-- MAGIC  src_sys_cd in("CMT_PL")""")
-- MAGIC df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_event_chlg")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df=spark.sql("""select distinct cast(monotonically_increasing_id() +65000 as BIGINT) as TRIP_GPS_WAYPOINTS_ID, 
-- MAGIC coalesce(driveid, "NOKEY") AS TRIP_SMRY_KEY, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS,cast(waypoints_lat as DECIMAL(35,15)) as WYPNT_LAT_NB,
-- MAGIC cast(waypoints_lon as DECIMAL(35,15)) as WYPNT_LNGTD_NB,coalesce(waypoints_ts, to_timestamp("9999-12-31")) AS UTC_TS,cast(avg_moving_speed_kmh as DECIMAL(18,10)) as AVG_SPD_KMH_RT,cast(max_speed_kmh as DECIMAL(18,10)) as MAX_SPD_KMH_RT,
-- MAGIC cast(speed_limit_kmh as DECIMAL(18,10)) as SPD_LMT_KMH_QTY,link_id as LINK_ID,cast(display_code as STRING) as DSPLY_CD,cast(prepended as STRING) as PRPND_STTS_SRC_IN,
-- MAGIC case when prepended='true' THEN 'Y' when prepended='false' THEN 'N' when prepended IS NULL then " " else '@' end as PRPND_STTS_DERV_IN,user_st_cd as PLCY_ST_CD,prgrm_cd as PRGRM_CD,
-- MAGIC null as TAG_MAC_ADDR_NM ,null as TAG_TRIP_NB ,null as SHRT_VEH_ID ,
-- MAGIC coalesce(cast(load_dt as DATE),to_date("9999-12-31")) as LOAD_DT, coalesce(src_sys_cd,"NOKEY") AS SRC_SYS_CD, coalesce(cast(LOAD_HR_TS as TIMESTAMP), to_timestamp("9999-12-31")) as LOAD_HR_TS 
-- MAGIC from dhf_iot_harmonized_prod.waypoints_realtime where src_sys_cd in("CMT_PL")""")
-- MAGIC df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.TRIP_GPS_WAYPOINTS_chlg")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) from dhf_iot_cmt_raw_prod.trip_summary_realtime where src_sys_cd  ='CMT_PL' --and load_date<'2023-05-15'   22,457,124,007

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) from dhf_iot_cmt_raw_prod.badge_daily where src_sys_cd  ='@' and load_date<'2023-05-15'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC merge into dhf_iot_cmt_raw_prod.trip_labels_daily a using (select VNDR_ACCT_ID,PRGRM_TP_CD,src_sys_cd,user_st_cd
-- MAGIC         from (select VNDR_ACCT_ID,CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
-- MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
-- MAGIC     ELSE PRGRM_TP_CD
-- MAGIC    END PRGRM_TP_CD,CASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
-- MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
-- MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
-- MAGIC    END src_sys_cd,plcy_st_cd as user_st_cd,row_number() over (partition by VNDR_ACCT_ID order by EVNT_SYS_TS desc ) as row from dhf_iot_harmonized_prod.program_enrollment where VNDR_CD='CMT') where row=1 and src_sys_cd in  ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM')) b on a.short_user_id=b.VNDR_ACCT_ID and a.load_date<='2023-05-15' and a.src_sys_cd ='@' and a.program_id is null when matched then update set a.user_state=b.user_st_cd , a.program_id=b.PRGRM_TP_CD , a.src_sys_cd=b.src_sys_cd

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC merge into dhf_iot_cmt_raw_prod.trip_summary_daily a using (select VNDR_ACCT_ID,PRGRM_TP_CD,src_sys_cd,user_st_cd
-- MAGIC         from (select VNDR_ACCT_ID,CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
-- MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
-- MAGIC     ELSE PRGRM_TP_CD
-- MAGIC    END PRGRM_TP_CD,CASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
-- MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
-- MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
-- MAGIC    END src_sys_cd,plcy_st_cd as user_st_cd,row_number() over (partition by VNDR_ACCT_ID order by EVNT_SYS_TS desc ) as row from dhf_iot_harmonized_prod.program_enrollment where VNDR_CD='CMT') where row=1 and src_sys_cd in  ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM')) b on a.short_user_id=b.VNDR_ACCT_ID and a.load_date<='2023-05-15' and a.src_sys_cd ='@' and a.program_id is null when matched then update set a.user_state=b.user_st_cd , a.program_id=b.PRGRM_TP_CD , a.src_sys_cd=b.src_sys_cd

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC merge into dhf_iot_cmt_raw_prod.driver_profile_daily a using (select VNDR_ACCT_ID,PRGRM_TP_CD,src_sys_cd,user_st_cd
-- MAGIC         from (select VNDR_ACCT_ID,CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
-- MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
-- MAGIC     ELSE PRGRM_TP_CD
-- MAGIC    END PRGRM_TP_CD,CASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
-- MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
-- MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
-- MAGIC    END src_sys_cd,plcy_st_cd as user_st_cd,row_number() over (partition by VNDR_ACCT_ID order by EVNT_SYS_TS desc ) as row from dhf_iot_harmonized_prod.program_enrollment where VNDR_CD='CMT') where row=1 and src_sys_cd in  ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM')) b on a.short_user_id=b.VNDR_ACCT_ID and a.load_date<='2023-05-15' and a.src_sys_cd ='@' and a.program_id is null when matched then update set a.user_state=b.user_st_cd , a.program_id=b.PRGRM_TP_CD , a.src_sys_cd=b.src_sys_cd

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC merge into dhf_iot_cmt_raw_prod.driver_summary_daily a using (select VNDR_ACCT_ID,PRGRM_TP_CD,src_sys_cd,user_st_cd
-- MAGIC         from (select VNDR_ACCT_ID,CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
-- MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
-- MAGIC     ELSE PRGRM_TP_CD
-- MAGIC    END PRGRM_TP_CD,CASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
-- MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
-- MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
-- MAGIC    END src_sys_cd,plcy_st_cd as user_st_cd,row_number() over (partition by VNDR_ACCT_ID order by EVNT_SYS_TS desc ) as row from dhf_iot_harmonized_prod.program_enrollment where VNDR_CD='CMT') where row=1 and src_sys_cd in  ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM')) b on a.short_user_id=b.VNDR_ACCT_ID and a.load_date<='2023-05-15' and a.src_sys_cd ='@' and a.program_id is null when matched then update set a.user_state=b.user_st_cd , a.program_id=b.PRGRM_TP_CD , a.src_sys_cd=b.src_sys_cd

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC merge into dhf_iot_cmt_raw_prod.fraud_daily a using (select VNDR_ACCT_ID,PRGRM_TP_CD,src_sys_cd,user_st_cd
-- MAGIC         from (select VNDR_ACCT_ID,CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
-- MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
-- MAGIC     ELSE PRGRM_TP_CD
-- MAGIC    END PRGRM_TP_CD,CASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
-- MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
-- MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
-- MAGIC    END src_sys_cd,plcy_st_cd as user_st_cd,row_number() over (partition by VNDR_ACCT_ID order by EVNT_SYS_TS desc ) as row from dhf_iot_harmonized_prod.program_enrollment where VNDR_CD='CMT') where row=1 and src_sys_cd in  ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM')) b on a.short_user_id=b.VNDR_ACCT_ID and a.load_date<='2023-05-15' and a.src_sys_cd ='@' and a.program_id is null when matched then update set a.user_state=b.user_st_cd , a.program_id=b.PRGRM_TP_CD , a.src_sys_cd=b.src_sys_cd

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC merge into dhf_iot_cmt_raw_prod.heartbeat_daily a using (select VNDR_ACCT_ID,PRGRM_TP_CD,src_sys_cd,user_st_cd
-- MAGIC         from (select VNDR_ACCT_ID,CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
-- MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
-- MAGIC     ELSE PRGRM_TP_CD
-- MAGIC    END PRGRM_TP_CD,CASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
-- MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
-- MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
-- MAGIC    END src_sys_cd,plcy_st_cd as user_st_cd,row_number() over (partition by VNDR_ACCT_ID order by EVNT_SYS_TS desc ) as row from dhf_iot_harmonized_prod.program_enrollment where VNDR_CD='CMT') where row=1 and src_sys_cd in  ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM')) b on a.short_user_id=b.VNDR_ACCT_ID and a.load_date<='2023-05-15' and a.src_sys_cd ='@' and a.program_id is null when matched then update set a.user_state=b.user_st_cd , a.program_id=b.PRGRM_TP_CD , a.src_sys_cd=b.src_sys_cd

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC merge into dhf_iot_cmt_raw_prod.user_permissions_daily a using (select VNDR_ACCT_ID,PRGRM_TP_CD,src_sys_cd,user_st_cd
-- MAGIC         from (select VNDR_ACCT_ID,CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
-- MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
-- MAGIC     ELSE PRGRM_TP_CD
-- MAGIC    END PRGRM_TP_CD,CASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
-- MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
-- MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
-- MAGIC    END src_sys_cd,plcy_st_cd as user_st_cd,row_number() over (partition by VNDR_ACCT_ID order by EVNT_SYS_TS desc ) as row from dhf_iot_harmonized_prod.program_enrollment where VNDR_CD='CMT') where row=1 and src_sys_cd in  ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM')) b on a.short_user_id=b.VNDR_ACCT_ID and a.load_date<='2023-05-15' and a.src_sys_cd ='@' and a.program_id is null when matched then update set a.user_state=b.user_st_cd , a.program_id=b.PRGRM_TP_CD , a.src_sys_cd=b.src_sys_cd

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select SRC_SYS_CD,count(*) from dhf_iot_cmt_raw_prod.trip_detail_realtime where load_dt>='2023-08-17' group by SRC_SYS_CD --and src_sys_cd in  ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM')

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) from dhf_iot_cmt_raw_prod.trip_summary_daily where load_date>='2023-08-17' --group by SRC_SYS_CD --and src_sys_cd in  ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM')

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) from dhf_iot_cmt_raw_prod.trip_summary_realtime where load_dt='2023-05-10'

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val df=spark.sql("select distinct load_dt from dhf_iot_cmt_raw_prod.waypoints_realtime order by load_dt").where("load_dt>='2023-01-02'  load_dt<='2023-01-22'")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC merge into dhf_iot_cmt_raw_prod.badge_daily a using (select VNDR_ACCT_ID,PRGRM_TP_CD,src_sys_cd,user_st_cd
-- MAGIC         from (select VNDR_ACCT_ID,CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
-- MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
-- MAGIC     ELSE PRGRM_TP_CD
-- MAGIC    END PRGRM_TP_CD,CASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
-- MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
-- MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
-- MAGIC    END src_sys_cd,plcy_st_cd as user_st_cd,row_number() over (partition by VNDR_ACCT_ID order by EVNT_SYS_TS desc ) as row from dhf_iot_harmonized_prod.program_enrollment where VNDR_CD='CMT') where row=1 and src_sys_cd in  ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM')) b on a.short_user_id=b.VNDR_ACCT_ID and a.load_date<='2023-05-15' and a.src_sys_cd ='@' and a.program_id is null when matched then update set a.user_state=b.user_st_cd , a.program_id=b.PRGRM_TP_CD , a.src_sys_cd=b.src_sys_cd

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select distinct VNDR_ACCT_ID,data_clctn_id,
-- MAGIC    CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
-- MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
-- MAGIC     ELSE PRGRM_TP_CD
-- MAGIC    END prgrm_cd,CASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
-- MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
-- MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
-- MAGIC    END src_sys_cd,EVNT_SYS_TS,plcy_st_cd as user_st_cd
-- MAGIC   from (select
-- MAGIC   distinct VNDR_ACCT_ID,data_clctn_id,PRGRM_TP_CD,EVNT_SYS_TS,
-- MAGIC   coalesce(plcy_st_cd,' ') as plcy_st_cd
-- MAGIC   ,row_number() over (partition by DATA_CLCTN_ID order by EVNT_SYS_TS desc) as rnk
-- MAGIC   from dhf_iot_harmonized_prod.program_enrollment
-- MAGIC   where upper(VNDR_CD)= 'CMT' 
-- MAGIC   )where rnk = 1

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select VNDR_ACCT_ID,src_sys_cd
-- MAGIC         from (select VNDR_ACCT_ID,CASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
-- MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
-- MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
-- MAGIC    END src_sys_cd,row_number() over (partition by VNDR_ACCT_ID order by KFK_TS desc ) as row from dhf_iot_harmonized_prod.program_enrollment where VNDR_CD='CMT') where row=1 and src_sys_cd in  ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM')

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select distinct src_sys_cd from dhf_iot_cmt_raw_prod.trip_labels_daily where  load_date<'2023-05-15'--   38.032,412,681

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # df=spark.sql("select distinct cast(load_dt as string) from dhf_iot_cmt_raw_prod.matchsrc").collect()
-- MAGIC df=*([row[0] for row in  spark.sql("select distinct cast(load_dt as string) from dhf_iot_cmt_raw_prod.dup_trips").collect()]),
-- MAGIC # for item in df:
-- MAGIC #  print(item)
-- MAGIC #  load_date=str(item)
-- MAGIC spark.sql(f"merge into dhf_iot_harmonized_prod.trip_point a using dhf_iot_cmt_raw_prod.matchsrc b on a.trip_smry_key=b.driveid and a.load_dt=b.load_dt and a.src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') and a.load_dt in {df} when matched then delete")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC spark.sql("select driveid ,load_dt from (select *,row_number() over (partition by driveid order by load_dt desc) as row from (select distinct driveid,load_dt from dhf_iot_cmt_raw_prod.trips_src where src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') )) where row>1").write.format("delta").mode("append").saveAsTable("dhf_iot_cmt_raw_prod.dup_trips")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(distinct driveid) from dhf_iot_cmtpl_raw_prod.trip_summary_realtime 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select goal_state_count, legacy_count, goal.load_date  from
-- MAGIC
-- MAGIC (select count(*) as legacy_count, load_date from
-- MAGIC
-- MAGIC (SELECT distinct
-- MAGIC
-- MAGIC email,
-- MAGIC
-- MAGIC short_user_id,
-- MAGIC
-- MAGIC badge_name,
-- MAGIC
-- MAGIC badge_progress,
-- MAGIC
-- MAGIC date_awarded,
-- MAGIC
-- MAGIC load_date
-- MAGIC
-- MAGIC FROM
-- MAGIC
-- MAGIC dhf_iot_cmtpl_raw_prod.badge_daily
-- MAGIC
-- MAGIC where load_date between '2021-06-11'and '2023-05-17')
-- MAGIC
-- MAGIC group by 2)leg
-- MAGIC
-- MAGIC join
-- MAGIC
-- MAGIC (select count(*) as goal_state_count, load_date from
-- MAGIC
-- MAGIC (SELECT distinct
-- MAGIC
-- MAGIC email,
-- MAGIC
-- MAGIC short_user_id,
-- MAGIC
-- MAGIC badge_name,
-- MAGIC
-- MAGIC badge_progress,
-- MAGIC
-- MAGIC date_awarded,
-- MAGIC
-- MAGIC load_date
-- MAGIC
-- MAGIC FROM
-- MAGIC
-- MAGIC dhf_iot_cmt_raw_prod.badge_daily
-- MAGIC
-- MAGIC where load_date BETWEEN '2021-06-11'and '2023-05-17')
-- MAGIC
-- MAGIC group by 2)goal
-- MAGIC
-- MAGIC on leg.load_date = goal.load_date
-- MAGIC
-- MAGIC where goal_state_count <> legacy_count

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) as goal_state_count, load_date from
-- MAGIC
-- MAGIC (SELECT distinct
-- MAGIC
-- MAGIC email,
-- MAGIC
-- MAGIC short_user_id,
-- MAGIC
-- MAGIC badge_name,
-- MAGIC
-- MAGIC badge_progress,
-- MAGIC
-- MAGIC date_awarded,
-- MAGIC
-- MAGIC user_state,
-- MAGIC
-- MAGIC program_id,
-- MAGIC
-- MAGIC src_sys_cd,
-- MAGIC
-- MAGIC load_date
-- MAGIC
-- MAGIC FROM
-- MAGIC
-- MAGIC dhf_iot_cmt_raw_prod.badge_daily
-- MAGIC
-- MAGIC where load_date BETWEEN '2021-06-11'and '2023-05-17')
-- MAGIC
-- MAGIC group by 2

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select distinct load_date from dhf_iot_cmtpl_raw_prod.trip_summary_realtime order by load_date desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(distinct a.driveid) from dhf_iot_cmtpl_raw_prod.trip_summary_realtime a left anti join  dhf_iot_cmt_raw_prod.trip_summary_realtime b on a.driveid=b.driveid and a.load_date=b.load_dt

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- select distinct load_dt from dhf_iot_cmt_raw_prod.waypoints_realtime --where load_dt<'2023-05-18'
-- MAGIC select * from dhf_iot_cmt_raw_prod.waypoints_realtime where load_dt > '2022-03-01' --and load_dt< '2022-04-31'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select *,datediff(day,min,max ) as diff from (select driveid,max(load_dt) as max,min(load_dt) as min from  dhf_iot_cmt_raw_prod.trips_src group by driveid having count(*)>1) where datediff(day,max,min )!=0 and max<='2023-05-17' and min<='2023-05-17' --or min!=max

-- COMMAND ----------

-- MAGIC
-- MAGIC %sql
-- MAGIC select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_detail_seconds where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
-- MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_curated_prod.trip_detail where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
-- MAGIC -- select * from dhf_iot_curated_prod.device_status_chlg --where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
-- MAGIC -- select src_sys_cd,max(load_DT) from dhf_iot_curated_prod.device_Summary where load_DT !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
-- MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_curated_prod.OUTBOUND_SCORE_ELEMENTs where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
-- MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_curated_prod.inBOUND_SCORE_ELEMENTS where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
-- MAGIC -- select load_dt,count(*) from dhf_iot_curated_prod.inBOUND_SCORE_ELEMENTS where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by load_dt order by load_dt desc;
-- MAGIC

-- COMMAND ----------

merge into dhf_iot_harmonized_prod.trip_point a using dhf_iot_cmt_raw_prod.matchsrc b on a.trip_smry_key=b.driveid and a.src_sys_cd=b.src_sys_cd and a.load_dt=b.load_dt and a.src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') when matched then update set a.src_sys_cd=b.src_sys_cd;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select account_id,id from dhf_iot_cmt_raw_prod.trip_summary_realtime where driveid in ('1E938106-DE69-4889-A93E-09863EC63773') and load_dt ='2023-07-27'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select distinct DATA_CLCTN_ID,VNDR_ACCT_ID from dhf_iot_harmonized_prod.program_enrollment where DATA_CLCTN_ID='e1c9cf4b-8e48-44df-bd88-b5e5b88777c3'

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val program_enrollmentDF=spark.sql(s"""select distinct VNDR_ACCT_ID,data_clctn_id,
-- MAGIC CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
-- MAGIC     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
-- MAGIC     ELSE PRGRM_TP_CD
-- MAGIC END PRGRM_TP_CD,EVNT_SYS_TS,plcy_nb,
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
-- MAGIC ,rank() over (partition by DATA_CLCTN_ID, VNDR_ACCT_ID order by EVNT_SYS_TS desc) as rnk
-- MAGIC from dhf_iot_harmonized_prod.program_enrollment
-- MAGIC where upper(VNDR_CD)= 'CMT'
-- MAGIC )where rnk = 1""").filter("DATA_CLCTN_ID='e1c9cf4b-8e48-44df-bd88-b5e5b88777c3'")
-- MAGIC display(program_enrollmentDF)
-- MAGIC val summry_enrlmtDF=trip_summary_json_df_s3loc.join(program_enrollmentDF,trip_summary_json_df_s3loc("id") ===  program_enrollmentDF("VNDR_ACCT_ID") && trip_summary_json_df_s3loc("account_id") ===  program_enrollmentDF("DATA_CLCTN_ID") && trip_summary_json_df_s3loc("program_id") ===  program_enrollmentDF("PRGRM_TP_CD") ,"left")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select account_id,id from dhf_iot_cmt_raw_prod.trip_summary_realtime where driveid in ('1E938106-DE69-4889-A93E-09863EC63773') and load_dt ='2023-07-27'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dhf_iot_cmt_raw_prod.outbound_mobile_score_elements where trip_smry_key in ('1E938106-DE69-4889-A93E-09863EC63773') and load_dt ='2023-07-27'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC show tables in  dhf_iot_raw_score_prod

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df=spark.read.json("s3://pcds-databricks-common-785562577411/iot/raw/smartride/cmt-pl/fdr1-tripdriver-Scoring/db_load_date=2023-07-27/")
-- MAGIC display(df.filter("value like '%1E938106-DE69-4889-A93E-09863EC63773%'"))

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dhf_iot_raw_score_prod.fdr_trip_driver_score where LOAD_DATE =current_date() and src_sys_cd is null and driveid in ('1E938106-DE69-4889-A93E-09863EC63773') --and load_date ='2023-06-22'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select distinct load_date from dhf_iot_raw_score_prod.fdr_trip_driver_score where src_sys_cd is null--where driveid in ('C6A48C0C-787D-40C5-B9CE-A78766DEF8A7') and load_date ='2023-06-22'

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select (*) from dhf_iot_harmonized_prod.trip_driver_score where TRIP_SMRY_KEY in 
-- MAGIC ('1B9B23EA-3624-4087-8544-AD233ED78614','51ABE728-6227-43C4-ABE1-E7D7D3EB2360','C6A48C0C-787D-40C5-B9CE-A78766DEF8A7','D7296CAD-7D66-4C56-A32F-9883C2065646','D8BB478E-58DF-420C-A3D0-469963A6FBEE','DA99525B-477D-4ABC-807B-7227EF09FFCD','E0F3F0AA-D51B-460A-8392-9B48E519BBF4')
-- MAGIC --('C6A48C0C-787D-40C5-B9CE-A78766DEF8A7','E0F3F0AA-D51B-460A-8392-9B48E519BBF4','1B9B23EA-3624-4087-8544-AD233ED78614','D7296CAD-7D66-4C56-A32F-9883C2065646')

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select distinct SRC_SYS_CD from dhf_iot_harmonized_prod.trip_driver_score

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select (*) from dhf_iot_harmonized_prod.trip_driver_score_chlg where TRIP_SMRY_KEY in 
-- MAGIC ('1B9B23EA-3624-4087-8544-AD233ED78614','51ABE728-6227-43C4-ABE1-E7D7D3EB2360','C6A48C0C-787D-40C5-B9CE-A78766DEF8A7','D7296CAD-7D66-4C56-A32F-9883C2065646','D8BB478E-58DF-420C-A3D0-469963A6FBEE','DA99525B-477D-4ABC-807B-7227EF09FFCD','E0F3F0AA-D51B-460A-8392-9B48E519BBF4')
-- MAGIC --('C6A48C0C-787D-40C5-B9CE-A78766DEF8A7','E0F3F0AA-D51B-460A-8392-9B48E519BBF4','1B9B23EA-3624-4087-8544-AD233ED78614','D7296CAD-7D66-4C56-A32F-9883C2065646')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.window import *
-- MAGIC from pyspark import SparkContext
-- MAGIC from pyspark import SQLContext
-- MAGIC from delta.tables import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC import time
-- MAGIC #scoring
-- MAGIC def removeDuplicatesMicrobatch_DHFGeneric(df, partitionKeys, orderbyCols):
-- MAGIC   print("entering removeduplicates microbatch function \n")
-- MAGIC   partition_cols=partitionKeys.split(",")
-- MAGIC   orderyby_cols=orderbyCols.split(",")
-- MAGIC   w = Window.partitionBy(*partition_cols).orderBy(*[desc(c) for c in orderyby_cols])
-- MAGIC   firsRowDF = df.withColumn("rownum", row_number().over(w)).where(col("rownum") == 1).drop("rownum")
-- MAGIC   print("leaving removeduplicates microbatch function")
-- MAGIC   return firsRowDF
-- MAGIC
-- MAGIC def type1_defaultMergePartitionied_fdr_scoring(df, target, merge_key, update_key): #sde #2
-- MAGIC   df = removeDuplicatesMicrobatch_DHFGeneric(df,merge_key, 'ETL_ROW_EFF_DTS')
-- MAGIC #   display(df)
-- MAGIC   
-- MAGIC   print("entering type1_defaultMergePartitionied_DHFGeneric")
-- MAGIC   dt = DeltaTable.forName(spark, target)
-- MAGIC   match_condition = (" AND ".join(list(map((lambda x: f"(events.{x.strip()}=updates.{x.strip()} OR (events.{x.strip()} is null AND updates.{x.strip()} is null))"),merge_key.split(","))))) + f" AND events.SRC_SYS_CD in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM','@')"
-- MAGIC   print(match_condition)
-- MAGIC   set_condition = ",".join(list(map((lambda x: f"{x.strip()}=updates.{x.strip()}"),update_key.split(","))))
-- MAGIC   print(set_condition)
-- MAGIC   sc = dict(subString.split("=") for subString in set_condition.split(","))
-- MAGIC   print(sc)
-- MAGIC   dt.alias('events').merge(df.alias('updates'), match_condition).whenMatchedUpdate(set = sc).whenNotMatchedInsertAll().execute()
-- MAGIC   # df.write.format("delta").mode("append").saveAsTable("dhf_iot_raw_prod.trip_driver_test")
-- MAGIC   display(df)
-- MAGIC
-- MAGIC     
-- MAGIC def merge_cmt_pl_trip_driver_score(microBatchDF, batchId, rawDB, harmonizedDB, target):
-- MAGIC #   microBatchDF=microBatchDF.filter("MODL_ACRNYM_TT like '%FDR%'")
-- MAGIC   merge_keys = "MODL_NM,MODL_VRSN_TT,MODL_ACRNYM_TT,ANLYTC_TP_CD,TRIP_INSTC_ID,TRIP_SMRY_KEY,DATA_CLCTN_ID,DRIVR_KEY,POLICY_KEY,TRIP_CT,EVNT_SRC,DRVNG_DISTNC_QTY,PHN_MTN_CT,HNDHLD_CT,TAP_CT,PHN_HNDLNG_CT,TRNSCTN_TP_CD,MDL_OTPT_TP_CD,MDL_OTPT_SCR_QTY,MDL_OTPT_SCR_TS,WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_TP_CD,WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY,WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS,HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_TP_CD,HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY,HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS,PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_CD,PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY,PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS,DRVNG_DISTNC_MPH_QTY,TRNSPRT_MODE_CD,TRIP_END_TS,TIME_ZONE_OFFST_NB,USER_LBL_DT,OPRTN_SCR_IN,ADJST_DISTNC_KM_QTY,AVG_ANNL_ADJ_KM,EXPSR_YR,MOVNG_SEC_QTY,LAST_TRIP_DT,LOG_EXPSR_YR,SCR_CALC_DT,SCR_CALC_TS,SCR_QTY,RQ5_QTY,SCR_DAYS_CT,TRNSCTN_ID,prgrm_term_beg_dt_used,prgrm_term_end_dt_used,ENTRP_CUST_NB,LAST_RQST_DT,PLCY_ST_CD,PRGRM_TERM_BEG_DT,PRGRM_TERM_END_DT,PRGRM_STTS_DSC,DATA_CLCTN_STTS_DSC,DEVC_STTS_DT,PRGRM_TP_CD,PGRM_CD,SRC_SYS_CD,SBJT_ID,SBJCT_TP_CD,VNDR_CD,DEVC_ID,DEVC_STTS_CD,INSTL_GRC_END_DT,LOG_ANNL_ADJST_MILES_QTY,RQ5A_QTY,PCTG_TRIP_MSSNG,PRGRM_SCR_MODL_CD"
-- MAGIC   update_keys = get_update_keys(microBatchDF, target, merge_keys)
-- MAGIC   print(f" \n merge keys : {merge_keys} ")
-- MAGIC   print(f" \n update keys : {update_keys} ")
-- MAGIC   type1_defaultMergePartitionied_fdr_scoring(microBatchDF, f"{harmonizedDB}.{target}", merge_keys, update_keys)
-- MAGIC
-- MAGIC def get_update_keys(df, target, merge_keys):
-- MAGIC   print('getting updateable keys')
-- MAGIC   update_keys = ""
-- MAGIC   for key in df.columns:
-- MAGIC     if key not in ('ETL_ROW_EFF_DTS','SRC_SYS_CD',f'{target.upper()}_ID') and key not in merge_keys:
-- MAGIC       update_keys = update_keys + key + ','
-- MAGIC     
-- MAGIC   return update_keys[:-1]
-- MAGIC microBatchDF=spark.sql("select (*) from dhf_iot_harmonized_prod.trip_driver_score_chlg where TRIP_SMRY_KEY in ('1B9B23EA-3624-4087-8544-AD233ED78614','51ABE728-6227-43C4-ABE1-E7D7D3EB2360','C6A48C0C-787D-40C5-B9CE-A78766DEF8A7','D7296CAD-7D66-4C56-A32F-9883C2065646','D8BB478E-58DF-420C-A3D0-469963A6FBEE','DA99525B-477D-4ABC-807B-7227EF09FFCD','E0F3F0AA-D51B-460A-8392-9B48E519BBF4')")
-- MAGIC merge_cmt_pl_trip_driver_score(microBatchDF, 2, "rawDB", "dhf_iot_raw_prod", "trip_driver_test")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dhf_iot_raw_prod. trip_driver_test events inner join dhf_iot_raw_prod. trip_driver_test updates on   (events.TRIP_SMRY_KEY=updates.TRIP_SMRY_KEY OR (events.TRIP_SMRY_KEY is null AND updates.TRIP_SMRY_KEY is null)) AND events.SRC_SYS_CD in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM','@')   

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dhf_iot_raw_prod. trip_driver_test

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*),MODL_NM,MODL_VRSN_TT,MODL_ACRNYM_TT,ANLYTC_TP_CD
-- MAGIC ,TRIP_INSTC_ID,TRIP_SMRY_KEY,DATA_CLCTN_ID,DRIVR_KEY,POLICY_KEY
-- MAGIC ,TRIP_CT,EVNT_SRC,DRVNG_DISTNC_QTY,PHN_MTN_CT,HNDHLD_CT,TAP_CT
-- MAGIC ,PHN_HNDLNG_CT,TRNSCTN_TP_CD,MDL_OTPT_TP_CD,MDL_OTPT_SCR_QTY
-- MAGIC ,MDL_OTPT_SCR_TS,WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_TP_CD
-- MAGIC ,WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY,WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS
-- MAGIC ,HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_TP_CD,HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY
-- MAGIC ,HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS,PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_CD
-- MAGIC ,PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY,PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS
-- MAGIC ,DRVNG_DISTNC_MPH_QTY,TRNSPRT_MODE_CD,TRIP_END_TS,TIME_ZONE_OFFST_NB,USER_LBL_DT,OPRTN_SCR_IN,ADJST_DISTNC_KM_QTY
-- MAGIC ,AVG_ANNL_ADJ_KM,EXPSR_YR,MOVNG_SEC_QTY,LAST_TRIP_DT,LOG_EXPSR_YR,SCR_CALC_DT,SCR_CALC_TS,SCR_QTY,RQ5_QTY,SCR_DAYS_CT
-- MAGIC ,TRNSCTN_ID,prgrm_term_beg_dt_used,prgrm_term_end_dt_used,ENTRP_CUST_NB,LAST_RQST_DT,PLCY_ST_CD,PRGRM_TERM_BEG_DT
-- MAGIC ,PRGRM_TERM_END_DT,PRGRM_STTS_DSC,DATA_CLCTN_STTS_DSC,DEVC_STTS_DT,PRGRM_TP_CD,PGRM_CD,SRC_SYS_CD
-- MAGIC ,SBJT_ID,SBJCT_TP_CD,VNDR_CD,DEVC_ID,DEVC_STTS_CD,INSTL_GRC_END_DT,LOG_ANNL_ADJST_MILES_QTY,RQ5A_QTY,PCTG_TRIP_MSSNG
-- MAGIC ,PRGRM_SCR_MODL_CD
-- MAGIC FROM dhf_iot_harmonized_prod.trip_driver_score
-- MAGIC group by MODL_NM,MODL_VRSN_TT,MODL_ACRNYM_TT,ANLYTC_TP_CD
-- MAGIC ,TRIP_INSTC_ID,TRIP_SMRY_KEY,DATA_CLCTN_ID,DRIVR_KEY,POLICY_KEY
-- MAGIC ,TRIP_CT,EVNT_SRC,DRVNG_DISTNC_QTY,PHN_MTN_CT,HNDHLD_CT,TAP_CT
-- MAGIC ,PHN_HNDLNG_CT,TRNSCTN_TP_CD,MDL_OTPT_TP_CD,MDL_OTPT_SCR_QTY
-- MAGIC ,MDL_OTPT_SCR_TS,WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_TP_CD
-- MAGIC ,WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY,WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS
-- MAGIC ,HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_TP_CD,HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY
-- MAGIC ,HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS,PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_CD
-- MAGIC ,PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY,PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS
-- MAGIC ,DRVNG_DISTNC_MPH_QTY,TRNSPRT_MODE_CD,TRIP_END_TS,TIME_ZONE_OFFST_NB,USER_LBL_DT,OPRTN_SCR_IN,ADJST_DISTNC_KM_QTY
-- MAGIC ,AVG_ANNL_ADJ_KM,EXPSR_YR,MOVNG_SEC_QTY,LAST_TRIP_DT,LOG_EXPSR_YR,SCR_CALC_DT,SCR_CALC_TS,SCR_QTY,RQ5_QTY,SCR_DAYS_CT
-- MAGIC ,TRNSCTN_ID,prgrm_term_beg_dt_used,prgrm_term_end_dt_used,ENTRP_CUST_NB,LAST_RQST_DT,PLCY_ST_CD,PRGRM_TERM_BEG_DT
-- MAGIC ,PRGRM_TERM_END_DT,PRGRM_STTS_DSC,DATA_CLCTN_STTS_DSC,DEVC_STTS_DT,PRGRM_TP_CD,PGRM_CD,SRC_SYS_CD
-- MAGIC ,SBJT_ID,SBJCT_TP_CD,VNDR_CD,DEVC_ID,DEVC_STTS_CD,INSTL_GRC_END_DT,LOG_ANNL_ADJST_MILES_QTY,RQ5A_QTY,PCTG_TRIP_MSSNG
-- MAGIC ,PRGRM_SCR_MODL_CD
-- MAGIC having count(*) > 1
-- MAGIC
-- MAGIC -- '1B9B23EA-3624-4087-8544-AD233ED78614','51ABE728-6227-43C4-ABE1-E7D7D3EB2360','C6A48C0C-787D-40C5-B9CE-A78766DEF8A7','D7296CAD-7D66-4C56-A32F-9883C2065646','D8BB478E-58DF-420C-A3D0-469963A6FBEE','DA99525B-477D-4ABC-807B-7227EF09FFCD','E0F3F0AA-D51B-460A-8392-9B48E519BBF4'
-- MAGIC  

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select distinct load_dt from dhf_iot_harmonized_prod.trip_gps_waypoints where src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') and load_dt <'2021-06-11' --187,654,987,894 1084467835554   1080116291470  1107963212229 1237110225895

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC update dhf_iot_cmt_raw_prod.trip_summary_daily set src_sys_cd="CMT_PL_SRMCM" where program_id="SmartRideMobileContinuous" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.badge_daily set src_sys_cd="CMT_PL_SRMCM" where program_id="SmartRideMobileContinuous" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.fraud_daily set src_sys_cd="CMT_PL_SRMCM" where program_id="SmartRideMobileContinuous" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.driver_profile_daily set src_sys_cd="CMT_PL_SRMCM" where program_id="SmartRideMobileContinuous" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.trip_labels_daily set src_sys_cd="CMT_PL_SRMCM" where program_id="SmartRideMobileContinuous" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.driver_summary_daily set src_sys_cd="CMT_PL_SRMCM" where program_id="SmartRideMobileContinuous" and src_sys_cd='@';
-- MAGIC -- update dhf_iot_cmt_raw_prod.user_permissions_daily set src_sys_cd="CMT_PL_SRMCM" where program_id="SmartRideMobileContinuous" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.heartbeat_daily set src_sys_cd="CMT_PL_SRMCM" where program_id="SmartRideMobileContinuous" and src_sys_cd='@';

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC update dhf_iot_cmt_raw_prod.trip_summary_daily set src_sys_cd="CMT_PL_FDR" where program_id="FocusedDriving" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.badge_daily set src_sys_cd="CMT_PL_FDR" where program_id="FocusedDriving" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.fraud_daily set src_sys_cd="CMT_PL_FDR" where program_id="FocusedDriving" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.driver_profile_daily set src_sys_cd="CMT_PL_FDR" where program_id="FocusedDriving" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.trip_labels_daily set src_sys_cd="CMT_PL_FDR" where program_id="FocusedDriving" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.driver_summary_daily set src_sys_cd="CMT_PL_FDR" where program_id="FocusedDriving" and src_sys_cd='@';
-- MAGIC -- update dhf_iot_cmt_raw_prod.user_permissions_daily set src_sys_cd="CMT_PL_FDR" where program_id="FocusedDriving" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.heartbeat_daily set src_sys_cd="CMT_PL_FDR" where program_id="FocusedDriving" and src_sys_cd='@';

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC update dhf_iot_cmt_raw_prod.trip_summary_daily set src_sys_cd="CMT_PL_SRM" where program_id="SmartRideMobile" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.badge_daily set src_sys_cd="CMT_PL_SRM" where program_id="SmartRideMobile" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.fraud_daily set src_sys_cd="CMT_PL_SRM" where program_id="SmartRideMobile" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.driver_profile_daily set src_sys_cd="CMT_PL_SRM" where program_id="SmartRideMobile" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.trip_labels_daily set src_sys_cd="CMT_PL_SRM" where program_id="SmartRideMobile" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.driver_summary_daily set src_sys_cd="CMT_PL_SRM" where program_id="SmartRideMobile" and src_sys_cd='@';
-- MAGIC -- update dhf_iot_cmt_raw_prod.user_permissions_daily set src_sys_cd="CMT_PL_SRM" where program_id="SmartRideMobile" and src_sys_cd='@';
-- MAGIC update dhf_iot_cmt_raw_prod.heartbeat_daily set src_sys_cd="CMT_PL_SRM" where program_id="SmartRideMobile" and src_sys_cd='@';

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- select distinct load_date from dhf_iot_cmtpl_raw_prod.trip_detail_realtime
-- MAGIC select count(*) from dhf_iot_cmt_raw_prod.trip_detail_realtime where src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') --where load_date='2022-07-17'
-- MAGIC -- select count(*) from dhf_iot_cmt_raw_prod.trip_detail_realtime where load_dt='2022-07-17' --3,171,841,252   3,171,692,335

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("set spark.sql.autoBroadcastJoinThreshold=-1")

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val s="dfg kv"
-- MAGIC s.tr

-- COMMAND ----------

select * from dhf_iot_zubie_raw_prod.trippoints order by  load_dt desc

-- COMMAND ----------

SELECT RIGHT('0' + RTRIM(MONTH('12-31-2012')), 2); 

-- COMMAND ----------

select DISTINCT SRC_SYS_CD from dhf_iot_harmonized_prod.trip_summary

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select distinct RIGHT('0' + RTRIM(hour(load_hour)), 2),cast(hour(load_hour) as string),LPAD(TRIM(CAST(hour(load_hour) AS VARCHAR(2))),2,'0') from dhf_iot_fmc_raw_prod.tripsummary where load_date=current_date()-3

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select distinct load_dt from dhf_iot_harmonized_prod.trip_summary where src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') WHERE load_dt<'2021-06-11' order by load_dt  --187,654,987,894 1084467835554   1080116291470  1107963212229 1237110225895

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select distinct load_dt from dhf_iot_cmt_raw_prod.waypoints_realtime WHERE load_dt<'2021-06-11' order by load_dt

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
-- MAGIC val trip_src=spark.sql("select a.* from (select distinct driveid,accountid,load_dt,src_sys_cd from dhf_iot_cmt_raw_prod.trip_detail_realtime where load_dt <'2021-06-12')a inner join (select distinct drive_id,load_dt from dhf_iot_cmt_raw_prod.trip_detail_realtime where src_sys_cd is null and load_dt <'2021-06-12') on a.driveid=b.drive_id and a.load_dt=b.load_dt")
-- MAGIC val trip2=trip_src.join(program_enrollmentDF,trip_src("account_id") ===  program_enrollmentDF("DATA_CLCTN_ID"),"left")
-- MAGIC trip2.createOrReplaceTempView("tab")
-- MAGIC spark.sql("merge into dhf_iot_cmt_raw_prod.trip_detail_realtime a using tab b on a.drive_id=b.driveid and a.load_dt=b.load_dt and a.load_dt<'2021-06-12' and a.src_sys_cd is null when matched then update set a.src_sys_cd=b.src_sys_cd_pe ")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select distinct drive_id,load_dt from dhf_iot_cmt_raw_prod.trip_detail_realtime where src_sys_cd is null

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("set spark.sql.shuffle.partitions=auto")
-- MAGIC days_start_count=0
-- MAGIC days_end_count=10
-- MAGIC cmt_pl_trip_detail_realtime_missing=spark.sql(f"""select  a.* from 
-- MAGIC (select cast(monotonically_increasing_id()+65000+ coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_prod.trip_point),0) as BIGINT) as TRIP_POINT_ID,
-- MAGIC drive_id as TRIP_SMRY_KEY, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, time as TM_EPCH,
-- MAGIC timestamp_millis(time) as UTC_TS,cast(mm_lat as decimal(18,10)) as LAT_NB,cast(mm_lon as decimal(18,10)) as LNGTD_NB,
-- MAGIC cast(mm_dist_km as decimal(35,15)) as DISTNC_TRVL_QTY,
-- MAGIC cast(gps_speed as decimal(35,15)) as GPS_PNT_SPD_QTY,cast(gps_heading as decimal(15,6)) as HEADNG_DEG_QTY,gps_valid as GPS_VALID_STTS_SRC_IN,
-- MAGIC case when gps_valid = 1 THEN 'Y' when gps_valid=0 THEN 'N' When gps_valid IS NULL THEN " " else '@' end as GPS_VALID_STTS_DERV_IN,
-- MAGIC cast(accel_lon_smoothed as decimal(18,10)) as LNGTDNL_ACCLRTN_RT,cast(accel_lat_smoothed as decimal(18,10)) as LATRL_ACCLRTN_RT,accel_valid as ACLRTN_VALID_STTS_SRC_IN,
-- MAGIC case when accel_valid=1 THEN 'Y' when accel_valid=0 THEN 'N' when accel_valid IS NULL THEN " " else '@' end as ACLRTN_VALID_STTS_DERV_IN,cast(roll_rate as decimal(18,10)) as ROLL_RT,
-- MAGIC cast(pitch_rate as decimal(18,10)) as PITCH_RT,cast(yaw_rate as decimal(18,10)) as YAW_RT,
-- MAGIC distraction as DISTRCTN_STTS_SRC_IN,case when distraction=1 THEN 'Y' when distraction=0 THEN 'N' When distraction IS NULL THEN " " Else '@' end as DISTRCTN_STTS_DERV_IN,
-- MAGIC mm_state as MM_ST_CD,coalesce(src_sys_cd ,"NOKEY")as SRC_SYS_CD,coalesce(cast(LOAD_DT as DATE), to_date("9999-12-31")) as LOAD_DT,coalesce(LOAD_HR_TS ,to_timestamp("9999-12-31")) as LOAD_HR_TS 
-- MAGIC  from dhf_iot_cmt_raw_prod.trip_detail_realtime where src_sys_cd in("CMT_PL_SRM","CMT_PL_SRMCM","CMT_PL_FDR") and load_dt>=current_date-{days_end_count} and LOAD_DT<=current_date-{days_start_count}) a left anti join
-- MAGIC (select  * from dhf_iot_harmonized_prod.trip_point where load_dt>=current_date-{days_end_count} and LOAD_DT<=current_date-{days_start_count} and src_sys_cd like '%CMT_PL%') b 
-- MAGIC on   a.TRIP_SMRY_KEY=b.TRIP_SMRY_KEY  AND a.UTC_TS=b.UTC_TS AND a.SRC_SYS_CD = b.SRC_SYS_CD where TRIP_SMRY_KEY is not null""")
-- MAGIC cmt_pl_trip_detail_realtime_missing.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_point_chlg")

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

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC import org.apache.spark.sql.functions._
-- MAGIC val df0=spark.range(1 ,20).toDF("key_s")
-- MAGIC val df=spark.sql("select * from dhf_iot_rivian_raw_prod.trips_odometer ").withColumn("rand",floor(rand()*199)).withColumn("salt_key",concat(col("vin"),lit("_"),col("rand")))
-- MAGIC val df2=spark.sql("select * from dhf_iot_rivian_raw_prod.trips_summary ").crossJoin(df0).withColumn("ran",concat(col("vin"),lit("_"),col("key_s")))
-- MAGIC val df3=df.as("df").join(df2.as("df2"),df("salt_key")===df2("ran"),"left").selectExpr("df.*").drop("rand","salt_key")
-- MAGIC display(df0)
-- MAGIC

-- COMMAND ----------

select explode(array(0,2 19))

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select distinct min from (select  TRIP_SMRY_KEY,min(LOAD_DT) as min from dhf_iot_harmonized_prod.trip_summary_daily group by TRIP_SMRY_KEY having count(*)>1)

-- COMMAND ----------

-- MAGIC %py
-- MAGIC df=spark.sql("""select a.* from (select distinct cast(monotonically_increasing_id() +1+ 2 as BIGINT) as BADGE_DAILY_ID, short_user_id as DRIVER_KEY, email as EMAIL_ID,badge_name AS BDG_NM,badge_progress AS BDG_PRGRS_NB,cast(date_awarded as TIMESTAMP) AS BDG_ASGN_TS, user_state as PLCY_ST_CD,program_id as PRGRM_CD,src_sys_cd as SRC_SYS_CD,coalesce(load_date,to_date("9999-12-31")) as LOAD_DT,current_timestamp() as LOAD_HR_TS, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS from dhf_iot_cmt_raw_prod.badge_daily) a left anti join dhf_iot_harmonized_prod.badge_daily b on a.EMAIL_ID=b.EMAIL_ID and a.DRIVER_KEY=b.DRIVER_KEY and a.BDG_NM=b.BDG_NM and a.BDG_PRGRS_NB=b.BDG_PRGRS_NB""")
-- MAGIC df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.badge_daily")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dhf_iot_cmt_raw_prod.trip_summary_daily a left anti join dhf_iot_harmonized_prod.trip_summary_daily b on a.driveid=b.TRIP_SMRY_KEY

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dhf_iot_harmonized_prod.trip_summary_daily where SRC_SYS_CD like '%CMT_PL%'   --TRIP_SMRY_KEY='036D79C9-591F-4720-B2E8-2536E7FCBD47'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) from dhf_iot_cmt_raw_prod.trip_detail_realtime where load_dt>='2022-07-01' and load_dt <'2022-10-01'    --1,524,320,943,809  317,024,260,138

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from dhf_iot_harmonized_prod.program_enrollment where EVNT_TP_CD like '%scored%' and SCR_QTY >999

-- COMMAND ----------

-- MAGIC %py
-- MAGIC a=[1,2,2,3,4,7,7]
-- MAGIC b=[]
-- MAGIC dup=[]
-- MAGIC co={}
-- MAGIC
-- MAGIC for item in a:
-- MAGIC   if item in b:
-- MAGIC     dup.append(item)
-- MAGIC     co=co[item]+1
-- MAGIC   else:
-- MAGIC     b.append(item)
-- MAGIC print(list(set(b)-set(dup)))
-- MAGIC c=[i for i in b if i not in dup]
-- MAGIC print(c)

-- COMMAND ----------

-- MAGIC
-- MAGIC %py
-- MAGIC from collections import Counter
-- MAGIC m='deepthi'
-- MAGIC dup=[]
-- MAGIC counter2=0
-- MAGIC for char in m:
-- MAGIC   if char =='e':
-- MAGIC     counter2+=1
-- MAGIC   # else:
-- MAGIC   #   dup.append(dup)
-- MAGIC print(Counter(m))
-- MAGIC c={char:count for char,count in Counter(m).items() if count>1}
-- MAGIC print(c)
-- MAGIC   

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*),MODL_NM,MODL_VRSN_TT,MODL_ACRNYM_TT,ANLYTC_TP_CD
-- MAGIC ,TRIP_INSTC_ID,TRIP_SMRY_KEY,DATA_CLCTN_ID,DRIVR_KEY,POLICY_KEY
-- MAGIC ,TRIP_CT,EVNT_SRC,DRVNG_DISTNC_QTY,PHN_MTN_CT,HNDHLD_CT,TAP_CT
-- MAGIC ,PHN_HNDLNG_CT,TRNSCTN_TP_CD,MDL_OTPT_TP_CD,MDL_OTPT_SCR_QTY
-- MAGIC ,MDL_OTPT_SCR_TS,WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_TP_CD
-- MAGIC ,WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY,WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS
-- MAGIC ,HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_TP_CD,HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY
-- MAGIC ,HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS,PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_CD
-- MAGIC ,PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY,PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS
-- MAGIC ,DRVNG_DISTNC_MPH_QTY,TRNSPRT_MODE_CD,TRIP_END_TS,TIME_ZONE_OFFST_NB,USER_LBL_DT,OPRTN_SCR_IN,ADJST_DISTNC_KM_QTY
-- MAGIC ,AVG_ANNL_ADJ_KM,EXPSR_YR,MOVNG_SEC_QTY,LAST_TRIP_DT,LOG_EXPSR_YR,SCR_CALC_DT,SCR_CALC_TS,SCR_QTY,RQ5_QTY,SCR_DAYS_CT
-- MAGIC ,TRNSCTN_ID,prgrm_term_beg_dt_used,prgrm_term_end_dt_used,ENTRP_CUST_NB,LAST_RQST_DT,PLCY_ST_CD,PRGRM_TERM_BEG_DT
-- MAGIC ,PRGRM_TERM_END_DT,PRGRM_STTS_DSC,DATA_CLCTN_STTS_DSC,DEVC_STTS_DT,PRGRM_TP_CD,PGRM_CD,SRC_SYS_CD
-- MAGIC ,SBJT_ID,SBJCT_TP_CD,VNDR_CD,DEVC_ID,DEVC_STTS_CD,INSTL_GRC_END_DT,LOG_ANNL_ADJST_MILES_QTY,RQ5A_QTY,PCTG_TRIP_MSSNG
-- MAGIC ,PRGRM_SCR_MODL_CD
-- MAGIC FROM dhf_iot_harmonized_prod.trip_driver_score
-- MAGIC group by MODL_NM,MODL_VRSN_TT,MODL_ACRNYM_TT,ANLYTC_TP_CD
-- MAGIC ,TRIP_INSTC_ID,TRIP_SMRY_KEY,DATA_CLCTN_ID,DRIVR_KEY,POLICY_KEY
-- MAGIC ,TRIP_CT,EVNT_SRC,DRVNG_DISTNC_QTY,PHN_MTN_CT,HNDHLD_CT,TAP_CT
-- MAGIC ,PHN_HNDLNG_CT,TRNSCTN_TP_CD,MDL_OTPT_TP_CD,MDL_OTPT_SCR_QTY
-- MAGIC ,MDL_OTPT_SCR_TS,WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_TP_CD
-- MAGIC ,WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY,WGHT_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS
-- MAGIC ,HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_TP_CD,HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY
-- MAGIC ,HNDHLD_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS,PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_CD
-- MAGIC ,PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_QTY,PHN_HNDLNG_DISTRCTN_CT_PER_HUND_KM_MODL_OTPT_SCR_TS
-- MAGIC ,DRVNG_DISTNC_MPH_QTY,TRNSPRT_MODE_CD,TRIP_END_TS,TIME_ZONE_OFFST_NB,USER_LBL_DT,OPRTN_SCR_IN,ADJST_DISTNC_KM_QTY
-- MAGIC ,AVG_ANNL_ADJ_KM,EXPSR_YR,MOVNG_SEC_QTY,LAST_TRIP_DT,LOG_EXPSR_YR,SCR_CALC_DT,SCR_CALC_TS,SCR_QTY,RQ5_QTY,SCR_DAYS_CT
-- MAGIC ,TRNSCTN_ID,prgrm_term_beg_dt_used,prgrm_term_end_dt_used,ENTRP_CUST_NB,LAST_RQST_DT,PLCY_ST_CD,PRGRM_TERM_BEG_DT
-- MAGIC ,PRGRM_TERM_END_DT,PRGRM_STTS_DSC,DATA_CLCTN_STTS_DSC,DEVC_STTS_DT,PRGRM_TP_CD,PGRM_CD,SRC_SYS_CD
-- MAGIC ,SBJT_ID,SBJCT_TP_CD,VNDR_CD,DEVC_ID,DEVC_STTS_CD,INSTL_GRC_END_DT,LOG_ANNL_ADJST_MILES_QTY,RQ5A_QTY,PCTG_TRIP_MSSNG
-- MAGIC ,PRGRM_SCR_MODL_CD
-- MAGIC having count(*) > 1

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*) from dhf_iot_harmonized_prod.trip_point_chlg   331,715,168,498

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC optimize dhf_iot_cmt_raw_prod.trip_summary_realtime where load_dt>=add_months(load_dt,-3 ) and load_dt<current_date()-1