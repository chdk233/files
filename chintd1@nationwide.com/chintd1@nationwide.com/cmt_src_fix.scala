// Databricks notebook source
// MAGIC %sql
// MAGIC show tables in dhf_iot_cmt_raw_prod

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct src_sys_cd from dhf_iot_cmt_raw_prod.waypoints_realtime

// COMMAND ----------

// MAGIC %sql
// MAGIC update	dhf_iot_cmt_raw_prod.badge_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_cmt_raw_prod.driver_profile_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_cmt_raw_prod.driver_summary_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_cmt_raw_prod.events_realtime	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_cmt_raw_prod.fraud_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_cmt_raw_prod.heartbeat_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_cmt_raw_prod.outbound_mobile_score_elements	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_cmt_raw_prod.trip_detail_realtime	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_cmt_raw_prod.trip_labels_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_cmt_raw_prod.trip_summary_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_cmt_raw_prod.trip_summary_realtime	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_cmt_raw_prod.user_permissions_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_cmt_raw_prod.waypoints_realtime	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC update	dhf_iot_harmonized_prod.badge_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_harmonized_prod.driver_profile_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_harmonized_prod.driver_summary_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_harmonized_prod.fraud_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_harmonized_prod.heartbeat_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_harmonized_prod.trip_driver_score	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_harmonized_prod.trip_labels_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_harmonized_prod.trip_summary_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC update	dhf_iot_harmonized_prod.user_permissions_daily	set src_sys_cd='CMT_PL' WHERE src_sys_cd ='@' or src_sys_cd is null	;
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct * from dhf_iot_cmt_raw_prod.trip_interval_rt
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select a.* from (select distinct email as EMAIL_ID,short_user_id as DRIVER_KEY, deviceid as DEVC_KEY, anomaly_type as ANMLY_TP_CD, cast(anomaly_device_time as TIMESTAMP) AS ANMLY_DEVC_TS, cast(anomaly_server_time as TIMESTAMP) as ANMLY_SRVR_TS, coalesce(CAST(max(load_date) AS timestamp),to_timestamp("9999-12-31")) as LOAD_HR_TS, SRC_SYS_CD from dhf_iot_cmt_raw_prod.fraud_daily where src_sys_cd like '%CMT_PL%' group by email, short_user_id, deviceid, anomaly_type,anomaly_device_time,anomaly_server_time,src_sys_cd) a left anti join
// MAGIC 	(select DRIVER_KEY,EMAIL_ID,DEVC_KEY,ANMLY_TP_CD,ANMLY_DEVC_TS,ANMLY_SRVR_TS,SRC_SYS_CD from dhf_iot_harmonized_prod.fraud_daily where  src_sys_cd like '%CMT_PL%') b 
// MAGIC 	on   a.EMAIL_ID=b.EMAIL_ID  AND a.DRIVER_KEY=b.DRIVER_KEY  AND a.DEVC_KEY=b.DEVC_KEY  AND a.ANMLY_TP_CD=b.ANMLY_TP_CD  AND a.ANMLY_DEVC_TS=b.ANMLY_DEVC_TS  AND a.ANMLY_SRVR_TS=b.ANMLY_SRVR_TS  AND a.SRC_SYS_CD = b.SRC_SYS_CD where DRIVER_KEY is not null

// COMMAND ----------

// MAGIC %sql
// MAGIC (select distinct email as EMAIL_ID,short_user_id as DRIVER_KEY, deviceid as DEVC_KEY, anomaly_type as ANMLY_TP_CD, cast(anomaly_device_time as TIMESTAMP) AS ANMLY_DEVC_TS, cast(anomaly_server_time as TIMESTAMP) as ANMLY_SRVR_TS from dhf_iot_cmt_raw_prod.fraud_daily where src_sys_cd like '%CMT_PL%') minus (select EMAIL_ID,DRIVER_KEY,DEVC_KEY,ANMLY_TP_CD,ANMLY_DEVC_TS,ANMLY_SRVR_TS from dhf_iot_harmonized_prod.fraud_daily where  src_sys_cd like '%CMT_PL%')

// COMMAND ----------

// MAGIC %sql
// MAGIC select CUST_NM,DRIVER_KEY,SRC_CUST_KEY,ACCT_ID,USR_NM,EMAIL_ID,MOBL_NB,GROUP_ID,UNQ_GROUP_NM,PLCY_NB,FLT_ID,FLT_NM,TEAM_ID,TEAM_NM,LAST_SHRT_VEHCL_ID,VIN_NB,VHCL_MAKE_NM,VHCL_MODEL_NM,LST_TAG_MAC_ADDR_NM,DEVC_KEY,DEVC_MODL_NM,DEVC_BRND_NM,DEVC_MFRR_NM,OS_NM,OS_VRSN_NM,APP_VRSN_NM,LAST_DRV_TS,DAYS_SINCE_LAST_TRIP_CT,LAST_ACTVTY_TS,RGSTRTN_TS,USER_LOGGED_SRC_IN,USER_LOGGED_DERV_IN,LAST_AUTHRZN_TS,PTNTL_ISSUES_TT,USR_ACCT_EXP_DT,MIN(LOAD_DT) AS MN from dhf_iot_harmonized_prod.user_permissions_daily where SRC_SYS_CD LIKE '%CMT_PL%'  group by CUST_NM,DRIVER_KEY,SRC_CUST_KEY,ACCT_ID,USR_NM,EMAIL_ID,MOBL_NB,GROUP_ID,UNQ_GROUP_NM,PLCY_NB,FLT_ID,FLT_NM,TEAM_ID,TEAM_NM,LAST_SHRT_VEHCL_ID,VIN_NB,VHCL_MAKE_NM,VHCL_MODEL_NM,LST_TAG_MAC_ADDR_NM,DEVC_KEY,DEVC_MODL_NM,DEVC_BRND_NM,DEVC_MFRR_NM,OS_NM,OS_VRSN_NM,APP_VRSN_NM,LAST_DRV_TS,DAYS_SINCE_LAST_TRIP_CT,LAST_ACTVTY_TS,RGSTRTN_TS,USER_LOGGED_SRC_IN,USER_LOGGED_DERV_IN,LAST_AUTHRZN_TS,PTNTL_ISSUES_TT,USR_ACCT_EXP_DT having count(*)>1 ORDER BY MN--where trip_smry_key in ('E898102B-2D0E-470B-A7FD-890910257475')
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.user_permissions_daily where DRIVER_KEy=601894609 and usr_nm='madelyn367103'

// COMMAND ----------

// MAGIC %sql
// MAGIC select EMAIL_ID,FLEET_ID,GROUP_ID,PLCY_NB,DRIVER_KEY,TAG_USER_SRC_IN,TAG_USER_DERV_IN,TEAM_ID,USER_NM,AVG_SCR_QTY,AVG_ACLRTN_SCR_QTY,AVG_BRKE_SCR_QTY,AVG_TURN_SCR_QTY,AVG_SPD_SCR_QTY,AVG_PHN_MTN_SCR_QTY,TOT_DISTNC_KM_QTY,TOT_TRIP_CNT,SCR_INTRV_DRTN_QTY,USER_DRIV_LBL_HNRD_SRC_IN,USER_DRIV_LBL_HNRD_DERV_IN,SCR_TS,FAM_GROUP_CD,min(LOAD_DT)AS MN from dhf_iot_harmonized_prod.driver_summary_daily group by EMAIL_ID,FLEET_ID,GROUP_ID,PLCY_NB,DRIVER_KEY,TAG_USER_SRC_IN,TAG_USER_DERV_IN,TEAM_ID,USER_NM,AVG_SCR_QTY,AVG_ACLRTN_SCR_QTY,AVG_BRKE_SCR_QTY,AVG_TURN_SCR_QTY,AVG_SPD_SCR_QTY,AVG_PHN_MTN_SCR_QTY,TOT_DISTNC_KM_QTY,TOT_TRIP_CNT,SCR_INTRV_DRTN_QTY,USER_DRIV_LBL_HNRD_SRC_IN,USER_DRIV_LBL_HNRD_DERV_IN,SCR_TS,FAM_GROUP_CD having count(*)>1 order by MN

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.driver_summary_daily where DRIVER_KEY=422497317 and EMAIL_ID='garyturner71@outlook.com' and TOT_DISTNC_KM_QTY=2239.560000

// COMMAND ----------

// MAGIC %sql
// MAGIC select EMAIL_ID,FLEET_ID,GROUP_ID,PLCY_NB,DRIVER_KEY,TAG_USER_SRC_IN,TAG_USER_DERV_IN,TEAM_ID,USER_NM,AVG_SCR_QTY,AVG_ACLRTN_SCR_QTY,AVG_BRKE_SCR_QTY,AVG_TURN_SCR_QTY,AVG_SPD_SCR_QTY,AVG_PHN_MTN_SCR_QTY,TOT_DISTNC_KM_QTY,TOT_TRIP_CNT,SCR_INTRV_DRTN_QTY,USER_DRIV_LBL_HNRD_SRC_IN,USER_DRIV_LBL_HNRD_DERV_IN,SCR_TS,FAM_GROUP_CD from dhf_iot_harmonized_prod.driver_summary_daily group by EMAIL_ID,FLEET_ID,GROUP_ID,PLCY_NB,DRIVER_KEY,TAG_USER_SRC_IN,TAG_USER_DERV_IN,TEAM_ID,USER_NM,AVG_SCR_QTY,AVG_ACLRTN_SCR_QTY,AVG_BRKE_SCR_QTY,AVG_TURN_SCR_QTY,AVG_SPD_SCR_QTY,AVG_PHN_MTN_SCR_QTY,TOT_DISTNC_KM_QTY,TOT_TRIP_CNT,SCR_INTRV_DRTN_QTY,USER_DRIV_LBL_HNRD_SRC_IN,USER_DRIV_LBL_HNRD_DERV_IN,SCR_TS,FAM_GROUP_CD having count(*)>1

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.trip_labels_daily where trip_smry_key in (select TRIP_SMRY_KEY from dhf_iot_harmonized_prod.trip_labels_daily group by TRIP_SMRY_KEY,USER_LBL_NM,USER_LBL_TS having count(*)>1) order by LOAD_DT
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC update dhf_iot_cmt_raw_prod.trip_labels_daily set load_date='2023-05-17' where driveid ='E898102B-2D0E-470B-A7FD-890910257475'

// COMMAND ----------

// MAGIC %sql
// MAGIC select (*) from dhf_iot_cmt_raw_prod.trip_labels_daily where driveid not in (select TRIP_SMRY_KEY from dhf_iot_harmonized_prod.trip_labels_daily  )-- order by LOAD_DaTe

// COMMAND ----------

// MAGIC %sql
// MAGIC delete from dhf_iot_harmonized_prod.driver_summary_daily where SRC_SYS_CD='CMT_PL' and PRGRM_CD in ('SmartRideMobile','SmartRideMobileContinuous','FocusedDriving')

// COMMAND ----------

// MAGIC %sql
// MAGIC -- show tables in dhf_iot_harmonized_prod
// MAGIC DELETE from dhf_iot_harmonized_prod.heartbeat_daily   where DRIVER_KEY=75868911 and CURR_TS is null and SRC_SYS_CD='CMT_PL'

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from (select distinct email as EMAIL_ID,short_user_id as DRIVER_KEY, cast(`current_date` as TIMESTAMP) AS CURR_TS, coalesce(CAST(max(load_date) AS timestamp),to_timestamp("9999-12-31")) as LOAD_HR_TS, SRC_SYS_CD from dhf_iot_cmt_raw_prod.heartbeat_daily where src_sys_cd like '%CMT_PL%' group by email,short_user_id,`current_date`,src_sys_cd) a left anti join
// MAGIC 	(select DRIVER_KEY,CURR_TS,SRC_SYS_CD from dhf_iot_harmonized_prod.heartbeat_daily where src_sys_cd like '%CMT_PL%') b 
// MAGIC 	on   a.DRIVER_KEY=b.DRIVER_KEY  and a.CURR_TS=b.CURR_TS AND a.SRC_SYS_CD = b.SRC_SYS_CD

// COMMAND ----------

// MAGIC %sql
// MAGIC update dhf_iot_cmt_raw_prod.heartbeat_daily set src_sys_cd='CMT_PL_SRMCM' where short_user_id=75868911 and email='kimdavidson4'

// COMMAND ----------

// MAGIC %sql
// MAGIC -- show tables in dhf_iot_harmonized_prod
// MAGIC select DRIVER_KEY,CURR_TS from dhf_iot_harmonized_prod.heartbeat_daily   group by DRIVER_KEY,CURR_TS having count(*)>1

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table dhf_iot_curated_prod.device_status_fin