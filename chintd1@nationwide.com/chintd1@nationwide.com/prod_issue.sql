-- Databricks notebook source
-- MAGIC %sql
-- MAGIC insert into dhf_iot_harmonized_prod.event_code values ('1','','','CMT_CL_VF','');
-- MAGIC insert into dhf_iot_harmonized_prod.event_code values ('2','harsh braking','','CMT_CL_VF','harsh braking');
-- MAGIC insert into dhf_iot_harmonized_prod.event_code values ('3','harsh cornering','','CMT_CL_VF','harsh cornering');
-- MAGIC insert into dhf_iot_harmonized_prod.event_code values ('4','harsh acceleration','','CMT_CL_VF','harsh acceleration');
-- MAGIC insert into dhf_iot_harmonized_prod.event_code values ('5','speeding','','CMT_CL_VF','speeding');
-- MAGIC insert into dhf_iot_harmonized_prod.event_code values ('6','phone motion','','CMT_CL_VF','phone motion');
-- MAGIC insert into dhf_iot_harmonized_prod.event_code values ('7','phone call','','CMT_CL_VF','phone call');
-- MAGIC insert into dhf_iot_harmonized_prod.event_code values ('8','tapping','','CMT_CL_VF','tapping');
-- MAGIC insert into dhf_iot_harmonized_prod.event_code values ('9','Idle Time','','CMT_CL_VF','Idle Time');

-- COMMAND ----------

desc select * from dhf_logs_prod.pipeline_status_metrics

-- COMMAND ----------

select distinct tp.ENRLD_VIN_NB, TRIP_SMRY_KEY, DEVC_KEY, UTC_TS as PSTN_TS, TIME_ZONE_OFFST_NB as TIME_ZONE_OFFST_NUM, SPD_RT as SPD_KPH_RT, ENGIN_RPM_RT, LAT_NB, LNGTD_NB, SRC_SYS_CD, LOAD_DT, LOAD_HR_TS, trim(coalesce(pe.PLCY_ST_CD, ods.PLCY_RT_ST_CD)) as PLCY_RT_ST_CD from global_temp.TRIP_POINT tp left join ( select trim(ENRLD_VIN_NB) as ENRLD_VIN_NB, PLCY_RT_ST_CD, ACTV_STRT_DT, ACTV_END_DT from dhf_iot_harmonized_prod.ods_table where VHCL_STTS_CD = 'E' and LOAD_DT = ( select max(LOAD_DT) from dhf_iot_harmonized_prod.ods_table ) ) ods on ods.ENRLD_VIN_NB = tp.ENRLD_VIN_NB and ods.ACTV_STRT_DT <= tp.UTC_TS and tp.UTC_TS <= ods.ACTV_END_DT and tp.SRC_SYS_CD like "%SR%" left join ( select VIN_NB, PLCY_ST_CD, PRGRM_TERM_BEG_DT, PRGRM_TERM_END_DT from dhf_iot_harmonized_prod.program_enrollment where DATA_CLCTN_STTS = 'Active' and (PRGRM_STTS_CD in ('Active', 'Cancelled')) ) pe on pe.VIN_NB = tp.ENRLD_VIN_NB and pe.PRGRM_TERM_BEG_DT <= tp.UTC_TS and tp.UTC_TS <= pe.PRGRM_TERM_END_DT and tp.SRC_SYS_CD like "%SM%" where SRC_SYS_CD not in ('AZUGA_CL_PP', 'ZUBIE_CL_PP', 'CMT_CL_VF') and tp.LOAD_DT >= date('2023-1-30') union ALL select distinct ts.VEH_KEY as ENRLD_VIN_NB, tp.TRIP_SMRY_KEY, ts.DEVC_KEY, tp.UTC_TS as PSTN_TS, ts.TIME_ZONE_OFFST_NB as TIME_ZONE_OFFST_NUM, tp.SPD_RT as SPD_KPH_RT, tp.ENGIN_RPM_RT, tp.LAT_NB, tp.LNGTD_NB, tp.SRC_SYS_CD, tp.LOAD_DT, tp.LOAD_HR_TS, trim(ods.PLCY_RT_ST_CD) as PLCY_RT_ST_CD from global_temp.TRIP_POINT tp inner join dhf_iot_harmonized_prod.TRIP_SUMMARY ts on ts.TRIP_SMRY_KEY = tp.TRIP_SMRY_KEY left join ( select trim(ENRLD_VIN_NB) as ENRLD_VIN_NB, PLCY_RT_ST_CD, ACTV_STRT_DT, ACTV_END_DT from dhf_iot_harmonized_prod.ods_table where VHCL_STTS_CD = 'E' and LOAD_DT = ( select max(LOAD_DT) from dhf_iot_harmonized_prod.ods_table ) ) ods on ods.ENRLD_VIN_NB = ts.VEH_KEY and ods.ACTV_STRT_DT <= tp.UTC_TS and tp.UTC_TS <= ods.ACTV_END_DT where tp.SRC_SYS_CD = 'IMS_SR_4X' and tp.LOAD_DT >= date('2023-1-30') order by PSTN_TS

-- COMMAND ----------

select distinct ts.VEH_KEY as ENRLD_VIN_NB, chlg.TRIP_SMRY_KEY, chlg.DEVC_KEY, chlg.PSTN_TS, ts.TIME_ZONE_OFFST_NB as TIME_ZONE_OFFST_NUM, chlg.SPD_KPH_RT, chlg.ENGIN_RPM_RT, chlg.LAT_NB, chlg.LNGTD_NB, chlg.SRC_SYS_CD, chlg.LOAD_DT, chlg.LOAD_HR_TS, trim(ods.PLCY_RT_ST_CD) as PLCY_RT_ST_CD from (select distinct * from dhf_iot_harmonized_prod.trip_detail_seconds_chlg where SRC_SYS_CD = 'IMS_SR_4X' and LOAD_DT >= date('2023-1-29') and ENRLD_VIN_NB is null ) chlg inner join 
(select       distinct *     from       dhf_iot_harmonized_prod.trip_summary     where       SRC_SYS_CD = 'IMS_SR_4X'       and LOAD_DT >= date('2023-1-29') ) ts 
on chlg.TRIP_SMRY_KEY = ts.TRIP_SMRY_KEY 
left join 
(select       trim(ENRLD_VIN_NB) as ENRLD_VIN_NB,       PLCY_RT_ST_CD,       ACTV_STRT_DT,       ACTV_END_DT     from       dhf_iot_harmonized_prod.ods_table     where       VHCL_STTS_CD = 'E'       and LOAD_DT = (select           max(LOAD_DT)         from dhf_iot_harmonized_prod.ods_table)) ods on ods.ENRLD_VIN_NB = ts.VEH_KEY   and ods.ACTV_STRT_DT <= chlg.PSTN_TS   and chlg.PSTN_TS <= ods.ACTV_END_DT

-- COMMAND ----------

describe history dhf_logs_prod.harmonize_metrics

-- COMMAND ----------

select distinct ts.VEH_KEY as ENRLD_VIN_NB, chlg.TRIP_SMRY_KEY, chlg.DEVC_KEY, chlg.PSTN_TS, ts.TIME_ZONE_OFFST_NB as TIME_ZONE_OFFST_NUM, chlg.SPD_KPH_RT, chlg.ENGIN_RPM_RT, chlg.LAT_NB, chlg.LNGTD_NB, chlg.SRC_SYS_CD, chlg.LOAD_DT, chlg.LOAD_HR_TS, trim(ods.PLCY_RT_ST_CD) as PLCY_RT_ST_CD from 
(select       distinct *     from       dhf_iot_harmonized_prod.trip_detail_seconds_chlg     where       SRC_SYS_CD = 'IMS_SR_4X'       and LOAD_DT >= date('2023-1-29')       and ENRLD_VIN_NB is null ) chlg 
inner join 
(select       distinct *     from       dhf_iot_harmonized_prod.trip_summary     where       SRC_SYS_CD = 'IMS_SR_4X'       and LOAD_DT >= date('2023-1-29') ) ts 
on chlg.TRIP_SMRY_KEY = ts.TRIP_SMRY_KEY 
left join 
(select       trim(ENRLD_VIN_NB) as ENRLD_VIN_NB,       PLCY_RT_ST_CD,       ACTV_STRT_DT,       ACTV_END_DT     from       dhf_iot_harmonized_prod.ods_table     where       VHCL_STTS_CD = 'E'       and LOAD_DT = (select           max(LOAD_DT)         from dhf_iot_harmonized_prod.ods_table)) ods on ods.ENRLD_VIN_NB = ts.VEH_KEY   and ods.ACTV_STRT_DT <= chlg.PSTN_TS   and chlg.PSTN_TS <= ods.ACTV_END_DT

-- COMMAND ----------

-- select * from dhf_iot_harmonized_prod.trip_point where src_sys_cd='IMS_SR_4X' AND LOAD_DT="2023-02-10" ORDER BY ETL_ROW_EFF_DTS DESC
select current_timestamp

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val secretScope = dbutils.secrets.list(scope = "pcdm-iot") 
-- MAGIC
-- MAGIC display(secretScope) 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC drop table dhf_iot_curated_prod.trip_detail_hstry;
-- MAGIC -- drop table dhf_iot_harmonized_prod.td_reprocess_trips;
-- MAGIC -- drop table dhf_iot_harmonized_prod.tds_trips;
-- MAGIC -- drop table dhf_iot_harmonized_prod.trip_detail_seconds_te;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC show tables in dhf_iot_harmonized_prod  td_missing_trips td_reprocess_trips  td_reprocess_trips_2 tds_trips trip_detail_seconds_te

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select distinct load_date from  dhf_iot_cmtpl_raw_prod.trip_summary_realtime-- where src_sys_cd ='@'