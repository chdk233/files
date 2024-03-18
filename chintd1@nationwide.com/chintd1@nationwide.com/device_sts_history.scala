// Databricks notebook source
//device_status new 

val device_sts_df=spark.sql("""SELECT 
cast(monotonically_increasing_id() +1 + coalesce((select max(DEVICE_STATUS_ID) from dhf_iot_curated_prod.DEVICE_STATUS),0) as BIGINT)   as DEVICE_STATUS_ID,
ENRLD_VIN_NB,
coalesce(DEVC_KEY, 'NOKEY') as DEVC_KEY,
prog_inst_id as PRGRM_INSTC_ID,
current_timestamp() as ETL_ROW_EFF_DTS,
current_timestamp() as ETL_LAST_UPDT_DTS,
data_clctn_id,
STTS_EFCTV_TS,
STTS_EXPRTN_TS,
LAST_DEVC_ACTVTY_TS,
SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
CNCTD_STTS_FLAG,
DEVC_ACTV_NUM,
CASE
	WHEN datediff(DEVC_ACTV_NUM, LAST_DEVC_ACTVTY_TS) > 8 then "1"
	ELSE "0"
END AS DEVC_UNAVLBL_FLAG
from
(SELECT 
ENRLD_VIN_NB,
DEVC_KEY,
prog_inst_id,
data_clctn_id,
STTS_EFCTV_TS,
CASE 
	when STTS_EXPRTN_TS = HIGHDATE_TS then cast('9999-01-01' as timestamp) --cast('1/1/9999' as TIMESTAMP)
	else cast(STTS_EXPRTN_TS as timestamp)
end as STTS_EXPRTN_TS,
LAST_DEVC_ACTVTY_TS,
SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
CNCTD_STTS_FLAG,
LEAD(LAST_DEVC_ACTVTY_TS, 1, current_timestamp()) OVER(PARTITION BY ENRLD_VIN_NB ORDER BY STTS_EFCTV_TS) as DEVC_ACTV_NUM,
/*CASE
	WHEN DEVC_ACTV_NUM - LAST_DEVC_ACTVTY_TS then '1'
	ELSE '0'
(CASE 
	WHEN LEAD(LAST_DEVC_ACTVTY_TS) OVER(PARTITION BY DE.ENRLD_VIN_NB ORDER BY DE.STTS_EFCTV_TS) - LAST_DEVC_ACTVTY_TS >  then '1'
	ELSE '0'
END) AS DEVC_UNAVLBL_FLAG,*/
HIGHDATE_TS
FROM
(SELECT 
DE.ENRLD_VIN_NB,
DE.DEVC_KEY,
DE.prog_inst_id,
DE.data_clctn_id,
DE.STTS_EFCTV_TS,
LEAD(DE.STTS_EFCTV_TS, 1, DE_HIGHDATE.HIGHDATE_TS) OVER(PARTITION BY DE.ENRLD_VIN_NB ORDER BY DE.STTS_EFCTV_TS) AS STTS_EXPRTN_TS,
DE.LAST_DEVC_ACTVTY_TS,
DE.SRC_SYS_CD,
DE.MD5_HASH,
DE.LOAD_DT,
DE.LOAD_HR_TS,
DE.CNCTD_STTS_FLAG,
DE.DEVC_UNAVLBL_FLAG,
DE_HIGHDATE.HIGHDATE_TS
FROM
((select * from 
((select distinct
veh_key as ENRLD_VIN_NB,
device_nbr as DEVC_KEY,
prog_inst_id,
data_clctn_id,
event_start_ts as STTS_EFCTV_TS,
event_end_ts as STTS_EXPRTN_TS,
src_sys_cd as SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
case
  when event_key = 'DISCONNECT_EVENT' then "0"
  else "1"
end as CNCTD_STTS_FLAG,
event_end_ts as LAST_DEVC_ACTVTY_TS,
case
when datediff(current_timestamp(),event_end_ts) > 8 then "1"
else "0"
end as DEVC_UNAVLBL_FLAG
from  
(select distinct
VEH_KEY as veh_key,
TRIP_SMRY_KEY as event_key,
TRIP_START_TS as event_start_ts,
TRIP_END_TS as event_end_ts,
SRC_SYS_CD as src_sys_cd,
md5(CONCAT(VEH_KEY, TRIP_SMRY_KEY)) as md5_hash,
LOAD_DT as load_dt,
LOAD_HR_TS as load_hr_ts
from dhf_iot_harmonized_prod.trip_summary where  load_dt <'2022-09-12' and src_sys_cd='TIMS_SR' ) td
left join
(select distinct
VIN_NB as vin_nbr,
coalesce(DEVC_ID, 'NOKEY') as device_nbr,
PRGRM_INSTC_ID as prog_inst_id,
DATA_CLCTN_ID as data_clctn_id,
cast(PRGRM_TERM_BEG_DT as TIMESTAMP) as active_start_dt,
cast(PRGRM_TERM_END_DT as TIMESTAMP) as active_end_dt
from dhf_iot_harmonized_prod.integrated_enrollment) ie
on td.veh_key = ie.vin_nbr)
union
(select distinct
veh_key as ENRLD_VIN_NB,
device_nbr as DEVC_KEY,
prog_inst_id,
data_clctn_id,
event_start_ts as STTS_EFCTV_TS,
event_end_ts as STTS_EXPRTN_TS,
src_sys_cd as SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
case
  when event_key = 'DISCONNECT_EVENT' then "0"
  else "1"
end as CNCTD_STTS_FLAG,
event_end_ts as LAST_DEVC_ACTVTY_TS,
case
when datediff(current_timestamp(),event_end_ts) > 8 then "1"
else "0"
end as DEVC_UNAVLBL_FLAG
from 
(select distinct
VEH_KEY as veh_key,
EVNT_TP_CD as event_key,
UTC_TS as event_start_ts,
UTC_TS as event_end_ts,
SRC_SYS_CD as src_sys_cd,
md5(CONCAT(VEH_KEY, EVNT_TP_CD, CAST(UTC_TS as STRING))) as MD5_HASH,
LOAD_DT as load_dt,
LOAD_HR_TS as load_hr_ts,
rownb
from (select *,row_number() over (partition by NON_TRIP_EVNT_KEY, EVNT_TP_CD, UTC_TS order by load_hr_ts desc ) as rownb from dhf_iot_harmonized_prod.non_trip_event  where load_dt <'2022-09-12' and src_sys_cd='TIMS_SR')) td
left join
(select distinct
VIN_NB as vin_nbr,
coalesce(DEVC_ID, 'NOKEY') as device_nbr,
PRGRM_INSTC_ID as prog_inst_id,
DATA_CLCTN_ID as data_clctn_id,
cast(PRGRM_TERM_BEG_DT as TIMESTAMP) as active_start_dt,
cast(PRGRM_TERM_END_DT as TIMESTAMP) as active_end_dt
from dhf_iot_harmonized_prod.integrated_enrollment) ie
on td.veh_key = ie.vin_nbr
where td.rownb=1
order by STTS_EFCTV_TS))
) as DE
--------
inner join
(
select ENRLD_VIN_NB, MAX(STTS_EXPRTN_TS) as HIGHDATE_TS from 
((select distinct
veh_key as ENRLD_VIN_NB,
event_start_ts as STTS_EFCTV_TS,
event_end_ts as STTS_EXPRTN_TS,
src_sys_cd as SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
case
  when event_key = 'DISCONNECT_EVENT' then "0"
  else "1"
end as CNCTD_STTS_FLAG,
event_end_ts as LAST_DEVC_ACTVTY_TS,
case
when datediff(current_timestamp(),event_end_ts) > 8 then "1"
else "0"
end as DEVC_UNAVLBL_FLAG
from  
(select distinct
VEH_KEY as veh_key,
TRIP_SMRY_KEY as event_key,
TRIP_START_TS as event_start_ts,
TRIP_END_TS as event_end_ts,
SRC_SYS_CD as src_sys_cd,
md5(CONCAT(VEH_KEY, TRIP_SMRY_KEY)) as md5_hash,
LOAD_DT as load_dt,
LOAD_HR_TS as load_hr_ts
from dhf_iot_harmonized_prod.trip_summary where  load_dt <'2022-09-12' and src_sys_cd='TIMS_SR') td
left join
(select distinct
VIN_NB as vin_nbr,
coalesce(DEVC_ID, 'NOKEY') as device_nbr,
PRGRM_INSTC_ID as prog_inst_id,
DATA_CLCTN_ID as data_clctn_id,
cast(PRGRM_TERM_BEG_DT as TIMESTAMP) as active_start_dt,
cast(PRGRM_TERM_END_DT as TIMESTAMP) as active_end_dt
from dhf_iot_harmonized_prod.integrated_enrollment) ie
on td.veh_key = ie.vin_nbr)
union
(select distinct
veh_key as ENRLD_VIN_NB,
event_start_ts as STTS_EFCTV_TS,
event_end_ts as STTS_EXPRTN_TS,
src_sys_cd as SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
case
  when event_key = 'DISCONNECT_EVENT' then "0"
  else "1"
end as CNCTD_STTS_FLAG,
event_end_ts as LAST_DEVC_ACTVTY_TS,
case
when datediff(current_timestamp(),event_end_ts) > 8 then "1"
else "0"
end as DEVC_UNAVLBL_FLAG
from 
(select distinct
VEH_KEY as veh_key,
EVNT_TP_CD as event_key,
UTC_TS as event_start_ts,
UTC_TS as event_end_ts,
SRC_SYS_CD as src_sys_cd,
md5(CONCAT(VEH_KEY, EVNT_TP_CD, CAST(UTC_TS as STRING))) as MD5_HASH,
LOAD_DT as load_dt,
LOAD_HR_TS as load_hr_ts,
rownb
from (select *,row_number() over (partition by NON_TRIP_EVNT_KEY, EVNT_TP_CD, UTC_TS order by load_hr_ts desc ) as rownb from dhf_iot_harmonized_prod.non_trip_event  where load_dt <'2022-09-12' and src_sys_cd='TIMS_SR')) td
left join
(select distinct
VIN_NB as vin_nbr,
coalesce(DEVC_ID, 'NOKEY') as device_nbr,
PRGRM_INSTC_ID as prog_inst_id,
DATA_CLCTN_ID as data_clctn_id,
cast(PRGRM_TERM_BEG_DT as TIMESTAMP) as active_start_dt,
cast(PRGRM_TERM_END_DT as TIMESTAMP) as active_end_dt
from dhf_iot_harmonized_prod.integrated_enrollment) ie
on td.veh_key = ie.vin_nbr
where td.rownb=1
order by STTS_EFCTV_TS))
group by ENRLD_VIN_NB
) as DE_HIGHDATE
on DE.ENRLD_VIN_NB = DE_HIGHDATE.ENRLD_VIN_NB))) DE""").drop("DEVC_ACTV_NUM")
// display(device_sts_df)
device_sts_df.write.format("delta").mode("append").saveAsTable("dhf_iot_curated_prod.device_st")

// COMMAND ----------

// MAGIC %sql
// MAGIC select ENRLD_VIN_NB,STTS_EXPRTN_TS ,count(*) from dhf_iot_curated_prod.device_st where STTS_EXPRTN_TS='9999-01-01' group by ENRLD_VIN_NB,STTS_EXPRTN_TS having count(*)>1

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT 
// MAGIC cast(monotonically_increasing_id() +1 + coalesce((select max(DEVICE_STATUS_ID) from dhf_iot_curated_prod.DEVICE_STATUS),0) as BIGINT)   as DEVICE_STATUS_ID,
// MAGIC ENRLD_VIN_NB,
// MAGIC coalesce(DEVC_KEY, 'NOKEY') as DEVC_KEY,
// MAGIC prog_inst_id as PRGRM_INSTC_ID,
// MAGIC current_timestamp() as ETL_ROW_EFF_DTS,
// MAGIC current_timestamp() as ETL_LAST_UPDT_DTS,
// MAGIC data_clctn_id,
// MAGIC STTS_EFCTV_TS,
// MAGIC STTS_EXPRTN_TS,
// MAGIC LAST_DEVC_ACTVTY_TS,
// MAGIC SRC_SYS_CD,
// MAGIC MD5_HASH,
// MAGIC LOAD_DT,
// MAGIC LOAD_HR_TS,
// MAGIC CNCTD_STTS_FLAG,
// MAGIC DEVC_ACTV_NUM,
// MAGIC CASE
// MAGIC 	WHEN datediff(DEVC_ACTV_NUM, LAST_DEVC_ACTVTY_TS) > 8 then "1"
// MAGIC 	ELSE "0"
// MAGIC END AS DEVC_UNAVLBL_FLAG
// MAGIC from
// MAGIC (SELECT 
// MAGIC ENRLD_VIN_NB,
// MAGIC DEVC_KEY,
// MAGIC prog_inst_id,
// MAGIC data_clctn_id,
// MAGIC STTS_EFCTV_TS,
// MAGIC CASE 
// MAGIC 	when STTS_EXPRTN_TS = HIGHDATE_TS then cast('9999-01-01' as timestamp) --cast('1/1/9999' as TIMESTAMP)
// MAGIC 	else cast(STTS_EXPRTN_TS as timestamp)
// MAGIC end as STTS_EXPRTN_TS,
// MAGIC LAST_DEVC_ACTVTY_TS,
// MAGIC SRC_SYS_CD,
// MAGIC MD5_HASH,
// MAGIC LOAD_DT,
// MAGIC LOAD_HR_TS,
// MAGIC CNCTD_STTS_FLAG,
// MAGIC LEAD(LAST_DEVC_ACTVTY_TS, 1, current_timestamp()) OVER(PARTITION BY ENRLD_VIN_NB ORDER BY STTS_EFCTV_TS) as DEVC_ACTV_NUM,
// MAGIC /*CASE
// MAGIC 	WHEN DEVC_ACTV_NUM - LAST_DEVC_ACTVTY_TS then '1'
// MAGIC 	ELSE '0'
// MAGIC (CASE 
// MAGIC 	WHEN LEAD(LAST_DEVC_ACTVTY_TS) OVER(PARTITION BY DE.ENRLD_VIN_NB ORDER BY DE.STTS_EFCTV_TS) - LAST_DEVC_ACTVTY_TS >  then '1'
// MAGIC 	ELSE '0'
// MAGIC END) AS DEVC_UNAVLBL_FLAG,*/
// MAGIC HIGHDATE_TS
// MAGIC FROM
// MAGIC (SELECT 
// MAGIC DE.ENRLD_VIN_NB,
// MAGIC DE.DEVC_KEY,
// MAGIC DE.prog_inst_id,
// MAGIC DE.data_clctn_id,
// MAGIC DE.STTS_EFCTV_TS,
// MAGIC LEAD(DE.STTS_EFCTV_TS, 1, DE_HIGHDATE.HIGHDATE_TS) OVER(PARTITION BY DE.ENRLD_VIN_NB ORDER BY DE.STTS_EFCTV_TS) AS STTS_EXPRTN_TS,
// MAGIC DE.LAST_DEVC_ACTVTY_TS,
// MAGIC DE.SRC_SYS_CD,
// MAGIC DE.MD5_HASH,
// MAGIC DE.LOAD_DT,
// MAGIC DE.LOAD_HR_TS,
// MAGIC DE.CNCTD_STTS_FLAG,
// MAGIC DE.DEVC_UNAVLBL_FLAG,
// MAGIC DE_HIGHDATE.HIGHDATE_TS
// MAGIC FROM
// MAGIC ((select * from 
// MAGIC ((select distinct
// MAGIC veh_key as ENRLD_VIN_NB,
// MAGIC device_nbr as DEVC_KEY,
// MAGIC prog_inst_id,
// MAGIC data_clctn_id,
// MAGIC event_start_ts as STTS_EFCTV_TS,
// MAGIC event_end_ts as STTS_EXPRTN_TS,
// MAGIC src_sys_cd as SRC_SYS_CD,
// MAGIC MD5_HASH,
// MAGIC LOAD_DT,
// MAGIC LOAD_HR_TS,
// MAGIC case
// MAGIC   when event_key = 'DISCONNECT_EVENT' then "0"
// MAGIC   else "1"
// MAGIC end as CNCTD_STTS_FLAG,
// MAGIC event_end_ts as LAST_DEVC_ACTVTY_TS,
// MAGIC case
// MAGIC when datediff(current_timestamp(),event_end_ts) > 8 then "1"
// MAGIC else "0"
// MAGIC end as DEVC_UNAVLBL_FLAG
// MAGIC from  
// MAGIC (select distinct
// MAGIC VEH_KEY as veh_key,
// MAGIC TRIP_SMRY_KEY as event_key,
// MAGIC TRIP_START_TS as event_start_ts,
// MAGIC TRIP_END_TS as event_end_ts,
// MAGIC SRC_SYS_CD as src_sys_cd,
// MAGIC md5(CONCAT(VEH_KEY, TRIP_SMRY_KEY)) as md5_hash,
// MAGIC LOAD_DT as load_dt,
// MAGIC LOAD_HR_TS as load_hr_ts
// MAGIC from dhf_iot_harmonized_prod.trip_summary where  load_dt <'2022-09-12' and src_sys_cd='TIMS_SR') td
// MAGIC left join
// MAGIC (select distinct
// MAGIC VIN_NB as vin_nbr,
// MAGIC coalesce(DEVC_ID, 'NOKEY') as device_nbr,
// MAGIC PRGRM_INSTC_ID as prog_inst_id,
// MAGIC DATA_CLCTN_ID as data_clctn_id,
// MAGIC cast(PRGRM_TERM_BEG_DT as TIMESTAMP) as active_start_dt,
// MAGIC cast(PRGRM_TERM_END_DT as TIMESTAMP) as active_end_dt
// MAGIC from dhf_iot_harmonized_prod.integrated_enrollment) ie
// MAGIC on td.veh_key = ie.vin_nbr)
// MAGIC union
// MAGIC (select distinct
// MAGIC veh_key as ENRLD_VIN_NB,
// MAGIC device_nbr as DEVC_KEY,
// MAGIC prog_inst_id,
// MAGIC data_clctn_id,
// MAGIC event_start_ts as STTS_EFCTV_TS,
// MAGIC event_end_ts as STTS_EXPRTN_TS,
// MAGIC src_sys_cd as SRC_SYS_CD,
// MAGIC MD5_HASH,
// MAGIC LOAD_DT,
// MAGIC LOAD_HR_TS,
// MAGIC case
// MAGIC   when event_key = 'DISCONNECT_EVENT' then "0"
// MAGIC   else "1"
// MAGIC end as CNCTD_STTS_FLAG,
// MAGIC event_end_ts as LAST_DEVC_ACTVTY_TS,
// MAGIC case
// MAGIC when datediff(current_timestamp(),event_end_ts) > 8 then "1"
// MAGIC else "0"
// MAGIC end as DEVC_UNAVLBL_FLAG
// MAGIC from 
// MAGIC (select distinct
// MAGIC VEH_KEY as veh_key,
// MAGIC EVNT_TP_CD as event_key,
// MAGIC UTC_TS as event_start_ts,
// MAGIC UTC_TS as event_end_ts,
// MAGIC SRC_SYS_CD as src_sys_cd,
// MAGIC md5(CONCAT(VEH_KEY, EVNT_TP_CD, CAST(UTC_TS as STRING))) as MD5_HASH,
// MAGIC LOAD_DT as load_dt,
// MAGIC LOAD_HR_TS as load_hr_ts,
// MAGIC rownb
// MAGIC from (select *,row_number() over (partition by NON_TRIP_EVNT_KEY, EVNT_TP_CD, UTC_TS order by load_hr_ts desc ) as rownb from dhf_iot_harmonized_prod.non_trip_event  where load_dt <'2022-09-12' and src_sys_cd='TIMS_SR')) td
// MAGIC left join
// MAGIC (select distinct
// MAGIC VIN_NB as vin_nbr,
// MAGIC coalesce(DEVC_ID, 'NOKEY') as device_nbr,
// MAGIC PRGRM_INSTC_ID as prog_inst_id,
// MAGIC DATA_CLCTN_ID as data_clctn_id,
// MAGIC cast(PRGRM_TERM_BEG_DT as TIMESTAMP) as active_start_dt,
// MAGIC cast(PRGRM_TERM_END_DT as TIMESTAMP) as active_end_dt
// MAGIC from dhf_iot_harmonized_prod.integrated_enrollment) ie
// MAGIC on td.veh_key = ie.vin_nbr
// MAGIC where td.rownb=1
// MAGIC order by STTS_EFCTV_TS))

// COMMAND ----------

// MAGIC %sql
// MAGIC select ENRLD_VIN_NB, MAX(STTS_EXPRTN_TS) as HIGHDATE_TS from 
// MAGIC (
// MAGIC (select distinct
// MAGIC veh_key as ENRLD_VIN_NB,
// MAGIC event_start_ts as STTS_EFCTV_TS,
// MAGIC event_end_ts as STTS_EXPRTN_TS,
// MAGIC src_sys_cd as SRC_SYS_CD
// MAGIC from (select distinct
// MAGIC VEH_KEY as veh_key,
// MAGIC TRIP_START_TS as event_start_ts,
// MAGIC TRIP_END_TS as event_end_ts,
// MAGIC SRC_SYS_CD as src_sys_cd
// MAGIC from dhf_iot_harmonized_prod.trip_summary where  load_dt <'2022-09-12' and src_sys_cd='TIMS_SR') )
// MAGIC union
// MAGIC (select distinct
// MAGIC veh_key as ENRLD_VIN_NB,
// MAGIC event_start_ts as STTS_EFCTV_TS,
// MAGIC event_end_ts as STTS_EXPRTN_TS,
// MAGIC src_sys_cd as SRC_SYS_CD
// MAGIC from (select distinct
// MAGIC VEH_KEY as veh_key,
// MAGIC EVNT_TP_CD as event_key,
// MAGIC UTC_TS as event_start_ts,
// MAGIC UTC_TS as event_end_ts,
// MAGIC SRC_SYS_CD as src_sys_cd,
// MAGIC rownb
// MAGIC from (select *,row_number() over (partition by NON_TRIP_EVNT_KEY, EVNT_TP_CD, UTC_TS order by load_hr_ts desc ) as rownb from dhf_iot_harmonized_prod.non_trip_event  where load_dt <'2022-09-12' and src_sys_cd='TIMS_SR') )
// MAGIC where rownb=1
// MAGIC order by STTS_EFCTV_TS))
// MAGIC group by ENRLD_VIN_NB
// MAGIC

// COMMAND ----------

//device_status new 

val device_sts_df=spark.sql("""SELECT 
cast(monotonically_increasing_id() +1 + coalesce((select max(DEVICE_STATUS_ID) from dhf_iot_curated_prod.DEVICE_STATUS),0) as BIGINT)   as DEVICE_STATUS_ID,
ENRLD_VIN_NB,
coalesce(DEVC_KEY, 'NOKEY') as DEVC_KEY,
prog_inst_id as PRGRM_INSTC_ID,
current_timestamp() as ETL_ROW_EFF_DTS,
current_timestamp() as ETL_LAST_UPDT_DTS,
data_clctn_id,
STTS_EFCTV_TS,
STTS_EXPRTN_TS,
LAST_DEVC_ACTVTY_TS,
SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
CNCTD_STTS_FLAG,
DEVC_ACTV_NUM,
CASE
	WHEN datediff(DEVC_ACTV_NUM, LAST_DEVC_ACTVTY_TS) > 8 then "1"
	ELSE "0"
END AS DEVC_UNAVLBL_FLAG
from
(SELECT 
ENRLD_VIN_NB,
DEVC_KEY,
prog_inst_id,
data_clctn_id,
STTS_EFCTV_TS,
CASE 
	when STTS_EXPRTN_TS = HIGHDATE_TS then cast('9999-01-01' as timestamp) --cast('1/1/9999' as TIMESTAMP)
	else cast(STTS_EXPRTN_TS as timestamp)
end as STTS_EXPRTN_TS,
LAST_DEVC_ACTVTY_TS,
SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
CNCTD_STTS_FLAG,
LEAD(LAST_DEVC_ACTVTY_TS, 1, current_timestamp()) OVER(PARTITION BY ENRLD_VIN_NB ORDER BY STTS_EFCTV_TS) as DEVC_ACTV_NUM,
/*CASE
	WHEN DEVC_ACTV_NUM - LAST_DEVC_ACTVTY_TS then '1'
	ELSE '0'
(CASE 
	WHEN LEAD(LAST_DEVC_ACTVTY_TS) OVER(PARTITION BY DE.ENRLD_VIN_NB ORDER BY DE.STTS_EFCTV_TS) - LAST_DEVC_ACTVTY_TS >  then '1'
	ELSE '0'
END) AS DEVC_UNAVLBL_FLAG,*/
HIGHDATE_TS
FROM
(SELECT 
DE.ENRLD_VIN_NB,
DE.DEVC_KEY,
DE.prog_inst_id,
DE.data_clctn_id,
DE.STTS_EFCTV_TS,
LEAD(DE.STTS_EFCTV_TS, 1, DE_HIGHDATE.HIGHDATE_TS) OVER(PARTITION BY DE.ENRLD_VIN_NB ORDER BY DE.STTS_EFCTV_TS) AS STTS_EXPRTN_TS,
DE.LAST_DEVC_ACTVTY_TS,
DE.SRC_SYS_CD,
DE.MD5_HASH,
DE.LOAD_DT,
DE.LOAD_HR_TS,
DE.CNCTD_STTS_FLAG,
DE.DEVC_UNAVLBL_FLAG,
DE_HIGHDATE.HIGHDATE_TS
FROM
((select * from 
((select distinct
veh_key as ENRLD_VIN_NB,
device_nbr as DEVC_KEY,
prog_inst_id,
data_clctn_id,
event_start_ts as STTS_EFCTV_TS,
event_end_ts as STTS_EXPRTN_TS,
src_sys_cd as SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
case
  when event_key = 'DISCONNECT_EVENT' then "0"
  else "1"
end as CNCTD_STTS_FLAG,
event_end_ts as LAST_DEVC_ACTVTY_TS,
case
when datediff(current_timestamp(),event_end_ts) > 8 then "1"
else "0"
end as DEVC_UNAVLBL_FLAG
from  
(select distinct
VEH_KEY as veh_key,
TRIP_SMRY_KEY as event_key,
TRIP_START_TS as event_start_ts,
TRIP_END_TS as event_end_ts,
SRC_SYS_CD as src_sys_cd,
md5(CONCAT(VEH_KEY, TRIP_SMRY_KEY)) as md5_hash,
LOAD_DT as load_dt,
LOAD_HR_TS as load_hr_ts
from dhf_iot_harmonized_prod.trip_summary where  load_dt <'2022-09-12' and src_sys_cd='IMS_SR_4X') td
left join
(select distinct
VIN_NB as vin_nbr,
coalesce(DEVC_ID, 'NOKEY') as device_nbr,
PRGRM_INSTC_ID as prog_inst_id,
DATA_CLCTN_ID as data_clctn_id,
cast(PRGRM_TERM_BEG_DT as TIMESTAMP) as active_start_dt,
cast(PRGRM_TERM_END_DT as TIMESTAMP) as active_end_dt
from dhf_iot_harmonized_prod.integrated_enrollment) ie
on td.veh_key = ie.vin_nbr)
union
(select distinct
veh_key as ENRLD_VIN_NB,
device_nbr as DEVC_KEY,
prog_inst_id,
data_clctn_id,
event_start_ts as STTS_EFCTV_TS,
event_end_ts as STTS_EXPRTN_TS,
src_sys_cd as SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
case
  when event_key = 'DISCONNECT_EVENT' then "0"
  else "1"
end as CNCTD_STTS_FLAG,
event_end_ts as LAST_DEVC_ACTVTY_TS,
case
when datediff(current_timestamp(),event_end_ts) > 8 then "1"
else "0"
end as DEVC_UNAVLBL_FLAG
from 
(select distinct
VEH_KEY as veh_key,
EVNT_TP_CD as event_key,
UTC_TS as event_start_ts,
UTC_TS as event_end_ts,
SRC_SYS_CD as src_sys_cd,
md5(CONCAT(VEH_KEY, EVNT_TP_CD, CAST(UTC_TS as STRING))) as MD5_HASH,
LOAD_DT as load_dt,
LOAD_HR_TS as load_hr_ts,
rownb
from (select *,row_number() over (partition by NON_TRIP_EVNT_KEY, EVNT_TP_CD, UTC_TS order by load_hr_ts desc ) as rownb from dhf_iot_harmonized_prod.non_trip_event  where load_dt <'2022-09-12' and src_sys_cd='IMS_SR_4X')) td
left join
(select distinct
VIN_NB as vin_nbr,
coalesce(DEVC_ID, 'NOKEY') as device_nbr,
PRGRM_INSTC_ID as prog_inst_id,
DATA_CLCTN_ID as data_clctn_id,
cast(PRGRM_TERM_BEG_DT as TIMESTAMP) as active_start_dt,
cast(PRGRM_TERM_END_DT as TIMESTAMP) as active_end_dt
from dhf_iot_harmonized_prod.integrated_enrollment) ie
on td.veh_key = ie.vin_nbr
where td.rownb=1
order by STTS_EFCTV_TS))
) as DE
--------
inner join
(
select ENRLD_VIN_NB, MAX(STTS_EXPRTN_TS) as HIGHDATE_TS from 
((select distinct
veh_key as ENRLD_VIN_NB,
event_start_ts as STTS_EFCTV_TS,
event_end_ts as STTS_EXPRTN_TS,
src_sys_cd as SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
case
  when event_key = 'DISCONNECT_EVENT' then "0"
  else "1"
end as CNCTD_STTS_FLAG,
event_end_ts as LAST_DEVC_ACTVTY_TS,
case
when datediff(current_timestamp(),event_end_ts) > 8 then "1"
else "0"
end as DEVC_UNAVLBL_FLAG
from  
(select distinct
VEH_KEY as veh_key,
TRIP_SMRY_KEY as event_key,
TRIP_START_TS as event_start_ts,
TRIP_END_TS as event_end_ts,
SRC_SYS_CD as src_sys_cd,
md5(CONCAT(VEH_KEY, TRIP_SMRY_KEY)) as md5_hash,
LOAD_DT as load_dt,
LOAD_HR_TS as load_hr_ts
from dhf_iot_harmonized_prod.trip_summary where  load_dt <'2022-09-12' and src_sys_cd='IMS_SR_4X') td
left join
(select distinct
VIN_NB as vin_nbr,
coalesce(DEVC_ID, 'NOKEY') as device_nbr,
PRGRM_INSTC_ID as prog_inst_id,
DATA_CLCTN_ID as data_clctn_id,
cast(PRGRM_TERM_BEG_DT as TIMESTAMP) as active_start_dt,
cast(PRGRM_TERM_END_DT as TIMESTAMP) as active_end_dt
from dhf_iot_harmonized_prod.integrated_enrollment) ie
on td.veh_key = ie.vin_nbr)
union
(select distinct
veh_key as ENRLD_VIN_NB,
event_start_ts as STTS_EFCTV_TS,
event_end_ts as STTS_EXPRTN_TS,
src_sys_cd as SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
case
  when event_key = 'DISCONNECT_EVENT' then "0"
  else "1"
end as CNCTD_STTS_FLAG,
event_end_ts as LAST_DEVC_ACTVTY_TS,
case
when datediff(current_timestamp(),event_end_ts) > 8 then "1"
else "0"
end as DEVC_UNAVLBL_FLAG
from 
(select distinct
VEH_KEY as veh_key,
EVNT_TP_CD as event_key,
UTC_TS as event_start_ts,
UTC_TS as event_end_ts,
SRC_SYS_CD as src_sys_cd,
md5(CONCAT(VEH_KEY, EVNT_TP_CD, CAST(UTC_TS as STRING))) as MD5_HASH,
LOAD_DT as load_dt,
LOAD_HR_TS as load_hr_ts,
rownb
from (select *,row_number() over (partition by NON_TRIP_EVNT_KEY, EVNT_TP_CD, UTC_TS order by load_hr_ts desc ) as rownb from dhf_iot_harmonized_prod.non_trip_event  where load_dt <'2022-09-12' and src_sys_cd='IMS_SR_4X')) td
left join
(select distinct
VIN_NB as vin_nbr,
coalesce(DEVC_ID, 'NOKEY') as device_nbr,
PRGRM_INSTC_ID as prog_inst_id,
DATA_CLCTN_ID as data_clctn_id,
cast(PRGRM_TERM_BEG_DT as TIMESTAMP) as active_start_dt,
cast(PRGRM_TERM_END_DT as TIMESTAMP) as active_end_dt
from dhf_iot_harmonized_prod.integrated_enrollment) ie
on td.veh_key = ie.vin_nbr
where td.rownb=1
order by STTS_EFCTV_TS))
group by ENRLD_VIN_NB
) as DE_HIGHDATE
on DE.ENRLD_VIN_NB = DE_HIGHDATE.ENRLD_VIN_NB))) DE""").drop("DEVC_ACTV_NUM")
// display(device_sts_df)
device_sts_df.write.format("delta").mode("append").saveAsTable("dhf_iot_curated_prod.device_status")

// COMMAND ----------

//device_status new 

val device_sts_df=spark.sql("""SELECT 
cast(monotonically_increasing_id() +1 + coalesce((select max(DEVICE_STATUS_ID) from dhf_iot_curated_prod.DEVICE_STATUS),0) as BIGINT)   as DEVICE_STATUS_ID,
ENRLD_VIN_NB,
coalesce(DEVC_KEY, 'NOKEY') as DEVC_KEY,
prog_inst_id as PRGRM_INSTC_ID,
current_timestamp() as ETL_ROW_EFF_DTS,
current_timestamp() as ETL_LAST_UPDT_DTS,
data_clctn_id,
STTS_EFCTV_TS,
STTS_EXPRTN_TS,
LAST_DEVC_ACTVTY_TS,
SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
CNCTD_STTS_FLAG,
DEVC_ACTV_NUM,
CASE
	WHEN datediff(DEVC_ACTV_NUM, LAST_DEVC_ACTVTY_TS) > 8 then "1"
	ELSE "0"
END AS DEVC_UNAVLBL_FLAG
from
(SELECT 
ENRLD_VIN_NB,
DEVC_KEY,
prog_inst_id,
data_clctn_id,
STTS_EFCTV_TS,
CASE 
	when STTS_EXPRTN_TS = HIGHDATE_TS AND STTS_EFCTV_TS=HIGHDATE_EFF_TS then cast('9999-01-01' as timestamp) --cast('1/1/9999' as TIMESTAMP)
	else cast(STTS_EXPRTN_TS as timestamp)
end as STTS_EXPRTN_TS,
LAST_DEVC_ACTVTY_TS,
SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
CNCTD_STTS_FLAG,
LEAD(LAST_DEVC_ACTVTY_TS, 1, current_timestamp()) OVER(PARTITION BY ENRLD_VIN_NB ORDER BY STTS_EFCTV_TS) as DEVC_ACTV_NUM,
/*CASE
	WHEN DEVC_ACTV_NUM - LAST_DEVC_ACTVTY_TS then '1'
	ELSE '0'
(CASE 
	WHEN LEAD(LAST_DEVC_ACTVTY_TS) OVER(PARTITION BY DE.ENRLD_VIN_NB ORDER BY DE.STTS_EFCTV_TS) - LAST_DEVC_ACTVTY_TS >  then '1'
	ELSE '0'
END) AS DEVC_UNAVLBL_FLAG,*/
HIGHDATE_TS,HIGHDATE_EFF_TS
FROM
(SELECT 
DE.ENRLD_VIN_NB,
DE.DEVC_KEY,
DE.prog_inst_id,
DE.data_clctn_id,
DE.STTS_EFCTV_TS,
LEAD(DE.STTS_EFCTV_TS, 1, DE_HIGHDATE.HIGHDATE_TS) OVER(PARTITION BY DE.ENRLD_VIN_NB ORDER BY DE.STTS_EFCTV_TS ) AS STTS_EXPRTN_TS,
DE.LAST_DEVC_ACTVTY_TS,
DE.SRC_SYS_CD,
DE.MD5_HASH,
DE.LOAD_DT,
DE.LOAD_HR_TS,
DE.CNCTD_STTS_FLAG,
DE.DEVC_UNAVLBL_FLAG,
DE_HIGHDATE.HIGHDATE_EFF_TS,
DE_HIGHDATE.HIGHDATE_TS
FROM
((select * from 
((select distinct
veh_key as ENRLD_VIN_NB,
device_nbr as DEVC_KEY,
prog_inst_id,
data_clctn_id,
event_start_ts as STTS_EFCTV_TS,
event_end_ts as STTS_EXPRTN_TS,
src_sys_cd as SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
case
  when event_key = 'DISCONNECT_EVENT' then "0"
  else "1"
end as CNCTD_STTS_FLAG,
event_end_ts as LAST_DEVC_ACTVTY_TS,
case
when datediff(current_timestamp(),event_end_ts) > 8 then "1"
else "0"
end as DEVC_UNAVLBL_FLAG
from  
(select distinct
VEH_KEY as veh_key,
TRIP_SMRY_KEY as event_key,
TRIP_START_TS as event_start_ts,
TRIP_END_TS as event_end_ts,
SRC_SYS_CD as src_sys_cd,
md5(CONCAT(VEH_KEY, TRIP_SMRY_KEY)) as md5_hash,
LOAD_DT as load_dt,
LOAD_HR_TS as load_hr_ts
from dhf_iot_harmonized_prod.trip_summary where load_dt <'2022-09-22' and  src_sys_cd='IMS_SM_5X' and TRIP_START_TS!='9999-12-31T00:00:00.000+0000'and veh_key='4S4BRCCC2D3271115') td
left join
(select distinct
VIN_NB as vin_nbr,
coalesce(DEVC_ID, 'NOKEY') as device_nbr,
PRGRM_INSTC_ID as prog_inst_id,
DATA_CLCTN_ID as data_clctn_id,
cast(PRGRM_TERM_BEG_DT as TIMESTAMP) as active_start_dt,
cast(PRGRM_TERM_END_DT as TIMESTAMP) as active_end_dt
from dhf_iot_harmonized_prod.integrated_enrollment where LOAD_DT = (select max(LOAD_DT) from dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT)) ie
on td.veh_key = ie.vin_nbr)
union
(select distinct
veh_key as ENRLD_VIN_NB,
device_nbr as DEVC_KEY,
prog_inst_id,
data_clctn_id,
event_start_ts as STTS_EFCTV_TS,
event_end_ts as STTS_EXPRTN_TS,
src_sys_cd as SRC_SYS_CD,
MD5_HASH,
LOAD_DT,
LOAD_HR_TS,
case
  when event_key = 'DISCONNECT_EVENT' then "0"
  else "1"
end as CNCTD_STTS_FLAG,
event_end_ts as LAST_DEVC_ACTVTY_TS,
case
when datediff(current_timestamp(),event_end_ts) > 8 then "1"
else "0"
end as DEVC_UNAVLBL_FLAG
from 
(select distinct
VEH_KEY as veh_key,
EVNT_TP_CD as event_key,
UTC_TS as event_start_ts,
UTC_TS as event_end_ts,
SRC_SYS_CD as src_sys_cd,
md5(CONCAT(VEH_KEY, EVNT_TP_CD, CAST(UTC_TS as STRING))) as MD5_HASH,
LOAD_DT as load_dt,
LOAD_HR_TS as load_hr_ts,
rownb
from (select *,row_number() over (partition by veh_key, EVNT_TP_CD, UTC_TS order by load_hr_ts desc ) as rownb from dhf_iot_harmonized_prod.non_trip_event  where load_dt <'2022-09-22' and  src_sys_cd='IMS_SM_5X' and veh_key='4S4BRCCC2D3271115') 
where rownb=1) td
left join
(select distinct
VIN_NB as vin_nbr,
coalesce(DEVC_ID, 'NOKEY') as device_nbr,
PRGRM_INSTC_ID as prog_inst_id,
DATA_CLCTN_ID as data_clctn_id,
cast(PRGRM_TERM_BEG_DT as TIMESTAMP) as active_start_dt,
cast(PRGRM_TERM_END_DT as TIMESTAMP) as active_end_dt
from dhf_iot_harmonized_prod.integrated_enrollment  where LOAD_DT = (select max(LOAD_DT) from dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT)) ie
on td.veh_key = ie.vin_nbr
order by STTS_EFCTV_TS))
) as DE
--------
inner join
(
select ENRLD_VIN_NB,max(STTS_EFCTV_TS) as HIGHDATE_EFF_TS, MAX(STTS_EXPRTN_TS) as HIGHDATE_TS from 
(

(select distinct
veh_key as ENRLD_VIN_NB,
event_start_ts as STTS_EFCTV_TS,
event_end_ts as STTS_EXPRTN_TS
from  (select distinct
VEH_KEY as veh_key,
TRIP_START_TS as event_start_ts,
TRIP_END_TS as event_end_ts
from dhf_iot_harmonized_prod.trip_summary where load_dt <'2022-09-22' and  src_sys_cd='IMS_SM_5X' and TRIP_START_TS!='9999-12-31T00:00:00.000+0000'and veh_key='4S4BRCCC2D3271115')) 

union

(select distinct
veh_key as ENRLD_VIN_NB,
event_start_ts as STTS_EFCTV_TS ,
event_end_ts as STTS_EXPRTN_TS
from (select distinct
VEH_KEY as veh_key,
UTC_TS as event_start_ts ,
UTC_TS as event_end_ts from  dhf_iot_harmonized_prod.non_trip_event  where load_dt <'2022-09-22' and  src_sys_cd='IMS_SM_5X' and veh_key='4S4BRCCC2D3271115' ))

)
group by ENRLD_VIN_NB) as DE_HIGHDATE
on DE.ENRLD_VIN_NB = DE_HIGHDATE.ENRLD_VIN_NB))) DE""").drop("DEVC_ACTV_NUM")
display(device_sts_df)
// device_sts_df.write.format("delta").mode("overWrite").saveAsTable("dhf_iot_curated_prod.device_st")

// COMMAND ----------

// MAGIC %sql
// MAGIC select ENRLD_VIN_NB,max(STTS_EFCTV_TS) as HIGHDATE_EFF_TS, MAX(STTS_EXPRTN_TS) as HIGHDATE_TS from 
// MAGIC (
// MAGIC
// MAGIC (select distinct
// MAGIC veh_key as ENRLD_VIN_NB,
// MAGIC event_start_ts as STTS_EFCTV_TS,
// MAGIC event_end_ts as STTS_EXPRTN_TS
// MAGIC from  (select distinct
// MAGIC VEH_KEY as veh_key,
// MAGIC TRIP_START_TS as event_start_ts,
// MAGIC TRIP_END_TS as event_end_ts
// MAGIC from dhf_iot_harmonized_prod.trip_summary where load_dt <'2022-09-22' and  src_sys_cd='IMS_SM_5X' and TRIP_START_TS!='9999-12-31T00:00:00.000+0000'and veh_key='4S4BRCCC2D3271115')) 
// MAGIC
// MAGIC union
// MAGIC
// MAGIC (select distinct
// MAGIC veh_key as ENRLD_VIN_NB,
// MAGIC event_start_ts as STTS_EFCTV_TS ,
// MAGIC event_end_ts as STTS_EXPRTN_TS
// MAGIC from (select distinct
// MAGIC VEH_KEY as veh_key,
// MAGIC UTC_TS as event_start_ts ,
// MAGIC UTC_TS as event_end_ts from  dhf_iot_harmonized_prod.non_trip_event  where load_dt <'2022-09-22' and  src_sys_cd='IMS_SM_5X' and veh_key='4S4BRCCC2D3271115' ))
// MAGIC
// MAGIC )
// MAGIC group by ENRLD_VIN_NB

// COMMAND ----------

// MAGIC %sql
// MAGIC select *,LEAD(STTS_EFCTV_TS, 1, '2021-02-27T18:56:12.012+0000') OVER(PARTITION BY ENRLD_VIN_NB ORDER BY STTS_EFCTV_TS) AS STTS_EXPRTN_TS_new,('2021-02-27T18:56:12.012+0000') as HIGHDATE_TS from (select distinct
// MAGIC VEH_KEY as ENRLD_VIN_NB,
// MAGIC TRIP_START_TS as STTS_EFCTV_TS,
// MAGIC TRIP_END_TS as STTS_EXPRTN_TS
// MAGIC from dhf_iot_harmonized_prod.trip_summary where load_dt <'2022-09-22' and  src_sys_cd='IMS_SM_5X' and TRIP_START_TS!='9999-12-31T00:00:00.000+0000'and veh_key='4S4BRCCC2D3271115'
// MAGIC union all
// MAGIC select VEH_KEY as ENRLD_VIN_NB,
// MAGIC UTC_TS as STTS_EFCTV_TS,
// MAGIC UTC_TS as STTS_EXPRTN_TS from (select *,row_number() over (partition by veh_key, EVNT_TP_CD, UTC_TS order by load_hr_ts desc ) as rownb from dhf_iot_harmonized_prod.non_trip_event  where load_dt <'2022-09-22' and  src_sys_cd='IMS_SM_5X' and veh_key='4S4BRCCC2D3271115') 
// MAGIC where rownb=1)

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from dhf_iot_curated_prod.device_summary_chlg --group by load_dt--where ENRLD_VIN_NB='1C4PJMCS7HW530619'-- and STTS_EXPRTN_TS='9999-01-01T00:00:00.000+0000' 

// COMMAND ----------

// MAGIC %sql
// MAGIC select src_sys_cd,STTS_EXPRTN_TS,ENRLD_VIN_NB , COUNT(*) from dhf_iot_curated_prod.device_stATUS where STTS_EXPRTN_TS='9999-01-01T00:00:00.000+0000'   GROUP BY src_sys_cd, STTS_EXPRTN_TS,ENRLD_VIN_NB HAVING COUNT(*)>1

// COMMAND ----------

// MAGIC %sql
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select ENRLD_VIN_NB, MAX(STTS_EXPRTN_TS) as HIGHDATE_TS from 
// MAGIC (
// MAGIC
// MAGIC (select distinct
// MAGIC veh_key as ENRLD_VIN_NB,
// MAGIC event_end_ts as STTS_EXPRTN_TS
// MAGIC from  (select distinct
// MAGIC VEH_KEY as veh_key,
// MAGIC TRIP_END_TS as event_end_ts
// MAGIC from dhf_iot_harmonized_prod.trip_summary where load_dt <'2022-09-18' and  src_sys_cd='IMS_SM_5X' and veh_key='JTHBE1BL6FA002263' and TRIP_START_TS!='9999-12-31T00:00:00.000+0000' )) 
// MAGIC
// MAGIC union
// MAGIC
// MAGIC (select distinct
// MAGIC veh_key as ENRLD_VIN_NB,
// MAGIC event_end_ts as STTS_EXPRTN_TS
// MAGIC from (select distinct
// MAGIC VEH_KEY as veh_key,
// MAGIC UTC_TS as event_end_ts from  dhf_iot_harmonized_prod.non_trip_event  where load_dt <'2022-09-18' and  src_sys_cd='IMS_SM_5X'  and veh_key='JTHBE1BL6FA002263'))
// MAGIC
// MAGIC )
// MAGIC group by ENRLD_VIN_NB

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_curated_prod.device_status where enrld_vin_nb='1C4PJMCS7HW530619'

// COMMAND ----------

// MAGIC %sql
// MAGIC select src_sys_cd,STTS_EXPRTN_TS,ENRLD_VIN_NB , COUNT(*) from dhf_iot_curated_prod.device_st where STTS_EXPRTN_TS='9999-01-01T00:00:00.000+0000'  GROUP BY src_sys_cd, STTS_EXPRTN_TS,ENRLD_VIN_NB HAVING COUNT(*)>1

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from  dhf_iot_harmonized_prod.TRIP_SUMMARY  where  src_sys_cd='IMS_SM_5X' and veh_key='1FTFX1EVXAFD03026' --1FTFX1EVXAFD03026 1GNDT33S292130359 4S4BRCCC2D3271115

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/delta/curated/dhf_iot_curated_prod/DEVICE_STATUS/",True)

// COMMAND ----------

val environment="prod"
spark.sql(s"""delete from dhf_iot_harmonized_${environment}.integrated_enrollment where LOAD_DT <= current_date - 7;""")
 
val df=spark.sql(s""" with new_ie as (
    select
      distinct
        cast(
          row_number() over(
            order by
              NULL
          ) + coalesce(
            (
              select
                max(INTEGRATED_ENROLLMENT_ID)
              from
                dhf_iot_harmonized_${environment}.integrated_enrollment
            ),
            0
          ) as BIGINT
        ) as INTEGRATED_ENROLLMENT_ID,
      current_timestamp() as ETL_LAST_UPDT_DTS,
      coalesce(VIN_NB, ENRLD_VIN_NB, 'NOKEY') as VIN_NB,
      coalesce(DEVC_ID, DEVC_ID_NB) as DEVC_ID,
      cast(coalesce(
        ENRLMNT_EFCTV_DT,
        ENRLMNT_DT,
        to_date('9999-12-31')
      ) as string) as ENRLMNT_EFCTV_DT,
      PRGRM_INSTC_ID,
      DATA_CLCTN_ID,
      cast(
        coalesce(
          PRGRM_TERM_BEG_DT,
          ACTV_STRT_DT,
          to_date('1000-01-01')
        ) as DATE
      ) as PRGRM_TERM_BEG_DT,
      cast(
        coalesce(
          PRGRM_TERM_END_DT,
          ACTV_END_DT,
          to_date('9999-12-31')
        ) as DATE
      ) as PRGRM_TERM_END_DT,
      coalesce(PLCY_ST_CD, PLCY_RT_ST_CD, 'NOKEY') as PLCY_ST_CD,
      coalesce(pef.VNDR_CD, ods.SRC_SYS_CD) as SRC_SYS_CD,
      current_date() as LOAD_DT,
      to_timestamp(
        date_format(current_timestamp(), 'yyyy-MM-dd H:00:00+0000')
      ) as LOAD_HR_TS
    from
      (
        select
          distinct first(VNDR_CD) as VNDR_CD,
          DATA_CLCTN_ID,
          first(VIN_NB) as VIN_NB,
          first(DEVC_ID) as DEVC_ID,
          -- DEVC_ID as DEVC_ID,
          first(ENRLMNT_EFCTV_DT) as ENRLMNT_EFCTV_DT,
          PLCY_ST_CD,
          min(PRGRM_TERM_BEG_DT) as PRGRM_TERM_BEG_DT,
          max(PRGRM_TERM_END_DT) as PRGRM_TERM_END_DT,
          max(LOAD_HR_TS) as LOAD_HR_TS
        from
          (
            select
              distinct coalesce(a.VNDR_CD, 'PE') as VNDR_CD,
              a.DATA_CLCTN_ID as DATA_CLCTN_ID,
              a.VIN_NB,
              trim(a.DEVC_ID) as DEVC_ID,
              a.ENRLMNT_EFCTV_DT,
              a.PLCY_ST_CD,
              a.PRGRM_TERM_BEG_DT,
              a.PRGRM_TERM_END_DT,
              a.LOAD_HR_TS
            from
              dhf_iot_harmonized_${environment}.program_enrollment a
              inner join (
                select
                  distinct pe.DATA_CLCTN_ID,
                  pe.VIN_NB,
                  pe.PLCY_ST_CD
                from
                  dhf_iot_harmonized_${environment}.program_enrollment pe
                  inner join (
                    select
                      distinct VIN_NB,
                      max(EVNT_SYS_TS) as mrRec,
                      max(ENRLMNT_EFCTV_DT) as mrE
                    from
                      dhf_iot_harmonized_${environment}.program_enrollment
                    where
                      (
                        PRGRM_TERM_BEG_DT is not null
                        or PRGRM_TERM_END_DT is not null
                      )
                      and trim(VNDR_CD) not in ('CMT', 'LN')
                      and DATA_CLCTN_STTS = 'Active'
                      and PRGRM_STTS_CD in ('Active', 'Cancelled')
                    group by
                      VIN_NB
                  ) pe1 on pe.VIN_NB = pe1.VIN_NB
                  and pe.ENRLMNT_EFCTV_DT = mrE
                  and pe.EVNT_SYS_TS = mrRec
              ) b on a.DATA_CLCTN_ID = b.DATA_CLCTN_ID
              and a.VIN_NB = b.VIN_NB
              and a.PLCY_ST_CD = b.PLCY_ST_CD
          )
        group by
          DATA_CLCTN_ID,
          PLCY_ST_CD
      ) pef full
      outer join (
        select
          distinct 'ODS' as SRC_SYS_CD,
          PRGRM_INSTC_ID,
          trim(ods1.ENRLD_VIN_NB) as ENRLD_VIN_NB,
          DEVC_ID_NB,
          ENRLMNT_DT,
          PLCY_RT_ST_CD,
          ACTV_STRT_DT,
          ACTV_END_DT
        from
          dhf_iot_harmonized_${environment}.ods_table ods1
          inner join (
            select
              distinct trim(ENRLD_VIN_NB) as ENRLD_VIN_NB,
              max(PRGRM_INSTC_ID) as maxPIID
            from
              dhf_iot_harmonized_${environment}.ods_table
            where
              VHCL_STTS_CD = 'E'
              and ACTV_END_DT = '3500-01-01'
              and LOAD_DT = (select max(LOAD_DT) from dhf_iot_harmonized_${environment}.ods_table)
            group by
              ENRLD_VIN_NB
          ) ods2 on trim(ods1.ENRLD_VIN_NB) = trim(ods2.ENRLD_VIN_NB)
          and ods1.PRGRM_INSTC_ID = ods2.maxPIID
        where
          VHCL_STTS_CD = 'E'
          and ACTV_END_DT = '3500-01-01'
      ) ods on pef.VIN_NB = trim(ods.ENRLD_VIN_NB) )
      
    select * from new_ie
      
      ;""")
// display(df)
df.write.format("delta").mode("overWrite").saveAsTable(f"dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT_test")

// COMMAND ----------

spark.sql(s"""delete from dhf_iot_harmonized_${environment}.integrated_enrollment where LOAD_DT <= current_date - 7;""")
 
val df=spark.sql(s""" with new_ie as (
    select
      distinct
        cast(
          row_number() over(
            order by
              NULL
          ) + coalesce(
            (
              select
                max(INTEGRATED_ENROLLMENT_ID)
              from
                dhf_iot_harmonized_${environment}.integrated_enrollment
            ),
            0
          ) as BIGINT
        ) as INTEGRATED_ENROLLMENT_ID,
      current_timestamp() as ETL_LAST_UPDT_DTS,
      coalesce(VIN_NB, ENRLD_VIN_NB, 'NOKEY') as VIN_NB,
      coalesce(DEVC_ID, DEVC_ID_NB) as DEVC_ID_OLD,
      cast(coalesce(
        ENRLMNT_EFCTV_DT,
        ENRLMNT_DT,
        to_date('9999-12-31')
      ) as DATE) as ENRLMNT_EFCTV_DT,
      PRGRM_INSTC_ID,
      DATA_CLCTN_ID,
      cast(
        coalesce(
          PRGRM_TERM_BEG_DT,
          ACTV_STRT_DT,
          to_date('1000-01-01')
        ) as DATE
      ) as PRGRM_TERM_BEG_DT,
      cast(
        coalesce(
          PRGRM_TERM_END_DT,
          ACTV_END_DT,
          to_date('9999-12-31')
        ) as DATE
      ) as PRGRM_TERM_END_DT,
      coalesce(PLCY_ST_CD, PLCY_RT_ST_CD, 'NOKEY') as PLCY_ST_CD,
      coalesce(pef.VNDR_CD, ods.SRC_SYS_CD) as SRC_SYS_CD,
      current_date() as LOAD_DT,
      to_timestamp(
        date_format(current_timestamp(), 'yyyy-MM-dd H:00:00+0000')
      ) as LOAD_HR_TS
    from
      (
        select
          distinct first(VNDR_CD) as VNDR_CD,
          DATA_CLCTN_ID,
          first(VIN_NB) as VIN_NB,
          first(DEVC_ID) as DEVC_ID,
          -- DEVC_ID as DEVC_ID,
          first(ENRLMNT_EFCTV_DT) as ENRLMNT_EFCTV_DT,
          PLCY_ST_CD,
          min(PRGRM_TERM_BEG_DT) as PRGRM_TERM_BEG_DT,
          max(PRGRM_TERM_END_DT) as PRGRM_TERM_END_DT,
          max(LOAD_HR_TS) as LOAD_HR_TS
        from
          (
            select
              distinct coalesce(a.VNDR_CD, 'PE') as VNDR_CD,
              a.DATA_CLCTN_ID as DATA_CLCTN_ID,
              a.VIN_NB,
              trim(a.DEVC_ID) as DEVC_ID,
              a.ENRLMNT_EFCTV_DT,
              a.PLCY_ST_CD,
              a.PRGRM_TERM_BEG_DT,
              a.PRGRM_TERM_END_DT,
              a.LOAD_HR_TS
            from
              dhf_iot_harmonized_${environment}.program_enrollment a
              inner join (
                select
                  distinct pe.DATA_CLCTN_ID,
                  pe.VIN_NB,
                  pe.PLCY_ST_CD
                from
                  dhf_iot_harmonized_${environment}.program_enrollment pe
                  inner join (
                    select
                      distinct VIN_NB,
                      max(EVNT_SYS_TS) as mrRec,
                      max(ENRLMNT_EFCTV_DT) as mrE
                    from
                      dhf_iot_harmonized_${environment}.program_enrollment
                    where
                      (
                        PRGRM_TERM_BEG_DT is not null
                        or PRGRM_TERM_END_DT is not null
                      )
                      and trim(VNDR_CD) not in ('CMT', 'LN')
                      and DATA_CLCTN_STTS = 'Active'
                      and PRGRM_STTS_CD in ('Active', 'Cancelled')
                    group by
                      VIN_NB
                  ) pe1 on pe.VIN_NB = pe1.VIN_NB
                  and pe.ENRLMNT_EFCTV_DT = mrE
                  and pe.EVNT_SYS_TS = mrRec
              ) b on a.DATA_CLCTN_ID = b.DATA_CLCTN_ID
              and a.VIN_NB = b.VIN_NB
              and a.PLCY_ST_CD = b.PLCY_ST_CD
          )
        group by
          DATA_CLCTN_ID,
          PLCY_ST_CD
      ) pef full
      outer join (
        select
          distinct 'ODS' as SRC_SYS_CD,
          PRGRM_INSTC_ID,
          trim(ods1.ENRLD_VIN_NB) as ENRLD_VIN_NB,
          DEVC_ID_NB,
          ENRLMNT_DT,
          PLCY_RT_ST_CD,
          ACTV_STRT_DT,
          ACTV_END_DT
        from
          dhf_iot_harmonized_${environment}.ods_table ods1
          inner join (
            select
              distinct trim(ENRLD_VIN_NB) as ENRLD_VIN_NB,
              max(PRGRM_INSTC_ID) as maxPIID
            from
              dhf_iot_harmonized_${environment}.ods_table
            where
              VHCL_STTS_CD = 'E'
              and ACTV_END_DT = '3500-01-01'
              and LOAD_DT = (select max(LOAD_DT) from dhf_iot_harmonized_${environment}.ods_table)
            group by
              ENRLD_VIN_NB
          ) ods2 on trim(ods1.ENRLD_VIN_NB) = trim(ods2.ENRLD_VIN_NB)
          and ods1.PRGRM_INSTC_ID = ods2.maxPIID
        where
          VHCL_STTS_CD = 'E'
          and ACTV_END_DT = '3500-01-01'
      ) ods on pef.VIN_NB = trim(ods.ENRLD_VIN_NB) )
      
    select distinct v.*,case when trim(v.devc_id_old)in ('',0) then q.DEVC_ID_NEW else v.devc_id_old end devc_id from new_ie  v left join ( select  VIN_NB,DATA_CLCTN_ID,first(DEVC_ID) AS DEVC_ID_NEW from (select distinct z.DATA_CLCTN_ID,Z.VIN_NB,Z.DEVC_ID FROM (select * from dhf_iot_harmonized_prod.program_enrollment where  trim(DEVC_ID) not in ('','0')  and vin_nb  in (select vin_nb from  new_ie) and trim(VNDR_CD) not in ('CMT', 'LN')) z inner join (select VIN_NB,DATA_CLCTN_ID,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where  trim(DEVC_ID) not in ('','0')  and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB,DATA_CLCTN_ID) y on z.VIN_NB=y.VIN_NB and z.EVNT_SYS_TS=max_e and z.ENRLMNT_EFCTV_DT=max_tbd and z.DATA_CLCTN_ID=y.DATA_CLCTN_ID)  group by VIN_NB,DATA_CLCTN_ID ) q on v.VIN_NB=q.VIN_NB and v.DATA_CLCTN_ID=q.DATA_CLCTN_ID
      
      ;""").drop("devc_id_old")
df.write.format("delta").mode("append").saveAsTable(f"dhf_iot_harmonized_${environment}.INTEGRATED_ENROLLMENT")


// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT where vin_nb='2G1FB1E34D9142045'
// MAGIC -- update dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT set load_dt='2022-09-22' , load_hr_ts='2022-09-22T23:00:00.000+0000' where load_dt='2022-09-23';

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.program_enrollment where vin_nb in ('2G1FB1E34D9142045')

// COMMAND ----------

// MAGIC %sql
// MAGIC select (*) from  (select  VIN_NB,first(DEVC_ID) AS DEVC_ID_NEW from (select distinct Z.VIN_NB,Z.DEVC_ID FROM (select * from dhf_iot_harmonized_prod.program_enrollment where trim(DEVC_ID) not in ('',' ','0')  and vin_nb  in ('2G1FB1E34D9142045') and trim(VNDR_CD) not in ('CMT', 'LN')) z inner join (select VIN_NB,DATA_CLCTN_ID,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where   trim(DEVC_ID) not in ('',' ','0') and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB,DATA_CLCTN_ID) y on z.VIN_NB=y.VIN_NB and z.EVNT_SYS_TS=max_e and z.ENRLMNT_EFCTV_DT=max_tbd and z.DATA_CLCTN_ID=y.DATA_CLCTN_ID)  group by VIN_NB)  where vin_nb='2G1FB1E34D9142045'

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select (*) from dhf_iot_harmonized_prod.program_enrollment  where vin_nb='2G1FB1E34D9142045' and 
// MAGIC select VIN_NB,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where  vin_nb='2G1FB1E34D9142045' and  trim(DEVC_ID) not in ('',' ','0') and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select (*) from dhf_iot_harmonized_prod.program_enrollment  where vin_nb='1J4GL58K75W708706'

// COMMAND ----------

// MAGIC %sql
// MAGIC select VIN_NB,COUNT(*) from dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT_final WHERE SRC_SYS_CD ='IMS' group BY vin_nb HAVING COUNT(*)>1  --where vin_nb='2G1FB1E34D9142045'

// COMMAND ----------

// MAGIC %sql
// MAGIC select (*) from dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT_TEST where vin_nb='1J4GL58K75W708706'

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_curated_prod.device_status where load_dt <='2022-09-22'

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_curated_prod.device_status_CHLG a using (select DISTINCT vin_nb,devc_id,DATA_CLCTN_ID from dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT where load_dt='2022-09-22' AND VIN_NB IN (select DISTINCT ENRLD_VIN_NB from  dhf_iot_curated_prod.device_status where DEVC_KEY ='NOKEY' AND SRC_SYS_CD='IMS_SM_5X' AND ENRLD_VIN_NB IN (SELECT VIN_NB FROM dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT WHERE TRIM(DEVC_ID) NOT IN ('',0) AND DEVC_ID IS NOT NULL AND LOAD_DT='2022-09-22'))) b on
// MAGIC a.enrld_vin_nb=b.vin_nb and a.DATA_CLCTN_ID=b.DATA_CLCTN_ID AND a.DEVC_KEY  in ('NOKEY') AND A.SRC_SYS_CD='IMS_SM_5X' WHEN MATCHED THEN  update set a.DEVC_KEY=coalesce(b.DEVC_ID,'NOKEY')

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_curated_prod.device_status a using (select DISTINCT vin_nb,devc_id,DATA_CLCTN_ID from dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT where load_dt='2022-09-22' AND VIN_NB IN (select DISTINCT ENRLD_VIN_NB from  dhf_iot_curated_prod.device_status where DEVC_KEY ='NOKEY' AND SRC_SYS_CD='IMS_SM_5X' AND ENRLD_VIN_NB IN (SELECT VIN_NB FROM dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT WHERE TRIM(DEVC_ID) NOT IN ('',0) AND DEVC_ID IS NOT NULL AND LOAD_DT='2022-09-22'))) b on
// MAGIC a.enrld_vin_nb=b.vin_nb and a.DATA_CLCTN_ID=b.DATA_CLCTN_ID AND a.DEVC_KEY  in ('NOKEY') AND A.SRC_SYS_CD='IMS_SM_5X' WHEN MATCHED THEN  update set a.DEVC_KEY=coalesce(b.DEVC_ID,'NOKEY')

// COMMAND ----------

// MAGIC %sql
// MAGIC select DISTINCT ENRLD_VIN_NB,SRC_SYS_CD from  dhf_iot_curated_prod.device_status where DEVC_KEY ='NOKEY' AND SRC_SYS_CD='IMS_SM_5X' AND ENRLD_VIN_NB IN (SELECT VIN_NB FROM dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT WHERE TRIM(DEVC_ID) NOT IN ('',0) AND DEVC_ID IS NOT NULL AND LOAD_DT='2022-09-22')

// COMMAND ----------

// MAGIC %sql
// MAGIC select DISTINCT ENRLD_VIN_NB,SRC_SYS_CD from  dhf_iot_curated_prod.device_status where DEVC_KEY ='NOKEY' AND SRC_SYS_CD='IMS_SM_5X' AND ENRLD_VIN_NB IN (SELECT VIN_NB FROM dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT WHERE TRIM(DEVC_ID) NOT IN ('',0) AND DEVC_ID IS NOT NULL AND LOAD_DT='2022-09-22')

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from  dhf_iot_curated_prod.device_status where ENRLD_VIN_NB ='19XFC2F61KE019658'

// COMMAND ----------

// MAGIC %sql
// MAGIC select (*) from dhf_iot_harmonized_prod.program_ENROLLMENT where (vin_nb)='5UXTS1C06M9G10075'

// COMMAND ----------

// MAGIC %sql
// MAGIC select (*) from dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT where (vin_nb)='5UXTS1C06M9G10075'