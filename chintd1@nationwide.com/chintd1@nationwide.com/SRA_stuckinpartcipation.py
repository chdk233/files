# Databricks notebook source
# environment = str(dbutils.widgets.get('environment'))
# account_id = str(dbutils.widgets.get('account_id'))
account_id="785562577411"
environment="prod"

# COMMAND ----------

spark.sql(f""" CREATE DATABASE  IF NOT EXISTS dhf_iot_analytics_raw LOCATION 's3://pcds-databricks-common-{account_id}/iot/delta/raw/analytics/dhf_iot_analytics_raw';""")

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS dhf_iot_analytics_raw.sip_sra (
VIN STRING,
  PIID_InstallPCTTable BIGINT,
  Vendor STRING,
  Device_Nb STRING,
  DEVC_STTS_FRST_TS TIMESTAMP,
  DEVC_STTS_LAST_TS TIMESTAMP,
  LIFETM_DAYS_CT BIGINT,
  CNCTD_DAYS_CT BIGINT,
  LIFETM_SC_CT BIGINT,
  CNCTD_SC_CT BIGINT,
  DSCNCTD_SC_CT BIGINT,
  DEVC_CNCTD_PCT DECIMAL(35,15),
  DEVC_DSCNCTD_PCT DECIMAL(35,15),
  CNCTD_STTS_CT BIGINT,
  DSCNCTD_STTS_CT BIGINT,
  SCR_STRT_DT STRING,
  SCR_END_DT STRING,
  SCR_DAYS_CT INT,
  MODL_ACRNYM_TT STRING,
  MODL_OTPT_QTY INT,
  rated_score_1 INT,
  ANNL_MILG_QTY DECIMAL(35,15),
  PLSBL_DRIV_PCTG DECIMAL(35,15),
  Last_Score_Date TIMESTAMP,
  Start_Date_Assumed TIMESTAMP,
  End_Date_Assumed TIMESTAMP,
  CNCTD_DAYS_CT_Assumed INT,
  Number_of_Trips BIGINT,
  Total_Miles DECIMAL(28,10),
  Total_Adjusted_Miles DECIMAL(25,10),
  Total_Plausible_Miles DECIMAL(25,10),
  Total_Kilometers DECIMAL(28,10),
  Total_Adjusted_Kilometers DECIMAL(30,13),
  Total_Plausible_Kilometers DECIMAL(30,13),
  Total_Plausible_Driving_Seconds BIGINT,
  Total_NightTime_Driving_Seconds BIGINT,
  Total_Plausible_Idle_Seconds BIGINT,
  Total_Hard_Accelerations BIGINT,
  Total_Hard_Brakes BIGINT,
  LOAD_DT	date,
  LOAD_HR_TS timestamp)
USING delta
PARTITIONED BY (LOAD_DT)
LOCATION 's3://pcds-databricks-common-{account_id}/iot/delta/raw/analytics/dhf_iot_analytics_raw/sip_sra';""")


# COMMAND ----------

sra_df=spark.sql(f"""
select
coalesce(a.ENRLD_VIN_NB,b.ENRLD_VIN_NB) as VIN
,b.PRGRM_INSTC_ID as PIID_InstallPCTTable
,a.SRC_SYS_CD as Vendor
,coalesce(b.DEVC_KEY, a.DEVC_KEY) as Device_Nb


,b.DEVC_STTS_FRST_TS
,b.DEVC_STTS_LAST_TS
,b.LIFETM_DAYS_CT
,b.CNCTD_DAYS_CT
,b.LIFETM_SC_CT
,b.CNCTD_SC_CT
,b.DSCNCTD_SC_CT
,b.DEVC_CNCTD_PCT
,b.DEVC_DSCNCTD_PCT
,b.CNCTD_STTS_CT
,b.DSCNCTD_STTS_CT


,c.SCR_STRT_DT
,c.SCR_END_DT 
,c.SCR_DAYS_CT 
,c.MODL_ACRNYM_TT
,c.MODL_OTPT_QTY 
,c.rated_score_1
,c.ANNL_MILG_QTY
,c.PLSBL_DRIV_PCTG
,c.Last_Score_Date


,min(a.PE_STRT_LCL_TS) as Start_Date_Assumed
,max(a.PE_END_LCL_TS) as End_Date_Assumed
,datediff(min(date(a.PE_STRT_LCL_TS)), max(date(a.PE_END_LCL_TS))) as CNCTD_DAYS_CT_Assumed
,count(distinct a.TRIP_SMRY_KEY) as Number_of_Trips
,sum(a.MILE_CT) as Total_Miles
,sum(a.ADJST_MILE_CT) as Total_Adjusted_Miles
,sum(a.PLSBL_MILE_CT) as Total_Plausible_Miles
,sum(a.KM_CT) as Total_Kilometers
,sum(a.ADJST_MILE_CT*1.609) as Total_Adjusted_Kilometers
,sum(a.PLSBL_MILE_CT*1.609) as Total_Plausible_Kilometers
,sum(a.PLSBL_DRIV_SC_CT) as Total_Plausible_Driving_Seconds
,sum(a.NIGHT_TIME_DRVNG_SC_CT) as Total_NightTime_Driving_Seconds
,sum(a.PLSBL_IDLE_SC_CT) as Total_Plausible_Idle_Seconds
,sum(a.FAST_ACLRTN_CT) as Total_Hard_Accelerations
,sum(a.HARD_BRKE_CT) as Total_Hard_Brakes
from (select * from dhf_iot_curated_{environment}.trip_detail where src_sys_cd like '%SR%') as a
Full Outer join  (select * from dhf_iot_curated_{environment}.device_summary where src_sys_cd like '%SR%') as b on (a.ENRLD_VIN_NB = b.ENRLD_VIN_NB and a.SRC_SYS_CD = b.SRC_SYS_CD)

Left Join 

(SELECT a.ENRLD_VIN_NB, a.DEVC_KEY, a.SRC_SYS_CD, a.PRGRM_INSTC_ID, a.SCR_STRT_DT, a.SCR_END_DT, a.SCR_DAYS_CT, a.MODL_ACRNYM_TT, a.MODL_OTPT_QTY, case
when a.INSTL_PCTG < 95 then 998
when (a.PLSBL_DRIV_PCTG < 97 or a.PLSBL_DRIV_PCTG is null) then 997
else case
when a.MODL_OTPT_QTY < 1  then 1
when a.MODL_OTPT_QTY > 995  then 995
else a.MODL_OTPT_QTY end
end
as rated_score_1, a.ANNL_MILG_QTY, a.PLSBL_DRIV_PCTG, b.Last_Score_Date  
FROM dhf_iot_curated_{environment}.INbound_score_elements a

Join (SELECT CAST(DEVC_KEY AS BIGINT), ENRLD_VIN_NB, SRC_SYS_CD, PRGRM_INSTC_ID, Max(MODL_OTPT_TS) as Last_Score_Date  
      FROM dhf_iot_curated_{environment}.INbound_score_elements
      group by CAST(DEVC_KEY AS BIGINT), ENRLD_VIN_NB, PRGRM_INSTC_ID, SRC_SYS_CD) b 
      on (CAST(a.DEVC_KEY AS BIGINT)= b.DEVC_KEY) and  (a.ENRLD_VIN_NB = b.ENRLD_VIN_NB) and (a.SRC_SYS_CD = b.SRC_SYS_CD) and (a.PRGRM_INSTC_ID = b.PRGRM_INSTC_ID) where a.SRC_SYS_CD like '%SR%' and  b.SRC_SYS_CD like '%SR%' and MODL_ACRNYM_TT='ND1'  and (a.MODL_OTPT_TS = b.Last_Score_Date)) c 
	  on (coalesce(a.ENRLD_VIN_NB, b.ENRLD_VIN_NB) = c.ENRLD_VIN_NB) and (b.PRGRM_INSTC_ID = c.PRGRM_INSTC_ID)  and (a.SRC_SYS_CD = c.SRC_SYS_CD)

where a.SRC_SYS_CD like '%SR%'  

group by
coalesce(a.ENRLD_VIN_NB,b.ENRLD_VIN_NB)
,b.PRGRM_INSTC_ID
,a.SRC_SYS_CD
,coalesce(b.DEVC_KEY, a.DEVC_KEY)
,b.DEVC_STTS_FRST_TS
,b.DEVC_STTS_LAST_TS
,b.LIFETM_DAYS_CT
,b.CNCTD_DAYS_CT
,b.LIFETM_SC_CT
,b.CNCTD_SC_CT
,b.DSCNCTD_SC_CT
,b.DEVC_CNCTD_PCT
,b.DEVC_DSCNCTD_PCT
,b.CNCTD_STTS_CT
,b.DSCNCTD_STTS_CT
,c.SCR_STRT_DT
,c.SCR_END_DT 
,c.SCR_DAYS_CT 
,c.MODL_ACRNYM_TT
,c.MODL_OTPT_QTY 
,c.rated_score_1 
,c.ANNL_MILG_QTY
,c.PLSBL_DRIV_PCTG
,c.Last_Score_Date
 """)


# COMMAND ----------

from pyspark.sql.functions import *
sra_df.withColumn("LOAD_HR_TS",current_timestamp())     .withColumn("LOAD_DT",current_date()).write.format("delta").mode("append").partitionBy('LOAD_DT').saveAsTable("dhf_iot_analytics_raw.sip_sra")

# COMMAND ----------

# display(sra_df)