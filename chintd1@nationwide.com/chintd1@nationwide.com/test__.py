# Databricks notebook source
# MAGIC %sql
# MAGIC select load_dt,count(*) from dhf_iot_harmonized_prod.trip_point where src_sys_cd='IMS_SM_5X' group by load_dt order by load_dt desc

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.functions.input_file_name
# MAGIC val df=spark.read.format("binaryFile").load("s3://dw-internal-pl-cmt-telematics-785562577411/CMT/Archive/tar/load_date=2023-05-24/0900/DS_CMT_0058aade-0f89-4388-bc9a-f35dbcb2f542/")
# MAGIC val nbFiles = df.select(input_file_name()).distinct.count

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from  dhf_iot_harmonized_prod.trip_point where src_sys_cd='IMS_SM_5X' and load_dt in ('2022-08-17','2022-08-18')

# COMMAND ----------

# MAGIC
# MAGIC %python
# MAGIC import pyspark.sql 
# MAGIC from pyspark.sql import Row
# MAGIC from pyspark.sql.types import StructType, StructField, StringType
# MAGIC from pyspark.sql.functions import *
# MAGIC import datetime
# MAGIC from datetime import datetime
# MAGIC import io
# MAGIC import re
# MAGIC import json
# MAGIC from itertools import groupby
# MAGIC from operator import itemgetter
# MAGIC import io
# MAGIC import zipfile
# MAGIC import pandas as pd
# MAGIC from pyspark.sql.types import *
# MAGIC import datetime 
# MAGIC import json
# MAGIC import boto3
# MAGIC import time
# MAGIC import ast
# MAGIC from datetime import datetime as dt
# MAGIC import time
# MAGIC from pyspark.sql import functions as f #2
# MAGIC from pyspark.sql.session import SparkSession
# MAGIC from pyspark.sql import DataFrame
# MAGIC from dataclasses import dataclass
# MAGIC from pyspark.sql.functions import window 
# MAGIC from pyspark.sql import *
# MAGIC # df=spark.sql("""select cast(row_number() over(order by NULL) + coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_prod.trip_point),0) as BIGINT) as TRIP_POINT_ID, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, cast(acceleration as decimal(18,5)) as ACLRTN_RT, cast(ambientTemperature as decimal(15,6)) as AMBNT_TMPRTR_QTY, cast(barometericPressure as decimal(15,6)) as BRMTRC_PRESSR_QTY, cast(coolantTemperature as decimal(15,6)) as COOLNT_TMPRTR_QTY, cast(engineRPM as decimal(18,5)) as ENGIN_RPM_RT, cast(fuelLevel as decimal(15,6))as FUEL_LVL_QTY, cast(headingDegrees as decimal(15,6)) as HEADNG_DEG_QTY, coalesce(cast(utcDateTime as TIMESTAMP), to_timestamp("9999-12-31")) as UTC_TS, cast(speed as decimal(18,10)) as SPD_RT, cast(accelerometerData as string) as ACLRTMTR_DATA_RT, cast(horizontalAccuracy as decimal(15,6)) as HRZNTL_ACCRCY_QTY, cast(degreesLatitude as decimal(18,10)) as LAT_NB, cast(degreesLongitude as decimal(18,10)) as LNGTD_NB, cast(throttlePosition as decimal(15,6)) as THRTL_PSTN_NB, cast(verticalAccuracy as decimal(15,6)) as VRTCL_ACCRCY_QTY, tripSummaryId AS TRIP_SMRY_KEY, cast(hdop as decimal(18,10)) as HDOP_QTY, cast(Accel_Longitudinal as decimal(18,10)) as LNGTDNL_ACCLRTN_RT, cast(Accel_Lateral as decimal(18,10)) as LATRL_ACCLRTN_RT, cast(Accel_Vertical as decimal(18,10)) as VRTCL_ACCLRTN_RT,coalesce(deviceIdentifier,deviceSerialNumber,crc32(detectedvin)) AS DEVC_KEY, cast(timeZoneOffset as decimal(10,2)) as TIME_ZONE_OFFST_NB, coalesce(enrolledVin,detectedVin) AS ENRLD_VIN_NB, coalesce(load_date, to_date("9999-12-31")) as LOAD_DT, coalesce(sourcesystem,"NOKEY") AS SRC_SYS_CD, coalesce(load_hour, to_timestamp("9999-12-31")) as LOAD_HR_TS from dhf_iot_ims_raw_prod.telemetrypoints where sourcesystem="IMS_SM_5X" and load_date  in ('2022-08-17','2022-08-18')""")
# MAGIC # w = Window.partitionBy("TRIP_SMRY_KEY","UTC_TS").orderBy(col("ETL_ROW_EFF_DTS").desc())
# MAGIC # firsRowDF = df.withColumn("rownum", row_number().over(w)).where(col("rownum") == 1).drop("rownum")
# MAGIC # display(firsRowDF)
# MAGIC # firsRowDF.write.format("del¿ta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_point")
# MAGIC
# MAGIC # firsRowDF.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT (*) FROM (select tripsummaryid,utcdatetime,count(*) from  dhf_iot_ims_raw_prod.telemetrypoints where load_date in ('2022-08-17','2022-08-18') and sourcesystem='IMS_SM_5X'  group by tripsummaryid,utcdatetime)

# COMMAND ----------

# MAGIC %sql
# MAGIC select sourcesystem,count(*) from  dhf_iot_ims_raw_prod.telemetrypoints where load_date='2022-08-17' group by sourcesystem
# MAGIC -- select tripsummaryid,utcdatetime,count(*) from  dhf_iot_ims_raw_prod.telemetrypoints where load_date='2022-08-13' group by tripsummaryid,utcdatetime having count(*)>1

# COMMAND ----------

def smartmiles_score_df(harmonizedDB,curateDB):
  smartmiles_df=spark.sql(f"""SELECT
b.PRGRM_INSTC_ID
,a.SRC_SYS_CD
,a.DEVC_KEY
,a.ENRLD_VIN_NB
,MAX(PE_STRT_TS) AS MAX_PE_STRT_TS
,cast(max(a.PE_END_TS) as string) as MAX_PE_END_TS
,sum(DRVNG_SC_CT) as DRVNG_SC_CT
,sum(FAST_ACLRTN_CT) AS FAST_ACLRTN_CT
,sum(HARD_BRKE_CT) AS HARD_BRKE_CT
,sum(PLSBL_MILE_CT) AS PLSBL_MILE_CT
,sum(ADJST_MILE_CT) AS ADJST_MILE_CT
,sum(PLSBL_DRIV_SC_CT) AS PLSBL_DRIV_SC_CT
,sum(PLSBL_IDLE_SC_CT) AS PLSBL_IDLE_SC_CT
,(sum(PLSBL_DRIV_SC_CT) - sum(PLSBL_IDLE_SC_CT)) AS no_idle_time
,CASE WHEN sum(DRVNG_SC_CT)= 0 THEN 0.0 ELSE CAST (round((sum(PLSBL_DRIV_SC_CT)/sum(DRVNG_SC_CT)) * 100,2) AS double) END  AS plausible_percentage
,b.PLCY_ST_CD
,least(cast((sum(ADJST_MILE_CT*1.60934)/sum(MILE_CT*1.60934)) as double),1.5) as weighted_time_of_day
,greatest(0.000000001,least(2.0,cast(sum(STOP_SC_CT)/sum(MILE_CT*1.60934) as decimal(35,15)))) as STOPS_PER_KM_RT
,greatest(0.000000001,least(2.0,cast(sum(HARD_BRKE_CT)/sum(MILE_CT*1.60934) as decimal(35,15)))) as BRAKES_PER_KM_RT
,max(a.LOAD_HR_TS) AS LOAD_HR_TS
,max(a.LOAD_DT) AS LOAD_DT
from  ( 
	select	* 
	from	{curateDB}.trip_detail trips 
	inner join (select distinct trip_smry_key, load_dt as load_dt_mb, load_hr_ts as load_hr_ts_mb from dhf_iot_curated_prod.outbound_score_elements_chlg) micro_batch 
		on trips.TRIP_SMRY_KEY = micro_batch.TRIP_SMRY_KEY) a

inner join
(
    select
    VIN_NB,
    PRGRM_INSTC_ID,
    PRGRM_TERM_BEG_DT,
    PRGRM_TERM_END_DT,
    PLCY_ST_CD
    from {harmonizedDB}.INTEGRATED_ENROLLMENT
    where LOAD_DT = (select max(LOAD_DT) from {harmonizedDB}.INTEGRATED_ENROLLMENT)
) b
on a.ENRLD_VIN_NB = trim(b.VIN_NB)
inner join 
{curateDB}.device_summary device
ON device.ENRLD_VIN_NB = a.ENRLD_VIN_NB 
    
where  trim(b.PLCY_ST_CD) <> 'CA'
AND device.DATA_CLCTN_ID IS NOT NULL
and a.PE_STRT_TS between b.PRGRM_TERM_BEG_DT and b.PRGRM_TERM_END_DT
and device.PRGRM_INSTC_ID IS NOT NULL
and a.PE_STRT_TS  BETWEEN device.DEVC_STTS_FRST_TS AND device.DEVC_STTS_LAST_TS
AND a.SRC_SYS_CD like 'IMS_SM%'
group by b.PRGRM_INSTC_ID,a.SRC_SYS_CD,a.DEVC_KEY,a.ENRLD_VIN_NB,b.PLCY_ST_CD""")
  return smartmiles_df
  

# COMMAND ----------

def smartride_score_df(harmonizedDB,curateDB):
  current_date = datetime.now()
  print(f"Current Date is : {current_date}")
  date_filter_18_mo_ago = (current_date-dateutil.relativedelta.relativedelta(months=18)).strftime("%Y-%m-%d")
  print(f"date_filter_18_mo_ago : {date_filter_18_mo_ago}")
  smartride_df=spark.sql(f"""SELECT
b.PRGRM_INSTC_ID
,a.SRC_SYS_CD
,cast(a.DEVC as string) as DEVC_KEY
,a.ENRLD_VIN_NB
,MAX(PE_STRT_TS) AS MAX_PE_STRT_TS
,cast(max(a.PE_END_TS) as string) as MAX_PE_END_TS
,sum(DRVNG_SC_CT) as DRVNG_SC_CT
,sum(FAST_ACLRTN_CT) AS FAST_ACLRTN_CT
,sum(HARD_BRKE_CT) AS HARD_BRKE_CT
,sum(PLSBL_MILE_CT) AS PLSBL_MILE_CT
,sum(ADJST_MILE_CT) AS ADJST_MILE_CT
,sum(PLSBL_DRIV_SC_CT) AS PLSBL_DRIV_SC_CT
,sum(PLSBL_IDLE_SC_CT) AS PLSBL_IDLE_SC_CT
,(sum(PLSBL_DRIV_SC_CT) - sum(PLSBL_IDLE_SC_CT)) AS no_idle_time
,CASE WHEN sum(DRVNG_SC_CT)= 0 THEN 0.0 ELSE CAST (round((sum(PLSBL_DRIV_SC_CT)/sum(DRVNG_SC_CT)) * 100,2) AS double) END  AS plausible_percentage
,b.PLCY_ST_CD
,least(cast((sum(ADJST_MILE_CT*1.60934)/sum(MILE_CT*1.60934)) as double),1.5) as weighted_time_of_day
,greatest(0.000000001,least(2.0,cast(sum(STOP_SC_CT)/sum(MILE_CT*1.60934) as decimal(35,15)))) as STOPS_PER_KM_RT
,greatest(0.000000001,least(2.0,cast(sum(HARD_BRKE_CT)/sum(MILE_CT*1.60934) as decimal(35,15)))) as BRAKES_PER_KM_RT
,max(a.LOAD_HR_TS) AS LOAD_HR_TS
,max(a.LOAD_DT) AS LOAD_DT
from  ( 
	select	*,cast(devc_key as bigint) as devc
	from	{curateDB}.trip_detail trips 
	inner join (select distinct trip_smry_key, load_dt as load_dt_mb, load_hr_ts as load_hr_ts_mb from dhf_iot_curated_prod.outbound_score_elements_chlg) micro_batch 
		on trips.TRIP_SMRY_KEY = micro_batch.TRIP_SMRY_KEY) a

inner join
(
    select
    VIN_NB,
    PRGRM_INSTC_ID,
    PRGRM_TERM_BEG_DT,
    PRGRM_TERM_END_DT,
    PLCY_ST_CD
    from {harmonizedDB}.INTEGRATED_ENROLLMENT
    where PRGRM_TERM_BEG_DT >= '{date_filter_18_mo_ago}'
    and LOAD_DT = (select max(LOAD_DT) from {harmonizedDB}.INTEGRATED_ENROLLMENT)
) b
on a.ENRLD_VIN_NB = trim(b.VIN_NB)
inner join 
{curateDB}.device_summary device
ON device.ENRLD_VIN_NB = a.ENRLD_VIN_NB 
    
where trim( b.PLCY_ST_CD) <> 'CA'
and a.PE_STRT_TS between b.PRGRM_TERM_BEG_DT and b.PRGRM_TERM_END_DT
and device.PRGRM_INSTC_ID IS NOT NULL
and a.PE_STRT_TS  BETWEEN device.DEVC_STTS_FRST_TS AND device.DEVC_STTS_LAST_TS
AND a.SRC_SYS_CD in ('IMS_SR_4X','IMS_SR_5X','FMC_SR','TIMS_SR')
group by b.PRGRM_INSTC_ID,a.SRC_SYS_CD,a.devc,a.ENRLD_VIN_NB,b.PLCY_ST_CD""")
  return smartride_df

# COMMAND ----------

def final_score(view,curateDB):
  combined_df=spark.sql(f""" 
select distinct	cast(device.DEVC_KEY as string) as DEVC_KEY, device.ENRLD_VIN_NB as ENRLD_VIN_NB,
		cast(device.PRGRM_INSTC_ID as bigint) as PRGRM_INSTC_ID, device.DATA_CLCTN_ID as DATA_CLCTN_ID, trip.SRC_SYS_CD as SRC_SYS_CD,
		 MAX_PE_END_TS, cast(current_timestamp() as string) as TRANSACTION_TS,
		cast(device.DEVC_STTS_FRST_TS as string) as SCR_STRT_DT, cast(device.DEVC_STTS_LAST_TS as string) as SCR_END_DT,
		cast(device.LIFETM_DAYS_CT as int) as SCR_DAYS_CT, cast(device.CNCTD_DAYS_CT as BIGINT) as DAYS_INSTL_CT,
		cast(
		case 
			when ADJST_MILE_CT is Null then 0.0 
			else ADJST_MILE_CT 
		end as decimal(35,15)) as ADJST_MILES_QTY, cast(
		case 
			when PLSBL_MILE_CT is null then 0.0 
			else PLSBL_MILE_CT 
		end as decimal(35,15)) as SCRBD_MILES_QTY, cast(
		case 
			when ADJST_MILE_CT is Null then 0.0 
			else ADJST_MILE_CT end/CNCTD_DAYS_CT as decimal(35,15)) as AVG_DISTNC_QTY,
		cast (
		case 
			when CNCTD_DAYS_CT = 0 then 0.0 
			when ( 
		case 
			when HARD_BRKE_CT is null then 0 
			else cast(HARD_BRKE_CT as INT) end /CNCTD_DAYS_CT)> 7.0 then 7.0 
			else (
		case 
			when HARD_BRKE_CT is null then 0 
			else cast(HARD_BRKE_CT as INT) end/CNCTD_DAYS_CT) 
		end as decimal(35,15)) as HARD_BRK_PCTG_DISC_VAL, cast (
		case 
			when CNCTD_DAYS_CT = 0 then 0.0 
			when (
		case 
			when FAST_ACLRTN_CT is null then 0 
			else cast(FAST_ACLRTN_CT as int) end/CNCTD_DAYS_CT)> 4.0 then 4.0 
			else (
		case 
			when FAST_ACLRTN_CT is null then 0 
			else cast(FAST_ACLRTN_CT as int) end/CNCTD_DAYS_CT) 
		end as decimal(35,15)) as FAST_ACLRTN_PCTG_DISC_VAL, cast(
		case 
			when (PLSBL_IDLE_SC_CT is null 
	or (PLSBL_DRIV_SC_CT - PLSBL_IDLE_SC_CT) is null) then 0.0 
			else (PLSBL_IDLE_SC_CT/(PLSBL_IDLE_SC_CT+(PLSBL_DRIV_SC_CT - PLSBL_IDLE_SC_CT))) 
		end as decimal(35,15)) as IDLE_TIME_PCTG,cast(device.DEVC_CNCTD_PCT as decimal(35,15)) as INSTL_PCTG,
		cast(
		case 
			when (
		case 
			when DRVNG_SC_CT= 0 then 0.0 
			else cast (round((PLSBL_DRIV_SC_CT/DRVNG_SC_CT) * 100,2) as double) end) is null then 100.00 
			else (
		case 
			when DRVNG_SC_CT= 0 then 0.0 
			else cast (round((PLSBL_DRIV_SC_CT/DRVNG_SC_CT) * 100,2) as double) end) 
		end as decimal(35,15)) as PLSBL_DRIV_PCTG, cast(weighted_time_of_day as decimal(35,15)) as WGHT_TIME_OF_DY_RT,
		 STOPS_PER_KM_RT,
		BRAKES_PER_KM_RT,
		cast(device.DEVC_DSCNCTD_PCT as decimal(35,15)) as DSCNCTD_TIME_PCTG,
		cast(
		case 
			when (CNCTD_DAYS_CT is null 
	or CNCTD_DAYS_CT = 0.0) then 0.0 
			else (((
		case 
			when PLSBL_MILE_CT is null then 0.0 
			else PLSBL_MILE_CT end)/CNCTD_DAYS_CT) * 365) 
		end as decimal(35,15)) as ANNL_MILG_QTY, concat(concat(LPAD(trim(cast(if (device.CNCTD_SC_CT is null,' ',device.CNCTD_SC_CT) as varchar(3))),3,'000'),'_'),LPAD(trim(cast(if (device.DSCNCTD_SC_CT is null,' ',device.DSCNCTD_SC_CT) as varchar(3))),3,'000'))  as CNCT_DSCNCT_CT, cast(device.DSCNCTD_SC_CT as BIGINT) as UNINSTL_DAYS_CT,
		trip.LOAD_DT as LOAD_DT, trip.LOAD_HR_TS  
from	{view} trip 
INNER JOIN {curateDB}.device_summary device
	ON device.ENRLD_VIN_NB = trip.ENRLD_VIN_NB
    and device.PRGRM_INSTC_ID IS NOT NULL
and trip.MAX_PE_STRT_TS  BETWEEN device.DEVC_STTS_FRST_TS AND device.DEVC_STTS_LAST_TS
""")
  return combined_df
 

# COMMAND ----------

from datetime import datetime
import dateutil
smartmiles_score_df("dhf_iot_harmonized_prod","dhf_iot_curated_prod").createOrReplaceTempView("smartmiles_view")
smartmiles_df = final_score("smartmiles_view","dhf_iot_curated_prod")
  
smartride_score_df("dhf_iot_harmonized_prod","dhf_iot_curated_prod").createOrReplaceTempView("smartride_view")
smartride_df = final_score("smartride_view","dhf_iot_curated_prod")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from smartmiles_view

# COMMAND ----------

display(smartmiles_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from smartride_view where enrld_vin_nb='1B7HF16Y8TS608037'

# COMMAND ----------

display(smartride_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.device_summary where enrld_vin_nb='1B7HF16Y8TS608037'

# COMMAND ----------

def final_score(view,curateDB):
  combined_df=spark.sql(f""" 
select distinct	cast(device.DEVC_KEY as string) as DEVC_KEY, device.ENRLD_VIN_NB as ENRLD_VIN_NB,
		cast(device.PRGRM_INSTC_ID as bigint) as PRGRM_INSTC_ID, device.DATA_CLCTN_ID as DATA_CLCTN_ID, trip.SRC_SYS_CD as SRC_SYS_CD,
		 MAX_PE_END_TS, cast(current_timestamp() as string) as TRANSACTION_TS,
		cast(device.DEVC_STTS_FRST_TS as string) as SCR_STRT_DT, cast(device.DEVC_STTS_LAST_TS as string) as SCR_END_DT,
		cast(device.LIFETM_DAYS_CT as int) as SCR_DAYS_CT, cast(device.CNCTD_DAYS_CT as BIGINT) as DAYS_INSTL_CT,
		cast(
		case 
			when ADJST_MILE_CT is Null then 0.0 
			else ADJST_MILE_CT 
		end as decimal(35,15)) as ADJST_MILES_QTY, cast(
		case 
			when PLSBL_MILE_CT is null then 0.0 
			else PLSBL_MILE_CT 
		end as decimal(35,15)) as SCRBD_MILES_QTY, cast(
		case 
			when ADJST_MILE_CT is Null then 0.0 
			else ADJST_MILE_CT end/CNCTD_DAYS_CT as decimal(35,15)) as AVG_DISTNC_QTY,
		cast (
		case 
			when CNCTD_DAYS_CT = 0 then 0.0 
			when ( 
		case 
			when HARD_BRKE_CT is null then 0 
			else cast(HARD_BRKE_CT as INT) end /CNCTD_DAYS_CT)> 7.0 then 7.0 
			else (
		case 
			when HARD_BRKE_CT is null then 0 
			else cast(HARD_BRKE_CT as INT) end/CNCTD_DAYS_CT) 
		end as decimal(35,15)) as HARD_BRK_PCTG_DISC_VAL, cast (
		case 
			when CNCTD_DAYS_CT = 0 then 0.0 
			when (
		case 
			when FAST_ACLRTN_CT is null then 0 
			else cast(FAST_ACLRTN_CT as int) end/CNCTD_DAYS_CT)> 4.0 then 4.0 
			else (
		case 
			when FAST_ACLRTN_CT is null then 0 
			else cast(FAST_ACLRTN_CT as int) end/CNCTD_DAYS_CT) 
		end as decimal(35,15)) as FAST_ACLRTN_PCTG_DISC_VAL, cast(
		case 
			when (PLSBL_IDLE_SC_CT is null 
	or (PLSBL_DRIV_SC_CT - PLSBL_IDLE_SC_CT) is null) then 0.0 
			else (PLSBL_IDLE_SC_CT/(PLSBL_IDLE_SC_CT+(PLSBL_DRIV_SC_CT - PLSBL_IDLE_SC_CT))) 
		end as decimal(35,15)) as IDLE_TIME_PCTG,cast(device.DEVC_CNCTD_PCT as decimal(35,15)) as INSTL_PCTG,
		cast(
		case 
			when (
		case 
			when DRVNG_SC_CT= 0 then 0.0 
			else cast (round((PLSBL_DRIV_SC_CT/DRVNG_SC_CT) * 100,2) as double) end) is null then 100.00 
			else (
		case 
			when DRVNG_SC_CT= 0 then 0.0 
			else cast (round((PLSBL_DRIV_SC_CT/DRVNG_SC_CT) * 100,2) as double) end) 
		end as decimal(35,15)) as PLSBL_DRIV_PCTG, cast(weighted_time_of_day as decimal(35,15)) as WGHT_TIME_OF_DY_RT,
		 STOPS_PER_KM_RT,
		BRAKES_PER_KM_RT,
		cast(device.DEVC_DSCNCTD_PCT as decimal(35,15)) as DSCNCTD_TIME_PCTG,
		cast(
		case 
			when (CNCTD_DAYS_CT is null 
	or CNCTD_DAYS_CT = 0.0) then 0.0 
			else (((
		case 
			when PLSBL_MILE_CT is null then 0.0 
			else PLSBL_MILE_CT end)/CNCTD_DAYS_CT) * 365) 
		end as decimal(35,15)) as ANNL_MILG_QTY, concat(concat(LPAD(trim(cast(if (device.CNCTD_SC_CT is null,' ',device.CNCTD_SC_CT) as varchar(3))),3,'000'),'_'),LPAD(trim(cast(if (device.DSCNCTD_SC_CT is null,' ',device.DSCNCTD_SC_CT) as varchar(3))),3,'000'))  as CNCT_DSCNCT_CT, cast(device.DSCNCTD_SC_CT as BIGINT) as UNINSTL_DAYS_CT,
		trip.LOAD_DT as LOAD_DT, trip.LOAD_HR_TS  
from	{view} trip 
INNER JOIN {curateDB}.device_summary device
	ON device.ENRLD_VIN_NB = trip.ENRLD_VIN_NB
""")
 

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/delta/curated/dhf_iot_curated_prod/DEVICE_STATUS/SRC_SYS_CD=IMS_SR_4X/",True)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC       distinct
# MAGIC         cast(
# MAGIC           row_number() over(
# MAGIC             order by
# MAGIC               NULL
# MAGIC           ) + coalesce(
# MAGIC             (
# MAGIC               select
# MAGIC                 max(INTEGRATED_ENROLLMENT_ID)
# MAGIC               from
# MAGIC                 dhf_iot_harmonized_prod.integrated_enrollment
# MAGIC             ),
# MAGIC             0
# MAGIC           ) as BIGINT
# MAGIC         ) as INTEGRATED_ENROLLMENT_ID,
# MAGIC       current_timestamp() as ETL_LAST_UPDT_DTS,
# MAGIC       coalesce(VIN_NB, ENRLD_VIN_NB, 'NOKEY') as VIN_NB,
# MAGIC       coalesce(DEVC_ID, DEVC_ID_NB) as DEVC_ID,
# MAGIC       coalesce(
# MAGIC         ENRLMNT_EFCTV_DT,
# MAGIC         ENRLMNT_DT,
# MAGIC         to_date('9999-12-31')
# MAGIC       ) as ENRLMNT_EFCTV_DT,
# MAGIC       PRGRM_INSTC_ID,
# MAGIC       DATA_CLCTN_ID,
# MAGIC       cast(
# MAGIC         coalesce(
# MAGIC           PRGRM_TERM_BEG_DT,
# MAGIC           ACTV_STRT_DT,
# MAGIC           to_date('1000-01-01')
# MAGIC         ) as DATE
# MAGIC       ) as PRGRM_TERM_BEG_DT,
# MAGIC       cast(
# MAGIC         coalesce(
# MAGIC           PRGRM_TERM_END_DT,
# MAGIC           ACTV_END_DT,
# MAGIC           to_date('9999-12-31')
# MAGIC         ) as DATE
# MAGIC       ) as PRGRM_TERM_END_DT,
# MAGIC       coalesce(PLCY_ST_CD, PLCY_RT_ST_CD, 'NOKEY') as PLCY_ST_CD,
# MAGIC       coalesce(pef.VNDR_CD, ods.SRC_SYS_CD) as SRC_SYS_CD,
# MAGIC       current_date() as LOAD_DT,
# MAGIC       to_timestamp(
# MAGIC         date_format(current_timestamp(), 'yyyy-MM-dd H:00:00+0000')
# MAGIC       ) as LOAD_HR_TS
# MAGIC     from
# MAGIC       (select v.*, case when trim(DEVC_ID_SP)='' then DEVC_ID_NEW
# MAGIC                    else DEVC_ID_SP
# MAGIC                    end as DEVC_ID from (
# MAGIC         select
# MAGIC           distinct first(VNDR_CD) as VNDR_CD,
# MAGIC           DATA_CLCTN_ID,
# MAGIC           first(VIN_NB) as VIN_NB,
# MAGIC           first(DEVC_ID) as DEVC_ID_SP,
# MAGIC           -- DEVC_ID as DEVC_ID,
# MAGIC           first(ENRLMNT_EFCTV_DT) as ENRLMNT_EFCTV_DT,
# MAGIC           PLCY_ST_CD,
# MAGIC           min(PRGRM_TERM_BEG_DT) as PRGRM_TERM_BEG_DT,
# MAGIC           max(PRGRM_TERM_END_DT) as PRGRM_TERM_END_DT,
# MAGIC           max(LOAD_HR_TS) as LOAD_HR_TS
# MAGIC         from
# MAGIC           (
# MAGIC             select
# MAGIC               distinct coalesce(a.VNDR_CD, 'PE') as VNDR_CD,
# MAGIC               a.DATA_CLCTN_ID as DATA_CLCTN_ID,
# MAGIC               a.VIN_NB,
# MAGIC               trim(a.DEVC_ID) as DEVC_ID,
# MAGIC               a.ENRLMNT_EFCTV_DT,
# MAGIC               a.PLCY_ST_CD,
# MAGIC               a.PRGRM_TERM_BEG_DT,
# MAGIC               a.PRGRM_TERM_END_DT,
# MAGIC               a.LOAD_HR_TS
# MAGIC             from
# MAGIC               dhf_iot_harmonized_prod.program_enrollment a
# MAGIC               inner join (
# MAGIC                 select
# MAGIC                   distinct pe.DATA_CLCTN_ID,
# MAGIC                   pe.VIN_NB,
# MAGIC                   pe.PLCY_ST_CD
# MAGIC                 from
# MAGIC                   dhf_iot_harmonized_prod.program_enrollment pe
# MAGIC                   inner join (
# MAGIC                     select
# MAGIC                       distinct VIN_NB,
# MAGIC                       max(EVNT_SYS_TS) as mrRec,
# MAGIC                       max(ENRLMNT_EFCTV_DT) as mrE
# MAGIC                     from
# MAGIC                       dhf_iot_harmonized_prod.program_enrollment
# MAGIC                     where
# MAGIC                       (
# MAGIC                         PRGRM_TERM_BEG_DT is not null
# MAGIC                         or PRGRM_TERM_END_DT is not null
# MAGIC                       )
# MAGIC                       and trim(VNDR_CD) not in ('CMT', 'LN')
# MAGIC                       and DATA_CLCTN_STTS = 'Active'
# MAGIC                       and PRGRM_STTS_CD in ('Active', 'Cancelled')
# MAGIC                     group by
# MAGIC                       VIN_NB
# MAGIC                   ) pe1 on pe.VIN_NB = pe1.VIN_NB
# MAGIC                   and pe.ENRLMNT_EFCTV_DT = mrE
# MAGIC                   and pe.EVNT_SYS_TS = mrRec
# MAGIC               ) b on a.DATA_CLCTN_ID = b.DATA_CLCTN_ID
# MAGIC               and a.VIN_NB = b.VIN_NB
# MAGIC               and a.PLCY_ST_CD = b.PLCY_ST_CD
# MAGIC           )
# MAGIC         group by
# MAGIC           DATA_CLCTN_ID,
# MAGIC           PLCY_ST_CD
# MAGIC       ) v left join ( select  VIN_NB,first(DEVC_ID) AS DEVC_ID_NEW from (select distinct Z.VIN_NB,Z.DEVC_ID FROM (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD in ('Active', 'Cancelled') and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN')) z inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD in ('Active', 'Cancelled') and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) y on z.VIN_NB=y.VIN_NB and z.EVNT_SYS_TS=max_e and z.ENRLMNT_EFCTV_DT=max_tbd)  group by VIN_NB ) q on v.VIN_NB=q.VIN_NB)  pef full
# MAGIC       outer join (
# MAGIC         select
# MAGIC           distinct 'ODS' as SRC_SYS_CD,
# MAGIC           PRGRM_INSTC_ID,
# MAGIC           trim(ods1.ENRLD_VIN_NB) as ENRLD_VIN_NB,
# MAGIC           DEVC_ID_NB,
# MAGIC           ENRLMNT_DT,
# MAGIC           PLCY_RT_ST_CD,
# MAGIC           ACTV_STRT_DT,
# MAGIC           ACTV_END_DT
# MAGIC         from
# MAGIC           dhf_iot_harmonized_prod.ods_table ods1
# MAGIC           inner join (
# MAGIC             select
# MAGIC               distinct trim(ENRLD_VIN_NB) as ENRLD_VIN_NB,
# MAGIC               max(PRGRM_INSTC_ID) as maxPIID
# MAGIC             from
# MAGIC               dhf_iot_harmonized_prod.ods_table
# MAGIC             where
# MAGIC               VHCL_STTS_CD = 'E'
# MAGIC               and ACTV_END_DT = '3500-01-01'
# MAGIC               and LOAD_DT = (select max(LOAD_DT) from dhf_iot_harmonized_prod.ods_table)
# MAGIC             group by
# MAGIC               ENRLD_VIN_NB
# MAGIC           ) ods2 on trim(ods1.ENRLD_VIN_NB) = trim(ods2.ENRLD_VIN_NB)
# MAGIC           and ods1.PRGRM_INSTC_ID = ods2.maxPIID
# MAGIC         where
# MAGIC           VHCL_STTS_CD = 'E'
# MAGIC           and ACTV_END_DT = '3500-01-01'
# MAGIC       ) ods on pef.VIN_NB = trim(ods.ENRLD_VIN_NB)

# COMMAND ----------

def smartmiles_score_df(harmonizedDB,curateDB):
  smartmiles_df=spark.sql(f"""SELECT
b.PRGRM_INSTC_ID
,a.SRC_SYS_CD
,a.DEVC_KEY
,a.ENRLD_VIN_NB
,cast(max(a.PE_END_TS) as string) as MAX_PE_END_TS
,sum(DRVNG_SC_CT) as DRVNG_SC_CT
,sum(FAST_ACLRTN_CT) AS FAST_ACLRTN_CT
,sum(HARD_BRKE_CT) AS HARD_BRKE_CT
,sum(PLSBL_MILE_CT) AS PLSBL_MILE_CT
,sum(ADJST_MILE_CT) AS ADJST_MILE_CT
,sum(PLSBL_DRIV_SC_CT) AS PLSBL_DRIV_SC_CT
,sum(PLSBL_IDLE_SC_CT) AS PLSBL_IDLE_SC_CT
,(sum(PLSBL_DRIV_SC_CT) - sum(PLSBL_IDLE_SC_CT)) AS no_idle_time
,CASE WHEN sum(DRVNG_SC_CT)= 0 THEN 0.0 ELSE CAST (round((sum(PLSBL_DRIV_SC_CT)/sum(DRVNG_SC_CT)) * 100,2) AS double) END  AS plausible_percentage
,b.PLCY_ST_CD
,least(cast((sum(ADJST_MILE_CT*1.60934)/sum(MILE_CT*1.60934)) as double),1.5) as weighted_time_of_day
,greatest(0.000000001,least(2.0,cast(sum(STOP_SC_CT)/sum(MILE_CT*1.60934) as decimal(35,15)))) as STOPS_PER_KM_RT
,greatest(0.000000001,least(2.0,cast(sum(HARD_BRKE_CT)/sum(MILE_CT*1.60934) as decimal(35,15)))) as BRAKES_PER_KM_RT
,max(a.LOAD_HR_TS) AS LOAD_HR_TS
,max(a.LOAD_DT) AS LOAD_DT
from  ( 
	select	* 
	from	{curateDB}.trip_detail trips 
	inner join (select distinct trip_smry_key, load_dt as load_dt_mb, load_hr_ts as load_hr_ts_mb from global_temp.microBatch) micro_batch 
		on trips.TRIP_SMRY_KEY = micro_batch.TRIP_SMRY_KEY) a

inner join
(
    select
    VIN_NB,
    PRGRM_INSTC_ID,
    PRGRM_TERM_BEG_DT,
    PRGRM_TERM_END_DT,
    PLCY_ST_CD
    from {harmonizedDB}.INTEGRATED_ENROLLMENT
    where LOAD_DT = (select max(LOAD_DT) from {harmonizedDB}.INTEGRATED_ENROLLMENT)
) b
on a.ENRLD_VIN_NB = trim(b.VIN_NB)
inner join 
{curateDB}.device_summary device
ON device.ENRLD_VIN_NB = a.ENRLD_VIN_NB 
    
where  trim(b.PLCY_ST_CD) <> 'CA'
AND device.DATA_CLCTN_ID IS NOT NULL
and a.PE_STRT_TS between b.PRGRM_TERM_BEG_DT and b.PRGRM_TERM_END_DT
and device.PRGRM_INSTC_ID IS NOT NULL
and a.PE_STRT_TS  BETWEEN device.DEVC_STTS_FRST_TS AND device.DEVC_STTS_LAST_TS
AND a.SRC_SYS_CD like 'IMS_SM%'
group by b.PRGRM_INSTC_ID,a.SRC_SYS_CD,a.DEVC_KEY,a.ENRLD_VIN_NB,b.PLCY_ST_CD""")
  return smartmiles_df
  

# COMMAND ----------

current_date = datetime.now()
print(f"Current Date is : {current_date}")
date_filter_18_mo_ago = (current_date-dateutil.relativedelta.relativedelta(months=18)).strftime("%Y-%m-%d")
print(f"date_filter_18_mo_ago : {date_filter_18_mo_ago}")
smartride_df=spark.sql(f"""SELECT
b.PRGRM_INSTC_ID
,a.SRC_SYS_CD
,a.DEVC_KEY
,a.ENRLD_VIN_NB
,cast(max(a.PE_END_TS) as string) as MAX_PE_END_TS
,sum(DRVNG_SC_CT) as DRVNG_SC_CT
,sum(FAST_ACLRTN_CT) AS FAST_ACLRTN_CT
,sum(HARD_BRKE_CT) AS HARD_BRKE_CT
,sum(PLSBL_MILE_CT) AS PLSBL_MILE_CT
,sum(ADJST_MILE_CT) AS ADJST_MILE_CT
,sum(PLSBL_DRIV_SC_CT) AS PLSBL_DRIV_SC_CT
,sum(PLSBL_IDLE_SC_CT) AS PLSBL_IDLE_SC_CT
,(sum(PLSBL_DRIV_SC_CT) - sum(PLSBL_IDLE_SC_CT)) AS no_idle_time
,CASE WHEN sum(DRVNG_SC_CT)= 0 THEN 0.0 ELSE CAST (round((sum(PLSBL_DRIV_SC_CT)/sum(DRVNG_SC_CT)) * 100,2) AS double) END  AS plausible_percentage
,b.PLCY_ST_CD
,least(cast((sum(ADJST_MILE_CT*1.60934)/sum(MILE_CT*1.60934)) as double),1.5) as weighted_time_of_day
,greatest(0.000000001,least(2.0,cast(sum(STOP_SC_CT)/sum(MILE_CT*1.60934) as decimal(35,15)))) as STOPS_PER_KM_RT
,greatest(0.000000001,least(2.0,cast(sum(HARD_BRKE_CT)/sum(MILE_CT*1.60934) as decimal(35,15)))) as BRAKES_PER_KM_RT
,max(a.LOAD_HR_TS) AS LOAD_HR_TS
,max(a.LOAD_DT) AS LOAD_DT
from  ( 
	select	* 
	from	dhf_iot_curated_prod.trip_detail trips 
	inner join (select distinct trip_smry_key, load_dt as load_dt_mb, load_hr_ts as load_hr_ts_mb from dhf_iot_curated_prod.outbound_score_elements_chlg) micro_batch 
		on trips.TRIP_SMRY_KEY = micro_batch.TRIP_SMRY_KEY) a

inner join
(
    select
    VIN_NB,
    PRGRM_INSTC_ID,
    PRGRM_TERM_BEG_DT,
    PRGRM_TERM_END_DT,
    PLCY_ST_CD
    from dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT
    where PRGRM_TERM_BEG_DT >= '{date_filter_18_mo_ago}'
    and LOAD_DT = (select max(LOAD_DT) from dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT)
) b
on a.ENRLD_VIN_NB = trim(b.VIN_NB)
inner join 
dhf_iot_curated_prod.device_summary device
ON device.ENRLD_VIN_NB = a.ENRLD_VIN_NB 
    
where trim( b.PLCY_ST_CD) <> 'CA'
and a.PE_STRT_TS between b.PRGRM_TERM_BEG_DT and b.PRGRM_TERM_END_DT
--and device.PRGRM_INSTC_ID IS NOT NULL
and a.PE_STRT_TS  BETWEEN device.DEVC_STTS_FRST_TS AND device.DEVC_STTS_LAST_TS
AND a.SRC_SYS_CD in ('IMS_SR_4X','IMS_SR_5X','FMC_SR','TIMS_SR')
group by b.PRGRM_INSTC_ID,a.SRC_SYS_CD,a.DEVC_KEY,a.ENRLD_VIN_NB,b.PLCY_ST_CD""")
display(smartride_df)
  

# COMMAND ----------

def final_score(view,curateDB):
  combined_df=spark.sql(f""" 
select distinct	cast(device.DEVC_KEY as string) as DEVC_KEY, device.ENRLD_VIN_NB as ENRLD_VIN_NB,
		cast(device.PRGRM_INSTC_ID as bigint) as PRGRM_INSTC_ID, device.DATA_CLCTN_ID as DATA_CLCTN_ID, trip.SRC_SYS_CD as SRC_SYS_CD,
		 MAX_PE_END_TS, cast(current_timestamp() as string) as TRANSACTION_TS,
		cast(device.DEVC_STTS_FRST_TS as string) as SCR_STRT_DT, cast(device.DEVC_STTS_LAST_TS as string) as SCR_END_DT,
		cast(device.LIFETM_DAYS_CT as int) as SCR_DAYS_CT, cast(device.CNCTD_DAYS_CT as BIGINT) as DAYS_INSTL_CT,
		cast(
		case 
			when ADJST_MILE_CT is Null then 0.0 
			else ADJST_MILE_CT 
		end as decimal(35,15)) as ADJST_MILES_QTY, cast(
		case 
			when PLSBL_MILE_CT is null then 0.0 
			else PLSBL_MILE_CT 
		end as decimal(35,15)) as SCRBD_MILES_QTY, cast(
		case 
			when ADJST_MILE_CT is Null then 0.0 
			else ADJST_MILE_CT end/CNCTD_DAYS_CT as decimal(35,15)) as AVG_DISTNC_QTY,
		cast (
		case 
			when CNCTD_DAYS_CT = 0 then 0.0 
			when ( 
		case 
			when HARD_BRKE_CT is null then 0 
			else cast(HARD_BRKE_CT as INT) end /CNCTD_DAYS_CT)> 7.0 then 7.0 
			else (
		case 
			when HARD_BRKE_CT is null then 0 
			else cast(HARD_BRKE_CT as INT) end/CNCTD_DAYS_CT) 
		end as decimal(35,15)) as HARD_BRK_PCTG_DISC_VAL, cast (
		case 
			when CNCTD_DAYS_CT = 0 then 0.0 
			when (
		case 
			when FAST_ACLRTN_CT is null then 0 
			else cast(FAST_ACLRTN_CT as int) end/CNCTD_DAYS_CT)> 4.0 then 4.0 
			else (
		case 
			when FAST_ACLRTN_CT is null then 0 
			else cast(FAST_ACLRTN_CT as int) end/CNCTD_DAYS_CT) 
		end as decimal(35,15)) as FAST_ACLRTN_PCTG_DISC_VAL, cast(
		case 
			when (PLSBL_IDLE_SC_CT is null 
	or (PLSBL_DRIV_SC_CT - PLSBL_IDLE_SC_CT) is null) then 0.0 
			else (PLSBL_IDLE_SC_CT/(PLSBL_IDLE_SC_CT+(PLSBL_DRIV_SC_CT - PLSBL_IDLE_SC_CT))) 
		end as decimal(35,15)) as IDLE_TIME_PCTG,cast(device.DEVC_CNCTD_PCT as decimal(35,15)) as INSTL_PCTG,
		cast(
		case 
			when (
		case 
			when DRVNG_SC_CT= 0 then 0.0 
			else cast (round((PLSBL_DRIV_SC_CT/DRVNG_SC_CT) * 100,2) as double) end) is null then 100.00 
			else (
		case 
			when DRVNG_SC_CT= 0 then 0.0 
			else cast (round((PLSBL_DRIV_SC_CT/DRVNG_SC_CT) * 100,2) as double) end) 
		end as decimal(35,15)) as PLSBL_DRIV_PCTG, cast(weighted_time_of_day as decimal(35,15)) as WGHT_TIME_OF_DY_RT,
		 STOPS_PER_KM_RT,
		BRAKES_PER_KM_RT,
		cast(device.DEVC_DSCNCTD_PCT as decimal(35,15)) as DSCNCTD_TIME_PCTG,
		cast(
		case 
			when (CNCTD_DAYS_CT is null 
	or CNCTD_DAYS_CT = 0.0) then 0.0 
			else (((
		case 
			when PLSBL_MILE_CT is null then 0.0 
			else PLSBL_MILE_CT end)/CNCTD_DAYS_CT) * 365) 
		end as decimal(35,15)) as ANNL_MILG_QTY, concat(concat(LPAD(trim(cast(if (device.CNCTD_SC_CT is null,' ',device.CNCTD_SC_CT) as varchar(3))),3,'000'),'_'),LPAD(trim(cast(if (device.DSCNCTD_SC_CT is null,' ',device.DSCNCTD_SC_CT) as varchar(3))),3,'000'))  as CNCT_DSCNCT_CT, cast(device.DSCNCTD_SC_CT as BIGINT) as UNINSTL_DAYS_CT,
		trip.LOAD_DT as LOAD_DT, trip.LOAD_HR_TS  
from	{view} trip 
INNER JOIN {curateDB}.device_summary device
	ON device.ENRLD_VIN_NB = trip.ENRLD_VIN_NB
""")
  return combined_df

# COMMAND ----------

harmonizedDB="dhf_iot_harmonized_prod"
curateDB="dhf_iot_curated_prod"
microBatchDF=spark.sql("select * from dhf_iot_curated_prod.outbound_score_elements_chlg")
microBatchDF.createOrReplaceGlobalTempView("microBatch")
smartmiles_score_df(harmonizedDB,curateDB).createOrReplaceTempView("smartmiles_view")
smartmiles_df = final_score("smartmiles_view",curateDB)
  
smartride_score_df(harmonizedDB,curateDB).createOrReplaceTempView("smartride_view")
smartride_df = final_score("smartride_view",curateDB)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct enrld_vin_nb) from smartride_view --where enrld_vin_nb ='KL7CJLSB6FB124343'

# COMMAND ----------

# MAGIC %sql
# MAGIC select src_sys_cd,count(distinct enrld_vin_nb) from dhf_iot_curated_prod.outbound_score_elements where load_dt>='2022-09-20' group by src_sys_cd

# COMMAND ----------

# MAGIC %sql
# MAGIC select src_sys_cd,count(distinct enrld_vin_nb) from dhf_iot_curated_prod.trip_detail where load_dt>='2022-09-20' group by src_sys_cd

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.trip_detail where load_dt>='2022-09-20' and enrld_vin_nb not in (select enrld_vin_nb from dhf_iot_curated_prod.outbound_score_elements)

# COMMAND ----------

# MAGIC %python
# MAGIC curatedDB='dhf_iot_curated_prod'
# MAGIC def get_data_from_deviceStatus(curatedDB):
# MAGIC   
# MAGIC   df =  spark.sql(f"""SELECT dc.*
# MAGIC                   from (select * from {curatedDB}.device_status where enrld_vin_nb='1FMCU0EG6CKA47710') dc inner join 
# MAGIC 				  (select distinct ENRLD_VIN_NB as vin,PRGRM_INSTC_ID as prgm_id,DATA_CLCTN_ID as dt_cln_id,DEVC_KEY as devc_nbr,SRC_SYS_CD as source from dhf_iot_curated_prod.device_summary_chlg where enrld_vin_nb='1FMCU0EG6CKA47710') mb on
# MAGIC 				  dc.ENRLD_VIN_NB =mb.vin 
# MAGIC                   where (PRGRM_INSTC_ID is not null and SRC_SYS_CD not like 'IMS_SM%') or (DATA_CLCTN_ID is not null and SRC_SYS_CD like 'IMS_SM%')""")
# MAGIC   return df
# MAGIC get_data_from_deviceStatus(curatedDB).createOrReplaceTempView("table")
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select (*) from table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (select * from dhf_iot_curated_prod.device_status  where  enrld_vin_nb='1FMCU0EG6CKA47710') dc inner join 
# MAGIC 				  (select distinct ENRLD_VIN_NB as vin,PRGRM_INSTC_ID as prgm_id,DATA_CLCTN_ID as dt_cln_id,DEVC_KEY as devc_nbr,SRC_SYS_CD as source  from dhf_iot_curated_prod.device_summary_chlg where  enrld_vin_nb='1FMCU0EG6CKA47710') mb on
# MAGIC 				  dc.ENRLD_VIN_NB =mb.vin and dc.SRC_SYS_CD =mb.source and dc.DEVC_KEY =mb.devc_nbr and dc.PRGRM_INSTC_ID =mb.prgm_id and dc.DATA_CLCTN_ID =mb.dt_cln_id
# MAGIC                   where (PRGRM_INSTC_ID is not null and SRC_SYS_CD not like 'IMS_SM%') or (DATA_CLCTN_ID is not null and SRC_SYS_CD like 'IMS_SM%')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select count(*) from dhf_iot_curated_prod.inbound_score_elements where   OUTBOUND_SCORE_ELEMENTS_ID not in (select OUTBOUND_SCORE_ELEMENTS_ID from dhf_iot_curated_prod.outbound_score_elements where load_dt>='2022-09-20')
# MAGIC select * from dhf_iot_curated_prod.device_status where  enrld_vin_nb='1FMCU0EG6CKA47710' and ( (PRGRM_INSTC_ID is not null and SRC_SYS_CD not like 'IMS_SM%') or (DATA_CLCTN_ID is not null and SRC_SYS_CD like 'IMS_SM%'))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select * from dhf_iot_harmonized_prod.program_enrollment where trim(vin_nb)='5N1DR2MN3J1111111'  --'JA32X2HUXEU002778'
# MAGIC -- DEVC_STTS_CD='Shipped' or 'Delivered' DATA_CLCTN_STTS='Active'
# MAGIC select count(*) from (select distinct a.VIN_NB,a.DEVC_ID from (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN'))a inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(PRGRM_TERM_BEG_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where  DEVC_ID not in ('',' ','0') and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) b on a.VIN_NB=b.VIN_NB and a.EVNT_SYS_TS=max_e and a.PRGRM_TERM_BEG_DT=max_tbd ) --group by VIN_NB having count(*)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC (select distinct a.VIN_NB,a.DEVC_ID from (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN'))a inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(PRGRM_TERM_BEG_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where  DEVC_ID not in ('',' ','0') and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) b on a.VIN_NB=b.VIN_NB and a.EVNT_SYS_TS=max_e and a.PRGRM_TERM_BEG_DT=max_tbd)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct  vin_nb from dhf_iot_harmonized_prod.program_enrollment where vin_nb not in (select distinct a.VIN_NB from (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN'))a inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(PRGRM_TERM_BEG_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where  DEVC_ID not in ('',' ','0') and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) b on a.VIN_NB=b.VIN_NB and a.EVNT_SYS_TS=max_e and a.PRGRM_TERM_BEG_DT=max_tbd) and trim(VNDR_CD) not in ('CMT', 'LN')

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct VNDR_CD from dhf_iot_harmonized_prod.program_enrollment where trim(VNDR_CD) not in ('CMT', 'LN') --where trim(vin_nb)='1GNDT33S292130359' --and DEVC_ID!=' '  order by KFK_TS desc JHMFE1F77NX007692 1GNDT33S292130359
# MAGIC
# MAGIC -- select DEVC_ID from dhf_iot_harmonized_prod.program_enrollment where  DEVC_ID not in ('',' ','0')    order by DEVC_ID desc --DEVC_ID!='' and DEVC_ID!=' ' 1C3EL55R66N236833

# COMMAND ----------

# MAGIC %sql
# MAGIC select DEVC_ID from dhf_iot_harmonized_prod.program_enrollment where TRIM(DEVC_ID) not in ('','0') ORDER BY DEVC_ID  --trim(vin_nb)='1VWBT7A35GC051663'
# MAGIC -- select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN') AND trim(vin_nb)='WBAXH5C56DDW12517'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_harmonized_prod.program_enrollment where  trim(vin_nb)='4T1BE32K63U150293'
# MAGIC -- select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN') AND trim(vin_nb)='5TEUU42N26Z255540'
# MAGIC -- select VIN_NB,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN')  AND trim(vin_nb)='WBAXH5C56DDW12517' group by VIN_NB

# COMMAND ----------

# MAGIC %sql
# MAGIC select DISTINCT VIN_NB,DEVC_ID from dhf_iot_harmonized_prod.program_enrollment WHERE VIN_NB IN  (select DISTINCT VIN_NB  from dhf_iot_harmonized_prod.integrated_enrollment where VIN_NB not in (select distinct a.VIN_NB from (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN')) z inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) y on z.VIN_NB=y.VIN_NB and z.EVNT_SYS_TS=max_e and z.ENRLMNT_EFCTV_DT=max_tbd) and SRC_SYS_CD='IMS' )  --where DATA_CLCTN_ID is  null--vin_nb='19XFC2F58HE201082'  --'19UUB5F42MA001056'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT VIN_NB,DEVC_ID,DEVC_ID_NEW ,
# MAGIC case when trim(DEVC_ID)='' then DEVC_ID_NEW
# MAGIC else DEVC_ID
# MAGIC end new_id
# MAGIC from ((select v.*,DEVC_ID_NEW from dhf_iot_harmonized_prod.program_enrollment v left join (select distinct Z.VIN_NB,Z.DEVC_ID AS DEVC_ID_NEW FROM (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN')) z inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) y on z.VIN_NB=y.VIN_NB and z.EVNT_SYS_TS=max_e and z.ENRLMNT_EFCTV_DT=max_tbd) q on v.VIN_NB=q.VIN_NB)) where trim(VNDR_CD) not in ('CMT', 'LN')

# COMMAND ----------

# MAGIC %sql
# MAGIC (select v.*,DEVC_ID_NEW from dhf_iot_harmonized_prod.program_enrollment v left join (select distinct Z.VIN_NB,Z.DEVC_ID AS DEVC_ID_NEW FROM (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN')) z inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) y on z.VIN_NB=y.VIN_NB and z.EVNT_SYS_TS=max_e and z.ENRLMNT_EFCTV_DT=max_tbd) q on v.VIN_NB=q.VIN_NB)

# COMMAND ----------

# MAGIC %sql
# MAGIC select (*) from dhf_iot_harmonized_prod.program_enrollment where vin_nb   not in (select vin_nb from  ((select distinct v.*,DEVC_ID_NEW from dhf_iot_harmonized_prod.program_enrollment v left join ( select  VIN_NB,first(DEVC_ID) AS DEVC_ID_NEW from (select distinct Z.VIN_NB,Z.DEVC_ID FROM (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN')) z inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) y on z.VIN_NB=y.VIN_NB and z.EVNT_SYS_TS=max_e and z.ENRLMNT_EFCTV_DT=max_tbd) group by VIN_NB ) q on v.VIN_NB=q.VIN_NB)))

# COMMAND ----------

# MAGIC %sql
# MAGIC select vin_nb from  ((select distinct v.*,DEVC_ID_NEW from dhf_iot_harmonized_prod.program_enrollment v left join ( select  VIN_NB,first(DEVC_ID) AS DEVC_ID_NEW from (select distinct Z.VIN_NB,Z.DEVC_ID FROM (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN')) z inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) y on z.VIN_NB=y.VIN_NB and z.EVNT_SYS_TS=max_e and z.ENRLMNT_EFCTV_DT=max_tbd) group by VIN_NB ) q on v.VIN_NB=q.VIN_NB)) where vin_nb not in (select vin_nb from dhf_iot_harmonized_prod.program_enrollment)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select  VIN_NB ,count(*) from  ( select  VIN_NB,first(DEVC_ID) AS DEVC_ID from (select distinct Z.VIN_NB,Z.DEVC_ID FROM (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN')) z inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) y on z.VIN_NB=y.VIN_NB and z.EVNT_SYS_TS=max_e and z.ENRLMNT_EFCTV_DT=max_tbd) group by VIN_NB,DEVC_ID ) group by VIN_NB having count(*)>1
# MAGIC
# MAGIC  ( select  VIN_NB,first(DEVC_ID) AS DEVC_ID from (select distinct Z.VIN_NB,Z.DEVC_ID FROM (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN')) z inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) y on z.VIN_NB=y.VIN_NB and z.EVNT_SYS_TS=max_e and z.ENRLMNT_EFCTV_DT=max_tbd) where vin_nb='' group by VIN_NB,DEVC_ID )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select  VIN_NB ,count(*) from  ( select  VIN_NB,first(DEVC_ID) AS DEVC_ID from (select distinct Z.VIN_NB,Z.DEVC_ID FROM (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN')) z inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) y on z.VIN_NB=y.VIN_NB and z.EVNT_SYS_TS=max_e and z.ENRLMNT_EFCTV_DT=max_tbd) group by VIN_NB,DEVC_ID ) group by VIN_NB having count(*)>1
# MAGIC
# MAGIC  ( select  VIN_NB,first(DEVC_ID) AS DEVC_ID from (select distinct Z.VIN_NB,Z.DEVC_ID FROM (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN')) z inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD='Active' and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) y on z.VIN_NB=y.VIN_NB and z.EVNT_SYS_TS=max_e and z.ENRLMNT_EFCTV_DT=max_tbd) where vin_nb='JHMFE1F77NX007692' group by VIN_NB,DEVC_ID )
# MAGIC