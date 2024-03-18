# Databricks notebook source
# MAGIC %sql
# MAGIC Select 
# MAGIC outbound_score_elements_id,
# MAGIC DEVC_KEY,
# MAGIC ENRLD_VIN_NB,
# MAGIC SRC_SYS_CD,
# MAGIC DATA_CLCTN_ID,
# MAGIC PRGRM_INSTC_ID,
# MAGIC case when SRC_SYS_CD LIKE ('%SM%') THEN DATA_CLCTN_ID
# MAGIC      when SRC_SYS_CD LIKE ('%SR%') THEN PRGRM_INSTC_ID
# MAGIC    END AS PRGRM_INSTC_ID2 from dhf_iot_curated_prod.inbound_score_elements where SRC_SYS_CD ='IMS_SR_4X'

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC -- select greatest(0.000000001,least(2.0,cast(2/sum(2*1.60934) as decimal(35,15)))) as STOPS_PER_KM_RT--0.621372736649807%sql
# MAGIC select greatest(0.000000001,least(2.0,cast(1/sum(1*1.60934) as decimal(35,15)))) as STOPS_PER_KM_RT--0.621372736649807

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (select	* 
# MAGIC 	from	dhf_iot_curated_prod.trip_detail trips 
# MAGIC 	inner join (select distinct trip_smry_key, load_dt as load_dt_mb, load_hr_ts as load_hr_ts_mb from dhf_iot_curated_prod.OUTBOUND_SCORE_ELEMENTS_CHLG) micro_batch 
# MAGIC 		on trips.TRIP_SMRY_KEY = micro_batch.TRIP_SMRY_KEY and src_sys_cd='IMS_SM_5X') a
# MAGIC
# MAGIC inner join
# MAGIC (
# MAGIC     select
# MAGIC     VIN_NB,
# MAGIC     PRGRM_INSTC_ID,
# MAGIC     PRGRM_TERM_BEG_DT,
# MAGIC     PRGRM_TERM_END_DT,
# MAGIC     PLCY_ST_CD
# MAGIC     from dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT
# MAGIC     where LOAD_DT = (select max(LOAD_DT) from dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT)
# MAGIC ) b
# MAGIC on a.ENRLD_VIN_NB = trim(b.VIN_NB)
# MAGIC inner join 
# MAGIC dhf_iot_curated_prod.device_summary device
# MAGIC ON device.ENRLD_VIN_NB = a.ENRLD_VIN_NB 
# MAGIC     
# MAGIC where  trim(b.PLCY_ST_CD) <> 'CA'
# MAGIC AND device.DATA_CLCTN_ID IS NOT NULL
# MAGIC and a.PE_STRT_TS between b.PRGRM_TERM_BEG_DT and b.PRGRM_TERM_END_DT
# MAGIC -- and device.PRGRM_INSTC_ID IS NOT NULL
# MAGIC and a.PE_STRT_TS  BETWEEN device.DEVC_STTS_FRST_TS AND device.DEVC_STTS_LAST_TS
# MAGIC AND a.SRC_SYS_CD like 'IMS_SM%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select	* 
# MAGIC 	from	dhf_iot_curated_prod.trip_detail trips 
# MAGIC 	inner join (select distinct trip_smry_key, load_dt as load_dt_mb, load_hr_ts as load_hr_ts_mb from dhf_iot_curated_prod.OUTBOUND_SCORE_ELEMENTS_CHLG) micro_batch 
# MAGIC 		on trips.TRIP_SMRY_KEY = micro_batch.TRIP_SMRY_KEY

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct CONCAT(
# MAGIC RPAD(TRIM(IF(tb1.DEVC_KEY IS NULL,' ', CAST(tb1.DEVC_KEY AS VARCHAR(25)))),25,' ')
# MAGIC ,'                    '
# MAGIC ,RPAD(TRIM(IF(tb1.ENRLD_VIN_NB IS NULL,' ', CAST(tb1.ENRLD_VIN_NB AS VARCHAR(17)))),17,' ')
# MAGIC ,LPAD(TRIM(IF(tb1.PRGRM_INSTC_ID IS NULL,' ', CAST(tb1.PRGRM_INSTC_ID AS VARCHAR(36)))),36,'0')
# MAGIC ,RPAD(REGEXP_REPLACE(IF(tb1.MODL_OTPT_TS IS NULL,' ', CAST(tb1.MODL_OTPT_TS AS VARCHAR(10))),'-',''),8,' ')
# MAGIC ,RPAD(REGEXP_REPLACE(IF(tb1.SCR_STRT_DT IS NULL,' ', CAST(tb1.SCR_STRT_DT AS VARCHAR(10))),'-',''),8,' ')
# MAGIC ,RPAD(REGEXP_REPLACE(IF(tb1.SCR_END_DT IS NULL,' ', CAST(tb1.SCR_END_DT AS VARCHAR(10))),'-',''),8,' ')
# MAGIC ,LPAD(CAST(IF(tb1.SCR_DAYS_CT IS NULL,0,CAST(tb1.SCR_DAYS_CT AS INT)) AS VARCHAR(5)),3,'0')
# MAGIC ,RPAD(TRIM(CAST(tb1.score_model_1 AS VARCHAR(4))),4,' ')
# MAGIC ,LPAD(TRIM(CAST(tb1.PURE_SCR_1_QTY AS VARCHAR(3))),3,'0')
# MAGIC ,LPAD(TRIM(IF((CASE when (tb1.INSTL_PCTG < 95) then 998 when (tb1.PLSBL_DRIV_PCTG < 97 or tb1.PLSBL_DRIV_PCTG is null ) then 997 else (case when (tb1.PURE_SCR_1_QTY < 1) then 1 when (tb1.PURE_SCR_1_QTY > 995) then 995 else tb1.PURE_SCR_1_QTY end) end) IS NULL,' ', CAST((CASE when (tb1.INSTL_PCTG < 95) then 998 when (tb1.PLSBL_DRIV_PCTG < 97 or tb1.PLSBL_DRIV_PCTG is null ) then 997 else (case when (tb1.PURE_SCR_1_QTY < 1) then 1 when (tb1.PURE_SCR_1_QTY > 995) then 995 else tb1.PURE_SCR_1_QTY end) end) AS VARCHAR(3)))),3,'0')
# MAGIC ,'          '
# MAGIC ,RPAD(TRIM(CAST(tb1.score_model_2 AS VARCHAR(4))),4,' ')
# MAGIC ,LPAD(TRIM(IF(tb1.PURE_SCR_2_QTY IS NULL,' ', CAST(tb1.PURE_SCR_2_QTY AS VARCHAR(3)))),3,'0')
# MAGIC ,LPAD(TRIM(IF((CASE when (tb1.INSTL_PCTG < 95) then 998 when (tb1.PLSBL_DRIV_PCTG < 97 or tb1.PLSBL_DRIV_PCTG is null ) then 997 else (case when (tb1.PURE_SCR_2_QTY < 1) then 1 when (tb1.PURE_SCR_2_QTY > 995) then 995 else tb1.PURE_SCR_2_QTY end) end) IS NULL,' ', CAST((CASE when (tb1.INSTL_PCTG < 95) then 998 when (tb1.PLSBL_DRIV_PCTG < 97 or tb1.PLSBL_DRIV_PCTG is null ) then 997 else (case when (tb1.PURE_SCR_2_QTY < 1) then 1 when (tb1.PURE_SCR_2_QTY > 995) then 995 else tb1.PURE_SCR_2_QTY end) end) AS VARCHAR(3)))),3,'0')
# MAGIC ,'          '
# MAGIC ,'    '
# MAGIC ,'000'
# MAGIC ,'000'
# MAGIC ,'          '
# MAGIC ,'    '
# MAGIC ,'000'
# MAGIC ,'000'
# MAGIC ,'          '
# MAGIC ,'    '
# MAGIC ,'000'
# MAGIC ,'000'
# MAGIC ,'          '
# MAGIC ,'    '
# MAGIC ,'000'
# MAGIC ,'000'
# MAGIC ,'          '
# MAGIC ,'    '
# MAGIC ,'000'
# MAGIC ,'000'
# MAGIC ,'          '
# MAGIC ,'    '
# MAGIC ,'000'
# MAGIC ,'000'
# MAGIC ,'          '
# MAGIC ,'    '
# MAGIC ,'000'
# MAGIC ,'000'
# MAGIC ,'          '
# MAGIC ,'    '
# MAGIC ,'000'
# MAGIC ,'000'
# MAGIC ,'          '
# MAGIC ,LPAD(CAST(IF(tb1.DSCNCTD_TIME_PCTG IS NULL,0,CAST(tb1.DSCNCTD_TIME_PCTG*100 AS INT)) AS VARCHAR(5)),5,'0')
# MAGIC ,LPAD(TRIM(IF(tb1.ANNL_MILG_QTY IS NULL,' ', CAST(tb1.ANNL_MILG_QTY AS VARCHAR(6)))),6,'0')
# MAGIC ,RPAD(TRIM(IF(tb1.CNCT_DSCNCT_CT IS NULL,' ', CAST(tb1.CNCT_DSCNCT_CT AS VARCHAR(7)))),7,' ')
# MAGIC ,CONCAT(CONCAT(CONCAT(CONCAT (LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(CAST(tb1.UNINSTL_DAYS_CT/86400 AS INT) AS VARCHAR(3)))),3,'0') , ':'),
# MAGIC LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(CAST((tb1.UNINSTL_DAYS_CT%86400)/3600 AS INT) AS VARCHAR(3)))),2,'0') , ':'),
# MAGIC LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(CAST(((tb1.UNINSTL_DAYS_CT%86400)%3600)/60 AS INT)AS VARCHAR(3)))),2,'0') , ':'),
# MAGIC LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(((tb1.UNINSTL_DAYS_CT%86400)%3600)%60 AS VARCHAR(3)))),2,'0'))
# MAGIC ,'                                             ')
# MAGIC FROM 
# MAGIC (
# MAGIC Select 
# MAGIC outbound_score_elements_id,
# MAGIC DEVC_KEY,
# MAGIC ENRLD_VIN_NB,
# MAGIC coalesce(DATA_CLCTN_ID,PRGRM_INSTC_ID) as PRGRM_INSTC_ID,
# MAGIC MODL_OTPT_TS,
# MAGIC SCR_STRT_DT,
# MAGIC SCR_END_DT,
# MAGIC SCR_DAYS_CT,
# MAGIC 'ND1' As score_model_1,
# MAGIC CASE WHEN MODL_ACRNYM_TT='ND1' THEN MODL_OTPT_QTY ELSE 0 END AS PURE_SCR_1_QTY,
# MAGIC 'SM1' As score_model_2,
# MAGIC CASE WHEN MODL_ACRNYM_TT2='SM1' THEN MODL_OTPT_QTY2 ELSE 0 END AS PURE_SCR_2_QTY,
# MAGIC cast(DSCNCTD_TIME_PCTG as double),
# MAGIC cast(ANNL_MILG_QTY as double) as ANNL_MILG_QTY,
# MAGIC CNCT_DSCNCT_CT,
# MAGIC UNINSTL_DAYS_CT,
# MAGIC cast(INSTL_PCTG as double) as INSTL_PCTG,
# MAGIC cast(PLSBL_DRIV_PCTG as double) as PLSBL_DRIV_PCTG,
# MAGIC LOAD_DT
# MAGIC from (select distinct a.*,b.MODL_ACRNYM_TT as MODL_ACRNYM_TT2,b.MODL_OTPT_QTY as MODL_OTPT_QTY2 from (select *,row_number() over (partition by ENRLD_VIN_NB,SRC_SYS_CD,MODL_NM,MODL_ACRNYM_TT order by TRANSACTION_TS desc) as row_nb from (select ib.* from {curateDB}.inbound_score_elements ib inner join dhf_iot_curated_prod.outbound_score_elements ob on ib.outbound_score_elements_id = ob.outbound_score_elements_id  and to_date(ob.LOAD_DT,'yyyyMMdd') = DATE(CURRENT_DATE)-1) )  a inner join dhf_iot_curated_prod.inbound_score_elements b on a.enrld_vin_nb=b.enrld_vin_nb and a.MODL_ACRNYM_TT="ND1" AND b.MODL_ACRNYM_TT="SM1" where row_nb=1) """).drop("row_nb")

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.outbound_score_elements where ENRLD_VIN_NB in ('5N1AR2MM5EC677811')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.trip_detail where ENRLD_VIN_NB in ('5N1AR2MM5EC677811') and load_dt >'2022-09-22'

# COMMAND ----------


import pyspark.sql
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import *
import datetime
import io
import re
import json
from itertools import groupby
from operator import itemgetter
import io
import zipfile
import pandas as pd
from pyspark.sql.types import *
import datetime #4
import json
import boto3
import time
import ast
from datetime import date,datetime
import time
from pyspark.sql import functions as f
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame
from dataclasses import dataclass
from pyspark.sql.functions import window
from pyspark.sql import *

# COMMAND ----------

  
def daily_score_extract(curateDB, target_table):
  
#   microBatchDF.createOrReplaceGlobalTempView("microBatch")
  
  final_df = spark.sql(f"""SELECT distinct CONCAT(
RPAD(TRIM(IF(tb1.DEVC_KEY IS NULL,' ', CAST(cast(tb1.DEVC_KEY as bigint) AS VARCHAR(25)))),25,' ')
,'                    '
,RPAD(TRIM(IF(tb1.ENRLD_VIN_NB IS NULL,' ', CAST(tb1.ENRLD_VIN_NB AS VARCHAR(17)))),17,' ')
,LPAD(TRIM(IF(tb1.PRGRM_INSTC_ID IS NULL,' ', CAST(tb1.PRGRM_INSTC_ID AS VARCHAR(36)))),36,'0')
,RPAD(REGEXP_REPLACE(IF(tb1.MODL_OTPT_TS IS NULL,' ', CAST(tb1.MODL_OTPT_TS AS VARCHAR(10))),'-',''),8,' ')
,RPAD(REGEXP_REPLACE(IF(tb1.SCR_STRT_DT IS NULL,' ', CAST(tb1.SCR_STRT_DT AS VARCHAR(10))),'-',''),8,' ')
,RPAD(REGEXP_REPLACE(IF(tb1.SCR_END_DT IS NULL,' ', CAST(tb1.SCR_END_DT AS VARCHAR(10))),'-',''),8,' ')
,LPAD(CAST(IF(tb1.SCR_DAYS_CT IS NULL,0,CAST(tb1.SCR_DAYS_CT AS INT)) AS VARCHAR(5)),3,'0')
,RPAD(TRIM(CAST(tb1.score_model_1 AS VARCHAR(4))),4,' ')
,LPAD(TRIM(CAST(tb1.PURE_SCR_1_QTY AS VARCHAR(3))),3,'0')
,LPAD(TRIM(IF((CASE when (tb1.INSTL_PCTG < 95) then 998 when (tb1.PLSBL_DRIV_PCTG < 97 or tb1.PLSBL_DRIV_PCTG is null ) then 997 else (case when (tb1.PURE_SCR_1_QTY < 1) then 1 when (tb1.PURE_SCR_1_QTY > 995) then 995 else tb1.PURE_SCR_1_QTY end) end) IS NULL,' ', CAST((CASE when (tb1.INSTL_PCTG < 95) then 998 when (tb1.PLSBL_DRIV_PCTG < 97 or tb1.PLSBL_DRIV_PCTG is null ) then 997 else (case when (tb1.PURE_SCR_1_QTY < 1) then 1 when (tb1.PURE_SCR_1_QTY > 995) then 995 else tb1.PURE_SCR_1_QTY end) end) AS VARCHAR(3)))),3,'0')
,'          '
,RPAD(TRIM(CAST(tb1.score_model_2 AS VARCHAR(4))),4,' ')
,LPAD(TRIM(IF(tb1.PURE_SCR_2_QTY IS NULL,' ', CAST(tb1.PURE_SCR_2_QTY AS VARCHAR(3)))),3,'0')
,LPAD(TRIM(IF((CASE when (tb1.INSTL_PCTG < 95) then 998 when (tb1.PLSBL_DRIV_PCTG < 97 or tb1.PLSBL_DRIV_PCTG is null ) then 997 else (case when (tb1.PURE_SCR_2_QTY < 1) then 1 when (tb1.PURE_SCR_2_QTY > 995) then 995 else tb1.PURE_SCR_2_QTY end) end) IS NULL,' ', CAST((CASE when (tb1.INSTL_PCTG < 95) then 998 when (tb1.PLSBL_DRIV_PCTG < 97 or tb1.PLSBL_DRIV_PCTG is null ) then 997 else (case when (tb1.PURE_SCR_2_QTY < 1) then 1 when (tb1.PURE_SCR_2_QTY > 995) then 995 else tb1.PURE_SCR_2_QTY end) end) AS VARCHAR(3)))),3,'0')
,'          '
,'    '
,'000'
,'000'
,'          '
,'    '
,'000'
,'000'
,'          '
,'    '
,'000'
,'000'
,'          '
,'    '
,'000'
,'000'
,'          '
,'    '
,'000'
,'000'
,'          '
,'    '
,'000'
,'000'
,'          '
,'    '
,'000'
,'000'
,'          '
,'    '
,'000'
,'000'
,'          '
,LPAD(CAST(IF(tb1.DSCNCTD_TIME_PCTG IS NULL,0,CAST(tb1.DSCNCTD_TIME_PCTG*100 AS INT)) AS VARCHAR(5)),5,'0')
,LPAD(TRIM(IF(tb1.ANNL_MILG_QTY IS NULL,' ', CAST(tb1.ANNL_MILG_QTY AS VARCHAR(6)))),6,'0')
,RPAD(TRIM(IF(tb1.CNCT_DSCNCT_CT IS NULL,' ', CAST(tb1.CNCT_DSCNCT_CT AS VARCHAR(7)))),7,' ')
,CONCAT(CONCAT(CONCAT(CONCAT (LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(CAST(tb1.UNINSTL_DAYS_CT/86400 AS INT) AS VARCHAR(3)))),3,'0') , ':'),
LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(CAST((tb1.UNINSTL_DAYS_CT%86400)/3600 AS INT) AS VARCHAR(3)))),2,'0') , ':'),
LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(CAST(((tb1.UNINSTL_DAYS_CT%86400)%3600)/60 AS INT)AS VARCHAR(3)))),2,'0') , ':'),
LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(((tb1.UNINSTL_DAYS_CT%86400)%3600)%60 AS VARCHAR(3)))),2,'0'))
,'                                             ')
FROM 
(
Select 
outbound_score_elements_id,
DEVC_KEY,
ENRLD_VIN_NB,
coalesce(DATA_CLCTN_ID,PRGRM_INSTC_ID) as PRGRM_INSTC_ID,
MODL_OTPT_TS,
SCR_STRT_DT,
SCR_END_DT,
SCR_DAYS_CT,
'ND1' As score_model_1,
CASE WHEN MODL_ACRNYM_TT='ND1' THEN MODL_OTPT_QTY ELSE 0 END AS PURE_SCR_1_QTY,
'SM1' As score_model_2,
CASE WHEN MODL_ACRNYM_TT2='SM1' THEN MODL_OTPT_QTY2 ELSE 0 END AS PURE_SCR_2_QTY,
cast(DSCNCTD_TIME_PCTG as double),
cast(ANNL_MILG_QTY as double) as ANNL_MILG_QTY,
CNCT_DSCNCT_CT,
UNINSTL_DAYS_CT,
cast(INSTL_PCTG as double) as INSTL_PCTG,
cast(PLSBL_DRIV_PCTG as double) as PLSBL_DRIV_PCTG,
LOAD_DT
from (select distinct a.*,b.MODL_ACRNYM_TT as MODL_ACRNYM_TT2,b.MODL_OTPT_QTY as MODL_OTPT_QTY2 from (select *,row_number() over (partition by ENRLD_VIN_NB,SRC_SYS_CD,MODL_NM,MODL_ACRNYM_TT order by TRANSACTION_TS desc) as row_nb from (select ib.* from {curateDB}.inbound_score_elements ib inner join {curateDB}.outbound_score_elements ob on ib.outbound_score_elements_id = ob.outbound_score_elements_id  and to_date(ob.LOAD_DT,'yyyyMMdd') = DATE(CURRENT_DATE)-1) )  a inner join {curateDB}.inbound_score_elements b on a.enrld_vin_nb=b.enrld_vin_nb and a.MODL_ACRNYM_TT="ND1" AND b.MODL_ACRNYM_TT="SM1" where row_nb=1)) tb1 """).drop("row_nb")

  
  currentdate = date.today()
  print(f"Current Date is : {currentdate}")
  if ('dev') in curateDB:
    path = 's3://pcds-databricks-common-786994105833/iot/extracts/daily-scoring'
  elif ('test') in curateDB:
    path = 's3://pcds-databricks-common-168341759447/iot/extracts/daily-scoring'
  elif ('prod') in curateDB:
    path = 's3://pcds-databricks-common-785562577411/iot/extracts/daily-scoring'

  df_extract = final_df.select(concat(*final_df.columns).alias('data'))
  display(df_extract)
#   df_extract.coalesce(1).write.format("text").option("header", "false").mode("overwrite").save(f"{path}/mainframe")
#   df_extract.coalesce(1).write.format("text").option("header", "false").mode("append").save(f"{path}/load_date={currentdate}")
  
  df_extract.withColumn('LOAD_DT',current_date()).write.format("delta").mode("append").saveAsTable(f"{curateDB}.extracts_dailyscoring_test")
  
  #send_to_ftp(path + '/mainframe')
  
  
  





# COMMAND ----------

daily_score_extract("dhf_iot_curated_prod", "extracts_dailyscoring")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from  dhf_iot_curated_prod.extracts_dailyscoring_test where data like ('%4T4BF3EKXAR054968%')

# COMMAND ----------

# MAGIC %sql
# MAGIC (select distinct a.*,b.MODL_ACRNYM_TT as MODL_ACRNYM_TT2,b.MODL_OTPT_QTY as MODL_OTPT_QTY2 from
# MAGIC       (select *,row_number() over (partition by ENRLD_VIN_NB,SRC_SYS_CD,MODL_NM,MODL_ACRNYM_TT order by TRANSACTION_TS desc) as row_nb from 
# MAGIC            (select ib.* from dhf_iot_curated_prod.inbound_score_elements ib inner join dhf_iot_curated_prod.outbound_score_elements ob on ib.outbound_score_elements_id =          ob.outbound_score_elements_id  and to_date(ob.LOAD_DT,'yyyyMMdd') = DATE(CURRENT_DATE)-2) )  a 
# MAGIC inner join dhf_iot_curated_prod.inbound_score_elements b on a.enrld_vin_nb=b.enrld_vin_nb  and a.outbound_score_elements_id=b.outbound_score_elements_id and a.MODL_ACRNYM_TT="ND1" AND b.MODL_ACRNYM_TT="SM1" where row_nb=1 and a.enrld_vin_nb='4T4BF3EKXAR054968')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.inbound_score_elements where  ENRLD_VIN_NB in ('4T4BF3EKXAR054968')

# COMMAND ----------

-%sql 
select * from  dhf_iot_curated_prod.extracts_dailyscoring_hitsory --where value like ('%WA1WKAFP8CA099691%')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (select distinct a.*,b.MODL_ACRNYM_TT as MODL_ACRNYM_TT2,b.MODL_OTPT_QTY as MODL_OTPT_QTY2 from (select *,row_number() over (partition by ENRLD_VIN_NB,SRC_SYS_CD,MODL_NM,MODL_ACRNYM_TT order by TRANSACTION_TS desc) as row_nb from (select ib.* from dhf_iot_curated_prod.inbound_score_elements ib inner join dhf_iot_curated_prod.outbound_score_elements ob on ib.outbound_score_elements_id = ob.outbound_score_elements_id  and to_date(ob.LOAD_DT,'yyyyMMdd') = DATE(CURRENT_DATE)-1) )  a inner join dhf_iot_curated_prod.inbound_score_elements b on a.enrld_vin_nb=b.enrld_vin_nb and a.MODL_ACRNYM_TT="ND1" AND b.MODL_ACRNYM_TT="SM1" where row_nb=1) where  ENRLD_VIN_NB in ('WA1WKAFP8CA099691')

# COMMAND ----------

LPAD(CAST(IF(tb1.DSCNCTD_TIME_PCTG IS NULL,0,CAST(tb1.DSCNCTD_TIME_PCTG*100 AS INT)) AS VARCHAR(5)),5,'0')
,LPAD(TRIM(IF(tb1.ANNL_MILG_QTY IS NULL,' ', CAST(tb1.ANNL_MILG_QTY AS VARCHAR(6)))),6,'0')
,RPAD(TRIM(IF(tb1.CNCT_DSCNCT_CT IS NULL,' ', CAST(tb1.CNCT_DSCNCT_CT AS VARCHAR(7)))),7,' ')
,CONCAT(CONCAT(CONCAT(CONCAT (LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(CAST(tb1.UNINSTL_DAYS_CT/86400 AS INT) AS VARCHAR(3)))),3,'0') , ':'),
LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(CAST((tb1.UNINSTL_DAYS_CT%86400)/3600 AS INT) AS VARCHAR(3)))),2,'0') , ':'),
LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(CAST(((tb1.UNINSTL_DAYS_CT%86400)%3600)/60 AS INT)AS VARCHAR(3)))),2,'0') , ':'),
LPAD(TRIM(IF(tb1.UNINSTL_DAYS_CT IS NULL,' ', CAST(((tb1.UNINSTL_DAYS_CT%86400)%3600)%60 AS VARCHAR(3)))),2,'0'))

 00193    6651.7 003_002 002: 15: 28 :09 
 00194    14.933 115_228 002: 15: 28 :11 
 

# COMMAND ----------

# MAGIC %sql
# MAGIC  select ((228491%86400)%3600)%60

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.inbound_score_elements where PRGRM_INSTC_ID is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.INbound_score_elements where  ENRLD_VIN_NB in ('1C6HJTAG4LL198542')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.outbound_score_elements where  ENRLD_VIN_NB in ('1C6HJTAG4LL198542')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.DEVICE_SUMMARY where ENRLD_VIN_NB in ('19UDE2F33HA012859') ORDER BY LOAD_DT DESC--and   SRC_SYS_CD='IMS_SM_5X'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_harmonized_prod.trip_point where src_sys_cd ='IMS_SM_5X' and load_dt>='2022-10-04' and trip_smry_key in ('cdf08d51-c3b9-46a2-924e-d40bb3f0923c') 

# COMMAND ----------

# MAGIC %sql
# MAGIC select ENRLD_VIN_NB ,count(*) from dhf_iot_curated_prod.trip_detail where SRC_SYS_CD='IMS_SM_5X' group by ENRLD_VIN_NB having count(*)>1 --where ENRLD_VIN_NB in ('1C6HJTAG4LL198542') ORDER BY LOAD_DT DESC--and   SRC_SYS_CD='IMS_SM_5X'

# COMMAND ----------

# MAGIC %sql
# MAGIC select  ENRLD_VIN_NB,count(*) from dhf_iot_curated_prod.trip_detail where load_dt in ('2022-11-23') and src_sys_cd='IMS_SM_5X'  group by ENRLD_VIN_NB having count(*)> 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.trip_detail where ENRLD_VIN_NB in ('4T4BF3EK3BR1860351GKKNPLS0JZ229165') and load_dt='2022-11-09'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.DEVICE_Status where ENRLD_VIN_NB in ('4S4BTGLD6M3190880')  order by STTS_EFCTV_TS desc--and load_dt='2022-11-09'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.inbound_score_elements where ENRLD_VIN_NB in ('1GKKNPLS0JZ229165') and load_dt>='2022-10-11' --and MODL_ACRNYM_TT='ND1'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.outbound_score_elements where ENRLD_VIN_NB in ('5TDJZRFH2KS576717') and load_dt='2022-11-23'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct ENRLD_VIN_NB) from dhf_iot_curated_prod.outbound_score_elements where load_dt='2022-11-14'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct ENRLD_VIN_NB) from (select ENRLD_VIN_NB,src_sys_cd,max(PE_END_TS) as pe_end_ts from dhf_iot_curated_prod.trip_detail where load_dt>=current_date-1 group by ENRLD_VIN_NB,src_sys_cd) a inner join (select  ENRLD_VIN_NB as vin,src_sys_cd as src ,max(MAX_PE_END_TS) as MAX_PE_END_TS from dhf_iot_curated_prod.outbound_score_elements where load_dt>=current_date-1 group by ENRLD_VIN_NB,src_sys_cd) b on a.ENRLD_VIN_NB=b.vin and a.pe_end_ts!=b.MAX_PE_END_TS and a.src_sys_cd=b.src

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.trip_detail where ENRLD_VIN_NB in ('KNDPM3AC7M7869088') and load_dt>=current_date-1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.outbound_score_elements where ENRLD_VIN_NB in ('KNDPM3AC7M7869088') and load_dt>=current_date-1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.DEVICE_Status where ENRLD_VIN_NB in ('1C6SRFGT4KN541731')  order by STTS_EFCTV_TS desc--and load_dt='2022-11-09'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.DEVICE_Summary where ENRLD_VIN_NB in ('1C6SRFGT4KN541731') -- order by STTS_EFCTV_TS desc--and load_dt='2022-11-09'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct ENRLD_VIN_NB,src_sys_cd from (select ENRLD_VIN_NB,src_sys_cd,max(PE_END_TS) as pe_end_ts from dhf_iot_curated_prod.trip_detail where load_dt>=current_date-1 group by ENRLD_VIN_NB,src_sys_cd) a left join (select  ENRLD_VIN_NB as vin,src_sys_cd as src ,max(MAX_PE_END_TS) as MAX_PE_END_TS from dhf_iot_curated_prod.outbound_score_elements where load_dt>=current_date-1 group by ENRLD_VIN_NB,src_sys_cd) b on a.ENRLD_VIN_NB=b.vin where a.pe_end_ts!=b.MAX_PE_END_TS 

# COMMAND ----------

microBatchDF=spark.sql("select * from dhf_iot_curated_prod.outbound_score_elements_chlg where load_dt>=current_date-1")
missing_vins=spark.sql("select distinct ENRLD_VIN_NB,src_sys_cd from (select ENRLD_VIN_NB,src_sys_cd,max(PE_END_TS) as pe_end_ts from dhf_iot_curated_prod.trip_detail where load_dt>=current_date-1 group by ENRLD_VIN_NB,src_sys_cd) a left join (select  ENRLD_VIN_NB as vin,src_sys_cd as src ,max(MAX_PE_END_TS) as MAX_PE_END_TS from dhf_iot_curated_prod.outbound_score_elements where load_dt>=current_date-1 group by ENRLD_VIN_NB,src_sys_cd) b on a.ENRLD_VIN_NB=b.vin and a.src_sys_cd=b.src where a.pe_end_ts!=b.MAX_PE_END_TS")
microBatchDF.select("ENRLD_VIN_NB","src_sys_cd").union(missing_vins).distinct().createOrReplaceGlobalTempView("microBatch")

# COMMAND ----------

smartmiles_df=spark.sql("""select a.* from smartmilesdf a  left join (select  ENRLD_VIN_NB as vin,src_sys_cd as src ,max(MAX_PE_END_TS) as PE_END_TS from dhf_iot_curated_prod.outbound_score_elements  group by ENRLD_VIN_NB,src_sys_cd) b on a.ENRLD_VIN_NB=b.vin and a.src_sys_cd=b.src where a.MAX_PE_END_TS!=b.PE_END_TS""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct ENRLD_VIN_NB,src_sys_cd from (select ENRLD_VIN_NB ,src_sys_cd  ,max(MAX_PE_END_TS) as MAX_PE_END_TS from dhf_iot_curated_prod.outbound_score_elements where load_dt>=current_date -1  group by ENRLD_VIN_NB,src_sys_cd) a left join (select  ENRLD_VIN_NB as vin,src_sys_cd as src ,max(MAX_PE_END_TS) as MAX_PE_END_TS from dhf_iot_curated_prod.outbound_score_elements  group by ENRLD_VIN_NB,src_sys_cd) b on a.ENRLD_VIN_NB=b.vin and a.src_sys_cd=b.src where a.MAX_PE_END_TS!=b.MAX_PE_END_TS

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_harmonized_prod.integrated_enrollment where VIN_NB = '1C6SRFGT4KN541731'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_harmonized_prod.program_enrollment where VIN_NB = '1C6SRFGT4KN541731'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct SRC_SYS_CD from dhf_iot_curated_prod.inbound_score_elements where  load_dt='2022-11-24'

# COMMAND ----------

# MAGIC %sql
# MAGIC select src_sys_cd,count(distinct enrld_vin_nb) from dhf_iot_curated_prod.inbound_score_elements WHERE  load_dt ='2022-11-26' group by src_sys_cd
# MAGIC -- select distinct load_dt from dhf_iot_curated_prod.inbound_score_elements where src_sys_cd='IMS_SR_5X' 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- (select * from (select *,row_number() over (partition by trip_smry_key order by load_hr_ts desc) as row from (select a.* from dhf_iot_harmonized_prod.trip_detail_seconds a inner join dhf_iot_harmonized_prod.TD_reprocess_trips_2 b on a.trip_smry_key=b.trip_smry_key and a.enrld_vin_nb=b.enrld_vin_nb  and a.load_DT=b.load_dt )) where row=1)
# MAGIC select(count(*)) from dhf_iot_harmonized_prod.trip_detail_seconds a inner join dhf_iot_harmonized_prod.TD_reprocess_trips_2 b on a.trip_smry_key=b.trip_smry_key where a.load_dt in (select load_dt from dhf_iot_harmonized_prod.TD_reprocess_trips_2)

# COMMAND ----------

# MAGIC %sql
# MAGIC select load_dt,count(*) from dhf_iot_curated_prod.extracts_dailyscoring group by load_dt order by load_dt desc

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from dhf_iot_curated_prod.outbound_score_elements where load_dt>='2022-11-24'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select (*) from dhf_iot_curated_prod.outbound_score_elements  order by TRANSACTION_TS desc ----where TRANSACTION_TS='2022-11-26 13:15:47.439' --AND 
# MAGIC select  (*) from dhf_iot_curated_prod.outbound_score_elements where ENRLD_VIN_NB='KNDPRCA61H7099083'  order by TRANSACTION_TS desc --and date(TRANSACTION_TS)='2022-11-26' --

# COMMAND ----------

# MAGIC %sql
# MAGIC select (*) from dhf_iot_curated_prod.outbound_score_elements where  load_dt>='2022-11-24' and enrld_vin_nb='1C4RJFBG6KC619205' order by TRANSACTION_TS desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select (*) from dhf_iot_curated_prod.inbound_score_elements where  load_dt>='2022-11-24' and enrld_vin_nb='1C4RJFBG6KC619205' order by TRANSACTION_TS desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct enrld_vin_nb) from dhf_iot_curated_prod.inbound_score_elements where  load_dt='2022-11-28'  --and TRANSACTION_TS>'2022-11-26 15:46:31.779'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select (*) from dhf_iot_curated_prod.extracts_dailyscoring where data like '%1C4RJFBG6KC619205%'   order by load_dt desc

# COMMAND ----------

df=spark.read.text("s3://pcds-databricks-common-785562577411/iot/extracts/daily-scoring/load_date=2022-12-14/part-00000-tid-7219332195137202032-9aa90627-c87b-4dd1-9738-87f3da603cb7-18664-1-c000.txt")
display(df.filter("value like '%1C4RJFBG6KC619205%'"))

# COMMAND ----------

df=spark.read.text("s3://pcds-databricks-common-785562577411/iot/extracts/daily-scoring/load_date=2022-12-15/part-00000-tid-327565434273015581-f92427f4-b7c1-4a95-91ed-f914d45730d9-18728-1-c000.txt")
display(df.filter("value like '%1C4RJFBG6KC619205%'"))

# COMMAND ----------


import pyspark.sql
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import *
import datetime
import io
import re
import json
from itertools import groupby
from operator import itemgetter
import io
import zipfile
import pandas as pd
from pyspark.sql.types import *
import datetime #4
import json
import boto3
import time
import ast
from datetime import date,datetime
import time
from pyspark.sql import functions as f
from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame
from dataclasses import dataclass
from pyspark.sql.functions import window
from pyspark.sql import *

# COMMAND ----------

from pyspark.sql.functions import *
df=spark.read.table("dhf_iot_harmonized_prod.trip_summary")
df=df.drop("load_dt").withColumn("load_dt",col("trip_smry_key")).drop("trip_smry_key")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC update dhf_iot_generic_autoloader_prod.job_config set CONFIGVALUE='' where CONFIGVALUE is null