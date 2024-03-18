# Databricks notebook source
# MAGIC %sql
# MAGIC Select min(TRANSACTION_TS) from dhf_iot_curated_prod.outbound_score_elements --where enrld_vin_nb='2T1BR12E21C403854'

# COMMAND ----------

11/100


# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from dhf_iot_curated_prod.outbound_score_elements where DAYS_INSTL_CT<0--where enrld_vin_nb='2T1BR12E21C403854'

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from dhf_iot_curated_prod.outbound_score_elements where  enrld_vin_nb='5FNYF6H98MB055195'

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from dhf_iot_curated_prod.device_summary where enrld_vin_nb='5FNYF6H98MB055195'

# COMMAND ----------

# MAGIC %sql
# MAGIC Select enrld_vin_nb,trip_smry_key,src_sys_cd,load_dt,load_hr_ts,(MILE_CT),(ADJST_MILE_CT),(KM_CT) from dhf_iot_curated_prod.trip_detail where MILE_CT>0 and ADJST_MILE_CT<0 order by MILE_CT

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_harmonized_prod.trip_detail_seconds where trip_smry_key='00000022020880481663560381' and load_dt='2022-09-21' and src_sys_cd='IMS_SR_4X'

# COMMAND ----------

# MAGIC %sql
# MAGIC Select enrld_vin_nb,trip_smry_key,src_sys_cd,load_dt,load_hr_ts,(MILE_CT),(ADJST_MILE_CT),(KM_CT) from dhf_iot_curated_prod.trip_detail where MILE_CT<0 order by MILE_CT

# COMMAND ----------

# MAGIC %sql
# MAGIC Select enrld_vin_nb,trip_smry_key,min(MILE_CT),min(ADJST_MILE_CT),min(KM_CT),max(load_dt) from dhf_iot_curated_prod.trip_detail where enrld_vin_nb in ('1FTZR15V8XTB10570',
# MAGIC '2HKRM3H70FH524145',
# MAGIC '4T1BA32K16U089208',
# MAGIC '5N1AT2MT3LC721956',
# MAGIC 'JM3KFBBM5N0586177') and MILE_CT<0  group by enrld_vin_nb,trip_smry_key

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_harmonized_prod.trip_detail_seconds where trip_smry_key='85d4a6e5-4f1f-4734-a7e7-a4d762507fe4' and load_dt='2022-11-23' and pstn_ts='2022-11-22T18:14:56.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(DISTNC_MPH),sum(DISTNC_KPH) from dhf_iot_harmonized_prod.trip_detail_seconds where trip_smry_key='85d4a6e5-4f1f-4734-a7e7-a4d762507fe4' and load_dt='2022-11-23' and pstn_ts=''

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dhf_iot_harmonized_prod.trip_point where trip_smry_key='85d4a6e5-4f1f-4734-a7e7-a4d762507fe4' and load_dt='2022-11-23' --and ( utc_ts='2022-11-22T18:14:56.000+0000')
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select (*) from (select a.* from (select (*) from dhf_iot_harmonized_prod.trip_point where trip_smry_key='85d4a6e5-4f1f-4734-a7e7-a4d762507fe4' and load_dt='2022-11-23') a inner join (select distinct PSTN_TS from dhf_iot_harmonized_prod.trip_detail_seconds where trip_smry_key='85d4a6e5-4f1f-4734-a7e7-a4d762507fe4' and load_dt='2022-11-23' and  DISTNC_MPH <0)b on a.utc_ts=b.PSTN_TS)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from (select a.* from (select (*) from dhf_iot_harmonized_prod.trip_point where trip_smry_key='3c51e8bf-1b6b-4d0c-bcd5-2662ba421920' and load_dt='2022-11-28') a inner join (select distinct PSTN_TS from dhf_iot_harmonized_prod.trip_detail_seconds where trip_smry_key='3c51e8bf-1b6b-4d0c-bcd5-2662ba421920' and load_dt='2022-11-28' and  DISTNC_MPH <0)b on a.utc_ts=b.PSTN_TS)

# COMMAND ----------

# MAGIC %sql
# MAGIC select (*) from dhf_iot_harmonized_prod.trip_point where ACLRTN_RT =0 and load_dt>'2022-11-01' and src_sys_cd='IMS_SM_5X' AND ENGIN_RPM_RT IS NULL AND LAT_NB IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_harmonized_prod.trip_detail_seconds where trip_smry_key='8faf9a85-361c-4a06-8101-d6edd0ebf72b' and load_dt='2023-01-22' and pstn_ts='2023-01-22T00:59:31.000+0000'

# COMMAND ----------

from pyspark.sql.functions import col,from_json, regexp_extract,regexp_replace,date_format, explode, slice, size, split, element_at, lit, current_date, current_timestamp, to_date, to_timestamp
# Databricks notebook source
json_Df=spark.sql("select * from dhf_iot_cmt_raw_prod.kafka_replay_final_new")
json_Df = json_Df.select((regexp_replace('value', r'(\u0000\u0000\u0000\u0000\[)', '').alias('value')))
json_Df = json_Df.select((regexp_replace('value', r'(\u0000\u0000\u0000\u0000\uFFFD)', '').alias('value')))


# COMMAND ----------

json_Df = spark.read.json("s3://pcds-databricks-common-785562577411/iot/raw/smartride/smartmiles/program_enrollment_raw/db_load_date=*")
display(json_Df)