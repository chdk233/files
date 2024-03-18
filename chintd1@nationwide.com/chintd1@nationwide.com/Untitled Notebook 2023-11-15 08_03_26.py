# Databricks notebook source
env='prod'
account_id='785562577411'

# COMMAND ----------

spark.sql(f"""CREATE TABLE if not exists spark_catalog.dhf_iot_raw_{env}.sh_enrollment (
  key STRING,
  value STRING,
  topic STRING,
  partition INT,
  offset BIGINT,
  timestamp TIMESTAMP,
  timestampType INT,
  id STRING,
  time BIGINT,
  source STRING,
  type STRING,
  transactionType STRING,
  policyNumber STRING,
  policyState STRING,
  policyType STRING,
  enrollmentId STRING,
  enrollmentEffectiveDate STRING,
  enrollmentProcessDate STRING,
  enrollmentRemovalReason STRING,
  communicationType STRING,
  programType STRING,
  programStatus STRING,
  dataCollectionId STRING,
  dataCollectionStatus STRING,
  dataCollectionBeginDate STRING,
  dataCollectionEndDate STRING,
  vendorName STRING,
  vendorUserId STRING,
  deviceId STRING,
  deviceType STRING,
  checkOutType STRING,
  deviceStatus STRING,
  deviceStatusDate STRING,
  vendorAccountId STRING,
  deviceCategory STRING,
  deviceDiscountType STRING,
  src_sys_cd STRING,
  load_dt DATE,
  load_hr_ts TIMESTAMP,
  db_load_time TIMESTAMP,
  db_load_date DATE)
USING delta
PARTITIONED BY (src_sys_cd)
LOCATION 's3://pc-iot-raw-{account_id}/smarthome/tables/dhf_iot_raw_{env}/sh_enrollment'
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '4')""")

# COMMAND ----------

spark.sql(f"""CREATE TABLE spark_catalog.dhf_iot_curated_{env}.smarthome_device_status(
  SMARTHOME_DEVICE_STATUS_ID BIGINT,
  DEVICE_ID STRING,
  POLICY_KEY STRING,
  POLICY_KEY_ID BIGINT,
  PLCY_RT_ST_CD STRING,
  PLCY_DESC_TYPE STRING,
  ENRLMNT_EFF_DT DATE,
  DEVICE_DSCNT_TP STRING,
  DEVC_FIRST_STTS_DT DATE,
  DEVC_LST_STTS_DT DATE,
  DEVC_CATEGORY STRING,
  DEVC_STTS_CD STRING,
  PARTNER_MEMBER_ID STRING,
  DATA_COLLECTION_ID STRING,
  ACTV_STTS_IN BIGINT,
  ACTVTN_DT DATE,
  LIVE_STATUS_IN BIGINT,
  LIVE_LOAD_DT DATE,
  CONTEXT_TOPIC STRING,
  CONTEXT_ID STRING,
  CONTEXT_SOURCE STRING,
  CONTEXT_TIME STRING,
  CONTEXT_TYPE STRING,
  PRGRM_ENROLLMENT_ID STRING,
  QT_USER_ID STRING,
  QT_SRC_IDFR STRING,
  PLCY_TP INT,
  PRDCR_OFFC STRING,
  PRDCR_TP STRING,
  PLCY_TERM STRING,
  PROTCTV_DEVC_DISC_AMT STRING,
  HOME_CAR_DISC_AMT STRING,
  ACQUISITION_CHANNEL_CD STRING,
  SERVICING_CHANNEL_CD STRING,
  MEMBER_TYPE STRING,
  SRC_SYS_CD STRING,
  LOAD_DT DATE,
  LOAD_HR_TS TIMESTAMP,
  ETL_ROW_EFF_DTS TIMESTAMP,
  ETL_LAST_UPDT_DTS TIMESTAMP)
USING delta
PARTITIONED BY (SRC_SYS_CD, LOAD_DT)
LOCATION 's3://pcds-databricks-common-{account_id}/iot/delta/curated/dhf_iot_curated_{env}/SmartHome_Device_Status'
TBLPROPERTIES (
  'Type' = 'EXTERNAL',
  'delta.enableChangeDataFeed' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '4')""")

# COMMAND ----------

spark.sql(f"""CREATE TABLE spark_catalog.dhf_iot_curated_{env}.smarthome_device_summary (
  SMARTHOME_DEVICE_SUMMARY_ID BIGINT NOT NULL,
  DEVICE_ID STRING NOT NULL,
  POLICY_KEY STRING NOT NULL,
  POLICY_KEY_ID BIGINT,
  PLCY_RT_ST_CD STRING NOT NULL,
  ENRLMNT_EFF_DT DATE NOT NULL,
  PARTNER_MEMBER_ID STRING,
  DATA_COLLECTION_ID STRING,
  ACTVTN_DT DATE,
  INSTALLED_DATE DATE,
  DEVC_CNCT_IN STRING,
  DEVC_UNAVL_IN STRING,
  DEVC_FIRST_STTS_DT DATE,
  DEVC_LST_STTS_DT DATE,
  DEVC_STTS_CD STRING,
  LIVE_STATUS_IN BIGINT,
  LIVE_LOAD_DT DATE,
  CONTEXT_TOPIC STRING,
  CONTEXT_ID STRING,
  CONTEXT_SOURCE STRING,
  CONTEXT_TIME STRING,
  CONTEXT_TYPE STRING,
  PRGRM_ENROLLMENT_ID STRING,
  DEVC_CATEGORY STRING,
  PLCY_DESC_TYPE STRING,
  DEVC_SHIPPED_DT DATE,
  QT_USER_ID STRING,
  QT_SRC_IDFR STRING,
  PLCY_TP INT,
  PRDCR_OFFC STRING,
  PRDCR_TP STRING,
  PLCY_TERM STRING,
  PROTCTV_DEVC_DISC_AMT STRING,
  HOME_CAR_DISC_AMT STRING,
  ACQUISITION_CHANNEL_CD STRING,
  SERVICING_CHANNEL_CD STRING,
  MEMBER_TYPE STRING,
  SRC_SYS_CD STRING,
  LOAD_DT DATE,
  LOAD_HR_TS TIMESTAMP,
  ETL_ROW_EFF_DTS TIMESTAMP,
  ETL_LAST_UPDT_DTS TIMESTAMP)
USING delta
PARTITIONED BY (SRC_SYS_CD)
LOCATION 's3://pcds-databricks-common-{account_id}/iot/delta/curated/dhf_iot_curated_{env}/SmartHome_Device_Summary'
TBLPROPERTIES (
  'Type' = 'EXTERNAL',
  'delta.enableChangeDataFeed' = 'true',
  'delta.minReaderVersion' = '1',
  'delta.minWriterVersion' = '4')""")

# COMMAND ----------

df2=spark.read.option("header","true").csv("s3://dw-internal-pl-cmt-telematics-785562577411/CMT/CMTPLtrip_summaryrt_rt/load_date=2020-06-12/")
df2.filter("driev").display()

# COMMAND ----------

df=spark.read.option("header","true").csv("s3://dw-internal-pl-cmt-telematics-785562577411/CMT/CMTPLtrip_detail_rt/load_date=2020-06-12/")
df.count()   37486270	 2020-06-12	

# COMMAND ----------

dates=*([row['dt'] for row in spark.sql("select distinct load_date from dhf_iot_cmt_raw_prod.trip_summary_realtime where load_dt <'2021-06-01'").collect()]),'0','1'
for date in dates:
  df=spark.read.option("header","true").csv(f"s3://dw-internal-pl-cmt-telematics-785562577411/CMT/CMTPLtrip_detail_rt/load_date={date}/")
  df.createOrReplaceTempView("tabl")
  final_df=spark.sql(f"select a.* from tabl a left anti join (select * from dhf_iot_cmt_raw_prod.trip_detail_realtime where load_dt={date}) b on a.drive_id=b.drive_id and a.time=b.time").withColumnRenamed("gps_speed(m/s)","gps_speed")
  if final_df.count():
    final_df.write.format("delta").mode("append").saveAsTable("dhf_iot_cmt_raw_prod.trip_detail_realtime_temp")


# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct load_date from dhf_iot_cmtpl_raw_prod.trip_summary_realtime where load_dt <'2021-06-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_cmt_raw_prod.trip_detail_realtime

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tabl a left anti join (select * from dhf_iot_cmt_raw_prod.trip_detail_realtime where load_dt='2020-06-12') b on a.drive_id=b.drive_id and a.time=b.time

# COMMAND ----------

df.createOrReplaceTempView("tabl")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_cmt_raw_prod.trip_detail_realtime where load_dt='2020-06-12' and drive_id='F1FB7B6F-4544-40C9-9F27-BC89EA09DAAA'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_cmt_raw_prod.trip_summary_realtime where   driveid='F1FB7B6F-4544-40C9-9F27-BC89EA09DAAA'

# COMMAND ----------

