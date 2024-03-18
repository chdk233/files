# Databricks notebook source
import boto3
import fnmatch
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,copying files 
def  copyFilesToReplay(bucket,key):

  s3_client = boto3.client('s3')
  SOURCE_BUCKET = bucket
  SOURCE_PREFIX=key.rsplit('/',1)[0]

  response=s3_client.list_objects_v2(Bucket=bucket,Prefix=SOURCE_PREFIX)
  matching_objects=fnmatch.filter([obj['Key'] for obj in response['Contents']],key)
  # print(matching_objects)

  for obi_key in matching_objects:
    DESTINATION_BUCKET = bucket
    DESTINATION_PREFIX = obi_key.replace('load_date','load_dateReplay')
    s3_client.copy_object(
            CopySource = {'Bucket': SOURCE_BUCKET, 'Key': obi_key},
            Bucket = DESTINATION_BUCKET,
            Key = DESTINATION_PREFIX 
            )

# COMMAND ----------

account_id='785562577411'

files_list=spark.sql(f"""select 'dw-internal-pl-cmt-telematics-{account_id}' as bucket,
                                 concat(folder1, folder2,folder3,folder4) as key from 
              (select 'FDR/Archive/tar/' as folder1,
                  concat('load_date=',date(load_hr_ts),'/',LPAD(TRIM(CAST(hour(LOAD_HR_TS) AS VARCHAR(2))),2,'0'),'00/') as folder2,
                  concat('DS_CMT_',account_id,'/') as folder3,
                  concat('DS_CMT_FDR_',driveid,'.tar.gz') as folder4 
                  from dhf_iot_cmt_raw_prod.trip_summary_realtime a inner join 
                (select distinct dataCollectionId from dhf_iot_raw_prod.missing_enrollments) b 
        on a.account_id=b.dataCollectionId where a.src_sys_cd='CMT_PL_FDR' and a.load_dt >= '2023-08-21')""").collect()

for row in files_list:
    bucket=row[0]
    key=row[1]
    # print(f"copying file: bucket={bucket} key={key}")
    copyFilesToReplay(bucket,key)

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.* from (select distinct * from dhf_iot_cmt_raw_prod.trip_summary_realtime a inner join (select distinct dataCollectionId from dhf_iot_raw_prod.missing_enrollments) b on a.account_id=b.dataCollectionId where a.src_sys_cd='CMT_PL_FDR' and a.load_dt >'2023-08-21') a left anti join (select * from dhf_iot_cmt_raw_prod.trip_summary_realtime where src_sys_cd='CMT_PL_FDR' and db_load_date=current_date()) b on a.driveid=b.driveid

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.* from (select distinct * from dhf_iot_cmt_raw_prod.trip_summary_realtime a inner join (select distinct dataCollectionId from dhf_iot_raw_prod.missing_enrollments) b on a.account_id=b.dataCollectionId where a.src_sys_cd='CMT_PL_FDR' and a.load_dt >'2023-08-21') a left anti join (select * from dhf_iot_cmt_raw_prod.outbound_mobile_score_elements where src_sys_cd='CMT_PL_FDR' and db_load_date=current_date()) b on a.driveid=b.trip_smry_key

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.* from (select distinct * from dhf_iot_cmt_raw_prod.trip_summary_realtime a inner join (select distinct dataCollectionId from dhf_iot_raw_prod.missing_enrollments) b on a.account_id=b.dataCollectionId where a.src_sys_cd='CMT_PL_FDR' and a.load_dt >'2023-08-21') a left anti join (select * from dhf_iot_raw_score_prod.fdr_trip_driver_score where src_sys_cd='CMT_PL_FDR' and db_load_date=current_date()) b on a.driveid=b.driveid

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.* from (select distinct * from dhf_iot_cmt_raw_prod.trip_summary_realtime a inner join (select distinct dataCollectionId from dhf_iot_raw_prod.missing_enrollments) b on a.account_id=b.dataCollectionId where a.src_sys_cd='CMT_PL_FDR' and a.load_dt >'2023-08-21') a left anti join (select * from dhf_iot_harmonized_prod.trip_driver_score where src_sys_cd='CMT_PL_FDR' and date(ETL_LAST_UPDATE_DTS)=current_date()) b on a.driveid=b.TRIP_SMRY_KEY

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.* from (select distinct * from dhf_iot_cmt_raw_prod.trip_summary_realtime a inner join (select distinct dataCollectionId from dhf_iot_raw_prod.missing_enrollments) b on a.account_id=b.dataCollectionId where a.src_sys_cd='CMT_PL_FDR' and a.load_dt >'2023-08-21') a left anti join (select * from dhf_iot_curated_prod.trip_digest where src_sys_cd='CMT_PL_FDR' and date(ETL_ROW_EFF_DTS)=current_date()) b on a.driveid=b.TRIP_SMRY_KEY

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_raw_score_prod.fdr_trip_driver_score