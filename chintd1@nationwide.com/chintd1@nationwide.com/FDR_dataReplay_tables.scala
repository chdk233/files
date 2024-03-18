// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

// COMMAND ----------

// DBTITLE 1,Data_replay table for trips (program_enrollment schema update)

val df=spark.sql("""select os.* from (select * from dhf_iot_cmt_raw_prod.outbound_mobile_score_elements where load_dt in ('2023-06-21','2023-07-10')) os inner join (select distinct x.trnsctn_id,coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast((split(x.trnsctn_id,'_')[1]) as string), 'yyyyMMdd') AS TIMESTAMP)), to_date("9999-12-31")) AS LOAD_DT from (select a.* from dhf_iot_cmt_raw_prod.mf_trips a left anti join dhf_iot_cmt_raw_prod.replay_kafka_trips b on a.trnsctn_id=b.trnsctn_id) x left anti join dhf_iot_cmt_raw_prod.kafka_replay_tripLabel y on x.trnsctn_id=y.trnsctn_id where error_owner='PCD') mf on os.trnsctn_id=mf.trnsctn_id and os.load_dt=mf.load_dt """)
val df2=df.withColumn("transaction",expr("to_json(struct(trnsctn_id,trnsctn_tp_cd,src_sys_cd))"))
          .withColumn("tripLabelUpdate",expr("to_json(struct())"))
          .withColumn("tripDetail",expr("to_json(struct(trip_detail_path as filepath,trip_detail_count))"))
          .withColumn("derived",map(lit("phone_handling_ct"),col("phone_handling_ct")))
          .withColumn("raw",map(lit("deprecated_ct"),col("deprecated_ct"),lit("harsh_braking_ct"),col("harsh_braking_ct")
,lit("harsh_cornering_ct"),col("harsh_cornering_ct"),lit("harsh_acceleration_ct"),col("harsh_acceleration_ct")
,lit("speeding_ct"),col("speeding_ct"),lit("phone_motion_ct"),col("phone_motion_ct")
,lit("phone_call_ct"),col("phone_call_ct"),lit("handheld_ct"),col("handheld_ct")
,lit("handsfree_ct"),col("handsfree_ct"),lit("tapping_ct"),col("tapping_ct")
,lit("idle_time_ct"),col("idle_time_ct") ))
           .withColumn("eventsCount",to_json(struct(col("raw"),col("derived")))).drop("entrp_cust_nb","plcy_nb","last_rqst_dt","plcy_st_cd","prgrm_term_beg_dt","prgrm_term_end_dt","prgrm_stts_cd","data_clctn_stts_cd","devc_stts_dt","sbjt_id","sbjt_tp_cd","vndr_cd","devc_id","devc_stts_cd","instl_grc_end_dt")


val program_enrollmentDF=spark.sql(s"""select distinct VNDR_ACCT_ID,data_clctn_id as dtctlnid,
CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
    WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
    ELSE PRGRM_TP_CD
END PRGRM_TP_CD_ne,CASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
    WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
    WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' 
   END pe_src_sys_cd,EVNT_SYS_TS,plcy_nb,
prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,data_clctn_stts_cd,
devc_stts_dt,entrp_cust_nb,sbjt_id,sbjt_tp_cd,vndr_cd,
devc_id,devc_stts_cd,instl_grc_end_dt,plcy_st_cd, last_rqst_dt
from (select
distinct VNDR_ACCT_ID,data_clctn_id,PRGRM_TP_CD,EVNT_SYS_TS,
plcy_nb,prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,
coalesce(DATA_CLCTN_STTS,'') as data_clctn_stts_cd,
coalesce(devc_stts_dt,' ') devc_stts_dt,
coalesce(ENTRP_CUST_NB,' ') entrp_cust_nb,
coalesce(sbjt_id,' ') as sbjt_id,
coalesce(sbjt_tp_cd,' ') as sbjt_tp_cd,
coalesce(vndr_cd,' ') as vndr_cd,
coalesce(devc_id,' ') as devc_id,
coalesce(devc_stts_cd,' ') as devc_stts_cd,
coalesce(instl_grc_end_dt,' ') as instl_grc_end_dt,
coalesce(plcy_st_cd,' ') as plcy_st_cd,
coalesce(last_rqst_dt,' ') as last_rqst_dt,
PROGRAM_ENROLLMENT_ID ,
LOAD_HR_TS,
ETL_LAST_UPDT_DTS 
,rank() over (partition by DATA_CLCTN_ID order by EVNT_SYS_TS desc) as rnk
from dhf_iot_harmonized_prod.program_enrollment
where upper(VNDR_CD)= 'CMT'
)where rnk = 1""")

val summry_enrlmtDF=df2.join(program_enrollmentDF,df2("DATA_CLCTN_ID") ===  program_enrollmentDF("dtctlnid") ,"left").withColumn("src_sys_cd",coalesce(col("src_sys_cd"),col("pe_src_sys_cd")))
 
val final_df=summry_enrlmtDF.withColumn("transaction",expr("to_json(struct(trnsctn_id,trnsctn_tp_cd,src_sys_cd))")).withColumn("programEnrollment",expr("case when vndr_acct_id is null and dtctlnid is null and PRGRM_TP_CD_ne is null then to_json(struct()) else to_json(struct(entrp_cust_nb,plcy_nb,dtctlnid as data_clctn_id,driver_key,last_rqst_dt,plcy_st_cd,prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,data_clctn_stts_cd,devc_stts_dt,PRGRM_TP_CD_ne as prgrm_tp_cd,prgrm_cd,src_sys_cd,cast(cast(sbjt_id as int) as string) as sbjt_id,sbjt_tp_cd,vndr_cd,devc_id,devc_stts_cd,instl_grc_end_dt)) end ")).withColumn("payload",expr("to_json(struct(transaction,tripLabelUpdate,programEnrollment,tripDetail,eventsCount,trip_smry_json_tt as tripSummary))"))
            // .selectExpr("CAST(data_clctn_id AS STRING) as key", "CAST(payload AS STRING) as value")


// display(final_df.filter("VNDR_ACCT_ID is not null"))
final_df.write.format("delta").mode("append").saveAsTable("dhf_iot_cmt_raw_prod.replay_kafka_trips")



// COMMAND ----------

// DBTITLE 1,Data_replay table for tripLable (program_enrollment schema update)
// MAGIC %python
// MAGIC import io
// MAGIC import zipfile
// MAGIC import pandas as pd
// MAGIC from pyspark.sql.types import *
// MAGIC from pyspark.sql.functions import *
// MAGIC import datetime
// MAGIC import json
// MAGIC import boto3
// MAGIC import time
// MAGIC from pyspark.sql import Row
// MAGIC table_df = spark.sql("""select os.* from (select * from dhf_iot_cmt_raw_prod.outbound_mobile_score_elements where load_dt in ('2023-06-21','2023-07-10') and trnsctn_tp_cd='Trip Label Update') os inner join (select distinct x.trnsctn_id,coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast((split(x.trnsctn_id,'_')[1]) as string), 'yyyyMMdd') AS TIMESTAMP)), to_date("9999-12-31")) AS LOAD_DT from (select a.* from dhf_iot_cmt_raw_prod.mf_trips a left anti join dhf_iot_cmt_raw_prod.replay_kafka_trips b on a.trnsctn_id=b.trnsctn_id) x left anti join dhf_iot_cmt_raw_prod.kafka_replay_tripLabel y on x.trnsctn_id=y.trnsctn_id where error_owner='PCD') mf on os.trnsctn_id=mf.trnsctn_id and os.load_dt=mf.load_dt""").selectExpr("trnsctn_id","trnsctn_tp_cd","src_sys_cd","trip_smry_key","devc_key","user_lbl_nm","user_lbl_dt","plcy_st_cd","entrp_cust_nb","plcy_nb","data_clctn_id","driver_key","prgrm_term_beg_dt","prgrm_term_end_dt","prgrm_stts_cd","data_clctn_stts_cd","coalesce(devc_stts_dt,' ') as devc_stts_dt","prgrm_tp_cd","prgrm_cd","load_dt","db_load_time","db_load_date","load_hr_ts","cast(cast(sbjt_id as int) as string) as sbjt_id","sbjt_tp_cd","vndr_cd","devc_id","devc_stts_cd","instl_grc_end_dt","last_rqst_dt")
// MAGIC
// MAGIC kafka_df = table_df .withColumn("tripLabelUpdate",expr("to_json(struct(trip_smry_key,devc_key,user_lbl_nm,user_lbl_dt,plcy_st_cd,prgrm_cd))")).withColumn("transaction",expr("to_json(struct(trnsctn_id,trnsctn_tp_cd,src_sys_cd))")).withColumn("programEnrollment",expr("to_json(struct(entrp_cust_nb,plcy_nb,data_clctn_id,driver_key,last_rqst_dt,plcy_st_cd,prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,data_clctn_stts_cd,devc_stts_dt,prgrm_tp_cd,prgrm_cd,src_sys_cd,cast(cast(sbjt_id as int) as string) as sbjt_id,sbjt_tp_cd,vndr_cd,devc_id,devc_stts_cd,instl_grc_end_dt))")).withColumn("tripDetail",to_json(struct(lit(None)))).withColumn("eventsCount",to_json(struct(lit(None)))).withColumn("trip_smry_json_tt",to_json(struct(lit(None)))).withColumn("payload",expr("to_json(struct(transaction,tripLabelUpdate,programEnrollment,tripDetail,eventsCount,trip_smry_json_tt as tripSummary))"))
// MAGIC
// MAGIC
// MAGIC
// MAGIC kafka_df.write.format("delta").mode("append").saveAsTable("dhf_iot_cmt_raw_prod.kafka_replay_tripLabel")

// COMMAND ----------

// DBTITLE 1,merging both trip and triplable into one table
val dff=spark.sql("(select CAST(data_clctn_id AS STRING) as key, CAST(payload AS STRING) as value from dhf_iot_cmt_raw_prod.replay_kafka_trips a inner join dhf_iot_cmt_raw_prod.mf_trips b on a.trnsctn_id=b.trnsctn_id ) union (select CAST(data_clctn_id AS STRING) as key, CAST(payload AS STRING) as value from dhf_iot_cmt_raw_prod.kafka_replay_tripLabel x inner join dhf_iot_cmt_raw_prod.mf_trips y on x.trnsctn_id=y.trnsctn_id ) union (select CAST(data_clctn_id AS STRING) as key, CAST(payload AS STRING) as value from  (select * from dhf_iot_cmt_raw_prod.replay_kafka_trips where load_dt<'2023-05-23') m inner join (select * from dhf_iot_cmt_raw_prod.mf_trips where trnsctn_id is null and error_owner='PCD') n on n.key=m.data_clctn_id)")
dff.write.format("delta").mode("overWrite").saveAsTable("dhf_iot_cmt_raw_prod.kafka_replay_final_new") 


// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct trnsctn_id) from ((select distinct a.trnsctn_id  from dhf_iot_cmt_raw_prod.replay_kafka_trips a inner join dhf_iot_cmt_raw_prod.mf_trips b on a.trnsctn_id=b.trnsctn_id ) union (select distinct x.trnsctn_id  from dhf_iot_cmt_raw_prod.kafka_replay_tripLabel x inner join dhf_iot_cmt_raw_prod.mf_trips y on x.trnsctn_id=y.trnsctn_id ) union (select distinct m.trnsctn_id  from  (select * from dhf_iot_cmt_raw_prod.replay_kafka_trips where load_dt<'2023-05-23') m inner join (select * from dhf_iot_cmt_raw_prod.mf_trips where trnsctn_id is null and error_owner='PCD') n on n.key=m.data_clctn_id))

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_cmt_raw_prod.kafka_replay_tripLabel where programEnrollment not like '%devc_stts_dt%'

// COMMAND ----------

// DBTITLE 1,s3 path of files to be moved 
val df=spark.sql("(select distinct trip_detail_path as source_path from dhf_iot_cmt_raw_prod.replay_kafka_trips a inner join dhf_iot_cmt_raw_prod.mf_trips b on a.trnsctn_id=b.trnsctn_id )union (select distinct trip_detail_path as source_path from  (select * from dhf_iot_cmt_raw_prod.replay_kafka_trips where load_dt<'2023-05-23') m inner join (select * from dhf_iot_cmt_raw_prod.mf_trips where trnsctn_id is null and error_owner='PCD') n on n.key=m.data_clctn_id)").withColumn("target_path",$"source_path")
df.write.format("delta").mode("overWrite").saveAsTable("dhf_iot_cmt_raw_prod.s3_move") 
// display(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select count(*) from  dhf_iot_cmt_raw_prod.s3_move
// MAGIC update dhf_iot_cmt_raw_prod.s3_move set source_path=replace(source_path,'s3://pcds-databricks-common-785562577411/iot/raw/smartride/cmt-pl/realtime','s3://dw-internal-pl-cmt-telematics-785562577411/CMT/Archive')
// MAGIC

// COMMAND ----------

// DBTITLE 1,Function to copy tar files to PCDS buck where MF can access
// MAGIC %py
// MAGIC def python_boto3(source_filename,target_bucket_prefix,aws_accountid):
// MAGIC   import threading
// MAGIC   class CopyThread (threading.Thread):
// MAGIC     def __init__(self, **kwargs):
// MAGIC         threading.Thread.__init__(self)
// MAGIC         self.name          = "CopyThread-{}".format(
// MAGIC                                 str(kwargs.get("thread_number")))
// MAGIC         self.s3_session   = kwargs.get("s3_session")
// MAGIC         self.source_file_list    =kwargs.get("source_file_list")
// MAGIC         self.target_bucket_prefix    =kwargs.get("target_bucket_prefix")
// MAGIC         self.key_count=kwargs.get("key_count")
// MAGIC     def run(self):
// MAGIC       try:
// MAGIC         for source_file in self.source_file_list:
// MAGIC           source_bucket=source_file.split('/')[2]
// MAGIC           source_prefix=source_file.split("/",3)[3]
// MAGIC           target_bucket=self.target_bucket_prefix.split('/')[2]
// MAGIC           target_prefix=self.target_bucket_prefix.split('/',3)[3]+'/'.join(source_file.split("/")[-4:])
// MAGIC           copy_source = {
// MAGIC     'Bucket': source_bucket,
// MAGIC     'Key': source_prefix
// MAGIC }
// MAGIC           self.s3_session.meta.client.copy(copy_source, target_bucket, target_prefix)
// MAGIC         self.status=("{} completed successfully and copied {} files"\
// MAGIC                 .format(self.name,self.key_count))
// MAGIC       except Exception as e:
// MAGIC         print("Error occured for key '{}'\nHere is the error {}"\
// MAGIC                         .format(source_file,str(e)))
// MAGIC         self.status=("Error occured for key '{}'\nHere is the error {}"\
// MAGIC                         .format(source_file,str(e)))
// MAGIC         self.source_file=source_file
// MAGIC   try:
// MAGIC     thread_status =""
// MAGIC     import boto3
// MAGIC     import math
// MAGIC     sts = boto3.client('sts',region_name='us-east-1',endpoint_url ='https://sts.us-east-1.amazonaws.com')
// MAGIC     response2 = sts.assume_role(RoleArn=f'arn:aws:iam::{aws_accountid}:role/pcds-databricks-common-access',RoleSessionName='myDhfsession')
// MAGIC     s3_session = boto3.resource('s3',
// MAGIC                           aws_access_key_id=response2.get('Credentials').get('AccessKeyId'),
// MAGIC                           aws_secret_access_key=response2.get('Credentials').get('SecretAccessKey'),
// MAGIC                           aws_session_token=response2.get('Credentials').get('SessionToken'))
// MAGIC     source_filename_list=source_filename.split(",")
// MAGIC     temp_source_filename_list=source_filename_list.copy()
// MAGIC     batch_size=10
// MAGIC     thread_count=int(math.ceil(len(temp_source_filename_list)/batch_size))
// MAGIC     threads=[]
// MAGIC     for i in range(thread_count):
// MAGIC       if(len(source_filename_list)>0):
// MAGIC         source_file_list=temp_source_filename_list[:batch_size]
// MAGIC         del temp_source_filename_list[:batch_size]
// MAGIC         arguments={'thread_number':i,
// MAGIC              'key_count':len(source_file_list),
// MAGIC              's3_session':s3_session,
// MAGIC              'source_file_list':source_file_list,
// MAGIC              'target_bucket_prefix':target_bucket_prefix}
// MAGIC         thread = CopyThread(**arguments)
// MAGIC         thread.start()
// MAGIC         threads.append(thread)
// MAGIC     for t in threads:
// MAGIC       t.join()
// MAGIC     for t in threads:
// MAGIC       thread_status=thread_status+'@,'+t.status
// MAGIC     return f"{thread_status}"
// MAGIC   except Exception as e:
// MAGIC     error=str(e)
// MAGIC     status=(f"{thread_status}+'@,'+Error occured.Here is the error {error}")
// MAGIC     print(status)
// MAGIC     return f"{status}"
// MAGIC
// MAGIC spark.udf.register("Boto3S3FileCopy", python_boto3)

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
val s3PathForTarFiles=s"s3://pcds-databricks-common-785562577411/iot/raw/smartride/cmt-pl/realtime/tar/"
val aws_accountid="785562577411"

def saveTripDetailDataToS3()  = {  
  
  // println("s3FileLocationForTripdetail is "+s3FileLocationForTripdetail)
  
  if(!spark.sql("select source_path as path from dhf_iot_cmt_raw_prod.s3_move where source_path not like '%07f03c51-68dc-4143-aaa8-3cd2cc9bfe30%'").isEmpty) {
     val sourcefilepath = spark.sql("select source_path as path from dhf_iot_cmt_raw_prod.s3_move where source_path not like '%07f03c51-68dc-4143-aaa8-3cd2cc9bfe30%'").select("path").distinct
                         .withColumn("source_path_split",split(col("path"),"/",7)(6))
.withColumn("target_path",concat(lit(s3PathForTarFiles),col("source_path_split"))).drop("source_path_split").collect.map(_.getString(0)).mkString(",") 

   println(s"Boto3 start -${LocalDateTime.now().toString()}")
    var s3_copy_status = spark.sql(s"select Boto3S3FileCopy('${sourcefilepath}','${s3PathForTarFiles}','${aws_accountid}') S3_file_copy").select(explode(split(col("S3_file_copy"),"@,")))
    s3_copy_status.show(truncate=false)
  var error=s3_copy_status.filter($"col" rlike "(?i)Error")
  println(s"S3 Copy Error Count is : ${error.count()}")
  if (error.count() !=0)
    {
      println(s"Error while copying the files for thread : ${error.collect.map(_.getString(0)).mkString(",")}")
      throw new Exception(s"Error while copying the files for thread : ${error.collect.map(_.getString(0)).mkString(",")}");
    }
  }
}

// COMMAND ----------

saveTripDetailDataToS3()

// COMMAND ----------


// val df=spark.sql("""select os.* from (select * from dhf_iot_cmt_raw_prod.outbound_mobile_score_elements where load_dt in ('2023-06-21','2023-07-10')) os inner join (select distinct x.trnsctn_id,coalesce(TO_DATE(CAST(UNIX_TIMESTAMP(cast((split(x.trnsctn_id,'_')[1]) as string), 'yyyyMMdd') AS TIMESTAMP)), to_date("9999-12-31")) AS LOAD_DT from (select a.* from dhf_iot_cmt_raw_prod.mf_trips a left anti join dhf_iot_cmt_raw_prod.replay_kafka_trips b on a.trnsctn_id=b.trnsctn_id) x left anti join dhf_iot_cmt_raw_prod.kafka_replay_tripLabel y on x.trnsctn_id=y.trnsctn_id where error_owner='PCD') mf on os.trnsctn_id=mf.trnsctn_id and os.load_dt=mf.load_dt """)
// df.write.format("delta").mode("append").saveAsTable("dhf_iot_cmt_raw_prod.kafka_replay_missing_temp")

// COMMAND ----------


// import org.apache.spark.sql.functions._
// val summry_enrlmtDF=spark.sql("select * from dhf_iot_cmt_raw_prod.replay_kafka_2 ").filter("trip_smry_json_tt  like '%file_src_cd%'")

// val final_df=summry_enrlmtDF.drop("payload").withColumn("payload",expr("to_json(struct(transaction,tripLabelUpdate,programEnrollment,tripDetail,eventsCount,trip_smry_json_tt as tripSummary))"))

// // display(final_df)
// final_df.write.format("delta").mode("append").saveAsTable("dhf_iot_cmt_raw_prod.replay_kafka_trips")



// import org.apache.spark.sql.functions._
// val summry_enrlmtDF=spark.sql("select * from dhf_iot_cmt_raw_prod.replay_kafka_2 ").filter("trip_smry_json_tt not like '%file_src_cd%'")

// val final_df=summry_enrlmtDF.drop("payload","trip_smry_json_tt").withColumnRenamed("smry","trip_smry_json_tt").withColumn("payload",expr("to_json(struct(transaction,tripLabelUpdate,programEnrollment,tripDetail,eventsCount,trip_smry_json_tt as tripSummary))"))

// // display(final_df)
// final_df.write.format("delta").mode("overWrite").saveAsTable("dhf_iot_cmt_raw_prod.replay_kafka_trips")

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(DISTINCT trnsctn_id) from dhf_iot_cmt_raw_prod.mf_trips where error_owner='PCD' -- where key!='07f03c51-68dc-4143-aaa8-3cd2cc9bfe30'