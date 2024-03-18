// Databricks notebook source
val kafkaBootStrapServers=dbutils.widgets.get("kafkaBootStrapServers").toString
val truststoreLocation=dbutils.widgets.get("truststoreLocation").toString
val keystoreLocation=dbutils.widgets.get("keystoreLocation").toString
val schemaRegistryURL=dbutils.widgets.get("schemaRegistryURL").toString
val topic=dbutils.widgets.get("topic").toString
val secret_scope=dbutils.widgets.get("secret_scope").toString
val secret_truststorePassword=dbutils.widgets.get("secret_truststorePassword").toString
val secret_keystorePassword=dbutils.widgets.get("secret_keystorePassword").toString
val secret_keyPassword=dbutils.widgets.get("secret_keyPassword").toString


// COMMAND ----------

val truststorePassword     = dbutils.secrets.get(scope = secret_scope, key = secret_truststorePassword)
val keystorePassword       = dbutils.secrets.get(scope = secret_scope, key = secret_keystorePassword)
val keyPassword            = dbutils.secrets.get(scope = secret_scope, key = secret_keyPassword)

// COMMAND ----------

val kafkaOps = Map (
                          "kafka.bootstrap.servers" -> kafkaBootStrapServers,
                          "kafka.security.protocol" -> "SSL",
                          "kafka.ssl.truststore.location" -> truststoreLocation,
                          "kafka.ssl.truststore.password" -> truststorePassword,
                          "kafka.ssl.keystore.location" -> keystoreLocation,
                          "kafka.ssl.keystore.password" -> keystorePassword,
                          "kafka.ssl.key.password" -> keyPassword,
                          "topic" -> topic
                          )

// COMMAND ----------

   
val finaldf=spark.sql("select * from dhf_iot_cmt_raw_prod.kafka_replay_final_new")

finaldf.write
   .format("kafka")
   .options(kafkaOps)
   .save()

// COMMAND ----------

// DBTITLE 1,creation of data replay table
// %scala
// import org.apache.spark.sql._
// import org.apache.spark.sql.functions._
// val df=spark.sql("select * from dhf_iot_cmt_raw_prod.outbound_mobile_score_elements where prgrm_term_beg_dt is null or src_sys_cd is null")
// val df2=df.withColumn("transaction",expr("to_json(struct(trnsctn_id,trnsctn_tp_cd,src_sys_cd))"))
//           .withColumn("tripLabelUpdate",expr("to_json(struct())"))
//           .withColumn("tripDetail",expr("to_json(struct(trip_detail_path as filepath,trip_detail_count))"))
//           .withColumn("derived",map(lit("phone_handling_ct"),col("phone_handling_ct")))
//           .withColumn("raw",map(lit("deprecated_ct"),col("deprecated_ct"),lit("harsh_braking_ct"),col("harsh_braking_ct")
// ,lit("harsh_cornering_ct"),col("harsh_cornering_ct"),lit("harsh_acceleration_ct"),col("harsh_acceleration_ct")
// ,lit("speeding_ct"),col("speeding_ct"),lit("phone_motion_ct"),col("phone_motion_ct")
// ,lit("phone_call_ct"),col("phone_call_ct"),lit("handheld_ct"),col("handheld_ct")
// ,lit("handsfree_ct"),col("handsfree_ct"),lit("tapping_ct"),col("tapping_ct")
// ,lit("idle_time_ct"),col("idle_time_ct") ))
//            .withColumn("eventsCount",to_json(struct(col("raw"),col("derived")))).drop("entrp_cust_nb","plcy_nb","last_rqst_dt","plcy_st_cd","prgrm_term_beg_dt","prgrm_term_end_dt","prgrm_stts_cd","data_clctn_stts_cd","devc_stts_dt","sbjt_id","sbjt_tp_cd","vndr_cd","devc_id","devc_stts_cd","instl_grc_end_dt")


// val program_enrollmentDF=spark.sql(s"""select distinct VNDR_ACCT_ID,data_clctn_id as dtctlnid,
// CASE WHEN PRGRM_TP_CD = 'mobile' THEN 'SmartRideMobile'
//     WHEN PRGRM_TP_CD = 'mobile-continuous' THEN 'SmartRideMobileContinuous'
//     ELSE PRGRM_TP_CD
// END PRGRM_TP_CD_ne,CASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
//     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
//     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' 
//    END pe_src_sys_cd,EVNT_SYS_TS,plcy_nb,
// prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,data_clctn_stts_cd,
// devc_stts_dt,entrp_cust_nb,sbjt_id,sbjt_tp_cd,vndr_cd,
// devc_id,devc_stts_cd,instl_grc_end_dt,plcy_st_cd, last_rqst_dt
// from (select
// distinct VNDR_ACCT_ID,data_clctn_id,PRGRM_TP_CD,EVNT_SYS_TS,
// plcy_nb,prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,
// coalesce(DATA_CLCTN_STTS,'') as data_clctn_stts_cd,
// coalesce(devc_stts_dt,' ') devc_stts_dt,
// coalesce(ENTRP_CUST_NB,' ') entrp_cust_nb,
// coalesce(sbjt_id,' ') as sbjt_id,
// coalesce(sbjt_tp_cd,' ') as sbjt_tp_cd,
// coalesce(vndr_cd,' ') as vndr_cd,
// coalesce(devc_id,' ') as devc_id,
// coalesce(devc_stts_cd,' ') as devc_stts_cd,
// coalesce(instl_grc_end_dt,' ') as instl_grc_end_dt,
// coalesce(plcy_st_cd,' ') as plcy_st_cd,
// coalesce(last_rqst_dt,' ') as last_rqst_dt,
// PROGRAM_ENROLLMENT_ID ,
// LOAD_HR_TS,
// ETL_LAST_UPDT_DTS 
// ,rank() over (partition by DATA_CLCTN_ID order by EVNT_SYS_TS desc) as rnk
// from dhf_iot_harmonized_prod.program_enrollment
// where upper(VNDR_CD)= 'CMT'
// )where rnk = 1""")

// val summry_enrlmtDF=df2.join(program_enrollmentDF,df2("DATA_CLCTN_ID") ===  program_enrollmentDF("dtctlnid") ,"left").withColumn("src_sys_cd",coalesce(col("src_sys_cd"),col("pe_src_sys_cd")))
 
// val final_df=summry_enrlmtDF.withColumn("transaction",expr("to_json(struct(trnsctn_id,trnsctn_tp_cd,src_sys_cd))")).withColumn("programEnrollment",expr("case when vndr_acct_id is null and dtctlnid is null and PRGRM_TP_CD_ne is null then to_json(struct()) else to_json(struct(entrp_cust_nb,plcy_nb,dtctlnid as data_clctn_id,driver_key,last_rqst_dt,plcy_st_cd,prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,data_clctn_stts_cd,devc_stts_dt,PRGRM_TP_CD_ne as prgrm_tp_cd,prgrm_cd,src_sys_cd,cast(cast(sbjt_id as int) as string) as sbjt_id,sbjt_tp_cd,vndr_cd,devc_id,devc_stts_cd,instl_grc_end_dt)) end ")).withColumn("payload",expr("to_json(struct(transaction,tripLabelUpdate,programEnrollment,tripDetail,eventsCount,trip_smry_json_tt as tripSummary))"))
//             // .selectExpr("CAST(data_clctn_id AS STRING) as key", "CAST(payload AS STRING) as value")


// // display(final_df.filter("VNDR_ACCT_ID is not null"))
// final_df.write.format("delta").mode("overWrite").option("mergeSchema", "true").saveAsTable("dhf_iot_cmt_raw_prod.kafka_replay_new")



// COMMAND ----------

// val dff=spark.sql("select CAST(data_clctn_id AS STRING) as key, CAST(payload AS STRING) as value from dhf_iot_cmt_raw_prod.replay_kafka_2  union select CAST(data_clctn_id AS STRING) as key, CAST(payload AS STRING) as value from dhf_iot_cmt_raw_prod.kafka_replay_tripLabel ")
// dff.write.format("delta").mode("append").saveAsTable("dhf_iot_cmt_raw_prod.kafka_replay_final_new") 

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_cmt_raw_prod.kafka_replay_final_new where key='07f03c51-68dc-4143-aaa8-3cd2cc9bfe30'
// MAGIC key :"07f03c51-68dc-4143-aaa8-3cd2cc9bfe30", {\"trnsctn_id\":\"4CB90789-04FC-46E1-85B8-18C90A71E5A9_20230520_1600\"}
// MAGIC key :"07f03c51-68dc-4143-aaa8-3cd2cc9bfe30",{\"trnsctn_id\":\"49B3AA05-612C-460C-9C1A-5CF5AC972155_20230520_1600\"}
// MAGIC key :"07f03c51-68dc-4143-aaa8-3cd2cc9bfe30",{\"trnsctn_id\":\"C293D9BF-D796-4FFE-9EB9-C476A8BBAD84_20230520_1600\"}
// MAGIC key :"07f03c51-68dc-4143-aaa8-3cd2cc9bfe30",{\"trnsctn_id\":\"DBE460BC-5AC1-410A-9DC7-8DC5687E5AAA_20230520_1600\"}

// COMMAND ----------

// %python
// import io
// import zipfile
// import pandas as pd
// from pyspark.sql.types import *
// from pyspark.sql.functions import *
// import datetime
// import json
// import boto3
// import time
// from pyspark.sql import Row
// table_df = spark.sql("""select * from dhf_iot_cmt_raw_prod.outbound_mobile_score_elements where trnsctn_tp_cd='Trip Label Update'""").selectExpr("trnsctn_id","trnsctn_tp_cd","src_sys_cd","trip_smry_key","devc_key","user_lbl_nm","user_lbl_dt","plcy_st_cd","entrp_cust_nb","plcy_nb","data_clctn_id","driver_key","prgrm_term_beg_dt","prgrm_term_end_dt","prgrm_stts_cd","data_clctn_stts_cd","coalesce(devc_stts_dt,' ') as devc_stts_dt","prgrm_tp_cd","prgrm_cd","load_dt","db_load_time","db_load_date","load_hr_ts","cast(cast(sbjt_id as int) as string) as sbjt_id","sbjt_tp_cd","vndr_cd","devc_id","devc_stts_cd","instl_grc_end_dt","last_rqst_dt")

// kafka_df = table_df .withColumn("tripLabelUpdate",expr("to_json(struct(trip_smry_key,devc_key,user_lbl_nm,user_lbl_dt,plcy_st_cd,prgrm_cd))")).withColumn("transaction",expr("to_json(struct(trnsctn_id,trnsctn_tp_cd,src_sys_cd))")).withColumn("programEnrollment",expr("to_json(struct(entrp_cust_nb,plcy_nb,data_clctn_id,driver_key,last_rqst_dt,plcy_st_cd,prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,data_clctn_stts_cd,devc_stts_dt,prgrm_tp_cd,prgrm_cd,src_sys_cd,cast(cast(sbjt_id as int) as string) as sbjt_id,sbjt_tp_cd,vndr_cd,devc_id,devc_stts_cd,instl_grc_end_dt))")).withColumn("tripDetail",to_json(struct(lit(None)))).withColumn("eventsCount",to_json(struct(lit(None)))).withColumn("trip_smry_json_tt",to_json(struct(lit(None)))).withColumn("payload",expr("to_json(struct(transaction,tripLabelUpdate,programEnrollment,tripDetail,eventsCount,trip_smry_json_tt as tripSummary))"))



// kafka_df.write.format("delta").mode("append").saveAsTable("dhf_iot_cmt_raw_prod.kafka_replay_tripLabel")