// Databricks notebook source


// COMMAND ----------



// COMMAND ----------

dbutilogimport spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._


// COMMAND ----------

def compress(s: String): String = {

  def f(s2: String, res: String): String = {
    if (s2.isEmpty) res
    else {
      val (l,r) = s2.span(_ == s2.head)
      f(r, res + (if (l.size == 1) l+1 else l.head + l.size.toString))
    }
  }

  f(s, "")
}

// COMMAND ----------

def compress(s: String) = {
    val a : List[(Char,Int)] = List()
    s.toCharArray.foldLeft(a)((acc, elem) => acc match {
        case Nil => (elem, 1) :: Nil
        case (a, b) :: tail =>
            if (a == elem) (elem, b + 1) :: tail else (elem, 1) :: acc
    }).reverse
    .map{ case (a, b) => a.toString + b }
    .mkString("")
}

// COMMAND ----------

compress("abbccdd")

// COMMAND ----------

val s="aaaaaabbccdd"
val a : List[(Char,Int)] = List()
 s.toCharArray.foldLeft(a)

// COMMAND ----------

val a=List((1,4),(3,5),(4,6),(2,4))
// a.sortWith((x:(Int,Int),y:(Int,Int))=>x._1>y_.1)
a.sortWith(_._2>_._1)

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select * from dhf_iot_cmt_raw_prod.events_realtime where event_type=7
// MAGIC select (cast(parsename('100.2.2.30', 4)*1000000000.0 as decimal(12, 0)) +
// MAGIC               cast(parsename('100.2.2.30', 3)*1000000.0 as decimal(12, 0)) +
// MAGIC               cast(parsename('100.2.2.30', 2)*1000.0 as decimal(12, 0)) +
// MAGIC               cast(parsename('100.2.2.30', 1) as decimal(12, 0))
// MAGIC              ) as iplow_decimal

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select distinct PRGRM_TP_CD from dhf_iot_harmonized_prod.program_enrollment where VNDR_CD='CMT'
// MAGIC
// MAGIC SELECT DISTINCT CASE WHEN PRGRM_TP_CD='mobile' THEN 'SmartRideMobile'
// MAGIC    WHEN PRGRM_TP_CD='mobile-continuous' THEN 'SmartRideMobileContinuous' ELSE PRGRM_TP_CD END PRGRM_TP_CD_NEW,PRGRM_TP_CD from dhf_iot_harmonized_prod.program_enrollment where VNDR_CD='CMT'
// MAGIC
// MAGIC
// MAGIC -- SmartRideMobileContinuous SmartRideMobile   SmartRideMobile  SmartRideMobileContinuous

// COMMAND ----------

val program_enrollmentDF=spark.sql(s"""select distinct  pe.VNDR_ACCT_ID,pe.data_clctn_id,CASE WHEN pe.PRGRM_TP_CD='mobile' THEN 'SmartRideMobile'
   WHEN pe.PRGRM_TP_CD='mobile-continuous' THEN 'SmartRideMobileContinuous' ELSE pe.PRGRM_TP_CD END PRGRM_TP_CD,EVNT_SYS_TS,plcy_nb,prgrm_term_beg_dt,prgrm_term_end_dt,prgrm_stts_cd,coalesce(DATA_CLCTN_STTS,'') as data_clctn_stts_cd,coalesce(devc_stts_dt,' ') devc_stts_dt,coalesce(ENTRP_CUST_NB,' ') entrp_cust_nb,coalesce(sbjt_id,' ') as sbjt_id,coalesce(sbjt_tp_cd,' ') as sbjt_tp_cd,coalesce(vndr_cd,' ') as vndr_cd,coalesce(devc_id,' ') as devc_id,coalesce(devc_stts_cd,' ') as devc_stts_cd,coalesce(instl_grc_end_dt,' ') as instl_grc_end_dt,coalesce(plcy_st_cd,' ') as plcy_st_cd,coalesce(last_rqst_dt,' ') as last_rqst_dt from dhf_iot_harmonized_prod.program_enrollment pe inner join (select vndr_acct_id,
      data_clctn_id,
      PRGRM_TP_CD,
      max(EVNT_SYS_TS) as mx_EVNT_SYS_TS
      from dhf_iot_harmonized_prod.program_enrollment
      where upper(VNDR_CD)='CMT'
      group by vndr_acct_id,data_clctn_id,PRGRM_TP_CD
    ) pe1
    on pe.vndr_acct_id = pe1.vndr_acct_id and  pe.data_clctn_id = pe1.data_clctn_id and pe.PRGRM_TP_CD = pe1.PRGRM_TP_CD  and pe.EVNT_SYS_TS = pe1.mx_EVNT_SYS_TS
    where upper(VNDR_CD)='CMT'""") //driver ecn should be changed
// val summry_enrlmtDF=trip_summary_json_df_s3loc.join(program_enrollmentDF,trip_summary_json_df_s3loc("id") ===  program_enrollmentDF("VNDR_ACCT_ID") && trip_summary_json_df_s3loc("account_id") ===  program_enrollmentDF("DATA_CLCTN_ID") && trip_summary_json_df_s3loc("program_id") ===  program_enrollmentDF("PRGRM_TP_CD") ,"left")



// COMMAND ----------

program_enrollmentDF.count

// COMMAND ----------

display(program_enrollmentDF.select("PRGRM_TP_CD").distinct)

// COMMAND ----------

// MAGIC %sql
// MAGIC select load_dt,count(sub_type) from dhf_iot_cmt_raw_prod.events_realtime where sub_type is null group by load_dt

// COMMAND ----------

// MAGIC %sql
// MAGIC show create table dhf_iot_cmt_cl_raw_prod.trip_detail_realtime

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct load_dt from dhf_iot_zubie_raw_prod.trippoints --where  event_type=7
// MAGIC -- select hour(from_utc_timestamp(current_timestamp, 'EST'))

// COMMAND ----------

// MAGIC %sql
// MAGIC select a.* from (select distinct driveid,events_ts from dhf_iot_cmt_raw_prod.events_realtime where sub_type is null and event_type=7) a left anti join (select distinct driveid,events_ts from dhf_iot_cmt_raw_prod.events_realtime where sub_type is not null and event_type=7) b on a.driveid=b.driveid and a.events_ts=b.events_ts

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct a.driveid) from (select distinct driveid,events_ts from dhf_iot_cmt_raw_prod.events_realtime where sub_type is null and event_type=7) a left anti join (select distinct driveid,events_ts from dhf_iot_cmt_raw_prod.events_realtime where sub_type is not null and event_type=7) b on a.driveid=b.driveid and a.events_ts=b.events_ts

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history dhf_iot_harmonized_prod.trip_point_chlg

// COMMAND ----------

// MAGIC %sql
// MAGIC show tables in dhf_iot_cmt_raw_prod.badge_daily

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct load_dt from    dhf_iot_cmt_raw_prod.trip_summary_realtime

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from    dhf_iot_cmt_raw_prod.trip_summary_realtime order by db_load_time desc
// MAGIC -- select count(*) from dhf_iot_harmonized_prod.trip_gps_waypoints
// MAGIC
// MAGIC -- select distinct src_sys_cd from dhf_iot_harmonized_prod.heartbeat_daily  261651488

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.trip_gps_waypoints
// MAGIC alter table dhf_iot_harmonized_prod.trip_gps_waypoints add partitionS (SRC_SYS_CD,LOAD_DT,LOAD_HR_TS)

// COMMAND ----------

val df=spark.read.table("dhf_iot_harmonized_prod.trip_gps_waypoints")
df.write.format("delta").mode("overWrite").option("overWriteSchema","True").partitionBy("SRC_SYS_CD","LOAD_DT","LOAD_HR_TS").saveAsTable("dhf_iot_harmonized_prod.trip_gps_waypoints")

// COMMAND ----------

spark.sql("select * from  dhf_iot_harmonized_prod.trip_gps_waypoints").write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_gps_waypoints_bkp")

// COMMAND ----------

// MAGIC %sql
// MAGIC -- show tables in dhf_iot_cmt_raw_prod
// MAGIC -- ALTER TABLE dhf_iot_harmonized_prod.trip_gps_waypoints ADD COLUMNS( 
// MAGIC
// MAGIC -- PLCY_ST_CD	string	, 
// MAGIC
// MAGIC -- PRGRM_CD	string) 
// MAGIC
// MAGIC ALTER TABLE dhf_iot_harmonized_prod.trip_event ADD COLUMNS( 
// MAGIC
// MAGIC EVNT_SUB_TP_CD	string	, 
// MAGIC
// MAGIC EVNT_SVRTY_SRC_IN	string	, 
// MAGIC
// MAGIC EVNT_SVRTY_DERV_IN	string	, 
// MAGIC
// MAGIC EVT_VAL	decimal(15,6)	, 
// MAGIC
// MAGIC TURN_DEG_PER_SC_QTY	bigint	, 
// MAGIC
// MAGIC MAX_MSS_QTY	decimal(35,15)	, 
// MAGIC
// MAGIC SPD_DLT_KMH_QTY	decimal(18,10)	, 
// MAGIC
// MAGIC SPD_DLT_DRTN_SC_QTY	decimal(18,10)	, 
// MAGIC
// MAGIC PLCY_ST_CD	string	, 
// MAGIC
// MAGIC -- PRGRM_CD	string) 

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from  dhf_iot_raw_score_prod.nm2_trip_driver_score
// MAGIC -- select * from dhf_iot_harmonized_prod.trip_driver_score

// COMMAND ----------

// MAGIC %sql
// MAGIC desc dhf_iot_harmonized_prod.TRIP_event
// MAGIC -- DESCRIBE history dhf_iot_harmonized_prod.trip_summary_chlg
// MAGIC -- select cast('2022-03-26T04:54:18' as timestamp)
// MAGIC -- select load_date,count(*) from dhf_iot_fmc_raw_prod.tripsummary where sourcesystem='FMC_SM' group by LOAD_DATE  --=CURRENT_DATE-10
// MAGIC
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------



// COMMAND ----------

val df=spark.sql("""
select a.* from dhf_iot_harmonized_prod.azuga_hf_trip_point_chlg a LEFT ANTI JOIN dhf_iot_harmonized_prod.azuga_hf_trip_point b on a.TRIP_SMRY_KEY=b.TRIP_SMRY_KEY and a.DEVC_SRL_NB=b.DEVC_SRL_NB and a.TRIP_RECRD_EPOCH_TS=b.TRIP_RECRD_EPOCH_TS""")
df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.azuga_hf_trip_point_chlg")

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select count(*),src_sys_cd from dhf_iot_curatyed_prod.trip_detail where pe_start_ts=''
// MAGIC SELECT load_date=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date))
// MAGIC -- SELECT DATEADD(current_date, DATEDIFF(dd,0,current_date), 0)-3

// COMMAND ----------

// MAGIC %python
// MAGIC t='outbound/SmartMiles-FMC-SourceFiles/load_date%3D2022-10-26/1800/CC_FMC_e5f742f8-f2fa-40be-a3fd-7066d5e6df56.json'.replace('%3D','=')
// MAGIC f=t.replace('%3D','=')
// MAGIC print(t)

// COMMAND ----------

// MAGIC %sql
// MAGIC select (*) from dhf_iot_cmt_raw_prod.wAYPOINTS_realtime 
// MAGIC -- show tables in dhf_iot_cmt_pl_raw_prod

// COMMAND ----------

// MAGIC %sql
// MAGIC select  drive_id as TRIP_SMRY_KEY, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, cast(time as bigint)as TM_EPCH, cast(concat(from_unixtime(cast(time as bigint) div 1000), '.', (cast(time as bigint) % 1000))as timestamp) as UTC_TS, cast(mm_lat as decimal(18,10)) as LAT_NB, cast(mm_lon as decimal(18,10)) as LNGTD_NB, cast(mm_dist_km as decimal(35,15)) as DISTNC_TRVL_QTY, cast(gps_speed as decimal(35,15)) as GPS_PNT_SPD_QTY, cast(gps_heading as decimal(15,6)) as HEADNG_DEG_QTY, cast(gps_valid as INT) as GPS_VALID_STTS_SRC_IN, case when gps_valid = 1 THEN 'Y' when gps_valid=0 THEN 'N' When gps_valid IS NULL THEN " " else '@' end as GPS_VALID_STTS_DERV_IN, cast(accel_lon_smoothed as decimal(18,10)) as LNGTDNL_ACCLRTN_RT, cast(accel_lat_smoothed as decimal(18,10)) as LATRL_ACCLRTN_RT, cast(accel_valid as INT) as ACLRTN_VALID_STTS_SRC_IN, case when accel_valid=1 THEN 'Y' when accel_valid=0 THEN 'N' when accel_valid IS NULL THEN " " else '@' end as ACLRTN_VALID_STTS_DERV_IN, cast(roll_rate as decimal(18,10)) as ROLL_RT, cast(pitch_rate as decimal(18,10)) as PITCH_RT, cast(yaw_rate as decimal(18,10)) as YAW_RT, cast(distraction as INT) as DISTRCTN_STTS_SRC_IN, case when distraction=1 THEN 'Y' when distraction=0 THEN 'N' When distraction IS NULL THEN " " Else '@' end as DISTRCTN_STTS_DERV_IN, cast(mm_state as INT) as MM_ST_CD, cast(tag_lon_smoothed as decimal(18,10)) as TAG_LNGTDNL_SMOOTH, cast(tag_lat_smoothed as decimal(18,10)) as TAG_LAT_SMOOTH, cast(tag_vert_smoothed as decimal(18,10)) as TAG_VRTCL_SMOOTH, cast(tag_valid as INT) as TAG_VALID_STTS_SRC_IN, case when tag_valid=1 THEN 'Y' when tag_valid=0 THEN 'N' when tag_valid IS NULL THEN " " else '@' end as TAG_VALID_STTS_DERV_IN, coalesce(src_sys_cd ,"NOKEY")as SRC_SYS_CD, coalesce(cast(LOAD_DT as DATE), to_date("9999-12-31")) as LOAD_DT, coalesce(LOAD_HR_TS ,to_timestamp("9999-12-31")) as LOAD_HR_TS from dhf_iot_cmt_cl_raw_prod.trip_detail_realtime

// COMMAND ----------

cast(monotonically_increasing_id() +65000 + coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_prod.TRIP_POINT),0) as BIGINT)  as TRIP_POINT_ID

// COMMAND ----------

val environment="prod"
val df=spark.sql(s"""select  a.* from 
(select * from dhf_iot_cmt_cl_raw_${environment}.trip_detail_realtime where load_dt<='2023-01-31')  a left anti join
(select * from dhf_iot_harmonized_${environment}.trip_point where load_dt<='2023-01-31' and src_sys_cd ="CMT_CL_VF") b 
on   a.drive_id=b.trip_smry_key  and cast(concat(from_unixtime(cast(a.time as bigint) div 1000), '.', (cast(a.time as bigint) % 1000))as timestamp)=b.UTC_TS and a.src_sys_cd=b.src_sys_cd where drive_id is not null""")
df.createOrReplaceTempView("tab")
spark.sql("""select cast(monotonically_increasing_id() +65000 + coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_prod.TRIP_POINT),0) as BIGINT) as TRIP_POINT_ID, drive_id as TRIP_SMRY_KEY, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, cast(time as bigint)as TM_EPCH, cast(concat(from_unixtime(cast(time as bigint) div 1000), '.', (cast(time as bigint) % 1000))as timestamp) as UTC_TS, cast(mm_lat as decimal(18,10)) as LAT_NB, cast(mm_lon as decimal(18,10)) as LNGTD_NB, cast(mm_dist_km as decimal(35,15)) as DISTNC_TRVL_QTY, cast(gps_speed as decimal(35,15)) as GPS_PNT_SPD_QTY, cast(gps_heading as decimal(15,6)) as HEADNG_DEG_QTY, cast(gps_valid as INT) as GPS_VALID_STTS_SRC_IN, case when gps_valid = 1 THEN 'Y' when gps_valid=0 THEN 'N' When gps_valid IS NULL THEN " " else '@' end as GPS_VALID_STTS_DERV_IN, cast(accel_lon_smoothed as decimal(18,10)) as LNGTDNL_ACCLRTN_RT, cast(accel_lat_smoothed as decimal(18,10)) as LATRL_ACCLRTN_RT, cast(accel_valid as INT) as ACLRTN_VALID_STTS_SRC_IN, case when accel_valid=1 THEN 'Y' when accel_valid=0 THEN 'N' when accel_valid IS NULL THEN " " else '@' end as ACLRTN_VALID_STTS_DERV_IN, cast(roll_rate as decimal(18,10)) as ROLL_RT, cast(pitch_rate as decimal(18,10)) as PITCH_RT, cast(yaw_rate as decimal(18,10)) as YAW_RT, cast(distraction as INT) as DISTRCTN_STTS_SRC_IN, case when distraction=1 THEN 'Y' when distraction=0 THEN 'N' When distraction IS NULL THEN " " Else '@' end as DISTRCTN_STTS_DERV_IN, cast(mm_state as INT) as MM_ST_CD, cast(tag_lon_smoothed as decimal(18,10)) as TAG_LNGTDNL_SMOOTH, cast(tag_lat_smoothed as decimal(18,10)) as TAG_LAT_SMOOTH, cast(tag_vert_smoothed as decimal(18,10)) as TAG_VRTCL_SMOOTH, cast(tag_valid as INT) as TAG_VALID_STTS_SRC_IN, case when tag_valid=1 THEN 'Y' when tag_valid=0 THEN 'N' when tag_valid IS NULL THEN " " else '@' end as TAG_VALID_STTS_DERV_IN, coalesce(src_sys_cd ,"NOKEY")as SRC_SYS_CD, coalesce(cast(LOAD_DT as DATE), to_date("9999-12-31")) as LOAD_DT, coalesce(LOAD_HR_TS ,to_timestamp("9999-12-31")) as LOAD_HR_TS from tab""").write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_point_chlg".)
// display(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into dhf_iot_harmonized_prod.TRIP_POINT_chlg
// MAGIC select cast(monotonically_increasing_id() +65000 + coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_prod.TRIP_POINT),0) as BIGINT) as TRIP_POINT_ID, drive_id as TRIP_SMRY_KEY, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, cast(time as bigint)as TM_EPCH, cast(concat(from_unixtime(cast(time as bigint) div 1000), '.', (cast(time as bigint) % 1000))as timestamp) as UTC_TS, cast(mm_lat as decimal(18,10)) as LAT_NB, cast(mm_lon as decimal(18,10)) as LNGTD_NB, cast(mm_dist_km as decimal(35,15)) as DISTNC_TRVL_QTY, cast(gps_speed as decimal(35,15)) as GPS_PNT_SPD_QTY, cast(gps_heading as decimal(15,6)) as HEADNG_DEG_QTY, cast(gps_valid as INT) as GPS_VALID_STTS_SRC_IN, case when gps_valid = 1 THEN 'Y' when gps_valid=0 THEN 'N' When gps_valid IS NULL THEN " " else '@' end as GPS_VALID_STTS_DERV_IN, cast(accel_lon_smoothed as decimal(18,10)) as LNGTDNL_ACCLRTN_RT, cast(accel_lat_smoothed as decimal(18,10)) as LATRL_ACCLRTN_RT, cast(accel_valid as INT) as ACLRTN_VALID_STTS_SRC_IN, case when accel_valid=1 THEN 'Y' when accel_valid=0 THEN 'N' when accel_valid IS NULL THEN " " else '@' end as ACLRTN_VALID_STTS_DERV_IN, cast(roll_rate as decimal(18,10)) as ROLL_RT, cast(pitch_rate as decimal(18,10)) as PITCH_RT, cast(yaw_rate as decimal(18,10)) as YAW_RT, cast(distraction as INT) as DISTRCTN_STTS_SRC_IN, case when distraction=1 THEN 'Y' when distraction=0 THEN 'N' When distraction IS NULL THEN " " Else '@' end as DISTRCTN_STTS_DERV_IN, cast(mm_state as INT) as MM_ST_CD, cast(tag_lon_smoothed as decimal(18,10)) as TAG_LNGTDNL_SMOOTH, cast(tag_lat_smoothed as decimal(18,10)) as TAG_LAT_SMOOTH, cast(tag_vert_smoothed as decimal(18,10)) as TAG_VRTCL_SMOOTH, cast(tag_valid as INT) as TAG_VALID_STTS_SRC_IN, case when tag_valid=1 THEN 'Y' when tag_valid=0 THEN 'N' when tag_valid IS NULL THEN " " else '@' end as TAG_VALID_STTS_DERV_IN, coalesce(src_sys_cd ,"NOKEY")as SRC_SYS_CD, coalesce(cast(LOAD_DT as DATE), to_date("9999-12-31")) as LOAD_DT, coalesce(LOAD_HR_TS ,to_timestamp("9999-12-31")) as LOAD_HR_TS from tab

// COMMAND ----------

// MAGIC %sql
// MAGIC select cast(monotonically_increasing_id() +65000 + coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_prod.TRIP_POINT),0) as BIGINT) as TRIP_POINT_ID, drive_id as TRIP_SMRY_KEY, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, cast(time as bigint)as TM_EPCH, cast(concat(from_unixtime(cast(time as bigint) div 1000), '.', (cast(time as bigint) % 1000))as timestamp) as UTC_TS, cast(mm_lat as decimal(18,10)) as LAT_NB, cast(mm_lon as decimal(18,10)) as LNGTD_NB, cast(mm_dist_km as decimal(35,15)) as DISTNC_TRVL_QTY, cast(gps_speed as decimal(35,15)) as GPS_PNT_SPD_QTY, cast(gps_heading as decimal(15,6)) as HEADNG_DEG_QTY, cast(gps_valid as INT) as GPS_VALID_STTS_SRC_IN, case when gps_valid = 1 THEN 'Y' when gps_valid=0 THEN 'N' When gps_valid IS NULL THEN " " else '@' end as GPS_VALID_STTS_DERV_IN, cast(accel_lon_smoothed as decimal(18,10)) as LNGTDNL_ACCLRTN_RT, cast(accel_lat_smoothed as decimal(18,10)) as LATRL_ACCLRTN_RT, cast(accel_valid as INT) as ACLRTN_VALID_STTS_SRC_IN, case when accel_valid=1 THEN 'Y' when accel_valid=0 THEN 'N' when accel_valid IS NULL THEN " " else '@' end as ACLRTN_VALID_STTS_DERV_IN, cast(roll_rate as decimal(18,10)) as ROLL_RT, cast(pitch_rate as decimal(18,10)) as PITCH_RT, cast(yaw_rate as decimal(18,10)) as YAW_RT, cast(distraction as INT) as DISTRCTN_STTS_SRC_IN, case when distraction=1 THEN 'Y' when distraction=0 THEN 'N' When distraction IS NULL THEN " " Else '@' end as DISTRCTN_STTS_DERV_IN, cast(mm_state as INT) as MM_ST_CD, cast(tag_lon_smoothed as decimal(18,10)) as TAG_LNGTDNL_SMOOTH, cast(tag_lat_smoothed as decimal(18,10)) as TAG_LAT_SMOOTH, cast(tag_vert_smoothed as decimal(18,10)) as TAG_VRTCL_SMOOTH, cast(tag_valid as INT) as TAG_VALID_STTS_SRC_IN, case when tag_valid=1 THEN 'Y' when tag_valid=0 THEN 'N' when tag_valid IS NULL THEN " " else '@' end as TAG_VALID_STTS_DERV_IN, coalesce(src_sys_cd ,"NOKEY")as SRC_SYS_CD, coalesce(cast(LOAD_DT as DATE), to_date("9999-12-31")) as LOAD_DT, coalesce(LOAD_HR_TS ,to_timestamp("9999-12-31")) as LOAD_HR_TS from dhf_iot_cmt_cl_raw_prod.trip_detail_realtime --where src_sys_cd in ("CMT_CL_VF") and LOAD_DT<=date('2023-1-31')

// COMMAND ----------

// MAGIC %python
// MAGIC dt=spark.sql("select cast(date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date)) as string) as date   ").collect()[0]["date"]
// MAGIC History_dates_fmc=([row[0] for row in spark.sql("select cast(date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date)) as string)   ").collect()])
// MAGIC print(History_dates_fmc)
// MAGIC print(dt)
// MAGIC av_aggregate_previous_week_file_count_chk = spark.sql(f"select case when count(*)<0 then '' else concat('av_aggregate data missing for the date: ',date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date-7)%7),current_date-7))) end as previous_count_av_aggregate from dhf_iot_rivian_raw_prod.av_aggregate where load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date-7)%7),current_date-7))").collect()[0]["previous_count_av_aggregate"]
// MAGIC print(av_aggregate_previous_week_file_count_chk)

// COMMAND ----------

import org.apache.spark.sql.functions._
val trip_summary_df=spark.read.table(f"dhf_iot_rivian_raw_prod.trips_summary").where("load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date-7)%7),current_date-7))").withColumn("tri",col("r"))
display(trip_summary_df)

// COMMAND ----------

// MAGIC %sql
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set CONFIGVALUE='rivian_trips_odometer_.*.csv' where  job_cd in (2005) and configname='file_name';
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set CONFIGVALUE='rivian_event_summary_.*.csv' where  job_cd in (2004) and configname='file_name';
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set CONFIGVALUE='rivian_trips_summary_.*.csv' where  job_cd in (2003) and configname='file_name';
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set CONFIGVALUE='rivian_trips_[^summary]*.csv' where  job_cd in (2002) and configname='file_name';
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set CONFIGVALUE='rivian_av_aggregate_.*.csv' where  job_cd in (2001) and configname='file_name';

// COMMAND ----------

// MAGIC %sql
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set CONFIGVALUE='IOT_rivian_trips_odometer_.*.csv' where  job_cd in (2005) and configname='file_name';
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set CONFIGVALUE='IOT_rivian_event_summary_.*.csv' where  job_cd in (2004) and configname='file_name';
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set CONFIGVALUE='IOT_rivian_trips_summary_.*.csv' where  job_cd in (2003) and configname='file_name';
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set CONFIGVALUE='IOT_rivian_trips_[^summary]*.csv' where  job_cd in (2002) and configname='file_name';
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set CONFIGVALUE='IOT_rivian_av_aggregate_.*.csv' where  job_cd in (2001) and configname='file_name';

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_generic_autoloader_prod.job_config where job_cd in (3000,300001,300002,300003,300004,300005,300006,300007,300008)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.event_code

// COMMAND ----------

// MAGIC %scala
// MAGIC val df= spark.read.option("header","True").option("quote", "\"").option("escape", "\"").csv("s3://dw-internal-telematics-785562577411/test/error_message_20230714.csv")
// MAGIC df.write.format("delta").mode("overWrite").option("mergeSchema","true").saveAsTable("dhf_iot_cmt_raw_prod.mf_trips")
// MAGIC // display(df)
// MAGIC // 

// COMMAND ----------

// MAGIC %sql
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set CONFIGVALUE='' where job_cd in (3000,300001,300002,300003,300004,300005,300006,300007,300008) and CONFIGVALUE is null

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_generic_autoloader_prod.job_config where job_cd in (2001,2002,2003,2004,2005)

// COMMAND ----------

// MAGIC %sql
// MAGIC select LOAD_DATE,COUNT(*) from dhf_iot_harmonized_prod.trip_summary_missing where sourcesystem='IMS_SM_5X' GROUP BY LOAD_DATE order by load_date desc
// MAGIC -- -- describe history dhf_iot_harmonized_prod.trip_summary_missing
// MAGIC -- restore dhf_iot_harmonized_prod.trip_summary_missing to  version  as of 0

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history dhf_iot_harmonized_prod.trip_point_chlg

// COMMAND ----------

// MAGIC %sql
// MAGIC select sum(DISTNC_MPH) from (select *,row_number() over (partition by trip_smry_key,ENRLD_VIN_NB,PSTN_TS order by load_hr_ts desc) as row  from dhf_iot_harmonized_prod.trip_detail_seconds where MSSNG_TOO_MANY_SEC_FLAG != 1 ) where row=1 and ENRLD_VIN_NB='2GNFLFE39G6206417'

// COMMAND ----------

// dbutils.fs.mkdirs("/pcds-iot-extracts-prod/rivian/")
dbutils.fs.rm("s3://pcds-internal-iot-rivian-telematics-785562577411/IOT_rivian_trips_20230317.00.csv",true)

// COMMAND ----------

// MAGIC %sql
// MAGIC desc dhf_iot_rivian_raw_prod.trips_odometer

// COMMAND ----------

// MAGIC %sql
// MAGIC delete  from dhf_iot_rivian_raw_prod.av_aggregate ;
// MAGIC delete from dhf_iot_rivian_raw_prod.trips ;
// MAGIC delete  from dhf_iot_rivian_raw_prod.trips_summary ;
// MAGIC delete  from dhf_iot_rivian_raw_prod.event_summary ;
// MAGIC delete  from dhf_iot_rivian_raw_prod.trips_odometer ;
// MAGIC delete from dhf_iot_rivian_stageraw_prod.rivian_stage_av_aggregate ;
// MAGIC delete from dhf_iot_rivian_stageraw_prod.rivian_stage_trips;
// MAGIC delete from dhf_iot_rivian_stageraw_prod.rivian_stage_trips_summary;
// MAGIC delete from dhf_iot_rivian_stageraw_prod.rivian_stage_event_summary;
// MAGIC delete from dhf_iot_rivian_stageraw_prod.rivian_stage_trips_odometer;

// COMMAND ----------

// MAGIC %sql
// MAGIC  select count(distinct trip_id) from dhf_iot_rivian_raw_prod.trips_summary where load_dt='2023-4-7'

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.trip_point where src_sys_cd='CMT_CL_VF' ORDER BY ETL_LAST_UPDT_DTS DESC

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_rivian_raw_prod.trips_odometer where trip_id like '%7FCTGAAA7PN019125_20230326%'
// MAGIC
// MAGIC -- select * from (select trip_id ,max(load_dt) as load_dt,count(*) from dhf_iot_rivian_raw_prod.trips_summary group by trip_id having count(*)>1) where load_dt='2023-03-03'

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_generic_autoloader_prod.job_config where job_cd in (6001)

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select job_cd,(CASE WHEN EXISTS  (select 1 from dhf_iot_generic_autoloader_prod.job_config ) then er else null end) load_dt from dhf_iot_generic_autoloader_prod.job_config where  job_cd in (100202)
// MAGIC -- select job_cd,coalesce(Column2,NULL) AS Column2 from dhf_iot_generic_autoloader_prod.job_config where  job_cd in (100202)

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue='arn:aws:sns:us-east-1:785562577411:pcds-iot-databricks-emailgroup'  where  configvalue in ('arn:aws:sns:us-east-1:785562577411:dw-cmt-pl-iot-telematics-emailgroup')
// MAGIC
// MAGIC -- arn:aws:sns:us-east-1:785562577411:pcds-iot-databricks-emailgroup
// MAGIC -- arn:aws:sns:us-east-1:785562577411:dw-cmt-pl-iot-telematics-emailgroup

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_cmt_cl_raw_prod.tripdetail_realtime --order by db_load_date desc
// MAGIC tag_lon_smoothed,tag_lat_smoothed,tag_vert_smoothed,tag_valid

// COMMAND ----------

import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.functions._
val df3=spark.read.format("parquet").load("dbfs:/mnt/dw-telematics-prod-read/warehouse/telematics_db/smartride_tripdetail_seconds/source_cd=IMS/batch=2016*").withColumn("path",input_file_name).withColumn("batch",split(split(input_file_name,"/",9)(7),"=",0)(1))

display(df3)

// COMMAND ----------

// MAGIC %scala
// MAGIC val df= spark.read.option("header","True").option("quote", "\"").option("escape", "\"").csv("s3://dw-internal-telematics-785562577411/test/export (15).csv").filter("name like '%2016%'").select("path")
// MAGIC val d=df.collect().toList
// MAGIC // val df3=spark.read.format("parquet").load(s"${d}")
// MAGIC print(d)
// MAGIC // var ar=List()
// MAGIC // for (row <- df.rdd.collect)
// MAGIC // {
// MAGIC //   ar=ar+row
// MAGIC // }
// MAGIC // df.collect().foreach { row =>
// MAGIC //    println(row)
// MAGIC // }
// MAGIC // df.write.format("delta").mode("append").saveAsTable("dhf_iot_generic_autoloader_prod.job_config")
// MAGIC // display(df)
// MAGIC // dbfs:/mnt/dw-telematics-prod-read/warehouse/telematics_db/smartride_tripdetail_seconds/source_cd=IMS/

// COMMAND ----------

// MAGIC %sql
// MAGIC update dhf_iot_generic_autoloader_prod.job_config set configvalue="select r.*,cast(`gps_speed(m/s)` as string) as gps_speed, regexp_replace(split(path,'/')[10],'.tar.gz','') as drive_id from {raw_view} r where time !=''" where job_cd  in (100202) and configname like '%sql_raw_%';

// COMMAND ----------

val df=spark.sql("select distinct trip_smry_key,load_dt,src_sys_cd from dhf_iot_harmonized_prod.trip_detail_seconds where MSSNG_TOO_MANY_SEC_FLAG != 1 ")
df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.TDS_trips")

// COMMAND ----------

// MAGIC %sql
// MAGIC select load_dt,count(*) from dhf_iot_cmt_cl_raw_prod.trip_summary_realtime group by load_dt

// COMMAND ----------

// MAGIC %sql 
// MAGIC desc dhf_iot_harmonized_prod.trip_summary_chlg --RSKPT_SPDNG' bigint

// COMMAND ----------

val df=spark.sql("select  a.* from  dhf_iot_harmonized_prod.TDS_trips  a left anti join dhf_iot_curated_prod.trip_detail b on a.trip_smry_key=b.trip_smry_key")
df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.TD_missing_trips")

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from dhf_iot_curated_prod.device_summary --where trip_smry_key='00000000103031281665135224' and src_syS_cd='IMS_SR_4X' AND LOAD_DT>'2022-10-01'

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into dhf_iot_curated_prod.trip_detail_chlg
// MAGIC select distinct TRIP_DETAIL_SECONDS_ID, ENRLD_VIN_NB, ETL_LAST_UPDT_DTS, ETL_ROW_EFF_DTS, PSTN_TS, PSTN_OFFST_TS, TIME_ZONE_OFFST_NUM, SPD_MPH_RT, SPD_KPH_RT, ENGIN_RPM_RT, DISTNC_MPH, DISTNC_KPH, FAST_ACLRTN_EVNT_COUNTR, HARD_BRKE_COUNTR, DRVNG_SC_CNT, IDLE_SC_CNT, STOP_SC_CNT, NIGHT_TIME_DRVNG_SEC_CNT, PLSBL_SC_CNT, CNTRD_NUM, SCRBD_FLD_DESC, LAT_NB, LNGTD_NB, TRIP_SMRY_KEY, DEVC_KEY, ORGNL_SPD_RT, ORGNL_RPM_RT, MSSNG_TOO_MANY_SEC_FLAG, LOAD_DT, SRC_SYS_CD, LOAD_HR_TS from (select * from (select *,row_number() over (partition by trip_smry_key order by load_hr_ts desc) as row from (select a.* from dhf_iot_harmonized_prod.trip_detail_seconds a inner join dhf_iot_harmonized_prod.TD_missing_trips b on a.trip_smry_key=b.trip_smry_key and a.src_sys_cd=b.src_sys_cd  )) where row=1)

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct ENRLD_VIN_NB) from dhf_iot_curated_prod.inbound_score_elements where load_dt>='2023-03-31'

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_curated_prod.outbound_score_elements where enrld_vin_nb='2T3DFREV1FW339802' and max_pe_end_ts='2023-03-30 18:00:15'

// COMMAND ----------

// MAGIC %sql
// MAGIC select DEVC_KEY,sum(MILE_CT) from dhf_iot_curated_prod.trip_detail where enrld_vin_nb='2T3DFREV1FW339802' group by DEVC_KEY

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_curated_prod.device_summary where enrld_vin_nb='2T3DFREV1FW339802' --group by DEVC_KEY

// COMMAND ----------

// MAGIC %sql
// MAGIC select *,row_number() over (partition by PRGRM_INSTC_ID,DATA_CLCTN_ID,SRC_SYS_CD,devc_key,ENRLD_VIN_NB,MAX_PE_END_TS order by MAX_PE_END_TS desc)as rownb from (select a.* from global_temp.smartmilesdf a  left join (select  ENRLD_VIN_NB as vin,src_sys_cd as src ,max(MAX_PE_END_TS) as PE_END_TS from {curateDB}.outbound_score_elements  where src_sys_cd like '%SM%' group by ENRLD_VIN_NB,src_sys_cd) b on a.ENRLD_VIN_NB=b.vin and a.src_sys_cd=b.src where (a.MAX_PE_END_TS!=b.PE_END_TS or b.PE_END_TS is null)) 
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select max(load_dt) from dhf_iot_curated_prod.outbound_score_elements --where load_dt>='2023-02-31'

// COMMAND ----------

// MAGIC %sql
// MAGIC  with t as(
// MAGIC       select *,row_number() over (partition by ENRLD_VIN_NB,MAX_PE_END_TS order by MAX_PE_END_TS desc)as rownb from dhf_iot_curated_prod.outbound_score_elements
// MAGIC       where load_dt>='2023-03-30')
// MAGIC       select * from t where rownb > 1
// MAGIC       

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_curated_prod.outbound_score_elements a 
// MAGIC USING (
// MAGIC       with t as (
// MAGIC       select *,row_number() over (partition by ENRLD_VIN_NB,MAX_PE_END_TS order by MAX_PE_END_TS desc) as rownb from dhf_iot_curated_prod.outbound_score_elements
// MAGIC       where load_dt>='2023-03-30')
// MAGIC       select * from t where rownb > 1
// MAGIC  
// MAGIC ) 
// MAGIC as b on a.OUTBOUND_SCORE_ELEMENTS_ID=b.OUTBOUND_SCORE_ELEMENTS_ID and a.enrld_vin_nb=b.enrld_vin_nb and a.MAX_PE_END_TS=b.MAX_PE_END_TS  when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC -- describe history dhf_iot_curated_prod.outbound_score_elements 
// MAGIC restore dhf_iot_curated_prod.outbound_score_elements  to version as of 12586 

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_curated_prod.outbound_score_elements a using
// MAGIC            (select distinct a.enrld_vin_nb,a.MAX_PE_END_TS from (select * from dhf_iot_curated_prod.outbound_score_elements where load_dt>='2023-03-30') a left anti join (select * from dhf_iot_curated_prod.inbound_score_elements where load_dt>='2023-03-30')b on a.enrld_vin_nb=b.enrld_vin_nb and a.MAX_PE_END_TS=b.MAX_PE_END_TS) b
// MAGIC            on a.enrld_vin_nb=b.enrld_vin_nb and a.MAX_PE_END_TS=b.MAX_PE_END_TS  when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select count(distinct a.enrld_vin_nb) from (select * from dhf_iot_curated_prod.outbound_score_elements where load_dt>'2023-03-30') a left anti join (select * from dhf_iot_curated_prod.inbound_score_elements where load_dt>'2023-03-30')b on a.enrld_vin_nb=b.enrld_vin_nb and a.MAX_PE_END_TS=b.MAX_PE_END_TS
// MAGIC select count( enrld_vin_nb) from  dhf_iot_curated_prod.outbound_score_elements where load_dt='2023-04-06'
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct a.enrld_vin_nb,a.SRC_SYS_CD) from (select * from dhf_iot_curated_prod.outbound_score_elements where load_dt>'2023-03-30') a left anti join (select * from dhf_iot_curated_prod.inbound_score_elements where load_dt>'2023-03-30')b on a.enrld_vin_nb=b.enrld_vin_nb and a.MAX_PE_END_TS=b.MAX_PE_END_TS and a.PRGRM_INSTC_ID=b.PRGRM_INSTC_ID and a.DATA_CLCTN_ID=b.DATA_CLCTN_ID and a.SRC_SYS_CD=b.SRC_SYS_CD and a.devc_key=b.devc_key

// COMMAND ----------

val df=spark.sql("select distinct a.enrld_vin_nb,a.SRC_SYS_CD from (select * from dhf_iot_curated_prod.outbound_score_elements where load_dt>='2023-03-30') a left anti join (select * from dhf_iot_curated_prod.inbound_score_elements where load_dt>='2023-03-30')b on a.enrld_vin_nb=b.enrld_vin_nb and a.MAX_PE_END_TS=b.MAX_PE_END_TS and a.PRGRM_INSTC_ID=b.PRGRM_INSTC_ID and a.DATA_CLCTN_ID=b.DATA_CLCTN_ID and a.SRC_SYS_CD=b.SRC_SYS_CD and a.devc_key=b.devc_key")
df.write.format("delta").mode("append").saveAsTable("dhf_iot_curated_prod.outbound_reload")

// COMMAND ----------

val df=spark.sql("select * from dhf_iot_curated_prod.outbound_reload")
// df.write.format("delta").mode("append").saveAsTable("dhf_iot_curated_prod.outbound_score_elements_chlg")
display(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC select load_dt,count( enrld_vin_nb) from dhf_iot_curated_prod.outbound_score_elements group by load_dt order by load_dt desc--where load_dt='2023-04-02'

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select load_dt,src_sys_cd,count(*) from dhf_iot_harmonized_prod.TD_missing_trips group by load_dt,src_sys_cd -- where load_dt='2022-10-07'
// MAGIC select count(distinct trip_smry_key) from dhf_iot_harmonized_prod.TD_missing_trips 
// MAGIC -- select count(*) from (select * from (select *,row_number() over (partition by trip_smry_key order by load_hr_ts desc) as row from (select a.* from dhf_iot_harmonized_prod.trip_detail_seconds a inner join dhf_iot_harmonized_prod.TD_missing_trips b on a.trip_smry_key=b.trip_smry_key and a.src_sys_cd=b.src_sys_cd and a.load_dt=b.load_dt )) where row=1)

// COMMAND ----------

// MAGIC %sql
// MAGIC select sum(DISTNC_MPH) from (select *,row_number() over (partition by trip_smry_key,ENRLD_VIN_NB,PSTN_TS order by load_hr_ts desc) as row  from dhf_iot_harmonized_prod.trip_detail_seconds where MSSNG_TOO_MANY_SEC_FLAG != 1 ) where row=1 and ENRLD_VIN_NB='5UXKR0C57J0Y04253'

// COMMAND ----------

// MAGIC %sql
// MAGIC select YEAR(LOAD_DT),COUNT(*) from dhf_iot_harmonized_prod.trip_point where src_sys_cd='OCTO_SR' GROUP BY YEAR(LOAD_DT)

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select distinct src_sys_cd from dhf_iot_harmonized_prod.trip_summary_chlg
// MAGIC select count(*) from dhf_iot_ims_raw_prod.tripsummary

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE into dhf_iot_curated_prod.device_status as target
// MAGIC USING (
// MAGIC       
// MAGIC       select *,cast(cast(devc_key as bigint) as string) as new_devc_key from dhf_iot_curated_prod.device_status where devc_key!='NOKEY' and  cast(cast(devc_key as bigint) as string) is not null 
// MAGIC
// MAGIC )
// MAGIC as source
// MAGIC ON source.enrld_vin_nb=target.enrld_vin_nb and source.devc_key=target.devc_key and source.device_status_id=target.device_status_id and source.SRC_SYS_CD=target.SRC_SYS_CD   
// MAGIC WHEN MATCHED THEN UPDATE set target.devc_key=source.new_devc_key

// COMMAND ----------

// MAGIC %sql
// MAGIC ALTER TABLE dhf_iot_curated_prod.extracts_dailyscoring SET TBLPROPERTIES
// MAGIC (delta.enableChangeDataFeed=true)

// COMMAND ----------

17432
17436
17422
17426
17428
17430

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select * from dhf_iot_harmonized_prod.trip_summary_chlg
// MAGIC -- select * from dhf_iot_raw_score_prod.nm2_trip_driver_score
// MAGIC
// MAGIC select 1 as TRIP_EVENT_ID,
// MAGIC coalesce(tdr.db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS,
// MAGIC case when tdr.src_sys_cd in ("CMT_PL_SRM","CMT_PL_SRMCM","CMT_PL_FDR") THEN coalesce(b.Description, "NOKEY") else coalesce(cast(tdr.event_type as STRING),"NOKEY") end as EVNT_TP_CD , 
// MAGIC coalesce(tdr.events_ts, to_timestamp("9999-12-31")) as UTC_TS,cast(tdr.events_lat as decimal(18,10)) as LAT_NB,tdr.sub_type as EVNT_SUB_TP_CD, 
// MAGIC cast(tdr.events_lon as decimal(18,10)) as LNGTD_NB, cast(tdr.severe as STRING) as EVNT_SVRTY_SRC_IN,
// MAGIC case when tdr.severe='true' THEN 'Y' when tdr.severe='false' THEN 'N' when tdr.severe IS NULL then " " else '@' end as EVNT_SVRTY_DERV_IN,
// MAGIC cast(tdr.value as decimal(15,6)) as EVT_VAL,tdr.driveid AS TRIP_SMRY_KEY, cast(tdr.speed_kmh as decimal(18,10)) as AVG_SPD_RT,tdr.turn_dps as TURN_DEG_PER_SC_QTY,
// MAGIC cast(tdr.max_mss as DECIMAL(35,15)) as MAX_MSS_QTY,cast(tdr.risk as decimal(18,10)) as RISK_QTY, tdr.displayed as EVNT_DSPLY_TP_CD,cast(tdr.speed_delta_kmh as DECIMAL(18,10)) as SPD_DLT_KMH_QTY, 
// MAGIC coalesce(cast(tdr.load_dt as DATE), to_date("9999-12-31")) as LOAD_DT,cast(tdr.duration_for_speed_delta_sec as DECIMAL(18,10)) as SPD_DLT_DRTN_SC_QTY,
// MAGIC null as	ACLRTN_RT	,
// MAGIC null as	DRVNG_SC_QTY	,
// MAGIC null as	EVNT_SVRTY_CD	,
// MAGIC null as	HEADNG_DEG_QTY	,
// MAGIC null as	SPD_RT	,
// MAGIC null as	EVT_RFRNC_VAL	,
// MAGIC null as	EVT_TMZN_OFFST_NUM	,
// MAGIC null as	EVNT_STRT_TS	,
// MAGIC null as	EVNT_END_TS	,
// MAGIC null as	TAG_MAC_ADDR_NM	,
// MAGIC null as	TAG_TRIP_NB	,
// MAGIC null as	SHRT_VEH_ID	,coalesce(tdr.src_sys_cd, "NOKEY") AS SRC_SYS_CD, coalesce(tdr.LOAD_HR_TS , to_timestamp("9999-12-31")) as LOAD_HR_TS , tdr.user_st_cd as PLCY_ST_CD,tdr.prgrm_cd as PRGRM_CD,
// MAGIC cast(tdr.duration_sec as int) as EVNT_DRTN_QTY from dhf_iot_cmt_raw_prod.events_realtime tdr left join dhf_iot_harmonized_prod.event_code b on tdr.src_sys_cd = b.source and tdr.event_type=b.code where 
// MAGIC  src_sys_cd in("CMT_PL_SRM","CMT_PL_SRMCM","CMT_PL_FDR")
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC   %sql
// MAGIC CREATE or replace TABLE `dhf_iot_harmonized_prod`.`trip_summary_chlg` (
// MAGIC   `TRIP_SMRY_KEY` STRING ,
// MAGIC   `TRIP_SUMMARY_ID` BIGINT ,
// MAGIC   `ETL_ROW_EFF_DTS` TIMESTAMP ,
// MAGIC   `ETL_LAST_UPDT_DTS` TIMESTAMP,
// MAGIC   `DEVC_KEY` STRING,
// MAGIC   `ACLRTN_QLTY_FL` STRING,
// MAGIC   `AVG_HDOP_QTY` DECIMAL(15,6),
// MAGIC   `AVG_SPD_MPH_RT` DECIMAL(18,5),
// MAGIC   `DRVNG_DISTNC_QTY` DECIMAL(15,6),
// MAGIC   `FUEL_CNSMPTN_QTY` DECIMAL(15,6),
// MAGIC   `IDLING_SC_QTY` DECIMAL(15,6),
// MAGIC   `MLFNCTN_STTS_FL` STRING,
// MAGIC   `MAX_SPD_RT` DECIMAL(18,5),
// MAGIC   `MSURET_UNIT_CD` STRING,
// MAGIC   `PLCY_NB` STRING,
// MAGIC   `TIME_ZONE_OFFST_NB` DECIMAL(10,2),
// MAGIC   `TRIP_SC_QTY` DECIMAL(15,6),
// MAGIC   `TRNSPRT_MODE_CD` STRING,
// MAGIC   `TRNSPRT_MODE_RSN_CD` STRING,
// MAGIC   `TRIP_START_TS` TIMESTAMP,
// MAGIC   `TRIP_END_TS` TIMESTAMP,
// MAGIC   `VEH_KEY` STRING,
// MAGIC   `LOAD_DT` DATE ,
// MAGIC   `SRC_SYS_CD` STRING ,
// MAGIC   `CMNT_TT` STRING,
// MAGIC   `TRIP_FUEL_CNSMD_IDL_QTY` DECIMAL(35,15),
// MAGIC   `VCHR_NB` INT,
// MAGIC   `DISTNC_MTRWY_QTY` DECIMAL(15,6),
// MAGIC   `DISTNC_URBAN_AREAS_QTY` DECIMAL(15,6),
// MAGIC   `DISTNC_ON_ROAD_QTY` DECIMAL(15,6),
// MAGIC   `DISTNC_UNKNOWN_QTY` DECIMAL(15,6),
// MAGIC   `TIME_MTRWY_QTY` DECIMAL(15,6),
// MAGIC   `TIME_URBAN_AREAS_QTY` DECIMAL(15,6),
// MAGIC   `TIME_ON_ROAD_QTY` DECIMAL(15,6),
// MAGIC   `TIME_UNKNOWN_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_DISTNC_MTRWY_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_DISTNC_URBAN_AREAS_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_DISTNC_ON_ROAD_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_DISTNC_UNKNOWN_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_TIME_MTRWY_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_TIME_URBAN_AREAS_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_TIME_ON_ROAD_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_TIME_UNKNOWN_ROAD_QTY` DECIMAL(15,6),
// MAGIC   `LOAD_HR_TS` TIMESTAMP,
// MAGIC   `DRIV_STTS_SRC_IN` STRING,
// MAGIC   `DRIV_STTS_DERV_IN` STRING,
// MAGIC   `ACCT_ID` STRING,
// MAGIC   `TRIP_START_LAT_NB` DECIMAL(35,15),
// MAGIC   `TRIP_START_LNGTD_NB` DECIMAL(35,15),
// MAGIC   `TRIP_END_LAT_NB` DECIMAL(35,15),
// MAGIC   `TRIP_END_LNGTD_NB` DECIMAL(35,15),
// MAGIC   `PLCY_ST_CD` STRING,
// MAGIC   `PRGRM_CD` STRING,
// MAGIC   `NIGHT_TIME_DRVNG_SEC_CNT` BIGINT,
// MAGIC   `OBD_MLG_QTY` DECIMAL(18,10),
// MAGIC   `GPS_MLG_QTY` DECIMAL(18,10),
// MAGIC   `HARD_BRKE_QTY` DECIMAL(18,10),
// MAGIC   `HARSH_ACCLRTN_QTY` DECIMAL(18,10),
// MAGIC   `OVER_SPD_QTY` DECIMAL(18,10),
// MAGIC   `DVC_VRSN_NB` STRING,
// MAGIC   `DRIVER_KEY` STRING,
// MAGIC   `TAG_MAC_ADDR_NM` STRING,
// MAGIC   `TAG_TRIP_NB` BIGINT,
// MAGIC   `SHRT_VEH_ID` BIGINT,
// MAGIC   `RSKPT_ACCEL_RT` BIGINT,
// MAGIC   `RSKPT_BRK_RT` BIGINT,
// MAGIC   `RSKPT_TRN_RT` BIGINT,
// MAGIC   `RSKPT_SPDNG` BIGINT,
// MAGIC   `RSKPT_PHN_MTN` BIGINT,
// MAGIC   `STAR_RTNG` BIGINT,
// MAGIC   `STAR_RTNG_ACCEL` BIGINT,
// MAGIC   `STAR_RTNG_BRK` BIGINT,
// MAGIC   `STAR_RTN_TRN` BIGINT,
// MAGIC   `STAR_RTNG_SPDNG` BIGINT,
// MAGIC   `STAR_RTNG_PHN_MTN` BIGINT,
// MAGIC   `IGNTN_STTS_SRC_CD` STRING,
// MAGIC   `CALC_ODMTR_QTY` DECIMAL(35,15),
// MAGIC   `ODMTR_BEG_ADJST_QTY` DECIMAL(35,15),
// MAGIC   `BASELINE_ODMTR_QTY` DECIMAL(35,15),
// MAGIC   `ODMTR_BEG_QTY` DECIMAL(35,15),
// MAGIC   `ENGIN_HR` DECIMAL(35,15),
// MAGIC   `BASELINE_ENGIN_HR` DECIMAL(35,15),
// MAGIC   `CO_NM` STRING,
// MAGIC   `GPS_STRT_ODMTR_QTY` DECIMAL(35,15),
// MAGIC   `GPS_END_ODMTR_QTY` DECIMAL(35,15))
// MAGIC USING delta
// MAGIC -- PARTITIONED BY (SRC_SYS_CD, LOAD_DT, LOAD_HR_TS)
// MAGIC LOCATION 's3://pcds-databricks-common-785562577411/iot/delta/harmonized/dhf_iot_harmonized_prod/trip_summary_chlg'
// MAGIC TBLPROPERTIES (
// MAGIC   'delta.enableChangeDataFeed' = 'true')  --ENGIN_HR, BASELINE_ODMTR_QTY, ODMTR_BEG_ADJST_QTY, IGNTN_STTS_SRC_CD, ODMTR_BEG_QTY, BASELINE_ENGIN_HR, CALC_ODMTR_QTY

// COMMAND ----------

// MAGIC %sql
// MAGIC delete from dhf_iot

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17432",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17436",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17422",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17426",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17428",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17430",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/35402",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/32026",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/35405",True)

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17432",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17436",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17422",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17426",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17428",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17430",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/35402",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/32026",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/38251",True)

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17434",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17403",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17409",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17420",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17429",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17435",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/32025",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/35405",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/35413",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/37474",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/38246",True)
// MAGIC
// MAGIC
// MAGIC #   17434    IOT_IMS_SM5X_PL_HARMONIZE_Trip_Poin

// COMMAND ----------

// MAGIC %python
// MAGIC
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/38244",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/38244",True)
// MAGIC
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/38245",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/38245",True)
// MAGIC
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/38247",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/38247",True)
// MAGIC
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17424",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17424",True)
// MAGIC
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17436",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17436",True)
// MAGIC
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17437",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17437",True)

// COMMAND ----------

// MAGIC %python
// MAGIC #FMC
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17420",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17426",True)
// MAGIC # # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17414",True)
// MAGIC # # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17416",True)
// MAGIC # # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17420",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17426",True)
// MAGIC # # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17414",True)
// MAGIC # # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17416",True)
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC #5x
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17405",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17406",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17407",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17408",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17410",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17411",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17432",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17434",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17405",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17406",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17407",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17408",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17410",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17411",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17432",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17434",True)
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC #tims
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17413",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17415",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17428",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/17431",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17431",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17415",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17428",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/17429",True)
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/35402",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/35402",True)
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration/35434",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventharmonization/35434",True)
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select * FROM dhf_logs_prod.e2h_job_creation_log WHERE group_id=37458
// MAGIC  [{'id': 37458, 'name': 'IOT-CMT-Daily', 'run_type': 'AVAILABLE_NOW', 'domain_policy_id': '9C61EF4F96001A9C', 'schedule': 'HOURLY', 'cron_schedule': None, 'handler_path': '/Repo/dhfsid@nationwide.com/pcds-dhf-prod-2.0/dhf/pyspark/main/harmonization/IoT/merge_stream_notebook_CMT_PL', 'is_custom_notebook': True, 'active': True, 'sensitive': False, 'job_type': 'HARMONIZATION', 'job_retry': None, 'databricks_usergroup': 'pcds-iot-smartsquad-prod-support', 'domain_userproperty': None, 'cluster_config': 'numWorkers#2 sparkVersion#8.3.x-scala2.12', 'project': 'FDR', 'data_classification': 'Sensitive Personal'}]

// COMMAND ----------

// MAGIC %python
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventcuration/18201",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration_curation/18201",True)
// MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventcuration/18601",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration_curation/18601",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventcuration/18202",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration_curation/18202",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventcuration/18203",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration_curation/18203",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventcuration/18602",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration_curation/18602",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventcuration/18613",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration_curation/18613",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventcuration/18603",True)
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/dhf/eventgeneration_curation/18603",True)
// MAGIC
// MAGIC # dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/delta/raw/ims/checkpoints/smartmiles5X/realtimecheckpoint",True)

// COMMAND ----------

// MAGIC %sql 
// MAGIC -- delete from dhf_iot_curated_prod.trip_detail where SRC_SYS_CD='OCTO_SR' ;
// MAGIC -- delete from dhf_iot_curated_prod.trip_detail_chlg  where SRC_SYS_CD='FMC_SR' ;
// MAGIC
// MAGIC -- delete from dhf_iot_curated_prod.device_status  where SRC_SYS_CD='FMC_SR';
// MAGIC -- delete from dhf_iot_curated_prod.device_status_chlg  where SRC_SYS_CD='FMC_SR';
// MAGIC
// MAGIC delete from dhf_iot_curated_prod.device_summary ;
// MAGIC delete from dhf_iot_curated_prod.device_summary_chlg ;
// MAGIC
// MAGIC -- delete from dhf_iot_curated_prod.program_summary;
// MAGIC -- delete from dhf_iot_curated_prod.program_summary_chlg;
// MAGIC
// MAGIC -- delete from dhf_iot_curated_prod.outbound_score_elements where load_dt>='2022-10-10';
// MAGIC -- delete from dhf_iot_curated_prod.outbound_score_elements_chlg where load_dt<'2022-10-10';
// MAGIC
// MAGIC -- delete from dhf_iot_harmonized_prod.trip_detail_seconds where SRC_SYS_CD='OCTO_SR' ;
// MAGIC -- delete from dhf_iot_harmonized_prod.trip_detail_seconds_chlg where SRC_SYS_CD='FMC_SR' and load_dt>='2022-09-09' and load_dt<'2022-09-28';
// MAGIC
// MAGIC -- delete from dhf_iot_curated_prod.inbound_score_elements;
// MAGIC
// MAGIC -- delete from dhf_iot_curated_prod.ca_annual_mileage_scoring_chlg;
// MAGIC -- delete from dhf_iot_curated_prod.ca_annual_mileage_scoring;
// MAGIC
// MAGIC -- -- delete from dhf_iot_curated_prod.extracts_dailyscoring_chlg;

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.types import *
// MAGIC def field_datatype(datatype):
// MAGIC   if isinstance(datatype, StructType):
// MAGIC     f_schema='StructType(['
// MAGIC     f_schema_list=[]
// MAGIC     for field in datatype.fields:
// MAGIC       datatype=field_datatype(field.dataType)
// MAGIC       f_schema_list.append(f'StructField("{field.name}",{datatype},{field.nullable})')
// MAGIC     f_schema+=','.join(f_schema_list)+'])'
// MAGIC     return f_schema
// MAGIC   elif isinstance(datatype, ArrayType):
// MAGIC     f_schema='ArrayType('
// MAGIC     a_schema=field_datatype(datatype.elementType)
// MAGIC     f_schema+=a_schema+')'
// MAGIC     return f_schema
// MAGIC   else:
// MAGIC     return f"{datatype}()"
// MAGIC def raw_table_schema(raw_df):
// MAGIC   schema='StructType(['
// MAGIC   schema_list=[]
// MAGIC   for field in raw_df.schema.fields:
// MAGIC     f_datatype=field_datatype(field.dataType)
// MAGIC     schema_list.append(f'StructField("{field.name}",{f_datatype},{field.nullable})')
// MAGIC   schema+=','.join(schema_list)+'])'
// MAGIC   return schema
// MAGIC
// MAGIC
// MAGIC
// MAGIC
// MAGIC df=spark.read.format("delta").table("dhf_iot_fmc_raw_prod.telemetrypoints")
// MAGIC raw_table_schema(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC -- update  dhf_iot_generic_autoloader_prod.job_config set CONFIGVALUE=',vehicle,telemetryPoints' where JOB_CD=9004 and CONFIGNAME='source_element_to_telemetrypoints';
// MAGIC -- update  dhf_iot_generic_autoloader_prod.job_config set CONFIGVALUE=',vehicle,telemetryPoints' where JOB_CD=9006 and CONFIGNAME='source_element_to_telemetrypoints';

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_generic_autoloader_prod.job_config where job_cd in (9004)
// MAGIC -- schema_raw_telemetryevents
// MAGIC -- schema_raw_telemetrypoints

// COMMAND ----------

// MAGIC %sql
// MAGIC -- describe history dhf_iot_curated_prod.device_status 
// MAGIC -- restore   dhf_iot_curated_prod.device_status  to VERSION as of  282
// MAGIC -- select src_sys_cd,load_dt from dhf_iot_curated_prod.device_status group by src_sys_cd,load_dt order by load_dt desc

// COMMAND ----------

// MAGIC %sql
// MAGIC -- delete from dhf_iot_harmonized_prod.trip_summary_chlg where SRC_SYS_CD='FMC_SR' and load_dt<'2022-09-28';
// MAGIC -- delete from dhf_iot_harmonized_prod.trip_point_chlg where SRC_SYS_CD='FMC_SR' and load_dt<'2022-09-28';
// MAGIC -- delete from dhf_iot_harmonized_prod.device_CHLG where SRC_SYS_CD='FMC_SR' and load_dt<'2022-09-28';
// MAGIC -- delete from dhf_iot_harmonized_prod.vehicle_chlg where SRC_SYS_CD='FMC_SR' and load_dt<'2022-09-28';
// MAGIC -- delete from dhf_iot_harmonized_prod.non_trip_event_chlg where SRC_SYS_CD='FMC_SR' and load_dt<'2022-09-28';
// MAGIC -- delete from dhf_iot_harmonized_prod.scoring_chlg where SRC_SYS_CD='FMC_SR' and load_dt<'2022-09-28';
// MAGIC -- delete from dhf_iot_harmonized_prod.histogram_scoring_interval_chlg where SRC_SYS_CD='FMC_SR' and load_dt<'2022-09-28';
// MAGIC
// MAGIC --  delete from dhf_iot_harmonized_prod.trip_summary  where SRC_SYS_CD='FMC_SR' and load_dt>='2022-09-28';
// MAGIC --  delete from dhf_iot_harmonized_prod.trip_event  where SRC_SYS_CD='FMC_SR' and load_dt<'2022-09-28';
// MAGIC --  delete from dhf_iot_harmonized_prod.trip_point  where SRC_SYS_CD='FMC_SR' and load_dt<'2022-09-28';
// MAGIC -- delete from dhf_iot_harmonized_prod.device  where SRC_SYS_CD='FMC_SR' and load_dt<'2022-09-28';
// MAGIC -- delete from dhf_iot_harmonized_prod.vehicle  where SRC_SYS_CD='FMC_SR' and load_dt<'2022-09-28';
// MAGIC --  delete from dhf_iot_harmonized_prod.non_trip_event  where SRC_SYS_CD='FMC_SR' and load_dt<'2022-09-28';
// MAGIC -- delete from dhf_iot_harmonized_prod.scoring  where SRC_SYS_CD='FMC_SR' and load_dt<'2022-09-28';
// MAGIC --  delete from dhf_iot_harmonized_prod.histogram_scoring_interval  where SRC_SYS_CD='FMC_SR' and load_dt<'2022-09-28';

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_ims_stageraw_prod.sm_trip_log --limit 1
// MAGIC -- show tables in dhf_iot_ims_stageraw_prod
// MAGIC -- select count(*) from  dhf_iot_ims_raw_prod.tripSummary --where sourcesystem='IMS_SM_5X' and load_date ='2023-03-05'

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC -- delete from dhf_iot_ims_raw_prod.tripSummary where sourcesystem='IMS_SM_5X' and load_date >'2022-07-19' and load_date <'2022-08-11';
// MAGIC -- delete from dhf_iot_ims_raw_prod.telemetryPoints  where sourcesystem='IMS_SM_5X' and load_date >'2022-07-19' and load_date <'2022-08-11';
// MAGIC -- delete from dhf_iot_ims_raw_prod.telemetryEvents  where sourcesystem='IMS_SM_5X' and load_date >'2022-07-19' and load_date <'2022-08-11';
// MAGIC -- delete from dhf_iot_ims_raw_prod.histogramScoringIntervals  where sourcesystem='IMS_SM_5X' and load_date >'2022-07-19' and load_date <'2022-08-11';
// MAGIC -- delete from dhf_iot_ims_raw_prod.scoring  where sourcesystem='IMS_SM_5X' and load_date >'2022-07-19' and load_date <'2022-08-11';
// MAGIC -- delete from dhf_iot_ims_raw_prod.nonTripEvent  where sourcesystem='IMS_SM_5X' and load_date >'2022-07-19' and load_date <'2022-08-11';
// MAGIC
// MAGIC -- delete from dhf_iot_fmc_raw_prod.tripSummary where load_date<'2022-09-28';
// MAGIC -- delete from dhf_iot_fmc_raw_prod.telemetryPoints where load_date<'2022-09-28';
// MAGIC
// MAGIC -- delete from dhf_iot_tims_raw_prod.tripSummary where load_date >'2022-07-19' and load_date <'2022-08-11';
// MAGIC -- delete from dhf_iot_tims_raw_prod.telemetryPoints  where  load_date >'2022-07-19' and load_date <'2022-08-11';
// MAGIC
// MAGIC -- delete from dhf_iot_octo_raw_prod.smartride_octo_cumulative ;
// MAGIC -- delete from dhf_iot_octo_raw_prod.smartride_octo_tripsummary;
// MAGIC -- delete from dhf_iot_octo_raw_prod.smartride_octo_trippoint ;
// MAGIC -- delete from dhf_iot_octo_raw_prod.smartride_octo_tripevent ;
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC -- optimize dhf_iot_harmonized_prod.trip_summary where load_dt<current_date();
// MAGIC -- optimize dhf_iot_curated_prod.device_SUMMARY_chlg;
// MAGIC -- optimize dhf_iot_curated_prod.device_SUMMARY;
// MAGIC -- optimize dhf_iot_curated_prod.device_STATUS_chlg;
// MAGIC optimize dhf_iot_curated_prod.device_STATUS;

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct load_dt from dhf_iot_cmt_raw_prod.trip_detail_realtime

// COMMAND ----------

// MAGIC %sql
// MAGIC select load_dt,count(*) from dhf_iot_cmt_raw_prod.trip_detaIL_realtime group by load_dt
// MAGIC -- select SRC_SYS_CD,max(ETL_LAST_UPDT_DTS),max(LOAD_HR_TS)   from dhf_iot_harmonized_prod.trip_point group by SRC_SYS_CD --where SRC_SYS_CD like '%CMT_PL%' --order by ETL_LAST_UPDT_DTS desc 51,605,471,100

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.badge_daily group by src_sys_cd
// MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.driver_profile_daily group by src_sys_cd
// MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.driver_summary_daily group by src_sys_cd
// MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.fraud_daily group by src_sys_cd
// MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.heartbeat_daily group by src_sys_cd
// MAGIC select src_sys_cd,max(load_dt) from dhf_iot_harmonized_prod.USER_PERMISSIONS_DAILY group by src_sys_cd
// MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_labels_daily group by src_sys_cd
// MAGIC -- select src_sys_cd,max(load_dt) from dhf_iot_harmonized_prod.trip_summary_daily group by src_sys_cd
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select sourcesystem,max(load_hour) from dhf_iot_ims_raw_prod.tripSummary where load_hour !='9999-12-31T00:00:00.000+0000' group by sourcesystem;
// MAGIC -- select sourcesystem,max(load_hour),min(load_date) from dhf_iot_ims_raw_prod.telemetrypoints where load_hour !='9999-12-31T00:00:00.000+0000' group by sourcesystem;
// MAGIC -- select * from dhf_iot_Tims_raw_prod.telemetrypoints where sourcesystem='TIMS_SR' ORDER BY load_date DESC;
// MAGIC --   select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.scoring_chlg group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.histogram_scoring_interval_chlg group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_summary_chlg group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.vehicle_chlg group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.device_chlg group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_event_chlg group by src_sys_cd;
// MAGIC  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_point_chlg group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.non_trip_event_chlg group by src_sys_cd;
// MAGIC   

// COMMAND ----------

// MAGIC %sql
// MAGIC -- -- select sourcesystem,max(load_hour) from dhf_iot_fmc_raw_prod.tripSummary group by sourcesystem;
// MAGIC -- -- select DISTINCT(load_date) from dhf_iot_Tims_raw_prod.tripSummary where sourcesystem='TIMS_SR' ORDER BY load_date DESC;
// MAGIC -- --   select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.scoring where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC -- --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.histogram_scoring_interval where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_summary where load_hr_ts <'9999-01-01T00:00:00.000+0000' group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.vehicle where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.device where load_hr_ts !='9999-12-31T00:00:00.000+0000'group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_event where load_hr_ts <'8999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_point where load_hr_ts <'8999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC --  select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.non_trip_event where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_gps_waypoints where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC   

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_cmt_raw_prod.outbound_mobile_score_elements where trip_smry_json_tt like '%"event_type":7%' and load_dt='2023-05-18'

// COMMAND ----------

// MAGIC %python
// MAGIC # trip_summary audit check
// MAGIC environment='prod'
// MAGIC cmt_cl_trip_summary_missing_trips=spark.sql(f"""select  a.* from 
// MAGIC (select * from dhf_iot_zubie_raw_{environment}.trip_summary ) a left anti join
// MAGIC (select * from dhf_iot_harmonized_{environment}.trip_summary where src_sys_cd="ZUBIE_CL_PP") b 
// MAGIC on   a.trip_key=b.trip_smry_key and  a.src_sys_cd=b.src_sys_cd where trip_key is not null""")
// MAGIC
// MAGIC chlg_query=f"""select distinct cast(row_number() over(order by NULL) + 500 + coalesce((select max(TRIP_SUMMARY_ID) from dhf_iot_harmonized_prod.trip_summary),0) as BIGINT) as TRIP_SUMMARY_ID, coalesce(ts.trip_key, 'NOKEY') AS TRIP_SMRY_KEY,coalesce(xref.ENRLD_VIN,xref.DETECTED_VIN,'NOKEY') as VEH_KEY, ts.db_load_time AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, NULL as DEVC_KEY, NULL as ACLRTN_QLTY_FL, NULL as AVG_HDOP_QTY, NULL as AVG_SPD_MPH_RT, NULL as DRVNG_DISTNC_QTY, NULL as FUEL_CNSMPTN_QTY, NULL as MLFNCTN_STTS_FL, NULL as MAX_SPD_RT, NULL as MSURET_UNIT_CD, NULL as PLCY_NB, NULL as TIME_ZONE_OFFST_NB, NULL as TRIP_SC_QTY, NULL as TRNSPRT_MODE_CD, NULL as TRNSPRT_MODE_RSN_CD, NULL as CMNT_TT, NULL as TRIP_FUEL_CNSMD_IDL_QTY, NULL as VCHR_NB, NULL as DISTNC_MTRWY_QTY, NULL as DISTNC_URBAN_AREAS_QTY, NULL as DISTNC_ON_ROAD_QTY, NUll as DISTNC_UNKNOWN_QTY, NULL as TIME_MTRWY_QTY, NULL as TIME_URBAN_AREAS_QTY, NULL as TIME_ON_ROAD_QTY, NULL as TIME_UNKNOWN_QTY, NULL as SPDNG_DISTNC_MTRWY_QTY, NULL as SPDNG_DISTNC_URBAN_AREAS_QTY, NULL as SPDNG_DISTNC_ON_ROAD_QTY, NULL as SPDNG_DISTNC_UNKNOWN_QTY, NULL as SPDNG_TIME_MTRWY_QTY, NULL as SPDNG_TIME_URBAN_AREAS_QTY, NULL as SPDNG_TIME_ON_ROAD_QTY, NULL as SPDNG_TIME_UNKNOWN_ROAD_QTY, NULL as DRIV_STTS_SRC_IN, NULL as DRIV_STTS_DERV_IN, NULL as ACCT_ID, NULL as DRIVER_KEY, NULL as TRIP_START_LAT_NB, NULL as TRIP_START_LNGTD_NB, NULL as TRIP_END_LAT_NB, NULL as TRIP_END_LNGTD_NB, NULL as PLCY_ST_CD, NULL as PRGRM_CD, NULL as NIGHT_TIME_DRVNG_SEC_CNT, cast(ts.obd_mileage as decimal(18,10)) AS OBD_MLG_QTY,cast(ts.gps_mileage as decimal(18,10)) AS GPS_MLG_QTY, cast(ts.hard_brake as decimal(18,10)) AS HARD_BRKE_QTY, cast(ts.harsh_accel as decimal(18,10)) AS HARSH_ACCLRTN_QTY, cast(ts.over_speed as decimal(18,10)) AS OVER_SPD_QTY, cast(ts.version as string) AS DVC_VRSN_NB, cast(ts.idle_time as decimal(15,6)) AS IDLING_SC_QTY,cast(ts.started as TIMESTAMP) AS TRIP_START_TS, cast(ts.ended as TIMESTAMP) AS TRIP_END_TS, NULL as TAG_MAC_ADDR_NM, NULL as TAG_TRIP_NB, NULL as SHRT_VEH_ID, NULL as RSKPT_ACCEL_RT, NULL as RSKPT_BRK_RT, NULL as RSKPT_PHN_MTN, NULL as RSKPT_SPDNG, NULL as RSKPT_TRN_RT, NULL as STAR_RTNG, NULL as STAR_RTNG_ACCEL, NULL as STAR_RTNG_BRK, NULL as STAR_RTN_TRN, NULL as STAR_RTNG_PHN_MTN,NULL as STAR_RTNG_SPDNG, ts.load_dt as LOAD_DT, coalesce(ts.src_sys_cd,"NOKEY") AS SRC_SYS_CD, ts.load_hr_ts as LOAD_HR_TS, "NOKEY" as POLICY_KEY, XXHASH64("NOKEY") as POLICY_KEY_ID from global_temp.trip_summary ts left join (select * from (select *,row_number() over (partition by VNDR_ACCT_KEY,VNDR_CAR_KEY order by ETL_LAST_UPDT_DTS desc,DVC_ACCT_STTS asc) rn from dhf_iot_harmonized_prod.xref_account_device_car) where rn=1) xref on ts.account_key = xref.VNDR_ACCT_KEY and ts.car_key = xref.VNDR_CAR_KEY where ts.src_sys_cd="ZUBIE_CL_PP" and ts.trip_key is not null"""
// MAGIC
// MAGIC
// MAGIC
// MAGIC if (cmt_cl_trip_summary_missing_trips.count()>0):
// MAGIC   spark.sql(f"{chlg_query}").write.format("delta").mode("append").saveAsTable(f"dhf_iot_harmonized_{environment}.trip_summary_chlg")
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select* from  dhf_iot_harmonized_prod.trip_summary_daily where  src_sys_cd like '%CMT_PL%'-- AND load_dt<='2020-01-15' AND LOAD_DT>'2018-12-31'
// MAGIC select distinct load_dt from dhf_iot_cmt_cl_raw_prod.trip_summary_realtime

// COMMAND ----------

// TO UPDATE TRIP_DETAIL
// COL: PE_STRT_TS,PE_END_TS,devc_key


val df_ts=spark.sql("select TRIP_SMRY_KEY,min(PSTN_TS) as PE_STRT_TS,max(PSTN_TS) as PE_END_TS,max(devc_key) as devc_key,max(ENRLD_VIN_NB) as ENRLD_VIN_NB,max(src_sys_cd) as src_sys_cd from dhf_iot_harmonized_prod.trip_detail_seconds where SRC_SYS_CD in ('IMS_SR_4X')  AND load_dt>'2018-01-01' AND load_dt<'2022-10-10' group by TRIP_SMRY_KEY,SRC_SYS_CD")
df_ts.createOrReplaceTempView("table_ts")

spark.sql("""merge into dhf_iot_curated_prod.trip_detail a
                using table_ts b on a.TRIP_SMRY_KEY=b.TRIP_SMRY_KEY and a.src_sys_cd=b.src_sys_cd  AND a.load_dt<'2022-10-10' and a.pe_strt_ts='9999-12-31T00:00:00.000+0000'
                when matched then update set a.PE_STRT_TS=b.PE_STRT_TS , a.PE_END_TS=b.PE_END_TS,a.devc_key=b.devc_key""")

// COMMAND ----------

// MAGIC %sql
// MAGIC  SELECT * from  dhf_iot_harmonized_prod.trip_detail_seconds_te a left anti join (SELECT * from  dhf_iot_harmonized_prod.trip_detail_seconds_te where load_dt >'2022-08-01' and load_dt<'2022-10-01') b on a.trip_smry_key=b.trip_smry_key--where src_sys_cd='OCTO_SR'

// COMMAND ----------

// MAGIC %sql
// MAGIC select COUNT(*) from  dhf_iot_harmonized_prod.trip_POINT where src_sys_cd in ('IMS_SR_4X','OCTO_SR') AND load_dt <'2021-01-01'--and load_dt=current_date()
// MAGIC -- select count(*) from dhf_iot_harmonized_prod.trip_point where load_dt >='2023-03-21' --order by ETL_LAST_UPDT_DTS desc; 384,635,350,090

// COMMAND ----------

// MAGIC %sql
// MAGIC select *  from dhf_iot_curated_prod.inBOUND_SCORE_ELEMENTS where load_dt> '2023-01-01' and  date(TRANSACTION_TS) =current_date()

// COMMAND ----------

// MAGIC
// MAGIC %sql
// MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_harmonized_prod.trip_detail_seconds where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_curated_prod.trip_detail where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_curated_prod.device_STATUS where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC select src_sys_cd,max(load_DT) from dhf_iot_curated_prod.device_Summary where load_DT !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_curated_prod.OUTBOUND_SCORE_ELEMENTs where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC -- select src_sys_cd,max(load_hr_ts) from dhf_iot_curated_prod.inBOUND_SCORE_ELEMENTS where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;
// MAGIC -- select load_dt,count(*) from dhf_iot_curated_prod.inBOUND_SCORE_ELEMENTS where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by load_dt order by load_dt desc;
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select src_sys_cd,max(load_hr_ts) from dhf_iot_curated_prod.inBOUND_SCORE_ELEMENTS where load_hr_ts !='9999-12-31T00:00:00.000+0000' group by src_sys_cd;

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_curated_prod.device_STATUS  where src_sys_cd in ('FMC_SM','IMS_SM_5X','IMS_SR_5X','TIMS_SR','FMC_SR') OR (src_sys_cd = 'IMS_SR_4X' AND LOAD_DT>add_months(current_date,-18))
// MAGIC
// MAGIC ((src.src_sys_cd like '%SR%' AND tgt.PRGRM_INSTC_ID = src.PRGRM_INSTC_ID) or (src.src_sys_cd like '%SR%' and tgt.DATA_CLCTN_ID = src.DATA_CLCTN_ID))

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select * from dhf_iot_harmonized_prod.trip_POINT where SRC_SYS_CD='FMC_SM' AND load_DT='2023-03-13' AND UTC_TS='2023-03-13T17:15:38.000+0000' --group by LOAD_DT='2022-10-16';
// MAGIC select add_months(current_date,-18)

// COMMAND ----------

val ims_trip_point_missing_trips=spark.sql(f"""select  a.* from 
(select * from dhf_iot_harmonized_prod.trip_point where load_dt>='2023-03-21'  and load_dt<'2023-03-23') a left anti join
(select * from dhf_iot_harmonized_prod.trip_detail_seconds where load_dt>='2023-03-21' and load_dt<'2023-03-23') b 
on   a.trip_smry_key=b.trip_smry_key  and a.UTC_TS=b.PSTN_TS and a.src_sys_cd=b.src_sys_cd where UTC_TS is not null""")

ims_trip_point_missing_trips.createOrReplaceGlobalTempView("tds_table")
val df=spark.sql("""select distinct tp.ENRLD_VIN_NB, TRIP_SMRY_KEY, DEVC_KEY, UTC_TS as PSTN_TS, TIME_ZONE_OFFST_NB as TIME_ZONE_OFFST_NUM, SPD_RT as SPD_KPH_RT, ENGIN_RPM_RT, LAT_NB, LNGTD_NB, SRC_SYS_CD, LOAD_DT, LOAD_HR_TS, trim(coalesce(pe.PLCY_ST_CD, ods.PLCY_RT_ST_CD)) as PLCY_RT_ST_CD from global_temp.tds_table tp left join ( select trim(ENRLD_VIN_NB) as ENRLD_VIN_NB, PLCY_RT_ST_CD, ACTV_STRT_DT, ACTV_END_DT from dhf_iot_harmonized_prod.ods_table where VHCL_STTS_CD = 'E' and LOAD_DT = ( select max(LOAD_DT) from dhf_iot_harmonized_prod.ods_table ) ) ods on ods.ENRLD_VIN_NB = tp.ENRLD_VIN_NB and ods.ACTV_STRT_DT = tp.UTC_TS and tp.UTC_TS = ods.ACTV_END_DT and tp.SRC_SYS_CD like "%SR%" left join ( select VIN_NB, PLCY_ST_CD, PRGRM_TERM_BEG_DT, PRGRM_TERM_END_DT from dhf_iot_harmonized_prod.program_enrollment where DATA_CLCTN_STTS = 'Active' and (PRGRM_STTS_CD in ('Active', 'Cancelled')) ) pe on pe.VIN_NB = tp.ENRLD_VIN_NB and pe.PRGRM_TERM_BEG_DT = tp.UTC_TS and tp.UTC_TS = pe.PRGRM_TERM_END_DT where SRC_SYS_CD not in ('AZUGA_CL_PP', 'ZUBIE_CL_PP', 'CMT_CL_VF','IMS_SR_4X') and tp.LOAD_DT >= date('2023-2-03') union ALL select distinct ts.VEH_KEY as ENRLD_VIN_NB, tp.TRIP_SMRY_KEY, ts.DEVC_KEY, tp.UTC_TS as PSTN_TS, ts.TIME_ZONE_OFFST_NB as TIME_ZONE_OFFST_NUM, tp.SPD_RT as SPD_KPH_RT, tp.ENGIN_RPM_RT, tp.LAT_NB, tp.LNGTD_NB, tp.SRC_SYS_CD, tp.LOAD_DT, tp.LOAD_HR_TS, trim(ods.PLCY_RT_ST_CD) as PLCY_RT_ST_CD from global_temp.tds_table tp inner join (select * from dhf_iot_harmonized_prod.TRIP_SUMMARY where load_dt>='2023-03-21') ts on ts.TRIP_SMRY_KEY = tp.TRIP_SMRY_KEY and ts.load_dt=tp.load_dt and ts.src_sys_cd=tp.src_sys_cd left join ( select trim(ENRLD_VIN_NB) as ENRLD_VIN_NB, PLCY_RT_ST_CD, ACTV_STRT_DT, ACTV_END_DT from dhf_iot_harmonized_prod.ods_table where VHCL_STTS_CD = 'E' and LOAD_DT = ( select max(LOAD_DT) from dhf_iot_harmonized_prod.ods_table ) ) ods on ods.ENRLD_VIN_NB = ts.VEH_KEY and ods.ACTV_STRT_DT = tp.UTC_TS and tp.UTC_TS = ods.ACTV_END_DT where tp.SRC_SYS_CD = 'IMS_SR_4X' and tp.LOAD_DT >= date('2023-02-03') order by PSTN_TS""")
// df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_detail_seconds_chlg")
// ims_trip_point_missing_trips.select("trip_smry_key").distinct().count()
display(df)

// COMMAND ----------

val ims_trip_detail_missing_trips=spark.sql(f"""select  a.* from 
(select distinct TRIP_SMRY_KEY ,LOAD_DT, SRC_SYS_CD, LOAD_HR_TS  from dhf_iot_harmonized_prod.trip_detail_seconds where load_dt>='2023-03-22'and load_dt<'2023-03-23') a left anti join
(select * from dhf_iot_curated_prod.trip_detail where load_dt>='2023-03-22'and load_dt<'2023-03-23') b 
on   a.trip_smry_key=b.trip_smry_key  and a.src_sys_cd=b.src_sys_cd where a.trip_smry_key is not null""")

ims_trip_detail_missing_trips.write.format("delta").mode("append").saveAsTable("dhf_iot_curated_prod.trip_detail_chlg")
// ims_trip_detail_missing_trips.select("trip_smry_key").distinct().count()
// display(df)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.trip_detail_seconds --where SRC_SYS_CD='IMS_SR_4X' order by load_hr_ts desc --group by LOAD_DT='2022-10-16';

// COMMAND ----------

// MAGIC %sql
// MAGIC select *   from dhf_iot_curated_prod.inBOUND_SCORE_ELEMENTS where enrld_vin_nb='1GKKNPLS0JZ229165'

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC -- select src_sys_cd,count(*) from dhf_iot_harmonized_prod.trip_detail_seconds_chlg group by src_sys_cd
// MAGIC -- select src_sys_cd,count(*) from dhf_iot_curated_prod.device_STATUS group by src_sys_cd
// MAGIC -- select src_sys_cd,count(*) from dhf_iot_curated_prod.device_SUMMARY_chlg group by src_sys_cd
// MAGIC -- select * from dhf_iot_curated_prod.device_StATUS where enrld_vin_nb='1FTFX1EVXAFD03026'
// MAGIC -- select * from dhf_iot_ims_raw_prod.tripSummary where enrolledvin='JA32X2HUXEU002778'
// MAGIC select * from dhf_iot_harmonized_prod.integrated_enrollment where vin_nb='1GKKNPLS0JZ229165'
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.ods_table where trim(ENRLD_VIN_NB)='1GKKNPLS0JZ229165'  --'19UUB5F42MA001056'

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select * from dhf_iot_harmonized_prod.program_enrollment where vin_nb in ('1GKKNPLS0JZ229165')

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from dhf_iot_curated_prod.trip_detail where load_dt='2022-11-25'   -- etl_row_eff_dts>='2022-11-18' order by etl_row_eff_dts desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.trip_detail_seconds where TRIP_SMRY_KEY='00000022075860081665693146' and src_sys_cd='IMS_SR_4X' and load_dt>'2022-10-11' 

// COMMAND ----------

// MAGIC %sql
// MAGIC select load_dt,count(*) from dhf_iot_harmonized_prod.trip_point_chlg where TRIP_SMRY_KEY='00000022075860081665693146' and src_sys_cd='IMS_SR_4X' and load_dt>'2022-10-11' group by load_dt

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from (select * from dhf_iot_ims_raw_prod.tripsummary where tripsummaryid='00000022075860081665693146') a inner join (select * from dhf_iot_ims_raw_prod.tripsummary where tripsummaryid='00000022075860081665693146' and load_date>'2022-10-11') b on a.tripsummaryid=b.tripsummaryid and a.load_date=b.load_date where a.load_date>'2022-10-11'

// COMMAND ----------

// MAGIC %python
// MAGIC from datetime import datetime, timedelta
// MAGIC from pyspark.sql.types import StructType,StructField, TimestampType, DecimalType, StringType, IntegerType, DateType, LongType
// MAGIC import json
// MAGIC from decimal import Decimal
// MAGIC from pytz import timezone
// MAGIC from pyspark.sql.window import Window
// MAGIC from pyspark.sql.functions import row_number, col
// MAGIC import builtins
// MAGIC from pyspark.sql.functions import row_number, col
// MAGIC # df=spark.sql("select * from dhf_iot_harmonized_prod.trip_detail_seconds a inner join (select TRIP_SMRY_KEY,load_dt,load_hr_ts from dhf_iot_curated_prod.trip_detail_chlg where load_dt='2022-11-17') b on a.TRIP_SMRY_KEY=b.TRIP_SMRY_KEY and a.load_dt=b.load_dt")
// MAGIC # time=spark.sql("select distinct cast(load_dt as string) from dhf_iot_curated_prod.trip_detail_chlg where load_dt='2022-11-17'").collect()
// MAGIC History_dates=*([row[0] for row in spark.sql("select distinct cast(load_dt as string) from dhf_iot_curated_prod.trip_detail_chlg where load_dt='2022-11-17'").collect()]),'0','1'
// MAGIC print(History_dates)
// MAGIC # print(time["load_dt"])
// MAGIC df1 = spark.sql(f"select distinct *, 1+dense_rank(TRIP_SMRY_KEY) over(order by TRIP_SMRY_KEY) as generated_trip_id from (select distinct a.TRIP_DETAIL_SECONDS_ID, a.ENRLD_VIN_NB, a.ETL_LAST_UPDT_DTS, a.ETL_ROW_EFF_DTS, a.PSTN_TS, a.PSTN_OFFST_TS, a.TIME_ZONE_OFFST_NUM, a.SPD_MPH_RT, a.SPD_KPH_RT, a.ENGIN_RPM_RT, a.DISTNC_MPH, a.DISTNC_KPH, a.FAST_ACLRTN_EVNT_COUNTR, a.HARD_BRKE_COUNTR, a.DRVNG_SC_CNT, a.IDLE_SC_CNT, a.STOP_SC_CNT, a.NIGHT_TIME_DRVNG_SEC_CNT, a.PLSBL_SC_CNT, a.CNTRD_NUM, a.SCRBD_FLD_DESC, a.LAT_NB, a.LNGTD_NB, a.TRIP_SMRY_KEY, a.DEVC_KEY, a.ORGNL_SPD_RT, a.ORGNL_RPM_RT, a.MSSNG_TOO_MANY_SEC_FLAG, a.LOAD_DT, a.SRC_SYS_CD, a.LOAD_HR_TS from  dhf_iot_harmonized_prod.trip_detail_seconds a inner join (select distinct TRIP_SMRY_KEY ,LOAD_DT, SRC_SYS_CD, LOAD_HR_TS from dhf_iot_curated_prod.trip_detail_chlg where load_dt='2022-11-17') b on a.TRIP_SMRY_KEY=b.TRIP_SMRY_KEY and a.LOAD_DT=b.LOAD_DT and a.SRC_SYS_CD=b.SRC_SYS_CD and a.LOAD_HR_TS=b.LOAD_HR_TS AND a.load_dt in {History_dates} ) where MSSNG_TOO_MANY_SEC_FLAG != 1").sort(col('TRIP_SMRY_KEY').asc(), col('PSTN_TS').asc())
// MAGIC display(df1)

// COMMAND ----------

// MAGIC %sql
// MAGIC select
// MAGIC   ts.ENRLD_VIN_NB as ENRLD_VIN_NB
// MAGIC , ts.TRIP_SMRY_KEY as TRIP_SMRY_KEY
// MAGIC , td.PE_STRT_TS as PE_STRT_TS
// MAGIC , ts.stated_mileage as ts_stated_mileage
// MAGIC , ts.correct_mileage as ts_correct_mileage
// MAGIC , td.mile_ct as td_mile_ct
// MAGIC , (ts.correct_mileage - td.mile_ct) as mileage_diff from
// MAGIC (
// MAGIC Select
// MAGIC   ENRLD_VIN_NB
// MAGIC , TRIP_SMRY_KEY
// MAGIC , sum(stated_mileage) as stated_mileage
// MAGIC , sum(correct_mileage) as correct_mileage from
// MAGIC (
// MAGIC Select
// MAGIC   ENRLD_VIN_NB
// MAGIC , TRIP_SMRY_KEY
// MAGIC , PSTN_TS
// MAGIC , SUM(DISTNC_MPH) as stated_mileage
// MAGIC , count(*) as cnt
// MAGIC , (SUM(DISTNC_MPH) / count(*)) as correct_mileage
// MAGIC from dhf_iot_harmonized_prod.trip_detail_seconds
// MAGIC where LOAD_DT  > date('2022-10-09')
// MAGIC --and LOAD_DT < date('2022-10-25')
// MAGIC --and ENRLD_VIN_NB = 'WBXPC9C43AWJ33622'
// MAGIC group by ENRLD_VIN_NB, TRIP_SMRY_KEY, PSTN_TS
// MAGIC )
// MAGIC group by ENRLD_VIN_NB, TRIP_SMRY_KEY
// MAGIC ) ts
// MAGIC join dhf_iot_curated_prod.trip_detail td
// MAGIC on ts.ENRLD_VIN_NB = td.ENRLD_VIN_NB
// MAGIC and ts.TRIP_SMRY_KEY = td.TRIP_SMRY_KEY
// MAGIC where (ts.correct_mileage - td.mile_ct) > .01
// MAGIC ;

// COMMAND ----------


val df=spark.sql("""select (*) from (select a.*,td.src_sys_cd,td.load_dt,td.mile_CT,td.mile_ct/a.ct  as dup_ct from 
(
select TRIP_SMRY_KEY,eNRLD_VIN_NB,sum(DISTNC_MPH) AS CT from 
(
select * from 
(
select *,row_number() over (partition by TRIP_SMRY_KEY,eNRLD_VIN_NB,PSTN_TS order by load_hr_ts desc) row from        dhf_iot_harmonized_prod.trip_detail_seconds where    MSSNG_TOO_MANY_SEC_FLAG != 1 
)
where row=1
) group by  TRIP_SMRY_KEY,eNRLD_VIN_NB
) a 
inner join dhf_iot_curated_prod.trip_detail td
on a.ENRLD_VIN_NB = td.ENRLD_VIN_NB
and a.TRIP_SMRY_KEY = td.TRIP_SMRY_KEY
where  td.mile_ct-a.ct >0.1)""") 
// df.cache()
display(df)

// COMMAND ----------

display(df.where("(mile_ct-ct) <0.1"))

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partition","auto")
spark.sql("""
select a.*,td.src_sys_cd,td.load_dt,td.mile_CT,td.mile_ct/a.ct  as dup_ct from 
(
select TRIP_SMRY_KEY,eNRLD_VIN_NB,sum(DISTNC_MPH) AS CT from 
(
select * from 
(
select *,row_number() over (partition by TRIP_SMRY_KEY,eNRLD_VIN_NB,PSTN_TS order by load_hr_ts desc) row from        dhf_iot_harmonized_prod.trip_detail_seconds where    MSSNG_TOO_MANY_SEC_FLAG != 1 
)
where row=1
) group by  TRIP_SMRY_KEY,eNRLD_VIN_NB
) a 
inner join dhf_iot_curated_prod.trip_detail td
on a.ENRLD_VIN_NB = td.ENRLD_VIN_NB
and a.TRIP_SMRY_KEY = td.TRIP_SMRY_KEY
where td.mile_ct-a.ct >0.1""").write.format("delta").mode("overwrite").option("mergeSchema","true").saveAsTable("dhf_iot_harmonized_prod.TD_reprocess_trips_2")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_curated_prod.trip_detail where trip_smry_key='b62df911-76be-4cb0-a804-9a8433b715f0' and load_dt >='2022-10-01'

// COMMAND ----------

// MAGIC %sql
// MAGIC describe history dhf_iot_harmonized_prod.trip_summary_chlg --where trip_smry_key='14419d36-a086-4e13-a382-d041350d1be1'  --and load_dt >='2022-10-01' --and pstn_ts='2022-10-27T15:04:36.000+0000' 

// COMMAND ----------

// MAGIC %sql
// MAGIC show create table dhf_iot_harmonized_prod.trip_summary

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE or replace TABLE `dhf_iot_harmonized_prod`.`trip_summary_chlg` (
// MAGIC   `TRIP_SMRY_KEY` STRING ,
// MAGIC   `TRIP_SUMMARY_ID` BIGINT ,
// MAGIC   `ETL_ROW_EFF_DTS` TIMESTAMP ,
// MAGIC   `ETL_LAST_UPDT_DTS` TIMESTAMP,
// MAGIC   `DEVC_KEY` STRING,
// MAGIC   `ACLRTN_QLTY_FL` STRING,
// MAGIC   `AVG_HDOP_QTY` DECIMAL(15,6),
// MAGIC   `AVG_SPD_MPH_RT` DECIMAL(18,5),
// MAGIC   `DRVNG_DISTNC_QTY` DECIMAL(15,6),
// MAGIC   `FUEL_CNSMPTN_QTY` DECIMAL(15,6),
// MAGIC   `IDLING_SC_QTY` DECIMAL(15,6),
// MAGIC   `MLFNCTN_STTS_FL` STRING,
// MAGIC   `MAX_SPD_RT` DECIMAL(18,5),
// MAGIC   `MSURET_UNIT_CD` STRING,
// MAGIC   `PLCY_NB` STRING,
// MAGIC   `TIME_ZONE_OFFST_NB` DECIMAL(10,2),
// MAGIC   `TRIP_SC_QTY` DECIMAL(15,6),
// MAGIC   `TRNSPRT_MODE_CD` STRING,
// MAGIC   `TRNSPRT_MODE_RSN_CD` STRING,
// MAGIC   `TRIP_START_TS` TIMESTAMP,
// MAGIC   `TRIP_END_TS` TIMESTAMP,
// MAGIC   `VEH_KEY` STRING,
// MAGIC   `LOAD_DT` DATE ,
// MAGIC   `SRC_SYS_CD` STRING ,
// MAGIC   `CMNT_TT` STRING,
// MAGIC   `TRIP_FUEL_CNSMD_IDL_QTY` DECIMAL(35,15),
// MAGIC   `VCHR_NB` INT,
// MAGIC   `DISTNC_MTRWY_QTY` DECIMAL(15,6),
// MAGIC   `DISTNC_URBAN_AREAS_QTY` DECIMAL(15,6),
// MAGIC   `DISTNC_ON_ROAD_QTY` DECIMAL(15,6),
// MAGIC   `DISTNC_UNKNOWN_QTY` DECIMAL(15,6),
// MAGIC   `TIME_MTRWY_QTY` DECIMAL(15,6),
// MAGIC   `TIME_URBAN_AREAS_QTY` DECIMAL(15,6),
// MAGIC   `TIME_ON_ROAD_QTY` DECIMAL(15,6),
// MAGIC   `TIME_UNKNOWN_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_DISTNC_MTRWY_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_DISTNC_URBAN_AREAS_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_DISTNC_ON_ROAD_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_DISTNC_UNKNOWN_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_TIME_MTRWY_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_TIME_URBAN_AREAS_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_TIME_ON_ROAD_QTY` DECIMAL(15,6),
// MAGIC   `SPDNG_TIME_UNKNOWN_ROAD_QTY` DECIMAL(15,6),
// MAGIC   `LOAD_HR_TS` TIMESTAMP,
// MAGIC   `DRIV_STTS_SRC_IN` STRING,
// MAGIC   `DRIV_STTS_DERV_IN` STRING,
// MAGIC   `ACCT_ID` STRING,
// MAGIC   `TRIP_START_LAT_NB` DECIMAL(35,15),
// MAGIC   `TRIP_START_LNGTD_NB` DECIMAL(35,15),
// MAGIC   `TRIP_END_LAT_NB` DECIMAL(35,15),
// MAGIC   `TRIP_END_LNGTD_NB` DECIMAL(35,15),
// MAGIC   `PLCY_ST_CD` STRING,
// MAGIC   `PRGRM_CD` STRING,
// MAGIC   `NIGHT_TIME_DRVNG_SEC_CNT` BIGINT,
// MAGIC   `OBD_MLG_QTY` DECIMAL(18,10),
// MAGIC   `GPS_MLG_QTY` DECIMAL(18,10),
// MAGIC   `HARD_BRKE_QTY` DECIMAL(18,10),
// MAGIC   `HARSH_ACCLRTN_QTY` DECIMAL(18,10),
// MAGIC   `OVER_SPD_QTY` DECIMAL(18,10),
// MAGIC   `DVC_VRSN_NB` STRING,
// MAGIC   `DRIVER_KEY` STRING,
// MAGIC   `TAG_MAC_ADDR_NM` STRING,
// MAGIC   `TAG_TRIP_NB` BIGINT,
// MAGIC   `SHRT_VEH_ID` BIGINT,
// MAGIC   `RSKPT_ACCEL_RT` BIGINT,
// MAGIC   `RSKPT_BRK_RT` BIGINT,
// MAGIC   `RSKPT_TRN_RT` BIGINT,
// MAGIC   `RSKPT_SPDNG` BIGINT,
// MAGIC   `RSKPT_PHN_MTN` BIGINT,
// MAGIC   `STAR_RTNG` BIGINT,
// MAGIC   `STAR_RTNG_ACCEL` BIGINT,
// MAGIC   `STAR_RTNG_BRK` BIGINT,
// MAGIC   `STAR_RTN_TRN` BIGINT,
// MAGIC   `STAR_RTNG_SPDNG` BIGINT,
// MAGIC   `STAR_RTNG_PHN_MTN` BIGINT,
// MAGIC   `IGNTN_STTS_SRC_CD` STRING,
// MAGIC   `CALC_ODMTR_QTY` DECIMAL(35,15),
// MAGIC   `ODMTR_BEG_ADJST_QTY` DECIMAL(35,15),
// MAGIC   `BASELINE_ODMTR_QTY` DECIMAL(35,15),
// MAGIC   `ODMTR_BEG_QTY` DECIMAL(35,15),
// MAGIC   `ENGIN_HR` DECIMAL(35,15),
// MAGIC   `BASELINE_ENGIN_HR` DECIMAL(35,15),
// MAGIC   `CO_NM` STRING,
// MAGIC   `GPS_STRT_ODMTR_QTY` DECIMAL(35,15),
// MAGIC   `GPS_END_ODMTR_QTY` DECIMAL(35,15))
// MAGIC USING delta
// MAGIC -- PARTITIONED BY (SRC_SYS_CD, LOAD_DT, LOAD_HR_TS)
// MAGIC LOCATION 's3://pcds-databricks-common-785562577411/iot/delta/harmonized/dhf_iot_harmonized_prod/trip_summary_chlg'
// MAGIC TBLPROPERTIES (
// MAGIC   'delta.enableChangeDataFeed' = 'true')  --ENGIN_HR, BASELINE_ODMTR_QTY, ODMTR_BEG_ADJST_QTY, IGNTN_STTS_SRC_CD, ODMTR_BEG_QTY, BASELINE_ENGIN_HR, CALC_ODMTR_QTY

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_curated_prod.trip_detail_chlg where trip_smry_key='14419d36-a086-4e13-a382-d041350d1be1' and load_dt >='2022-10-01'

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.trip_detail_seconds where trip_smry_key='14419d36-a086-4e13-a382-d041350d1be1'  and load_dt >='2022-10-01' and MSSNG_TOO_MANY_SEC_FLAG!=1 --and pstn_ts='2022-10-27T15:04:36.000+0000' 

// COMMAND ----------

// MAGIC %sql
// MAGIC select sum(distnc_mph) from (select *,row_number() over (partition by trip_smry_key,pstn_ts order by load_hr_ts desc ) as row_n from dhf_iot_harmonized_prod.trip_detail_seconds where trip_smry_key='b62df911-76be-4cb0-a804-9a8433b715f0' and load_dt >='2022-10-01') where row_n=1  --load_dt='2022-11-26'

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from (select ts.ENRLD_VIN_NB as ENRLD_VIN_NB, ts.TRIP_SMRY_KEY as TRIP_SMRY_KEY, td.PE_STRT_TS as PE_STRT_TS, ts.stated_mileage as ts_stated_mileage, ts.correct_mileage as ts_correct_mileage, td.mile_ct as td_mile_ct, (td.mile_ct - ts.correct_mileage) as mileage_diff from (	
// MAGIC Select ENRLD_VIN_NB, TRIP_SMRY_KEY, sum(stated_mileage) as stated_mileage, sum(correct_mileage) as correct_mileage from (	
// MAGIC Select ENRLD_VIN_NB, TRIP_SMRY_KEY, PSTN_TS, SUM(DISTNC_MPH) as stated_mileage, count(*) as cnt,  (SUM(DISTNC_MPH) / count(*)) as correct_mileage	
// MAGIC from dhf_iot_harmonized_prod.trip_detail_seconds	
// MAGIC where LOAD_DT  > date('2022-10-09')	and  MSSNG_TOO_MANY_SEC_FLAG != 1 
// MAGIC --and LOAD_DT < date('2022-10-25')	
// MAGIC --and ENRLD_VIN_NB = 'WBXPC9C43AWJ33622'	
// MAGIC group by ENRLD_VIN_NB, TRIP_SMRY_KEY, PSTN_TS	
// MAGIC )	
// MAGIC group by ENRLD_VIN_NB, TRIP_SMRY_KEY	
// MAGIC ) ts	
// MAGIC join dhf_iot_curated_prod.trip_detail td	
// MAGIC on ts.ENRLD_VIN_NB = td.ENRLD_VIN_NB	
// MAGIC and ts.TRIP_SMRY_KEY = td.TRIP_SMRY_KEY	
// MAGIC where (td.mile_ct - ts.correct_mileage) > .01	)
// MAGIC

// COMMAND ----------

// MAGIC %sql	
// MAGIC select count(*) from (select diff.ENRLD_VIN_NB as ENRLD_VIN_NB, Sum(ts_stated_mileage) as stated_mileage, sum(ts_correct_mileage) as correct_mileage, sum(td_mile_ct) as trip_det_mileage, sum(mileage_diff) as mileage_diff	
// MAGIC from (	
// MAGIC select ts.ENRLD_VIN_NB as ENRLD_VIN_NB, ts.TRIP_SMRY_KEY as TRIP_SMRY_KEY, td.PE_STRT_TS as PE_STRT_TS, ts.stated_mileage as ts_stated_mileage, ts.correct_mileage as ts_correct_mileage, td.mile_ct as td_mile_ct, (td.mile_ct - ts.correct_mileage) as mileage_diff from (	
// MAGIC Select ENRLD_VIN_NB, TRIP_SMRY_KEY, sum(stated_mileage) as stated_mileage, sum(correct_mileage) as correct_mileage from (	
// MAGIC Select ENRLD_VIN_NB, TRIP_SMRY_KEY, PSTN_TS, SUM(DISTNC_MPH) as stated_mileage, count(*) as cnt,  (SUM(DISTNC_MPH) / count(*)) as correct_mileage	
// MAGIC from dhf_iot_harmonized_prod.trip_detail_seconds	
// MAGIC where LOAD_DT  > date('2022-10-09')	and   MSSNG_TOO_MANY_SEC_FLAG != 1 
// MAGIC --and LOAD_DT < date('2022-10-25')	
// MAGIC --and ENRLD_VIN_NB = 'WBXPC9C43AWJ33622'	
// MAGIC group by ENRLD_VIN_NB, TRIP_SMRY_KEY, PSTN_TS	
// MAGIC )	
// MAGIC group by ENRLD_VIN_NB, TRIP_SMRY_KEY	
// MAGIC ) ts	
// MAGIC join dhf_iot_curated_prod.trip_detail td	
// MAGIC on ts.ENRLD_VIN_NB = td.ENRLD_VIN_NB	
// MAGIC and ts.TRIP_SMRY_KEY = td.TRIP_SMRY_KEY	
// MAGIC where (td.mile_ct - ts.correct_mileage) > .01	
// MAGIC ) diff	
// MAGIC group by  diff.ENRLD_VIN_NB	)
// MAGIC ;	
// MAGIC 	

// COMMAND ----------

// MAGIC %sql
// MAGIC Select summ.ENRLD_VIN_NB
// MAGIC , min(Trip_Date) as min_Trip_Date
// MAGIC , max(Trip_Date) as max_Trip_Date
// MAGIC , count(*) as Trip_Count
// MAGIC , sum(ts_stated_mileage) as stated_mileage
// MAGIC , sum(ts_correct_mileage) as correct_mileage
// MAGIC from (
// MAGIC select diff.ENRLD_VIN_NB as ENRLD_VIN_NB
// MAGIC , date(PSTN_TS) as Trip_Date
// MAGIC , diff.TRIP_SMRY_KEY
// MAGIC , cast(Sum(ts_stated_mileage)as Decimal(10,2)) as ts_stated_mileage
// MAGIC , cast(sum(ts_correct_mileage) as Decimal(10,2)) as ts_correct_mileage
// MAGIC --, cast(sum(td_mile_ct) as Decimal(10,2)) as trip_det_mileage
// MAGIC --, cast(sum(mileage_diff)as Decimal(10,2)) as mileage_diff
// MAGIC from (
// MAGIC select ts.ENRLD_VIN_NB as ENRLD_VIN_NB, ts.TRIP_SMRY_KEY as TRIP_SMRY_KEY, ts.PSTN_TS as PSTN_TS, ts.stated_mileage as ts_stated_mileage, ts.correct_mileage as ts_correct_mileage, td.mile_ct as td_mile_ct, (td.mile_ct - ts.correct_mileage) as mileage_diff from (
// MAGIC Select ENRLD_VIN_NB, TRIP_SMRY_KEY, PSTN_TS, sum(stated_mileage) as stated_mileage, sum(correct_mileage) as correct_mileage from (
// MAGIC Select ENRLD_VIN_NB, TRIP_SMRY_KEY, PSTN_TS, SUM(DISTNC_MPH) as stated_mileage, count(*) as cnt,  (SUM(DISTNC_MPH) / count(*)) as correct_mileage
// MAGIC from dhf_iot_harmonized_prod.trip_detail_seconds
// MAGIC where LOAD_DT  > date('2021-12-31')
// MAGIC --and LOAD_DT < date('2022-10-25')
// MAGIC --and ENRLD_VIN_NB = '58ADZ1B10NU136183'
// MAGIC group by ENRLD_VIN_NB, TRIP_SMRY_KEY, PSTN_TS
// MAGIC )
// MAGIC group by ENRLD_VIN_NB, TRIP_SMRY_KEY, PSTN_TS
// MAGIC ) ts
// MAGIC left outer join dhf_iot_curated_prod.trip_detail td
// MAGIC on ts.ENRLD_VIN_NB = td.ENRLD_VIN_NB
// MAGIC and ts.TRIP_SMRY_KEY = td.TRIP_SMRY_KEY
// MAGIC --where (td.mile_ct - ts.correct_mileage) > .01
// MAGIC where td.TRIP_SMRY_KEY is null
// MAGIC ) diff
// MAGIC group by  diff.ENRLD_VIN_NB, date(PSTN_TS),diff.TRIP_SMRY_KEY
// MAGIC ) summ
// MAGIC group by summ.ENRLD_VIN_NB

// COMMAND ----------

// MAGIC %sql
// MAGIC (select * from (select *,row_number() over (partition by trip_smry_key order by load_hr_ts desc) as row from (select a.* from dhf_iot_harmonized_prod.trip_detail_seconds a inner join dhf_iot_harmonized_prod.TD_reprocess_trips_2 b on a.trip_smry_key=b.trip_smry_key and a.enrld_vin_nb=b.enrld_vin_nb and a.src_sys_cd=b.src_sys_cd and a.load_DT=b.load_dt )) where row=1)

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into dhf_iot_curated_prod.trip_detail_chlg
// MAGIC select distinct TRIP_DETAIL_SECONDS_ID, ENRLD_VIN_NB, ETL_LAST_UPDT_DTS, ETL_ROW_EFF_DTS, PSTN_TS, PSTN_OFFST_TS, TIME_ZONE_OFFST_NUM, SPD_MPH_RT, SPD_KPH_RT, ENGIN_RPM_RT, DISTNC_MPH, DISTNC_KPH, FAST_ACLRTN_EVNT_COUNTR, HARD_BRKE_COUNTR, DRVNG_SC_CNT, IDLE_SC_CNT, STOP_SC_CNT, NIGHT_TIME_DRVNG_SEC_CNT, PLSBL_SC_CNT, CNTRD_NUM, SCRBD_FLD_DESC, LAT_NB, LNGTD_NB, TRIP_SMRY_KEY, DEVC_KEY, ORGNL_SPD_RT, ORGNL_RPM_RT, MSSNG_TOO_MANY_SEC_FLAG, LOAD_DT, SRC_SYS_CD, LOAD_HR_TS from (select * from (select *,row_number() over (partition by trip_smry_key order by load_hr_ts desc) as row from (select a.* from dhf_iot_harmonized_prod.trip_detail_seconds a inner join dhf_iot_harmonized_prod.TD_reprocess_trips_2 b on a.trip_smry_key=b.trip_smry_key  )) where row=1)

// COMMAND ----------

val df=spark.sql("""select a.* from dhf_iot_curated_prod.trip_detail a inner join dhf_iot_harmonized_prod.TD_reprocess_trips td
on a.ENRLD_VIN_NB = td.ENRLD_VIN_NB
and a.TRIP_SMRY_KEY = td.TRIP_SMRY_KEY and a.load_dt=td.load_dt""")
df.write.format("delta").mode("append").saveAsTable("dhf_iot_curated_prod.trip_detail_wrong_agg")

// COMMAND ----------

// MAGIC %sql
// MAGIC select a.* from dhf_iot_harmonized_prod.trip_detail_chlg a  
// MAGIC inner join dhf_iot_harmonized_prod.TD_reprocess_trips td
// MAGIC on a.ENRLD_VIN_NB = td.ENRLD_VIN_NB
// MAGIC and a.TRIP_SMRY_KEY = td.TRIP_SMRY_KEY and a.load_dt=td.load_dt

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_curated_prod.trip_detail_chlg --where trip_smry_key='1f9546a8-6b77-4f32-a19a-91f83d35bbdb' and load_dt='2022-10-22'

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from dhf_iot_harmonized_prod.TD_reprocess_trips_2  -- where enrld_vin_nb='1N4AL3AP1EC401576' --load_dt<='2022-09-16' --order by load_dt desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select FLEET_ID,VNDR_ACCT_KEY,DEVC_KEY,DVC_ACCT_STTS,VNDR_CAR_KEY,coalesce(DETECTED_VIN,'NOKEY') as DETECTED_VIN,coalesce(ENRLD_VIN,'NOKEY') as ENRLD_VIN,SUBSCRPTN_END_DT,DSCNT_STTS,ETL_ROW_EFF_DTS,ETL_LAST_UPDT_DTS,LOAD_DT,SRC_SYS_CD,LOAD_HR_TS,coalesce(ENRLD_VIN,DETECTED_VIN,'NOKEY') as VEH_KEY from ( select fleet_id as FLEET_ID, coalesce(account_key,'NOKEY') as VNDR_ACCT_KEY, coalesce(device_key,'NOKEY') as DEVC_KEY, device_account_satus as DVC_ACCT_STTS, coalesce(car_key,'NOKEY') as VNDR_CAR_KEY, case when reported_vin like '%UNINITIALIZED%' then NULL else trim(reported_vin) end as DETECTED_VIN, case when vin regexp('^[A-Za-z0-9]+$') then trim(vin) else NULL end as ENRLD_VIN, cast(subscription_ended as string) as SUBSCRPTN_END_DT, cast(disconnected as string) as DSCNT_STTS, db_load_time AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, load_dt as LOAD_DT, coalesce(src_sys_cd, 'NOKEY') AS SRC_SYS_CD, load_hr_ts as LOAD_HR_TS from dhf_iot_zubie_raw_prod.xref_account_device_car ) xref_raw

// COMMAND ----------



// COMMAND ----------

// MAGIC %sql
// MAGIC select count( trip_smry_key) from dhf_iot_curated_prod.trip_detail_wrong_agg --where load_dt<"2022-11-26"  --where enrld_vin_nb='1N4AL3AP1EC401576' --and load_dt='2022-09-01' --order by load_dt desc

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC MERGE into dhf_iot_curated_prod.trip_detail_chlg as a
// MAGIC USING (
// MAGIC       
// MAGIC       select (*) from dhf_iot_harmonized_prod.TD_reprocess_trips
// MAGIC
// MAGIC )
// MAGIC as td
// MAGIC on a.ENRLD_VIN_NB = td.ENRLD_VIN_NB
// MAGIC and a.TRIP_SMRY_KEY = td.TRIP_SMRY_KEY and a.load_dt=td.load_dt
// MAGIC WHEN MATCHED THEN delete

// COMMAND ----------

// MAGIC %sql
// MAGIC select  *  from dhf_iot_harmonized_prod.trip_detail_seconds where trip_smry_key='72604e9d-440b-4182-8fce-d1e4fce47ff0' and load_dt>='2022-04-28'
// MAGIC -- select  *  from dhf_iot_curated_prod.trip_detail where trip_smry_key='72604e9d-440b-4182-8fce-d1e4fce47ff0' and load_dt='2022-06-28'
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select  *  from dhf_iot_harmonized_prod.TD_reprocess_trips_2  --where eNRLD_VIN_NB  not in ('1J4GW48S34C217132','1J4GX48S63C586806')--group by load_dt --order by load_dt desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select sourcesystem,count(*) from (select tripsummaryid,sourcesystem, count(*) as dup_count from (select distinct tripsummaryid,sourcesystem,load_date,load_hour from  dhf_iot_ims_raw_prod.tripsummary) where load_date>'2022-09-17'  group by tripsummaryid,sourcesystem having count(*)>1) group by sourcesystem

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC select tripsummaryid,sourcesystem, count(*) as dup_count from (select distinct tripsummaryid,sourcesystem,load_date,load_hour from  dhf_iot_ims_raw_prod.tripsummary) where load_date>'2022-09-17' and sourcesystem='IMS_SM_5X' group by tripsummaryid,sourcesystem having count(*)>1

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_ims_raw_prod.tripsummary where tripsummaryid='72604e9d-440b-4182-8fce-d1e4fce47ff0' --and load_date='2022-10-17'

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_curated_prod.trip_detail where TRIP_SMRY_KEY='00000000118032311665179821'

// COMMAND ----------

// MAGIC %sql
// MAGIC select load_dt,count(distinct TRIP_SMRY_KEY) from dhf_iot_curated_prod.trip_detail_chlg group by load_dt

// COMMAND ----------

// MAGIC %sql
// MAGIC select load_dt,count(distinct TRIP_SMRY_KEY) from dhf_iot_curated_prod.trip_detail group by load_dt order by load_dt desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select src_sys_cd,count(*) from (select trip_smry_key,src_sys_cd, count(*) as dup_count from dhf_iot_harmonized_prod.trip_summary_chlg where load_dt>'2022-11-01' group by trip_smry_key,src_sys_cd having count(*)>1) group by src_sys_cd 

// COMMAND ----------

// MAGIC %sql
// MAGIC insert into dhf_iot_harmonized_prod.trip_detail_seconds_chlg
// MAGIC select distinct tp.ENRLD_VIN_NB, TRIP_SMRY_KEY, DEVC_KEY, UTC_TS as PSTN_TS, TIME_ZONE_OFFST_NB as TIME_ZONE_OFFST_NUM, SPD_RT as SPD_KPH_RT, ENGIN_RPM_RT, LAT_NB, LNGTD_NB, SRC_SYS_CD, LOAD_DT, LOAD_HR_TS, trim(coalesce(pe.PLCY_ST_CD, ods.PLCY_RT_ST_CD)) as PLCY_RT_ST_CD from dhf_iot_harmonized_prod.TRIP_POINT tp left join (select trim(ENRLD_VIN_NB) as ENRLD_VIN_NB, PLCY_RT_ST_CD, ACTV_STRT_DT, ACTV_END_DT from dhf_iot_harmonized_prod.ods_table where VHCL_STTS_CD = 'E' and LOAD_DT = (select max(LOAD_DT) from dhf_iot_harmonized_prod.ods_table)) ods on ods.ENRLD_VIN_NB =tp.ENRLD_VIN_NB and ods.ACTV_STRT_DT <= tp.UTC_TS and tp.UTC_TS <= ods.ACTV_END_DT left join (select VIN_NB, PLCY_ST_CD, PRGRM_TERM_BEG_DT, PRGRM_TERM_END_DT from dhf_iot_harmonized_prod.program_enrollment where DATA_CLCTN_STTS = 'Active' and (PRGRM_STTS_CD in ('Active', 'Cancelled'))) pe on pe.VIN_NB = tp.ENRLD_VIN_NB and pe.PRGRM_TERM_BEG_DT <= tp.UTC_TS and tp.UTC_TS <= pe.PRGRM_TERM_END_DT and tp.load_dt>='2022-10-10' where SRC_SYS_CD = 'IMS_SR_5X' and tp.LOAD_DT>='2022-10-10' and TRIP_SMRY_KEY not in (select distinct TRIP_SMRY_KEY from dhf_iot_harmonized_prod.trip_detail_seconds where src_sys_cd='IMS_SR_5X' and load_dt>="2022-10-10")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from (select distinct tp.ENRLD_VIN_NB, TRIP_SMRY_KEY, DEVC_KEY, UTC_TS as PSTN_TS, TIME_ZONE_OFFST_NB as TIME_ZONE_OFFST_NUM, SPD_RT as SPD_KPH_RT, ENGIN_RPM_RT, LAT_NB, LNGTD_NB, SRC_SYS_CD, LOAD_DT, LOAD_HR_TS, trim(coalesce(pe.PLCY_ST_CD, ods.PLCY_RT_ST_CD)) as PLCY_RT_ST_CD from dhf_iot_harmonized_prod.TRIP_POINT tp left join ( select trim(ENRLD_VIN_NB) as ENRLD_VIN_NB, PLCY_RT_ST_CD, ACTV_STRT_DT, ACTV_END_DT from dhf_iot_harmonized_prod.ods_table where VHCL_STTS_CD = 'E' and LOAD_DT = ( select max(LOAD_DT) from dhf_iot_harmonized_prod.ods_table ) ) ods on ods.ENRLD_VIN_NB = tp.ENRLD_VIN_NB and ods.ACTV_STRT_DT <= tp.UTC_TS and tp.UTC_TS <= ods.ACTV_END_DT and tp.SRC_SYS_CD like "%SR%" left join ( select VIN_NB, PLCY_ST_CD, PRGRM_TERM_BEG_DT, PRGRM_TERM_END_DT from dhf_iot_harmonized_prod.program_enrollment where DATA_CLCTN_STTS = 'Active' and (PRGRM_STTS_CD in ('Active', 'Cancelled')) ) pe on pe.VIN_NB = tp.ENRLD_VIN_NB and pe.PRGRM_TERM_BEG_DT <= tp.UTC_TS and tp.UTC_TS <= pe.PRGRM_TERM_END_DT where SRC_SYS_CD not in ('AZUGA_CL_PP', 'ZUBIE_CL_PP', 'CMT_CL_VF','IMS_SR_4X') and tp.LOAD_DT >= date('2023-1-30')) where ENRLD_VIN_NB is null

// COMMAND ----------

// MAGIC %sql
// MAGIC -- insert into dhf_iot_harmonized_prod.trip_detail_seconds_chlg
// MAGIC select distinct tp.ENRLD_VIN_NB, TRIP_SMRY_KEY, DEVC_KEY, UTC_TS as PSTN_TS, TIME_ZONE_OFFST_NB as TIME_ZONE_OFFST_NUM, SPD_RT as SPD_KPH_RT, ENGIN_RPM_RT, LAT_NB, LNGTD_NB, SRC_SYS_CD, LOAD_DT, LOAD_HR_TS, trim(coalesce(pe.PLCY_ST_CD, ods.PLCY_RT_ST_CD)) as PLCY_RT_ST_CD from (select a.* from dhf_iot_harmonized_prod.TRIP_POINT a inner join (select distinct  trip_smry_key,src_sys_cd,load_dt from dhf_iot_harmonized_prod.trip_detail_seconds) b on a.trip_smry_key!=b.trip_smry_key and a.src_sys_cd='IMS_SR_5X' AND a.src_sys_cd='IMS_SR_5X' and a.load_dt>'2022-10-10'  and  b.load_dt>='2022-11-19') tp left join (select trim(ENRLD_VIN_NB) as ENRLD_VIN_NB, PLCY_RT_ST_CD, ACTV_STRT_DT, ACTV_END_DT from dhf_iot_harmonized_prod.ods_table where VHCL_STTS_CD = 'E' and LOAD_DT = (select max(LOAD_DT) from dhf_iot_harmonized_prod.ods_table)) ods on ods.ENRLD_VIN_NB =tp.ENRLD_VIN_NB and ods.ACTV_STRT_DT <= tp.UTC_TS and tp.UTC_TS <= ods.ACTV_END_DT left join (select VIN_NB, PLCY_ST_CD, PRGRM_TERM_BEG_DT, PRGRM_TERM_END_DT from dhf_iot_harmonized_prod.program_enrollment where DATA_CLCTN_STTS = 'Active' and (PRGRM_STTS_CD in ('Active', 'Cancelled'))) pe on pe.VIN_NB = tp.ENRLD_VIN_NB and pe.PRGRM_TERM_BEG_DT <= tp.UTC_TS and tp.UTC_TS <= pe.PRGRM_TERM_END_DT   where tp.load_dt<'2022-11-19' 

// COMMAND ----------

// MAGIC %sql
// MAGIC select (*) from dhf_iot_harmonized_prod.trip_detail_seconds inner join dhf_iot_harmonized_prod.trip_detail_seconds on --where trip_smry_key='ffefac3d-22ba-4f6c-895d-b84b5871e220'

// COMMAND ----------

// MAGIC %sql
// MAGIC select trip_smry_key,src_sys_cd, count(*) as dup_count from dhf_iot_harmonized_prod.trip_summary_chlg where load_dt>'2022-11-01' group by trip_smry_key,src_sys_cd having count(*)>1

// COMMAND ----------

// MAGIC %sql
// MAGIC select load_dt,src_sys_cd, count(*) as dup_count from dhf_iot_harmonized_prod.trip_summary where load_dt='2022-11-15' group by load_dt,src_sys_cd 

// COMMAND ----------

// MAGIC %sql
// MAGIC -- select load_dt,count(distinct enrld_vin_nb) from dhf_iot_curated_prod.OUTBOUND_SCORE_ELEMENTS group by load_dt order by load_dt desc
// MAGIC select * from dhf_iot_curated_prod.OUTBOUND_SCORE_ELEMENTS where  load_dt='2022-11-23' order by TRANSACTION_TS desc
// MAGIC

// COMMAND ----------

val df=spark.sql("""select distinct cast( row_number() over( order by NULL ) + 20000 + coalesce( ( select max(TRIP_POINT_ID) from dhf_iot_harmonized_prod.trip_point ), 0 ) as BIGINT ) as TRIP_POINT_ID, cast(engineRPM as decimal(18, 5)) as ENGIN_RPM_RT, cast(hdop as decimal(18, 10)) as HDOP_QTY, tps.tripSummaryId AS TRIP_SMRY_KEY,coalesce(cast(utcDateTime as TIMESTAMP),to_timestamp("9999-12-31")) AS UTC_TS, cast(degreesLatitude as decimal(18, 10)) as LAT_NB, cast(degreesLongitude as decimal(18, 10)) as LNGTD_NB, cast(speed as decimal(18, 10)) as SPD_RT, cast(headingDegrees as decimal(15, 6)) as HEADNG_DEG_QTY, coalesce(sourcesystem, "NOKEY") AS SRC_SYS_CD, cast(accelerometerDataLong as decimal(18, 10)) as LNGTDNL_ACCLRTN_RT, cast(accelerometerDataLat as decimal(18, 10)) as LATRL_ACCLRTN_RT, coalesce(crc32(detectedvin), "NOKEY") AS DEVC_KEY, cast(timeZoneOffset as decimal(10, 2)) as TIME_ZONE_OFFST_NB, COALESCE(detectedVin, "NOKEY") as ENRLD_VIN_NB, coalesce(load_date, to_date("9999-12-31")) as LOAD_DT, coalesce(tps.load_hour, to_timestamp("9999-12-31")) as LOAD_HR_TS, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, cast(pcssetting as int) as PCS_STTNG_STTS_CD, cast(pcsopstatealarm as int) as PCS_OPRTN_ST_ALRM_SRC_IN, cast(pcsopstatebrake as int) as PCS_OP_ST_BRKE_SRC_IN, cast(lkaind as int) as LN_KP_ASST_STTS_CD, cast(lkabuzzer as int) as LN_KP_ASST_BUZZER_SRC_IN, cast(lkaassist as int) as LN_KP_ASST_SRC_IN, cast(cruisecontrolstate as int) as CRUSE_CNTL_ST_SRC_IN,( case when cruisecontrolstate = 0 then 'N' when cruisecontrolstate = 1 then 'Y' when isnull(cruisecontrolstate) then "" else '@' end ) as CRUSE_CNTL_ST_DERV_IN,( case when lkaassist = 0 then 'N' when lkaassist = 1 then 'Y' when isnull(lkaassist) then "" else '@' end ) as LN_KP_ASST_DERV_IN,( case when lkabuzzer = 0 then 'N' when lkabuzzer = 1 then 'Y' when isnull(lkabuzzer) then "" else '@' end ) as LN_KP_ASST_BUZZER_DERV_IN,( case when pcsopstatealarm = 0 then 'N' when pcsopstatealarm = 1 then 'Y' when isnull(pcsopstatealarm) then "" else '@' end ) as PCS_OPRTN_ST_ALRM_DERV_IN,( case when pcsopstatebrake = 0 then 'N' when pcsopstatebrake = 1 then 'Y' when isnull(pcsopstatebrake) then "" else '@' end ) as PCS_OPRTN_ST_BRKE_DERV_IN from (select distinct * from dhf_iot_tims_raw_prod.telemetrypoints) tps inner join (select distinct tripSummaryId, load_hour, count(*) as tripLen from dhf_iot_tims_raw_prod.telemetryPoints  group by tripSummaryId, load_hour  having tripLen < 43200) vtps on tps.tripSummaryId = vtps.tripSummaryId and tps.load_hour = vtps.load_hour where load_date > date('2023-2-27')""")
df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_point_chlg")

// COMMAND ----------

// MAGIC %sql
// MAGIC select   count(*) from
// MAGIC dhf_iot_harmonized_prod.trip_detail_seconds_chlg
// MAGIC where SRC_SYS_CD = 'IMS_SR_4X'
// MAGIC and LOAD_DT >= date('2023-2-3')
// MAGIC and ENRLD_VIN_NB is  null --20499788 5961276

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from (select   (*) from
// MAGIC dhf_iot_harmonized_prod.trip_detail_seconds_chlg
// MAGIC where SRC_SYS_CD = 'IMS_SR_4X'
// MAGIC and LOAD_DT >= date('2023-1-29')
// MAGIC and ENRLD_VIN_NB is  not null)a left anti join (select   (*) from
// MAGIC dhf_iot_harmonized_prod.trip_detail_seconds_chlg
// MAGIC where SRC_SYS_CD = 'IMS_SR_4X'
// MAGIC and LOAD_DT >= date('2023-1-29')
// MAGIC and ENRLD_VIN_NB is   null) b on a.trip_smry_key=b.trip_smry_key and a.pstn_ts=b.pstn_ts

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from (select   (*) from
// MAGIC dhf_iot_harmonized_prod.trip_detail_seconds_chlg
// MAGIC where SRC_SYS_CD = 'IMS_SR_4X'
// MAGIC and LOAD_DT >= date('2023-1-29')
// MAGIC and ENRLD_VIN_NB is  null)a left anti join (select   (*) from
// MAGIC dhf_iot_harmonized_prod.trip_detail_seconds_chlg
// MAGIC where SRC_SYS_CD = 'IMS_SR_4X'
// MAGIC and LOAD_DT >= date('2023-1-29')
// MAGIC and ENRLD_VIN_NB is  not null) b on a.trip_smry_key=b.trip_smry_key and a.pstn_ts=b.pstn_ts

// COMMAND ----------

// MAGIC %sql
// MAGIC select   (*) from
// MAGIC dhf_iot_harmonized_prod.trip_detail_seconds
// MAGIC where SRC_SYS_CD = 'IMS_SR_4X'
// MAGIC and LOAD_DT >= date('2023-1-29') and LOAD_HR_TS>'2023-01-31T23:00:00.000+0000' and trip_smry_key='00000001223884081675267240' and pstn_ts='2023-02-01T16:15:09.000+0000'

// COMMAND ----------

// MAGIC %sql
// MAGIC select   distinct load_dt from
// MAGIC dhf_iot_harmonized_prod.trip_detail_seconds_chlg
// MAGIC where SRC_SYS_CD = 'IMS_SR_4X'
// MAGIC and LOAD_DT >= date('2022-1-4')
// MAGIC and ENRLD_VIN_NB is   null and LOAD_HR_TS>'2023-02-01T23:00:00.000+0000' --order by LOAD_HR_TS desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from (select distinct tp.ENRLD_VIN_NB, TRIP_SMRY_KEY, DEVC_KEY, UTC_TS as PSTN_TS, TIME_ZONE_OFFST_NB as TIME_ZONE_OFFST_NUM, SPD_RT as SPD_KPH_RT, ENGIN_RPM_RT, LAT_NB, LNGTD_NB, SRC_SYS_CD, LOAD_DT, LOAD_HR_TS, trim(coalesce(pe.PLCY_ST_CD, ods.PLCY_RT_ST_CD)) as PLCY_RT_ST_CD from dhf_iot_harmonized_prod.TRIP_POINT tp left join ( select trim(ENRLD_VIN_NB) as ENRLD_VIN_NB, PLCY_RT_ST_CD, ACTV_STRT_DT, ACTV_END_DT from dhf_iot_harmonized_prod.ods_table where VHCL_STTS_CD = 'E' and LOAD_DT = ( select max(LOAD_DT) from dhf_iot_harmonized_prod.ods_table ) ) ods on ods.ENRLD_VIN_NB = tp.ENRLD_VIN_NB and ods.ACTV_STRT_DT <= tp.UTC_TS and tp.UTC_TS <= ods.ACTV_END_DT and tp.SRC_SYS_CD like "%SR%" left join ( select VIN_NB, PLCY_ST_CD, PRGRM_TERM_BEG_DT, PRGRM_TERM_END_DT from dhf_iot_harmonized_prod.program_enrollment where DATA_CLCTN_STTS = 'Active' and (PRGRM_STTS_CD in ('Active', 'Cancelled')) ) pe on pe.VIN_NB = tp.ENRLD_VIN_NB and pe.PRGRM_TERM_BEG_DT <= tp.UTC_TS and tp.UTC_TS <= pe.PRGRM_TERM_END_DT where SRC_SYS_CD not in ('AZUGA_CL_PP', 'ZUBIE_CL_PP', 'CMT_CL_VF','IMS_SR_4X') and tp.LOAD_DT >= date('2023-1-30') union ALL select distinct ts.VEH_KEY as ENRLD_VIN_NB, tp.TRIP_SMRY_KEY, ts.DEVC_KEY, tp.UTC_TS as PSTN_TS, ts.TIME_ZONE_OFFST_NB as TIME_ZONE_OFFST_NUM, tp.SPD_RT as SPD_KPH_RT, tp.ENGIN_RPM_RT, tp.LAT_NB, tp.LNGTD_NB, tp.SRC_SYS_CD, tp.LOAD_DT, tp.LOAD_HR_TS, trim(ods.PLCY_RT_ST_CD) as PLCY_RT_ST_CD from dhf_iot_harmonized_prod.TRIP_POINT tp inner join dhf_iot_harmonized_prod.TRIP_SUMMARY ts on ts.TRIP_SMRY_KEY = tp.TRIP_SMRY_KEY left join ( select trim(ENRLD_VIN_NB) as ENRLD_VIN_NB, PLCY_RT_ST_CD, ACTV_STRT_DT, ACTV_END_DT from dhf_iot_harmonized_prod.ods_table where VHCL_STTS_CD = 'E' and LOAD_DT = ( select max(LOAD_DT) from dhf_iot_harmonized_prod.ods_table ) ) ods on ods.ENRLD_VIN_NB = ts.VEH_KEY and ods.ACTV_STRT_DT <= tp.UTC_TS and tp.UTC_TS <= ods.ACTV_END_DT where tp.SRC_SYS_CD = 'IMS_SR_4X' and tp.LOAD_DT >= date('2023-02-03') order by PSTN_TS) where ENRLD_VIN_NB is null

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from (select  tp.ENRLD_VIN_NB, TRIP_SMRY_KEY, DEVC_KEY, UTC_TS as PSTN_TS, TIME_ZONE_OFFST_NB as TIME_ZONE_OFFST_NUM, SPD_RT as SPD_KPH_RT, ENGIN_RPM_RT, LAT_NB, LNGTD_NB, SRC_SYS_CD, LOAD_DT, LOAD_HR_TS, trim(coalesce(pe.PLCY_ST_CD, ods.PLCY_RT_ST_CD)) as PLCY_RT_ST_CD from dhf_iot_harmonized_prod.TRIP_POINT tp left join ( select trim(ENRLD_VIN_NB) as ENRLD_VIN_NB, PLCY_RT_ST_CD, ACTV_STRT_DT, ACTV_END_DT from dhf_iot_harmonized_prod.ods_table where VHCL_STTS_CD = 'E' and LOAD_DT = ( select max(LOAD_DT) from dhf_iot_harmonized_prod.ods_table ) ) ods on ods.ENRLD_VIN_NB = tp.ENRLD_VIN_NB and ods.ACTV_STRT_DT <= tp.UTC_TS and tp.UTC_TS <= ods.ACTV_END_DT and tp.SRC_SYS_CD like "%SR%" left join ( select VIN_NB, PLCY_ST_CD, PRGRM_TERM_BEG_DT, PRGRM_TERM_END_DT from dhf_iot_harmonized_prod.program_enrollment where DATA_CLCTN_STTS = 'Active' and (PRGRM_STTS_CD in ('Active', 'Cancelled')) ) pe on pe.VIN_NB = tp.ENRLD_VIN_NB and pe.PRGRM_TERM_BEG_DT <= tp.UTC_TS and tp.UTC_TS <= pe.PRGRM_TERM_END_DT  where SRC_SYS_CD not in ('AZUGA_CL_PP', 'ZUBIE_CL_PP', 'CMT_CL_VF','IMS_SR_4X')   and tp.LOAD_DT > date('2023-2-1')) where ENRLD_VIN_NB is null

// COMMAND ----------

// MAGIC %sql
// MAGIC select (*) from (select  ts.VEH_KEY as ENRLD_VIN_NB, tp.TRIP_SMRY_KEY, ts.DEVC_KEY, tp.UTC_TS as PSTN_TS, ts.TIME_ZONE_OFFST_NB as TIME_ZONE_OFFST_NUM, tp.SPD_RT as SPD_KPH_RT, tp.ENGIN_RPM_RT, tp.LAT_NB, tp.LNGTD_NB, tp.SRC_SYS_CD, tp.LOAD_DT, tp.LOAD_HR_TS from (select * from dhf_iot_harmonized_prod.TRIP_POINT where load_dt>='2023-02-02') tp inner join (select * from dhf_iot_harmonized_prod.TRIP_SUMMARY where load_dt>='2023-02-02') ts on ts.TRIP_SMRY_KEY = tp.TRIP_SMRY_KEY   left join ( select trim(ENRLD_VIN_NB) as ENRLD_VIN_NB, PLCY_RT_ST_CD, ACTV_STRT_DT, ACTV_END_DT from dhf_iot_harmonized_prod.ods_table where VHCL_STTS_CD = 'E' and LOAD_DT = ( select max(LOAD_DT) from dhf_iot_harmonized_prod.ods_table ) ) ods on ods.ENRLD_VIN_NB = ts.VEH_KEY and ods.ACTV_STRT_DT <= tp.UTC_TS and tp.UTC_TS <= ods.ACTV_END_DT where tp.SRC_SYS_CD = 'IMS_SR_4X' and tp.LOAD_DT >= date('2023-1-30') order by PSTN_TS) where ENRLD_VIN_NB is  null

// COMMAND ----------

val df=spark.read.table("dhf_iot_harmonized_prod.trip_summary_chlg").filter("load_dt>='2023-02-02'").withColumn("DRIVER_KEY",lit(null).cast(stringType))
df.write.format("delta").mode("overWrite").option("overWriteSchema","True").partitionBy("LOAD_DT","LOAD_HR_TS").saveAsTable("dhf_iot_harmonized_prod.trip_summary_chlg")

// COMMAND ----------


import org.apache.spark.sql.functions.{lit, udf}
import  org.apache.spark.sql.catalyst.expressions.objects.AssertNotNull
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.catalyst.expressions.objects.{AssertNotNull}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col,when}
import spark.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{lit, udf}
val df=spark.read.table("dhf_iot_harmonized_prod.trip_summary_chlg").filter("src_sys_cd='CMT_CL_VF'").withColumn("DRIVER_KEY",lit("NO_KEY").cast(StringType))
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("dhf_iot_harmonized_prod.trip_summary_chlg")

// COMMAND ----------

// MAGIC %sql
// MAGIC delete from dhf_iot_harmonized_prod.trip_summary_chlg where src_sys_cd='CMT_CL_VF' and DRIVER_KEY="NO_KEY"
// MAGIC -- DESC dhf_iot_harmonized_prod.trip_summary_chlg

// COMMAND ----------

val schema_enroll="""{\"context\":{\"id\":\"d0cd2b38-5f66-45bd-a88b-6b67dbcf9403\",\"source\":\"SmartRideProcessing\",\"time\":1678040918409,\"type\":\"com.nationwide.pls.telematics.auto.programs.v3.enrollment.created\"},\"smartRideEnrollment\":{\"transactionType\":\"PolicyEnrollment\",\"policy\":{\"policyNumber\":\"7205J 092480\",\"policyState\":\"CO\",\"drivers\":[{\"subjectId\":1.0,\"firstName\":\"Kyla\",\"lastName\":\"Hoskins\"},{\"subjectId\":2.0,\"firstName\":\"Eliot\",\"lastName\":\"Jones\"}],\"vehicles\":[],\"telematics\":{\"programs\":[{\"subjectId\":1.0,\"subjectType\":\"DRIVER\",\"enrollmentId\":\"567467\",\"programType\":\"SmartRideMobileContinuous\",\"programStatus\":\"Active\",\"enrollmentEffectiveDate\":\"2023-03-08\",\"enrollmentProcessDate\":\"2023-03-05\",\"programTermBeginDate\":\"2023-03-05\",\"programTermEndDate\":\"2024-01-18\",\"dataCollection\":{\"dataCollectionId\":\"f74c1712-1405-4420-a762-9ecad4f8133c\",\"vendorAccountId\":\"\",\"vendorAccountBeginDate\":\"2023-03-05\",\"vendor\":\"CMT\",\"dataCollectionStatus\":\"NotEnrolled\",\"device\":{\"deviceId\":\"3032568794\",\"deviceStatus\":\"NotRegistered\",\"deviceStatusDate\":\"2023-03-05\"}},\"scoreAndDiscount\":{}},{\"subjectId\":2.0,\"subjectType\":\"DRIVER\",\"enrollmentId\":\"567468\",\"programType\":\"SmartRideMobileContinuous\",\"programStatus\":\"Active\",\"enrollmentEffectiveDate\":\"2023-03-08\",\"enrollmentProcessDate\":\"2023-03-05\",\"programTermBeginDate\":\"2023-03-08\",\"programTermEndDate\":\"2024-01-18\",\"dataCollection\":{\"dataCollectionId\":\"7d699077-0cc3-4138-bb76-0b4039c4ba83\",\"vendorAccountId\":\"\",\"vendorAccountBeginDate\":\"2023-03-05\",\"vendor\":\"CMT\",\"dataCollectionStatus\":\"NotEnrolled\",\"device\":{}},\"scoreAndDiscount\":{}}],\"scoreAndDiscount\":{}}}}}"""
val scheam_enrolldf=spark.read.json(Seq(schema_enroll).toDS)
  val convertedudf = udf((unicodestring: String) => unicodestring.replace("\u0000\u0000\u0000\u0000[",""))
  val cleansedDF = scheam_enrolldf.withColumn("cleansed_value",convertedudf(scheam_enrolldf("_corrupt_record"))).drop("_corrupt_record")
display(cleansedDF)

// COMMAND ----------

// MAGIC %python
// MAGIC import os
// MAGIC paths =os.listdir('s3://dw-internal-pl-cmt-telematics-785562577411/CMT/Archive/tar/load_date=2023-05-18/')
// MAGIC
// MAGIC print(len(paths))
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

import org.apache.hadoop.fs.Path
val s3Path = new Path("s3://dw-internal-pl-cmt-telematics-785562577411/CMT/Archive/tar/load_date=2023-05-10/")
val contentSummary = s3Path.getFileSystem(sc.hadoopConfiguration).getContentSummary(s3Path)
val nbFiles = contentSummary.getFileCount()

// COMMAND ----------

import org.apache.hadoop.fs.Path
val s3Path = new Path("s3://dw-internal-pl-cmt-telematics-785562577411/CMT/Archive/tar/load_date=2023-05-08/0000/")
val contentSummary = s3Path.getFileSystem(sc.hadoopConfiguration).getContentSummary(s3Path)
val nbFiles = contentSummary.getFileCount()

// COMMAND ----------

val df=spark.read.format("binaryFile").load("s3://dw-internal-pl-cmt-telematics-785562577411/CMT/Archive/tar/load_date=2023-05-08/")
val nbFiles = df.select(input_file_name()).distinct.count

// COMMAND ----------

// MAGIC  %python
// MAGIC  len(dbutils.fs.ls('s3://dw-internal-pl-cmt-telematics-785562577411/CMT/Archive/tar/load_date=2023-05-07/'))