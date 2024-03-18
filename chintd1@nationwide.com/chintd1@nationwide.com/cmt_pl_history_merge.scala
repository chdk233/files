// Databricks notebook source
// MAGIC %scala
// MAGIC val df=spark.sql("""select distinct driveid,src_sys_cd,load_dt from dhf_iot_cmt_raw_prod.trip_summary_realtime where src_sys_cd not in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM')""") 
// MAGIC df.write.format("delta").mode("append").partitionBy("load_dt").saveAsTable("dhf_iot_cmt_raw_prod.trips_src")

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct driveid from (select distinct driveid,src_sys_cd from dhf_iot_cmt_raw_prod.trips_src where a.src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM')) group by  driveid having count(*) >1

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct * from dhf_iot_harmonized_prod.trip_summary_daily where TRIP_SMRY_KEY in (select TRIP_SMRY_KEY from dhf_iot_harmonized_prod.trip_summary_daily where SRC_SYS_CD like '%CMT_PL%' group by TRIP_SMRY_KEY having count(*)>1)

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_harmonized_prod.trip_summary_daily a using (select TRIP_SMRY_KEY from dhf_iot_harmonized_prod.trip_summary_daily where SRC_SYS_CD like '%CMT_PL%' group by TRIP_SMRY_KEY having count(*)>1) b on a.TRIP_SMRY_KEY=b.TRIP_SMRY_KEY when matched then delete

// COMMAND ----------

val df=spark.sql("select distinct * from dhf_iot_harmonized_prod.trip_summary_daily where TRIP_SMRY_KEY in (select TRIP_SMRY_KEY from dhf_iot_harmonized_prod.trip_summary_daily where SRC_SYS_CD like '%CMT_PL%' group by TRIP_SMRY_KEY having count(*)>1)")
df.write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_summary_daily_chlg")

// COMMAND ----------

// MAGIC
// MAGIC %sql
// MAGIC -- merge into dhf_iot_harmonized_prod.trip_event a using dhf_iot_cmt_raw_prod.multiplesrc b on a.trip_smry_key=b.driveid and a.src_sys_cd=b.src_sys_cd and a.load_dt=b.load_dt and a.src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') when matched then delete;

// COMMAND ----------

// MAGIC %sql
// MAGIC -- merge into dhf_iot_harmonized_prod.trip_gps_waypoints a using dhf_iot_cmt_raw_prod.multiplesrc b on a.trip_smry_key=b.driveid and a.src_sys_cd=b.src_sys_cd and a.load_dt=b.load_dt and a.src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') when matched then delete;

// COMMAND ----------

// %sql
// set spark.sql.autoBroadcastJoinThreshold=-1;
// merge into dhf_iot_harmonized_prod.trip_summary a using dhf_iot_cmt_raw_prod.multiplesrc b on a.trip_smry_key=b.driveid and a.src_sys_cd=b.src_sys_cd and a.load_dt=b.load_dt and a.src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') when matched then delete;

// COMMAND ----------

// MAGIC %sql
// MAGIC set spark.sql.autoBroadcastJoinThreshold=-1;
// MAGIC -- merge into dhf_iot_cmt_raw_prod.trip_summary_realtime a using dhf_iot_cmt_raw_prod.multiplesrc b on a.driveid=b.driveid and a.src_sys_cd=b.src_sys_cd  and a.load_dt=b.load_dt when matched then delete;
// MAGIC -- merge into dhf_iot_cmt_raw_prod.events_realtime a using dhf_iot_cmt_raw_prod.multiplesrc b on a.driveid=b.driveid and a.src_sys_cd=b.src_sys_cd  and a.load_dt=b.load_dt when matched then delete;
// MAGIC -- merge into dhf_iot_cmt_raw_prod.waypoints_realtime a using dhf_iot_cmt_raw_prod.multiplesrc b on a.driveid=b.driveid and a.src_sys_cd=b.src_sys_cd  and a.load_dt=b.load_dt when matched then delete;
// MAGIC merge into dhf_iot_cmt_raw_prod.trip_detail_realtime a using dhf_iot_cmt_raw_prod.multiplesrc b on a.drive_id=b.driveid and a.src_sys_cd=b.src_sys_cd and a.load_dt=b.load_dt when matched then delete;

// COMMAND ----------

// MAGIC %scala
// MAGIC val df=spark.sql("""select distinct driveid,load_dt,src_sys_cd from ( select distinct driveid,x.load_dt,x.src_sys_cd,y.src_sys_cd_pe,ENRLMNT_EFCTV_DT,row_number() over (partition by driveid,src_sys_cd order by ENRLMNT_EFCTV_DT desc) as row from (select a.* from dhf_iot_cmt_raw_prod.trip_summary_realtime a inner join (select distinct m.driveid,m.src_sys_cd,load_dt from dhf_iot_cmt_raw_prod.trips_src m INNER JOIN (select distinct driveid from (select distinct driveid,src_sys_cd from dhf_iot_cmt_raw_prod.trips_src where src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM')) group by  driveid having count(*) >1)n on m.driveid=n.driveid) b on a.driveid=b.driveid and a.load_dt=b.load_dt and a.src_sys_cd=b.src_sys_cd) x inner join (select distinct DATA_CLCTN_ID,ENRLMNT_EFCTV_DT,CASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
// MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
// MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
// MAGIC    END src_sys_cd_pe from dhf_iot_harmonized_prod.program_enrollment) y on x.account_id=y.DATA_CLCTN_ID and x.load_dt>=y.ENRLMNT_EFCTV_DT) where row=1 and src_sys_cd=src_sys_cd_pe""")
// MAGIC
// MAGIC df.write.format("delta").mode("append").saveAsTable("dhf_iot_cmt_raw_prod.matchsrc")

// COMMAND ----------

merge into dhf_iot_harmonized_prod.trip_point a using dhf_iot_cmt_raw_prod.matchsrc b on a.trip_smry_key=b.driveid and a.src_sys_cd=b.src_sys_cd and a.load_dt=b.load_dt and a.src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') when matched then update set a.src_sys_cd=b.src_sys_cd;

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from (select distinct driveid,x.load_dt,x.src_sys_cd,y.src_sys_cd_pe,ENRLMNT_EFCTV_DT,row_number() over (partition by driveid,src_sys_cd order by ENRLMNT_EFCTV_DT desc) as row from (select a.* from dhf_iot_cmt_raw_prod.trip_summary_realtime a inner join (select distinct driveid,load_dt from dhf_iot_cmt_raw_prod.trips_src where src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') group by  load_dt,driveid having count(*) >1) b on a.driveid=b.driveid and a.load_dt=b.load_dt) x inner join (select distinct DATA_CLCTN_ID,ENRLMNT_EFCTV_DT,CASE WHEN PRGRM_TP_CD in ('mobile','SmartRideMobile') THEN 'CMT_PL_SRM'
// MAGIC     WHEN PRGRM_TP_CD in ('mobile-continuous','SmartRideMobileContinuous') THEN 'CMT_PL_SRMCM'
// MAGIC     WHEN PRGRM_TP_CD in ('FocusedDriving') THEN 'CMT_PL_FDR' else '@'
// MAGIC    END src_sys_cd_pe from dhf_iot_harmonized_prod.program_enrollment) y on x.account_id=y.DATA_CLCTN_ID and x.load_dt>=y.ENRLMNT_EFCTV_DT) where row=1

// COMMAND ----------

// MAGIC %python
// MAGIC # df=spark.sql("select distinct cast(load_dt as string) from dhf_iot_cmt_raw_prod.matchsrc").collect()
// MAGIC df=*([row[0] for row in  spark.sql("select distinct cast(load_dt as string) from dhf_iot_cmt_raw_prod.matchsrc where load_dt<'2022-01-01'").collect()]),
// MAGIC # for item in df:
// MAGIC #  print(item)
// MAGIC #  load_date=str(item)
// MAGIC spark.sql(f"merge into dhf_iot_harmonized_prod.trip_point a using dhf_iot_cmt_raw_prod.matchsrc b on a.trip_smry_key=b.driveid and a.src_sys_cd=b.src_sys_cd and a.load_dt=b.load_dt and a.src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') and a.load_dt in {df} when matched then update set a.src_sys_cd=b.src_sys_cd;")

// COMMAND ----------

// MAGIC %python
// MAGIC # df=spark.sql("select distinct cast(load_dt as string) from dhf_iot_cmt_raw_prod.matchsrc").collect()
// MAGIC df=*([row[0] for row in  spark.sql("select distinct cast(load_dt as string) from dhf_iot_cmt_raw_prod.matchsrc ").collect()]),
// MAGIC # for item in df:
// MAGIC #  print(item)
// MAGIC #  load_date=str(item)
// MAGIC display(spark.sql(f"select distinct a.load_dt from  (select * from dhf_iot_harmonized_prod.trip_point where src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') and load_dt in {df}) a inner join dhf_iot_cmt_raw_prod.matchsrc b on a.trip_smry_key=b.driveid  and a.load_dt=b.load_dt and a.src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') and a.load_dt in {df} where a.src_sys_cd!=b.src_sys_cd;"))

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct cast(load_dt as string) from dhf_iot_cmt_raw_prod.matchsrc

// COMMAND ----------

// MAGIC %python
// MAGIC df2=*([row[0] for row in  spark.sql("select distinct cast(load_dt as string) from dhf_iot_cmt_raw_prod.multiplesrc").collect()]),
// MAGIC # for item in df:
// MAGIC #  print(item)
// MAGIC #  load_date=f'{item}'
// MAGIC spark.sql("merge into dhf_iot_cmt_raw_prod.trip_detail_realtime a using dhf_iot_cmt_raw_prod.multiplesrc b on a.drive_id=b.driveid and a.src_sys_cd=b.src_sys_cd and a.load_dt=b.load_dt and a.load_dt in {df2} when matched then delete;")

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(distinct a.driveid) from dhf_iot_cmt_raw_prod.trips_src a inner join  (select  distinct driveid from dhf_iot_cmt_raw_prod.trips_src where src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') group by driveid having count(*)>1) b  on a.driveid=b.driveid

// COMMAND ----------

// MAGIC %sql
// MAGIC select driveid ,load_dt from (select *,row_number() over (partition by driveid order by load_dt desc) as row from (select distinct driveid,load_dt from dhf_iot_cmt_raw_prod.trips_src where src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') )) where row>1

// COMMAND ----------

// MAGIC %python
// MAGIC # df=spark.sql("select distinct cast(load_dt as string) from dhf_iot_cmt_raw_prod.matchsrc").collect()
// MAGIC df=*([row[0] for row in  spark.sql("select distinct cast(load_dt as string) from dhf_iot_cmt_raw_prod.dup_trips").collect()]),
// MAGIC # for item in df:
// MAGIC #  print(item)
// MAGIC #  load_date=str(item)
// MAGIC spark.sql(f"merge into dhf_iot_harmonized_prod.trip_point a using dhf_iot_cmt_raw_prod.matchsrc b on a.trip_smry_key=b.driveid and a.load_dt=b.load_dt and a.src_sys_cd in ('CMT_PL_SRM','CMT_PL_FDR','CMT_PL_SRMCM') and a.load_dt in {df} when matched then delete")

// COMMAND ----------

val s="dfg kv"
s.split(" ")

// COMMAND ----------

def trn(input:List[String]):List[(String,String,String)]={
  input.map{x=>val Array(name,age,country)=x.split(",") (name,age,country) }
}

// COMMAND ----------

val a:List[(String,Int)]=List(("new",89809),("india",578216873),("ds",56))
// a.toList
// a.map{(x,y)=>(x._1,x._2,y._1,y._2)}
a.sortBy(_._2)

// COMMAND ----------

def fun(lis:List[(String,Int)]):Int={
  lis.map(x=>x._2).sum
}

// COMMAND ----------

val a=List(1,2,3,3,2,1,4,5,6)
a.sortWith(_>_)

// COMMAND ----------

val a=List(1,2,3,3,2,1,4,5,6)
var b=List[Int]()
for (i<-0 to a.length-1){
  if  (!b.contains(a(i))){b=b :+a(i)}
  else{
    b :+a(i)
    print(a(i))
  }
}
print(b)

// COMMAND ----------

def fibb(x:Int):List[String]={
  a=0
  b=1
  c=a+b
  b=c
  a=b
}

// COMMAND ----------

val l=List("we,35,ind","mo,30,ind")
// l.map{x=> val Array(name,age,country)=x.split(",") (name,age,country)}
l.sortBy()

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from dhf_iot_cmt_raw_prod.driver_profile_daily  --29595746  29,595,746  41,515,184

// COMMAND ----------

// MAGIC %sql
// MAGIC select (*) from dhf_iot_harmonized_prod.driver_profile_daily where DRIVER_KEY=708836650

// COMMAND ----------

// MAGIC %sql
// MAGIC select DRIVER_KEY,EMAIL_ID,USER_NM,min(load_dt),count(*) from dhf_iot_harmonized_prod.driver_profile_daily where load_dt<'2023-05-17'  group by 1,2,3 having count(*)>1--DRIVER_KEY=21062391

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_cmt_raw_prod.driver_profile_daily where short_user_id=281989530

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct LOAD_DT from dhf_iot_harmonized_prod.driver_profile_daily where SRC_SYS_CD='CMT_PL' AND PRGRM_CD in ('SmartRideMobileContinuous','SmartRideMobile') order by load_dt desc

// COMMAND ----------

// MAGIC %sql
// MAGIC select (*) from (select distinct short_user_id as DRIVER_KEY,
// MAGIC email as EMAIL_ID,
// MAGIC username USER_NM,
// MAGIC user_state as PLCY_ST_CD,
// MAGIC program_id as PRGRM_CD,
// MAGIC src_sys_cd,load_date as load_dt from dhf_iot_cmt_raw_prod.driver_profile_daily where load_date between '2021-06-11' and '2023-05-17') minus (select distinct DRIVER_KEY,
// MAGIC EMAIL_ID,
// MAGIC USER_NM,
// MAGIC PLCY_ST_CD,
// MAGIC PRGRM_CD,
// MAGIC SRC_SYS_CD,load_dt from dhf_iot_harmonized_prod.driver_profile_daily where load_dt between '2021-06-11' and '2023-05-17')

// COMMAND ----------

// MAGIC %sql
// MAGIC select count(*) from (select distinct DRIVER_KEY,
// MAGIC EMAIL_ID,
// MAGIC USER_NM,
// MAGIC PLCY_ST_CD,
// MAGIC PRGRM_CD,
// MAGIC SRC_SYS_CD from dhf_iot_harmonized_prod.driver_profile_daily where load_dt between '2021-06-11' and '2023-05-17')

// COMMAND ----------

// MAGIC %py
// MAGIC merge_key = "EMAIL_ID,FLEET_ID,GROUP_ID,PLCY_NB,DRIVER_KEY,TAG_USER_SRC_IN,TAG_USER_DERV_IN,TEAM_ID,USER_NM,AVG_SCR_QTY,AVG_ACLRTN_SCR_QTY,AVG_BRKE_SCR_QTY,AVG_TURN_SCR_QTY,AVG_SPD_SCR_QTY,AVG_PHN_MTN_SCR_QTY,TOT_DISTNC_KM_QTY,TOT_TRIP_CNT,SCR_INTRV_DRTN_QTY,USER_DRIV_LBL_HNRD_SRC_IN,USER_DRIV_LBL_HNRD_DERV_IN,SCR_TS,FAM_GROUP_CD,PLCY_ST_CD,PRGRM_CD,SRC_SYS_CD"
// MAGIC match_condition = (" AND ".join(list(map((lambda x: f"(a.{x.strip()}=b.{x.strip()} OR (a.{x.strip()} is null AND b.{x.strip()} is null))"),merge_key.split(","))))) 
// MAGIC print(match_condition)

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct b.src_sys_cd from dhf_iot_harmonized_prod.driver_summary_daily a inner join  (select * from  dhf_iot_harmonized_prod.driver_summary_daily where src_sys_cd NOT in ('CMT_PL')) b on   (a.EMAIL_ID=b.EMAIL_ID OR (a.EMAIL_ID is null AND b.EMAIL_ID is null)) AND (a.FLEET_ID=b.FLEET_ID OR (a.FLEET_ID is null AND b.FLEET_ID is null)) AND (a.GROUP_ID=b.GROUP_ID OR (a.GROUP_ID is null AND b.GROUP_ID is null)) AND (a.PLCY_NB=b.PLCY_NB OR (a.PLCY_NB is null AND b.PLCY_NB is null)) AND (a.DRIVER_KEY=b.DRIVER_KEY OR (a.DRIVER_KEY is null AND b.DRIVER_KEY is null)) AND (a.TAG_USER_SRC_IN=b.TAG_USER_SRC_IN OR (a.TAG_USER_SRC_IN is null AND b.TAG_USER_SRC_IN is null)) AND (a.TAG_USER_DERV_IN=b.TAG_USER_DERV_IN OR (a.TAG_USER_DERV_IN is null AND b.TAG_USER_DERV_IN is null)) AND (a.TEAM_ID=b.TEAM_ID OR (a.TEAM_ID is null AND b.TEAM_ID is null)) AND (a.USER_NM=b.USER_NM OR (a.USER_NM is null AND b.USER_NM is null)) AND (a.AVG_SCR_QTY=b.AVG_SCR_QTY OR (a.AVG_SCR_QTY is null AND b.AVG_SCR_QTY is null)) AND (a.AVG_ACLRTN_SCR_QTY=b.AVG_ACLRTN_SCR_QTY OR (a.AVG_ACLRTN_SCR_QTY is null AND b.AVG_ACLRTN_SCR_QTY is null)) AND (a.AVG_BRKE_SCR_QTY=b.AVG_BRKE_SCR_QTY OR (a.AVG_BRKE_SCR_QTY is null AND b.AVG_BRKE_SCR_QTY is null)) AND (a.AVG_TURN_SCR_QTY=b.AVG_TURN_SCR_QTY OR (a.AVG_TURN_SCR_QTY is null AND b.AVG_TURN_SCR_QTY is null)) AND (a.AVG_SPD_SCR_QTY=b.AVG_SPD_SCR_QTY OR (a.AVG_SPD_SCR_QTY is null AND b.AVG_SPD_SCR_QTY is null)) AND (a.AVG_PHN_MTN_SCR_QTY=b.AVG_PHN_MTN_SCR_QTY OR (a.AVG_PHN_MTN_SCR_QTY is null AND b.AVG_PHN_MTN_SCR_QTY is null)) AND (a.TOT_DISTNC_KM_QTY=b.TOT_DISTNC_KM_QTY OR (a.TOT_DISTNC_KM_QTY is null AND b.TOT_DISTNC_KM_QTY is null)) AND (a.TOT_TRIP_CNT=b.TOT_TRIP_CNT OR (a.TOT_TRIP_CNT is null AND b.TOT_TRIP_CNT is null)) AND (a.SCR_INTRV_DRTN_QTY=b.SCR_INTRV_DRTN_QTY OR (a.SCR_INTRV_DRTN_QTY is null AND b.SCR_INTRV_DRTN_QTY is null)) AND (a.USER_DRIV_LBL_HNRD_SRC_IN=b.USER_DRIV_LBL_HNRD_SRC_IN OR (a.USER_DRIV_LBL_HNRD_SRC_IN is null AND b.USER_DRIV_LBL_HNRD_SRC_IN is null)) AND (a.USER_DRIV_LBL_HNRD_DERV_IN=b.USER_DRIV_LBL_HNRD_DERV_IN OR (a.USER_DRIV_LBL_HNRD_DERV_IN is null AND b.USER_DRIV_LBL_HNRD_DERV_IN is null)) AND (a.SCR_TS=b.SCR_TS OR (a.SCR_TS is null AND b.SCR_TS is null)) AND (a.FAM_GROUP_CD=b.FAM_GROUP_CD OR (a.FAM_GROUP_CD is null AND b.FAM_GROUP_CD is null)) AND (a.PLCY_ST_CD=b.PLCY_ST_CD OR (a.PLCY_ST_CD is null AND b.PLCY_ST_CD is null)) AND (a.PRGRM_CD=b.PRGRM_CD OR (a.PRGRM_CD is null AND b.PRGRM_CD is null)) AND (a.SRC_SYS_CD=b.SRC_SYS_CD OR (a.SRC_SYS_CD is null AND b.SRC_SYS_CD is null)) and a.src_sys_cd='CMT_PL' and a.SRC_SYS_CD!=b.src_sys_cd and a.LOAD_DT<='2023-05-17'

// COMMAND ----------

// MAGIC %sql
// MAGIC select driver_key,src_sys_cd from (select distinct driver_key,src_sys_cd ,row_number() over (partition by DRIVER_KEY order by LOAD_DT) as row from (select * from dhf_iot_harmonized_prod.driver_profile_daily where LOAD_DT>'2023-05-15' and SRC_SYS_CD like '%CMT_PL%')) where row=1

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into  dhf_iot_harmonized_prod.driver_summary_daily a  using (select driver_key,PRGRM_CD,src_sys_cd from (select distinct driver_key,PRGRM_CD,src_sys_cd ,row_number() over (partition by DRIVER_KEY order by LOAD_DT) as row from (select * from dhf_iot_harmonized_prod.driver_summary_daily where LOAD_DT>'2023-05-15' and SRC_SYS_CD like '%CMT_PL_%')) where row=1)b on a.driver_key=b.driver_key and a.src_sys_cd!=b.src_sys_cd and a.LOAD_Dt<'2023-05-16' when matched then update set a.src_sys_cd=b.src_sys_cd ,a.PRGRM_CD=b.PRGRM_CD

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_harmonized_prod.driver_profile_daily a using (select * from (select *,row_number() over (partition by DRIVER_KEY,src_sys_cd order by load_dt desc)as rw from dhf_iot_harmonized_prod.driver_profile_daily) where rw>1)b on a.EMAIL_ID=b.EMAIL_ID and a.DRIVER_KEY=b.DRIVER_KEY and a.src_sys_cd=b.src_sys_cd and a.driver_profile_daily_id=b.driver_profile_daily_id when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into  dhf_iot_cmt_raw_prod.driver_summary_daily a  using (select driver_key,PRGRM_CD,src_sys_cd from (select distinct driver_key,PRGRM_CD,src_sys_cd ,row_number() over (partition by DRIVER_KEY order by LOAD_DT) as row from (select * from dhf_iot_harmonized_prod.driver_summary_daily where LOAD_DT>'2023-05-15' and SRC_SYS_CD like '%CMT_PL_%')) where row=1)b on a.short_user_id=b.driver_key and a.src_sys_cd!=b.src_sys_cd and a.LOAD_DaTe<'2023-05-16' when matched then update set a.src_sys_cd=b.src_sys_cd ,a.program_id=b.PRGRM_CD

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from (select * from dhf_iot_cmt_raw_prod.driver_profile_daily where LOAD_DaTe<'2023-05-16')a inner join (select driver_key,PRGRM_CD,src_sys_cd from (select distinct driver_key,PRGRM_CD,src_sys_cd ,row_number() over (partition by DRIVER_KEY order by LOAD_DT) as row from (select * from dhf_iot_harmonized_prod.driver_profile_daily where LOAD_DT>'2023-05-15' and SRC_SYS_CD like '%CMT_PL%')) where row=1)b on a.short_user_id=b.driver_key and a.src_sys_cd!=b.src_sys_cd

// COMMAND ----------

// MAGIC %sql
// MAGIC select distinct load_date from dhf_iot_cmtpl_raw_prod.driver_profile_daily --where short_user_id =474519563

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.driver_profile_daily where DRIVER_KEY in (select DRIVER_KEY from dhf_iot_harmonized_prod.driver_profile_daily group by DRIVER_KEY having count(*)>1)

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_harmonized_prod.driver_profile_daily a using (select driver_key,src_sys_cd from (select distinct driver_key,src_sys_cd ,row_number() over (partition by DRIVER_KEY order by LOAD_DT) as row from (select * from dhf_iot_harmonized_prod.driver_profile_daily where LOAD_DT>'2023-05-15' and SRC_SYS_CD like '%CMT_PL%')) where row=1) b on a.DRIVER_KEY=b.DRIVER_KEY and a.load_dt<'2023-05-16'  and a.SRC_SYS_CD!=b.src_sys_cd  when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_harmonized_prod.driver_profile_daily a using (select * from  dhf_iot_harmonized_prod.driver_profile_daily where src_sys_cd NOT in ('CMT_PL') and a.load_dt>'2023-05-15') b on a.DRIVER_KEY=b.DRIVER_KEY and a.load_dt<'2023-05-16'  and a.SRC_SYS_CD!=b.src_sys_cd  when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_harmonized_prod.fraud_daily a using (select * from  dhf_iot_harmonized_prod.fraud_daily where src_sys_cd NOT in ('CMT_PL')) b on   a.EMAIL_ID=b.EMAIL_ID  AND a.DRIVER_KEY=b.DRIVER_KEY  AND a.DEVC_KEY=b.DEVC_KEY  AND a.ANMLY_TP_CD=b.ANMLY_TP_CD  AND a.ANMLY_DEVC_TS=b.ANMLY_DEVC_TS  AND a.ANMLY_SRVR_TS=b.ANMLY_SRVR_TS and a.src_sys_cd='CMT_PL'  and a.SRC_SYS_CD!=b.src_sys_cd and  a.LOAD_DT<='2023-05-17' when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_harmonized_prod.heartbeat_daily a using (select * from  dhf_iot_harmonized_prod.heartbeat_daily where src_sys_cd NOT in ('CMT_PL')) b on   a.DRIVER_KEY=b.DRIVER_KEY  and a.CURR_TS=b.CURR_TS and a.src_sys_cd='CMT_PL'  and a.SRC_SYS_CD!=b.src_sys_cd and  a.LOAD_DT<='2023-05-17' when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC set spark.sql.autoBroadcastJoinThreshold=-1;
// MAGIC merge into dhf_iot_harmonized_prod.badge_daily a using (select * from  dhf_iot_harmonized_prod.badge_daily where src_sys_cd NOT in ('CMT_PL') and src_sys_cd like '%CMT_PL%') b on a.EMAIL_ID=b.EMAIL_ID  AND a.DRIVER_KEY=b.DRIVER_KEY  AND a.BDG_NM=b.BDG_NM  AND a.BDG_PRGRS_NB=b.BDG_PRGRS_NB and a.src_sys_cd='CMT_PL'  and a.SRC_SYS_CD!=b.src_sys_cd and  a.LOAD_DT<='2023-05-17' when matched then delete;

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_harmonized_prod.driver_summary_daily a using (select * from  dhf_iot_harmonized_prod.driver_summary_daily where src_sys_cd NOT in ('CMT_PL') and src_sys_cd like '%CMT_PL%') b on  a.EMAIL_ID=b.EMAIL_ID  AND a.FLEET_ID=b.FLEET_ID  AND a.GROUP_ID=b.GROUP_ID  AND a.PLCY_NB=b.PLCY_NB  AND a.DRIVER_KEY=b.DRIVER_KEY  AND a.TAG_USER_SRC_IN=b.TAG_USER_SRC_IN  AND a.TAG_USER_DERV_IN=b.TAG_USER_DERV_IN  AND a.TEAM_ID=b.TEAM_ID  AND a.AVG_SCR_QTY=b.AVG_SCR_QTY  AND a.AVG_ACLRTN_SCR_QTY=b.AVG_ACLRTN_SCR_QTY  AND a.AVG_BRKE_SCR_QTY=b.AVG_BRKE_SCR_QTY  AND a.AVG_TURN_SCR_QTY=b.AVG_TURN_SCR_QTY  AND a.AVG_SPD_SCR_QTY=b.AVG_SPD_SCR_QTY  AND a.AVG_PHN_MTN_SCR_QTY=b.AVG_PHN_MTN_SCR_QTY  AND a.TOT_DISTNC_KM_QTY=b.TOT_DISTNC_KM_QTY  AND a.TOT_TRIP_CNT=b.TOT_TRIP_CNT  AND a.SCR_INTRV_DRTN_QTY=b.SCR_INTRV_DRTN_QTY  AND a.USER_DRIV_LBL_HNRD_SRC_IN=b.USER_DRIV_LBL_HNRD_SRC_IN  AND a.USER_DRIV_LBL_HNRD_DERV_IN=b.USER_DRIV_LBL_HNRD_DERV_IN  AND a.SCR_TS=b.SCR_TS AND a.FAM_GROUP_CD = b.FAM_GROUP_CD and a.src_sys_cd='CMT_PL'  and a.SRC_SYS_CD!=b.src_sys_cd and  a.LOAD_DT<='2023-05-17' when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_harmonized_prod.user_permissions_daily a using (select * from  dhf_iot_harmonized_prod.user_permissions_daily where src_sys_cd NOT in ('CMT_PL') and src_sys_cd like '%CMT_PL%') b on  a.CUST_NM=b.CUST_NM  AND a.DRIVER_KEY=b.DRIVER_KEY  AND a.SRC_CUST_KEY=b.SRC_CUST_KEY  AND a.ACCT_ID=b.ACCT_ID  AND a.USR_NM=b.USR_NM  AND a.EMAIL_ID=b.EMAIL_ID  AND a.MOBL_NB=b.MOBL_NB  AND a.GROUP_ID=b.GROUP_ID  AND a.UNQ_GROUP_NM=b.UNQ_GROUP_NM  AND a.PLCY_NB=b.PLCY_NB  AND a.FLT_ID=b.FLT_ID  AND a.FLT_NM=b.FLT_NM  AND a.TEAM_ID=b.TEAM_ID  AND a.TEAM_NM=b.TEAM_NM  AND a.LAST_SHRT_VEHCL_ID=b.LAST_SHRT_VEHCL_ID  AND a.VIN_NB=b.VIN_NB  AND a.VHCL_MAKE_NM=b.VHCL_MAKE_NM  AND a.VHCL_MODEL_NM=b.VHCL_MODEL_NM  AND a.LST_TAG_MAC_ADDR_NM=b.LST_TAG_MAC_ADDR_NM  AND a.DEVC_KEY=b.DEVC_KEY  AND a.DEVC_MODL_NM=b.DEVC_MODL_NM  AND a.DEVC_BRND_NM=b.DEVC_BRND_NM  AND a.DEVC_MFRR_NM=b.DEVC_MFRR_NM  AND a.OS_NM=b.OS_NM  AND a.OS_VRSN_NM=b.OS_VRSN_NM  AND a.APP_VRSN_NM=b.APP_VRSN_NM  AND a.LAST_DRV_TS=b.LAST_DRV_TS  AND a.DAYS_SINCE_LAST_TRIP_CT=b.DAYS_SINCE_LAST_TRIP_CT  AND a.LAST_ACTVTY_TS=b.LAST_ACTVTY_TS  AND a.RGSTRTN_TS=b.RGSTRTN_TS  AND a.USER_LOGGED_SRC_IN=b.USER_LOGGED_SRC_IN  AND a.USER_LOGGED_DERV_IN=b.USER_LOGGED_DERV_IN  AND a.LAST_AUTHRZN_TS=b.LAST_AUTHRZN_TS  AND a.PTNTL_ISSUES_TT=b.PTNTL_ISSUES_TT  and a.src_sys_cd='CMT_PL'  and a.SRC_SYS_CD!=b.src_sys_cd and  a.LOAD_DT<='2023-05-17' when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_harmonized_prod.driver_summary_daily a using (select * from  dhf_iot_harmonized_prod.driver_summary_daily where src_sys_cd NOT in ('CMT_PL') and src_sys_cd like '%CMT_PL%' and LOAD_DT<='2023-05-17') b on a.DRIVER_KEY=b.DRIVER_KEY and a.src_sys_cd='CMT_PL'  and a.SRC_SYS_CD!=b.src_sys_cd and  a.LOAD_DT<='2023-05-17' when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_harmonized_prod.trip_labels_daily a using (select * from  dhf_iot_harmonized_prod.trip_labels_daily where src_sys_cd NOT in ('CMT_PL') and src_sys_cd like '%CMT_PL%' and LOAD_DT<='2023-05-17') b on a.TRIP_SMRY_KEY=b.TRIP_SMRY_KEY  AND a.USER_LBL_NM=b.USER_LBL_NM  AND a.USER_LBL_TS=b.USER_LBL_TS and a.src_sys_cd='CMT_PL'  and a.SRC_SYS_CD!=b.src_sys_cd and  a.LOAD_DT<='2023-05-17' when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_harmonized_prod.trip_summary_daily a using (select * from  dhf_iot_harmonized_prod.trip_summary_daily where src_sys_cd NOT in ('CMT_PL') and src_sys_cd like '%CMT_PL%') b on a.TRIP_SMRY_KEY=b.TRIP_SMRY_KEY  and a.src_sys_cd='CMT_PL'  and a.SRC_SYS_CD!=b.src_sys_cd and  a.LOAD_DT<='2023-05-17' when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC (select  distinct   DRIVER_KEY from dhf_iot_harmonized_prod.driver_profile_daily where SRC_SYS_CD like '%CMT_PL%') minus (select distinct short_user_id as DRIVER_KEY from dhf_iot_cmt_raw_prod.driver_profile_daily ) --order by LOAD_DT

// COMMAND ----------

// MAGIC %sql
// MAGIC  SELECT Count(*) as Harm_DupeCount,DRIVER_KEY,min(LOAD_DT) as min
// MAGIC
// MAGIC FROM dhf_iot_harmonized_prod.driver_profile_daily
// MAGIC
// MAGIC where SRC_SYS_CD in ('CMT_PL', 'CMT_PL_SRM', 'CMT_PL_SRMCM')
// MAGIC
// MAGIC group by 2
// MAGIC
// MAGIC having count(*) > 1 order by min

// COMMAND ----------

// MAGIC %sql
// MAGIC  SELECT Count(*) as Harm_DupeCount, EMAIL_ID,DRIVER_KEY,BDG_NM,BDG_PRGRS_NB,min(LOAD_DT) as min
// MAGIC
// MAGIC FROM dhf_iot_harmonized_prod.badge_daily
// MAGIC
// MAGIC where SRC_SYS_CD in ('CMT_PL', 'CMT_PL_SRM', 'CMT_PL_SRMCM')
// MAGIC
// MAGIC group by 2,3,4,5 
// MAGIC
// MAGIC having count(*) > 1 order by min

// COMMAND ----------

// MAGIC %sql
// MAGIC select a.* from (select distinct 1 as BADGE_DAILY_ID, short_user_id as DRIVER_KEY, email as EMAIL_ID,badge_name AS BDG_NM,badge_progress AS BDG_PRGRS_NB,cast(date_awarded as TIMESTAMP) AS BDG_ASGN_TS, user_state as PLCY_ST_CD,program_id as PRGRM_CD,src_sys_cd as SRC_SYS_CD,coalesce(load_date,to_date("9999-12-31")) as LOAD_DT,current_timestamp() as LOAD_HR_TS, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS from dhf_iot_cmt_raw_prod.badge_daily) a left anti join dhf_iot_harmonized_prod.badge_daily b on a.EMAIL_ID=b.EMAIL_ID and a.DRIVER_KEY=b.DRIVER_KEY and a.BDG_NM=b.BDG_NM and a.BDG_PRGRS_NB=b.BDG_PRGRS_NB

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_harmonized_prod.badge_daily a using (select * from (select *,row_number() over (partition by EMAIL_ID,DRIVER_KEY,BDG_NM,BDG_PRGRS_NB,src_sys_cd order by load_dt desc)as rw from dhf_iot_harmonized_prod.badge_daily) where rw>1)b on a.EMAIL_ID=b.EMAIL_ID and a.DRIVER_KEY=b.DRIVER_KEY and a.BDG_NM=b.BDG_NM and a.BDG_PRGRS_NB=b.BDG_PRGRS_NB and a.src_sys_cd=b.src_sys_cd and a.badge_daily_id=b.badge_daily_id when matched then delete

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_cmt_raw_prod.badge_daily where short_user_id=136472285 --and LOAD_DT>='2020-03-28' --and EMAIL_ID=''

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.badge_daily where DRIVER_KEY=151283045 --and LOAD_DT>='2020-03-28' --and EMAIL_ID=''

// COMMAND ----------

// MAGIC %sql
// MAGIC select driver_key,PRGRM_CD,src_sys_cd from (select distinct driver_key,PRGRM_CD,src_sys_cd ,row_number() over (partition by DRIVER_KEY order by LOAD_DT) as row from (select * from dhf_iot_harmonized_prod.badge_daily where DRIVER_KEY=136472285 and LOAD_DT>'2023-05-15' and SRC_SYS_CD like '%CMT_PL_%')) where row=1

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into  dhf_iot_cmt_raw_prod.badge_daily a  using (select driver_key,PRGRM_CD,src_sys_cd from (select distinct driver_key,PRGRM_CD,src_sys_cd ,row_number() over (partition by DRIVER_KEY order by LOAD_DT) as row from (select * from dhf_iot_harmonized_prod.badge_daily where LOAD_DT>'2023-05-15' and SRC_SYS_CD like '%CMT_PL_%')) where row=1)b on a.short_user_id=b.driver_key and a.src_sys_cd!=b.src_sys_cd and a.LOAD_DaTe<'2023-05-16' when matched then update set a.src_sys_cd=b.src_sys_cd ,a.program_id=b.PRGRM_CD

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into  dhf_iot_harmonized_prod.fraud_daily a  using (select driver_key,PRGRM_CD,src_sys_cd from (select distinct driver_key,PRGRM_CD,src_sys_cd ,row_number() over (partition by DRIVER_KEY order by LOAD_DT) as row from (select * from dhf_iot_harmonized_prod.fraud_daily where SRC_SYS_CD like '%CMT_PL_%')) where row=1)b on a.driver_key=b.driver_key and a.src_sys_cd!=b.src_sys_cd and a.LOAD_Dt<'2023-05-16' and a.src_sys_cd='CMT_PL' when matched then update set a.src_sys_cd=b.src_sys_cd ,a.PRGRM_CD=b.PRGRM_CD

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into  dhf_iot_cmt_raw_prod.fraud_daily a  using (select driver_key,PRGRM_CD,src_sys_cd from (select distinct driver_key,PRGRM_CD,src_sys_cd ,row_number() over (partition by DRIVER_KEY order by LOAD_DT) as row from (select * from dhf_iot_harmonized_prod.fraud_daily where SRC_SYS_CD like '%CMT_PL_%')) where row=1)b on a.short_user_id=b.driver_key and a.src_sys_cd!=b.src_sys_cd and a.LOAD_DaTe<'2023-05-16' and a.src_sys_cd='CMT_PL' when matched then update set a.src_sys_cd=b.src_sys_cd ,a.program_id=b.PRGRM_CD

// COMMAND ----------

// MAGIC %sql
// MAGIC set spark.sql.autoBroadcastJoinThreshold=-1;
// MAGIC merge into dhf_iot_harmonized_prod.badge_daily a using (select * from  dhf_iot_harmonized_prod.badge_daily where src_sys_cd NOT in ('CMT_PL') and src_sys_cd like '%CMT_PL%') b on (a.EMAIL_ID=b.EMAIL_ID or (a.EMAIL_ID='' and b.EMAIL_ID=''))  AND a.DRIVER_KEY=b.DRIVER_KEY  AND a.BDG_NM=b.BDG_NM  AND a.BDG_PRGRS_NB=b.BDG_PRGRS_NB and a.SRC_SYS_CD!=b.src_sys_cd  when matched then delete;

// COMMAND ----------

// MAGIC %sql
// MAGIC select  TRIP_SMRY_KEY,min(LOAD_DT) from dhf_iot_harmonized_prod.trip_summary_daily group by TRIP_SMRY_KEY having count(*)>1

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.trip_summary_daily where TRIP_SMRY_KEY='2E2A3B66-A262-400A-9F27-2B434B6886A4'

// COMMAND ----------

// MAGIC %sql
// MAGIC select a.* from (select distinct 1 as BADGE_DAILY_ID, short_user_id as DRIVER_KEY, email as EMAIL_ID,badge_name AS BDG_NM,badge_progress AS BDG_PRGRS_NB,cast(date_awarded as TIMESTAMP) AS BDG_ASGN_TS, user_state as PLCY_ST_CD,program_id as PRGRM_CD,src_sys_cd as SRC_SYS_CD,coalesce(load_date,to_date("9999-12-31")) as LOAD_DT,current_timestamp() as LOAD_HR_TS, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS from dhf_iot_cmt_raw_prod.badge_daily) a left anti join dhf_iot_harmonized_prod.badge_daily b on a.EMAIL_ID=b.EMAIL_ID and a.DRIVER_KEY=b.DRIVER_KEY and a.BDG_NM=b.BDG_NM and a.BDG_PRGRS_NB=b.BDG_PRGRS_NB

// COMMAND ----------

src_sys_cd like '%CMT_PL%'y

// COMMAND ----------

// MAGIC %sql
// MAGIC select EMAIL_ID,DRIVER_KEY,DEVC_KEY,ANMLY_TP_CD,ANMLY_DEVC_TS,ANMLY_SRVR_TS,min(LOAD_DT) as mina from dhf_iot_harmonized_prod.fraud_daily WHERE SRC_SYS_CD like '%CMT_PL%' group by EMAIL_ID,DRIVER_KEY,DEVC_KEY,ANMLY_TP_CD,ANMLY_DEVC_TS,ANMLY_SRVR_TS,SRC_SYS_CD having count(*)>1 order by mina

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.fraud_daily where DRIVER_KEY=28701923 --and ANMLY_TP_CD='TRIP_INTERRUPTED' --and LOAD_DT>='2020-03-28' --and EMAIL_ID=''

// COMMAND ----------

// MAGIC %sql
// MAGIC merge into dhf_iot_harmonized_prod.fraud_daily a using (select * from (select *,row_number() over (partition by EMAIL_ID,DRIVER_KEY,DEVC_KEY,ANMLY_TP_CD,ANMLY_DEVC_TS,ANMLY_SRVR_TS,src_sys_cd order by load_dt desc)as rw from dhf_iot_harmonized_prod.fraud_daily) where rw>1)b on a.EMAIL_ID=b.EMAIL_ID and a.DRIVER_KEY=b.DRIVER_KEY and a.DEVC_KEY=b.DEVC_KEY and a.ANMLY_TP_CD=b.ANMLY_TP_CD  and a.src_sys_cd=b.src_sys_cd and a.fraud_daily_id=b.fraud_daily_id when matched then delete