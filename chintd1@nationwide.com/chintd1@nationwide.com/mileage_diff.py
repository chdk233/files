# Databricks notebook source
# MAGIC %sql
# MAGIC Select enrld_vin_nb, mileage_dt, td_mile_ct, pd_mile_ct, td_total_mile_ct, pd_total_mile_ct, miles_difference From (
# MAGIC Select TD.VIN_NB as enrld_vin_nb, TD.mileage_dt as mileage_dt, td.mile_ct as td_mile_ct, PD.mile_ct as pd_mile_ct, td.total_mile_ct as td_total_mile_ct, PD.total_mile_ct as pd_total_mile_ct, (round(td.mile_ct,2) - round(PD.mile_ct,2)) AS Miles_difference From
# MAGIC (select
# MAGIC   s4.VIN_NB
# MAGIC , td2.SRC_SYS_CD as src_sys_cd
# MAGIC , s4.PRGRM_TERM_BEG_DT
# MAGIC , s4.PRGRM_TERM_END_DT
# MAGIC , s4.mileage_dt
# MAGIC , s4.mile_ct
# MAGIC , SUM(cast(td2.mile_ct as decimal(18,5))) as total_mile_ct
# MAGIC
# MAGIC
# MAGIC
# MAGIC from
# MAGIC
# MAGIC
# MAGIC
# MAGIC (
# MAGIC select distinct
# MAGIC   s3.VIN_NB as VIN_NB
# MAGIC ,SUBSTR(td1.PE_STRT_TS,0,10) as mileage_dt
# MAGIC , s3.PRGRM_TERM_BEG_DT
# MAGIC , s3.PRGRM_TERM_END_DT
# MAGIC ,sum(td1.mile_ct) as mile_ct
# MAGIC from
# MAGIC (
# MAGIC Select distinct
# MAGIC   s2.VIN_NB
# MAGIC , s2.PRGRM_TERM_BEG_DT as PRGRM_TERM_BEG_DT
# MAGIC , s2.PRGRM_TERM_END_DT as PRGRM_TERM_END_DT
# MAGIC , SUBSTR(td.PE_STRT_TS, 0,10) as mileage_dt
# MAGIC
# MAGIC
# MAGIC
# MAGIC from
# MAGIC (
# MAGIC select VIN_NB,PRGRM_TERM_BEG_DT,PRGRM_TERM_END_DT from dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT where LOAD_HR_TS = (select max(LOAD_HR_TS) from dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT)
# MAGIC )s2
# MAGIC
# MAGIC
# MAGIC
# MAGIC JOIN  dhf_iot_curated_prod.trip_detail td
# MAGIC ON td.ENRLD_VIN_NB = s2.VIN_NB
# MAGIC --and td.ENRLD_VIN_NB ='5UXJU4C05L9B38100'
# MAGIC and SUBSTR(td.PE_STRT_TS,0,10) >= s2.PRGRM_TERM_BEG_DT
# MAGIC and SUBSTR(td.PE_STRT_TS,0,10) <= s2.PRGRM_TERM_END_DT
# MAGIC
# MAGIC
# MAGIC
# MAGIC WHERE date(ETL_LAST_UPDT_DTS) = current_date()-1
# MAGIC and mile_ct is not null
# MAGIC and mile_ct <> 0
# MAGIC and SRC_SYS_CD = 'IMS_SM_5X'
# MAGIC GROUP BY
# MAGIC   s2.VIN_NB
# MAGIC , s2.PRGRM_TERM_BEG_DT
# MAGIC , s2.PRGRM_TERM_END_DT
# MAGIC , SUBSTR(td.PE_STRT_TS, 0,10)
# MAGIC ) s3
# MAGIC
# MAGIC
# MAGIC
# MAGIC JOIN  dhf_iot_curated_prod.trip_detail td1
# MAGIC ON td1.ENRLD_VIN_NB = s3.VIN_NB
# MAGIC and SUBSTR(td1.PE_STRT_TS,0,10) >= s3.PRGRM_TERM_BEG_DT
# MAGIC and SUBSTR(td1.PE_STRT_TS,0,10) <= s3.PRGRM_TERM_END_DT
# MAGIC and s3.mileage_dt = SUBSTR(td1.PE_STRT_TS,0,10)
# MAGIC WHERE ETL_LAST_UPDT_DTS < DATEADD(hour,12,current_date())
# MAGIC
# MAGIC
# MAGIC
# MAGIC GROUP BY
# MAGIC   s3.VIN_NB
# MAGIC , s3.mileage_dt
# MAGIC , s3.PRGRM_TERM_BEG_DT
# MAGIC , s3.PRGRM_TERM_END_DT
# MAGIC ,SUBSTR(td1.PE_STRT_TS,0,10)
# MAGIC )s4
# MAGIC
# MAGIC
# MAGIC
# MAGIC JOIN dhf_iot_curated_prod.trip_detail td2
# MAGIC ON td2.ENRLD_VIN_NB = s4.VIN_NB
# MAGIC and SRC_SYS_CD = 'IMS_SM_5X'
# MAGIC and SUBSTR(td2.PE_STRT_TS,0,10) >= s4.PRGRM_TERM_BEG_DT
# MAGIC and SUBSTR(td2.PE_STRT_TS,0,10) <= s4.PRGRM_TERM_END_DT
# MAGIC WHERE ETL_LAST_UPDT_DTS < DATEADD(hour,12,current_date())
# MAGIC
# MAGIC GROUP BY
# MAGIC   s4.VIN_NB
# MAGIC , td2.SRC_SYS_CD
# MAGIC , s4.PRGRM_TERM_BEG_DT
# MAGIC , s4.PRGRM_TERM_END_DT
# MAGIC , s4.mileage_dt
# MAGIC , s4.mile_ct) TD
# MAGIC
# MAGIC
# MAGIC
# MAGIC JOIN dhf_iot_curated_prod.smartmiles_program_details PD ON TD.VIN_NB=PD.enrld_vin_nb And TD.mileage_dt = PD.mileage_dt And date(PD.Etl_last_updt_dts) = current_date() And
# MAGIC PD.Event_type='MILEAGE_UPDATE') diff where diff.Miles_difference >10 ;

# COMMAND ----------

display(df)

# COMMAND ----------

display(_sqldf.filter("enrld_vin_nb='1G1BC5SM9K7109413'''"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.smartmiles_program_details where enrld_vin_nb= '1C4RJFCT7MC508855'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(enrld_vin_nb),mileage_dt,sum(ct) from  (select mileage_dt,enrld_vin_nb,max(mile_ct) as ct from dhf_iot_curated_prod.smartmiles_program_details group by  enrld_vin_nb,mileage_dt) group by mileage_dt order by mileage_dt desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct enrld_vin_nb) AS VIN_cn, SUM(hr.Mile_Ct) AS Mile_Cn,date(hr.PE_STRT_TS) as Trip_Dt FROM dhf_iot_curated_prod.trip_detail hr where src_sys_cd like '%SM%' and date(hr.PE_STRT_TS)='2022-11-07' and ETL_LAST_UPDT_DTS<'2022-11-09T12:11:24.217+0000' group by date(hr.PE_STRT_TS) order by date(hr.PE_STRT_TS) desc 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from (select * from dhf_iot_curated_prod.smartmiles_program_details where mileage_dt='2022-11-07' and Etl_Last_Updt_dts<'2022-11-10')a inner join (select ( enrld_vin_nb) AS VIN_cn, SUM(hr.Mile_Ct) AS Mile_Cn FROM dhf_iot_curated_prod.trip_detail hr where src_sys_cd like '%SM%' and date(hr.PE_STRT_TS)='2022-11-07' and ETL_LAST_UPDT_DTS<'2022-11-10T12:11:24.217+0000' group by (hr.enrld_vin_nb)  )b on a.enrld_vin_nb=b.VIN_cn

# COMMAND ----------

# MAGIC %sql
# MAGIC select mileage_dt,sum(mile_ct) from dhf_iot_curated_prod.smartmiles_program_details group by mileage_dt order by mileage_dt desc --where mileage_dt='2022-11-07' and load_dt<='2022-11-09'

# COMMAND ----------

# MAGIC %sql
# MAGIC select ENRLD_VIN_NB,PE_STRT_TS,PE_END_TS,MILE_CT,LOAD_HR_TS,LOAD_DT,ETL_ROW_EFF_DTS,ETL_LAST_UPDT_DTS from dhf_iot_curated_prod.trip_detail where enrld_vin_nb='1G1BC5SM9K7109413'

# COMMAND ----------

# MAGIC %sql
# MAGIC select DATEADD(hour,12,current_date())

# COMMAND ----------

# driver = "com.ibm.db2.jcc.DB2Driver"
# url = "jdbc:db2://nwiegateway-desktop-prod.nwie.net:50000/NW_DBG2"
# user = dbutils.secrets.get('pcdm-iot', 'srp_ods_un')
# password = dbutils.secrets.get('pcdm-iot', 'srp_ods_pw')
hostName = "telematicsprod.ctpecl4bnuoq.us-east-1.rds.amazonaws.com"
pgPort = "5432"
dbName = "telem01p" 
user = dbutils.secrets.get('pcdm-iot', 'srp_postgres_un')
password = dbutils.secrets.get('pcdm-iot', 'srp_postgres_pw')
currentschema = "telematics"
 
driverClass = "org.postgresql.Driver"
jdbcUrlForConfigDB = f"jdbc:postgresql://{hostName}:{pgPort}/{dbName}"
 
sqlConnectionProperties = {
"url": jdbcUrlForConfigDB, 
"user":user, 
"password": password, 
"currentSchema": currentschema, 
"Driver": driverClass
}

# COMMAND ----------

query = "(select * from telematics.mileage ) as temp"
 
table2 = spark.read.jdbc(url=jdbcUrlForConfigDB, table=query, properties=sqlConnectionProperties)
table2.createOrReplaceTempView("main_table")
# display(table)

# COMMAND ----------

query = "(select * from telematics.data_clctn ) as temp"
 
table = spark.read.jdbc(url=jdbcUrlForConfigDB, table=query, properties=sqlConnectionProperties)
table.createOrReplaceTempView("vin_table")
# display(table)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct enrld_vin_nb,DATA_CLCTN_ID,mileage_dt,round(mile_ct) from dhf_iot_curated_prod.smartmiles_program_details a inner join (select * from dhf_iot_harmonized_prod.integrated_enrollment where load_dt='2022-11-09') b on a.enrld_vin_nb=b.vin_nb  where mileage_dt='2022-11-07' and DATA_CLCTN_ID not in  (select data_clctn_id from (select a.data_clctn_seq_id,b.data_clctn_id,a.actual_mlg,a.actual_mlg_dt from main_table a inner join vin_table b on a.data_clctn_seq_id=b.data_clctn_seq_id where a.actual_mlg_dt='2022-11-07'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  dhf_iot_harmonized_prod.integrated_enrollment where DATA_CLCTN_ID='6e0346f2-f1e4-4009-aca4-292b797cf5dd'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table where data_clctn_id= '6e0346f2-f1e4-4009-aca4-292b797cf5dd'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from main_table where data_clctn_seq_id=450382

# COMMAND ----------

# MAGIC %sql
# MAGIC select (*) from table where actual_mlg_dt='2022-11-07' order by actual_mlg desc

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.smartmiles_program_details where mileage_dt='2022-11-07' order by mile_ct desc--and load_dt<='2022-11-09'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct load_dt from  dhf_iot_harmonized_prod.integrated_enrollment --where DATA_CLCTN_ID='2085789'

# COMMAND ----------

