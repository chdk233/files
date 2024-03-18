# Databricks notebook source
from datetime import date,timedelta
from pyspark.sql.functions import *

tables_list=[]
source_system=[]
days_start_count=1
days_end_count=10
mail_flag='true'
environment='prod'
audit_check_date = date.today()-timedelta(days=days_start_count)
print(f"Current Date is : {audit_check_date}")

# COMMAND ----------

import smtplib
import email
from email.utils import COMMASPACE
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email import encoders
 
 
email_to = 'chintd1@nationwide.com, milld57@nationwide.com, madhavk1@nationwide.com'
email_from = 'do-not-reply@nationwide.com'
 
# email function
def send_email(sender, recipients, subject, body):
  
  #build email structure
  MIME_ATTACHMENT = MIMEBase('application','vnd.ms-excel')
  recipients = [x.strip() for x in recipients.split(',')]
  msg = MIMEMultipart()
  msg['Subject'] = subject
  msg['From'] = sender
  msg['To'] = COMMASPACE.join(recipients)
  msg.preamble = subject
  msg.attach(MIMEText(body,'plain'))

  #send to recipients
  s = smtplib.SMTP('mail-gw.ent.nwie.net')
  s.sendmail(sender, recipients, msg.as_string())
  s.quit()

# COMMAND ----------

# trip_summary audit check

tims_trip_summary_missing_trips=spark.sql(f"""select  a.* from 
(select * from dhf_iot_tims_raw_{environment}.tripsummary where load_date>=current_date-{days_end_count} and load_date<=current_date-{days_start_count}) a left anti join
(select * from dhf_iot_harmonized_{environment}.trip_summary where load_dt>=current_date-{days_end_count}-6 and src_sys_cd in ("TIMS_SR")) b 
on   a.tripsummaryid=b.trip_smry_key and  a.sourcesystem=b.src_sys_cd where tripsummaryid is not null""")

chlg_query=f"""select distinct cast(row_number() over(order by NULL) +1500+ coalesce((select max(TRIP_SUMMARY_ID) from dhf_iot_harmonized_{environment}.trip_summary),0) as BIGINT) as TRIP_SUMMARY_ID, coalesce(tripSummaryId, 'NOKEY') AS TRIP_SMRY_KEY, coalesce(db_load_time, to_timestamp('9999-12-31')) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, cast(crc32(detectedvin) as string) AS DEVC_KEY, cast(timeZoneOffset as decimal(10,2)) AS TIME_ZONE_OFFST_NB, coalesce(cast(utcStartDateTime as TIMESTAMP), to_timestamp('9999-12-31')) AS TRIP_START_TS, coalesce(cast(utcEndDateTime as TIMESTAMP), to_timestamp('9999-12-31')) AS TRIP_END_TS, detectedVin AS VEH_KEY, coalesce(load_date, to_date('9999-12-31')) as LOAD_DT, coalesce(sourcesystem,'NOKEY') AS SRC_SYS_CD, coalesce(load_hour, to_timestamp('9999-12-31')) as LOAD_HR_TS from dhf_iot_tims_stageraw_{environment}.trip_summary_missing_trips where timestampdiff(hour,utcStartDateTime, utcEndDateTime )<12 and etl_load_date=current_date"""



if (tims_trip_summary_missing_trips.count()>0):
  tables_list= tables_list + ['Trip_summary']
  print("trips_missing")
#   tims_trip_summary_missing_trips.withColumn("etl_load_date",current_date()).withColumn("etl_load_time",current_timestamp()).write.format("delta").mode("append").option("mergeSchema","True").saveAsTable(f"dhf_iot_tims_stageraw_{environment}.trip_summary_missing_trips")
  
  spark.sql(f"{chlg_query}").write.format("delta").mode("append").saveAsTable(f"dhf_iot_harmonized_{environment}.trip_summary_chlg")



# COMMAND ----------

# trip_point audit check

tims_trip_point_missing_trips=spark.sql(f"""select  a.* from 
(select * from dhf_iot_tims_raw_{environment}.telemetrypoints where load_date>=current_date-{days_end_count} and load_date<=current_date-{days_start_count}) a left anti join
(select * from dhf_iot_harmonized_{environment}.trip_point where load_dt>=current_date-{days_end_count}-6 and src_sys_cd in ("TIMS_SR")) b 
on   a.tripsummaryid=b.trip_smry_key  and a.utcDateTime=b.UTC_TS and a.sourcesystem=b.src_sys_cd where utcDateTime is not null""")

chlg_query=f"""select distinct cast(row_number() over(order by NULL) + 20000 + coalesce((select max(TRIP_POINT_ID) from dhf_iot_harmonized_{environment}.trip_point), 0) as BIGINT) as TRIP_POINT_ID, cast(engineRPM as decimal(18, 5)) as ENGIN_RPM_RT, cast(hdop as decimal(18, 10)) as HDOP_QTY, tps.tripSummaryId AS TRIP_SMRY_KEY,coalesce(cast(utcDateTime as TIMESTAMP),to_timestamp("9999-12-31")) AS UTC_TS, cast(degreesLatitude as decimal(18, 10)) as LAT_NB, cast(degreesLongitude as decimal(18, 10)) as LNGTD_NB, cast(speed as decimal(18, 10)) as SPD_RT, cast(headingDegrees as decimal(15, 6)) as HEADNG_DEG_QTY, coalesce(sourcesystem, "NOKEY") AS SRC_SYS_CD, cast(accelerometerDataLong as decimal(18, 10)) as LNGTDNL_ACCLRTN_RT, cast(accelerometerDataLat as decimal(18, 10)) as LATRL_ACCLRTN_RT, coalesce(crc32(detectedvin), "NOKEY") AS DEVC_KEY, cast(timeZoneOffset as decimal(10, 2)) as TIME_ZONE_OFFST_NB, COALESCE(detectedVin, "NOKEY") as ENRLD_VIN_NB, coalesce(load_date, to_date("9999-12-31")) as LOAD_DT, coalesce(tps.load_hour, to_timestamp("9999-12-31")) as LOAD_HR_TS, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, cast(pcssetting as int) as PCS_STTNG_STTS_CD, cast(pcsopstatealarm as int) as PCS_OPRTN_ST_ALRM_SRC_IN, cast(pcsopstatebrake as int) as PCS_OP_ST_BRKE_SRC_IN, cast(lkaind as int) as LN_KP_ASST_STTS_CD, cast(lkabuzzer as int) as LN_KP_ASST_BUZZER_SRC_IN, cast(lkaassist as int) as LN_KP_ASST_SRC_IN, cast(cruisecontrolstate as int) as CRUSE_CNTL_ST_SRC_IN,(case when cruisecontrolstate = 0 then "N" when cruisecontrolstate = 1 then "Y" when isnull(cruisecontrolstate) then "" else "@" end) as CRUSE_CNTL_ST_DERV_IN,(case when lkaassist = 0 then "N" when lkaassist = 1 then "Y" when isnull(lkaassist) then "" else "@" end) as LN_KP_ASST_DERV_IN,(case when lkabuzzer = 0 then "N" when lkabuzzer = 1 then "Y" when isnull(lkabuzzer) then "" else "@" end) as LN_KP_ASST_BUZZER_DERV_IN,(case when pcsopstatealarm = 0 then "N" when pcsopstatealarm = 1 then "Y" when isnull(pcsopstatealarm) then "" else "@" end) as PCS_OPRTN_ST_ALRM_DERV_IN, (case when pcsopstatebrake = 0 then "N" when pcsopstatebrake = 1 then "Y" when isnull(pcsopstatebrake) then "" else "@" end) as PCS_OPRTN_ST_BRKE_DERV_IN from (select distinct * from dhf_iot_tims_stageraw_{environment}.trip_point_missing_trips where etl_load_date=current_date) tps inner join (select distinct tripSummaryId, load_hour, count(*) as tripLen from dhf_iot_tims_stageraw_{environment}.trip_point_missing_trips where etl_load_date=current_date  group by tripSummaryId, load_hour  having tripLen < 43200) vtps on tps.tripSummaryId = vtps.tripSummaryId and tps.load_hour = vtps.load_hour """


if (tims_trip_point_missing_trips.count()>0):
  print("trips_missing")
  tables_list= tables_list + ['Trip_point']
  tims_trip_point_missing_trips.withColumn("etl_load_date",current_date()).withColumn("etl_load_time",current_timestamp()).write.format("delta").mode("append").option("mergeSchema","True").saveAsTable(f"dhf_iot_tims_stageraw_{environment}.trip_point_missing_trips")
  
  spark.sql(f"{chlg_query}").write.format("delta").mode("append").saveAsTable(f"dhf_iot_harmonized_{environment}.trip_point_chlg")



# COMMAND ----------

# vehicle audit check

tims_vehicle_missing_vins=spark.sql(f"""select  distinct a.* from 
(select distinct detectedVin,load_date,load_hour,db_load_date,db_load_time,sourcesystem from dhf_iot_tims_raw_{environment}.tripsummary where load_date>=current_date-{days_end_count} and load_date<=current_date-{days_start_count}) a left anti join
(select * from dhf_iot_harmonized_{environment}.vehicle where src_sys_cd in ("TIMS_SR")) b 
on   a.detectedVin=b.enrld_vin_nb  and a.sourcesystem=b.src_sys_cd where detectedVin is not null""")

chlg_query=f"""select distinct coalesce(detectedVin, "NOKEY") AS VEH_KEY, cast(row_number() over(order by NULL) + 2500 + coalesce((select max(VEHICLE_ID) from dhf_iot_harmonized_{environment}.vehicle),0) as BIGINT) as VEHICLE_ID, coalesce(db_load_time, to_timestamp("9999-12-31")) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, coalesce(detectedVin) as DTCTD_VIN_NB, coalesce(detectedVin, "NOKEY") as ENRLD_VIN_NB, coalesce(load_date, to_date("9999-12-31")) as LOAD_DT, coalesce(SourceSystem, "NOKEY") AS SRC_SYS_CD, coalesce(load_hour, to_timestamp("9999-12-31")) as LOAD_HR_TS from dhf_iot_tims_stageraw_{environment}.vehicle_missing_vins where etl_load_date=current_date"""


if (tims_vehicle_missing_vins.count()>0):
  tables_list= tables_list + ['Vehicle']
  print("trips_missing")
  tims_vehicle_missing_vins.withColumn("etl_load_date",current_date()).withColumn("etl_load_time",current_timestamp()).write.format("delta").mode("append").saveAsTable(f"dhf_iot_tims_stageraw_{environment}.vehicle_missing_vins")
  spark.sql(f"{chlg_query}").write.format("delta").mode("append").saveAsTable(f"dhf_iot_harmonized_{environment}.vehicle_chlg")


# COMMAND ----------

# device audit check

tims_device_missing_vins=spark.sql(f"""select  distinct a.* from 
(select distinct load_date,load_hour,db_load_date,db_load_time,sourcesystem,detectedvin from dhf_iot_tims_raw_{environment}.tripsummary where load_date>=current_date-{days_end_count} and load_date<=current_date-{days_start_count}) a left anti join
(select * from dhf_iot_harmonized_{environment}.device where   src_sys_cd in ("TIMS_SR")) b 
on   cast(coalesce(crc32(a.detectedvin), 'NOKEY') as STRING)=b.DEVC_KEY  and a.sourcesystem=b.src_sys_cd where detectedvin is not null""")


chlg_query=f"""select distinct cast(row_number() over(order by NULL) +1500+ coalesce((select max(DEVICE_ID) from dhf_iot_harmonized_{environment}.device),0) as BIGINT) as DEVICE_ID, cast(coalesce(crc32(detectedvin), 'NOKEY') as STRING) AS DEVC_KEY, coalesce(db_load_time, to_timestamp('9999-12-31')) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, coalesce(load_date, to_date('9999-12-31')) as LOAD_DT, coalesce(SourceSystem, 'NOKEY') AS SRC_SYS_CD, coalesce(load_hour, to_timestamp('9999-12-31')) as LOAD_HR_TS from dhf_iot_tims_stageraw_{environment}.device_missing_vins where detectedvin is not null and etl_load_date=current_date """

if (tims_device_missing_vins.count()>0):
  tables_list= tables_list + ['Device']
  print("trips_missing")
  tims_device_missing_vins.withColumn("etl_load_date",current_date()).withColumn("etl_load_time",current_timestamp()).write.format("delta").mode("overWrite").option("mergeSchema","True").saveAsTable(f"dhf_iot_tims_stageraw_{environment}.device_missing_vins")
  spark.sql(f"{chlg_query}").write.format("delta").mode("append").saveAsTable(f"dhf_iot_harmonized_{environment}.device_chlg")




# COMMAND ----------

#alert mail
if mail_flag=='true':
  if len(tables_list)>0 :
    source_system='TIMS_SR'
    message = f"Hi team,\n\nFew TIMS trips are not processed into harmonized, Data replay completed for missing trips  for the following tables: \n{tables_list}  \n\nSource_system: \n{source_system} \n\nfor  run date: \n{audit_check_date}    \n\nThanks,\nP&C Data Management "
    subject = f"TIMS Harmonize Audit check failure: Few trips are not processed for the date  {audit_check_date}"
    send_email(email_from, email_to, subject, message)
  else:
    print("No data to send")
else:
  print("mail_flag is false")