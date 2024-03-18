# Databricks notebook source
from datetime import date,timedelta
from pyspark.sql.functions import *

tables_list=[]
source_system=[]
days_end_count=10
days_start_count=1
mail_flag='true'
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

ims_trip_summary_missing_trips=spark.sql(f"""select  a.* from 
(select * from dhf_iot_ims_raw_prod.tripsummary where load_date>=current_date-{days_end_count} and load_date<=current_date-{days_start_count}) a left anti join
(select * from dhf_iot_harmonized_prod.trip_summary where load_dt>=current_date-{days_end_count}-6 and src_sys_cd in ("IMS_SM_5X","IMS_SR_4X","IMS_SR_5X")) b 
on   a.tripsummaryid=b.trip_smry_key and  a.sourcesystem=b.src_sys_cd where tripsummaryid is not null""")

chlg_query="select distinct cast(row_number() over(order by NULL) + 500 + coalesce((select max(TRIP_SUMMARY_ID) from dhf_iot_harmonized_prod.trip_summary),0) as BIGINT) as TRIP_SUMMARY_ID, coalesce(tripSummaryId, 'NOKEY') AS TRIP_SMRY_KEY, coalesce(db_load_time, to_timestamp('9999-12-31')) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, coalesce(deviceIdentifier,deviceSerialNumber,crc32(detectedvin)) AS DEVC_KEY, cast(accelQuality as STRING) AS ACLRTN_QLTY_FL, cast(hdopAverage as decimal(15,6)) AS AVG_HDOP_QTY, cast(avgSpeed as decimal(18,5)) AS AVG_SPD_MPH_RT, cast(drivingDistance as decimal(15,6)) AS DRVNG_DISTNC_QTY, cast(fuelConsumption as decimal(15,6)) AS FUEL_CNSMPTN_QTY, cast(secondsOfIdling as decimal(15,6)) AS IDLING_SC_QTY, cast(milStatus as STRING) AS MLFNCTN_STTS_FL, cast(maxSpeed as decimal(18,5)) AS MAX_SPD_RT, cast(system as STRING) AS MSURET_UNIT_CD, cast(enterpriseReferenceId as STRING) AS PLCY_NB, cast(timeZoneOffset as decimal(10,2)) as TIME_ZONE_OFFST_NB, cast(totalTripSeconds as decimal(15,6)) AS TRIP_SC_QTY, cast(transportMode as STRING) AS TRNSPRT_MODE_CD, cast(transportModeReason as STRING) AS TRNSPRT_MODE_RSN_CD, coalesce(cast(utcStartDateTime as TIMESTAMP), to_timestamp('9999-12-31')) AS TRIP_START_TS, coalesce(cast(utcEndDateTime as TIMESTAMP), to_timestamp('9999-12-31')) AS TRIP_END_TS, coalesce(enrolledVin,detectedVin) AS VEH_KEY, coalesce(load_date, to_date('9999-12-31')) as LOAD_DT, coalesce(sourcesystem,'NOKEY') AS SRC_SYS_CD, coalesce(load_hour, to_timestamp('9999-12-31')) as LOAD_HR_TS,null as CMNT_TT,null as TRIP_FUEL_CNSMD_IDL_QTY,null as VCHR_NB,null as DISTNC_MTRWY_QTY,null as DISTNC_URBAN_AREAS_QTY,null as DISTNC_ON_ROAD_QTY,null as DISTNC_UNKNOWN_QTY,null as TIME_MTRWY_QTY,null as TIME_URBAN_AREAS_QTY,null as TIME_ON_ROAD_QTY,null as TIME_UNKNOWN_QTY ,null as SPDNG_DISTNC_MTRWY_QTY,null as SPDNG_DISTNC_URBAN_AREAS_QTY,null as SPDNG_DISTNC_ON_ROAD_QTY,null as SPDNG_DISTNC_UNKNOWN_QTY,null as SPDNG_TIME_MTRWY_QTY,null as SPDNG_TIME_URBAN_AREAS_QTY,null as SPDNG_TIME_ON_ROAD_QTY,null as SPDNG_TIME_UNKNOWN_ROAD_QTY,null as DRIV_STTS_SRC_IN,null as DRIV_STTS_DERV_IN,null as ACCT_ID,null as TRIP_START_LAT_NB,null as TRIP_START_LNGTD_NB,null as TRIP_END_LAT_NB,null as TRIP_END_LNGTD_NB,null as NIGHT_TIME_DRVNG_SEC_CNT,null as PLCY_ST_CD,null as PRGRM_CD,null as RSKPT_ACCEL_RT,null as RSKPT_BRK_RT,null as RSKPT_TRN_RT,null as RSKPT_SPDNG,null as RSKPT_PHN_MTN,null as STAR_RTNG,null as STAR_RTNG_ACCEL,null as STAR_RTNG_BRK,null as STAR_RTN_TRN,null as STAR_RTNG_SPDNG,null as STAR_RTNG_PHN_MTN,null as DRIVER_KEY,null as TAG_MAC_ADDR_NM,null as TAG_TRIP_NB,null as SHRT_VEH_ID,null as OBD_MLG_QTY,null as GPS_MLG_QTY,null as HARD_BRKE_QTY,null as HARSH_ACCLRTN_QTY,null as OVER_SPD_QTY,null as DVC_VRSN_NB from dhf_iot_ims_stageraw_prod.trip_summary_missing_trips where  timestampdiff(hour,cast(utcStartDateTime as TIMESTAMP), cast(utcEndDateTime as TIMESTAMP) )<12  and etl_load_date=current_date"



if (ims_trip_summary_missing_trips.count()>0):
  tables_list= tables_list + ['Trip_summary']
  print("trips_missing")
  ims_trip_summary_missing_trips.withColumn("etl_load_date",current_date()).withColumn("etl_load_time",current_timestamp()).write.format("delta").mode("append").option("mergeSchema","True").saveAsTable("dhf_iot_ims_stageraw_prod.trip_summary_missing_trips")
  source_system=source_system+([row[0] for row in (spark.sql("select distinct sourcesystem from  dhf_iot_ims_stageraw_prod.trip_summary_missing_trips where db_load_date=current_date ")).collect()])
  
  spark.sql(f"{chlg_query}").write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_summary_chlg")
#   message = f"Few {source_system} trips are not processed into harmonize trip_summary table for  run date: {audit_check_date}    \n\nThanks,\nP&C Data Management "
#   subject = f"IMS Harmonize Audit check failure: Few trips are not processed for the audit run date  {audit_check_date}"
#   send_email(email_from, email_to, subject, message)


# COMMAND ----------

# trip_point audit check

ims_trip_point_missing_trips=spark.sql(f"""select  a.* from 
(select * from dhf_iot_ims_raw_prod.telemetrypoints where load_date>=current_date-{days_end_count} and load_date<=current_date-{days_start_count}) a left anti join
(select * from dhf_iot_harmonized_prod.trip_point where load_dt>=current_date-{days_end_count}-6 and src_sys_cd in ("IMS_SM_5X","IMS_SR_4X","IMS_SR_5X")) b 
on   a.tripsummaryid=b.trip_smry_key  and a.utcDateTime=b.UTC_TS and a.sourcesystem=b.src_sys_cd where utcDateTime is not null""")

chlg_query="""select distinct cast(row_number() over(order by NULL ) + 65000 + coalesce((select max(TRIP_POINT_ID)from dhf_iot_harmonized_prod.trip_point),0) as BIGINT) as TRIP_POINT_ID,coalesce(enrolledVin, detectedVin, 'NOKEY') as ENRLD_VIN_NB,coalesce(deviceIdentifier,deviceSerialNumber,crc32(detectedvin),'NOKEY') AS DEVC_KEY,coalesce(db_load_time, to_timestamp('9999-12-31')) AS ETL_ROW_EFF_DTS,current_timestamp() as ETL_LAST_UPDT_DTS,cast(acceleration as decimal(18, 5)) as ACLRTN_RT,cast(ambientTemperature as decimal(15, 6)) as AMBNT_TMPRTR_QTY,cast(barometericPressure as decimal(15, 6)) as BRMTRC_PRESSR_QTY,cast(coolantTemperature as decimal(15, 6)) as COOLNT_TMPRTR_QTY,cast(engineRPM as decimal(18, 5)) as ENGIN_RPM_RT,cast(fuelLevel as decimal(15, 6)) as FUEL_LVL_QTY,cast(headingDegrees as decimal(15, 6)) as HEADNG_DEG_QTY,coalesce(cast(utcDateTime as TIMESTAMP),to_timestamp('9999-12-31')) as UTC_TS,cast(timeZoneOffset as DECIMAL(10, 2)) as TIME_ZONE_OFFST_NB,cast(speed as decimal(18, 10)) as SPD_RT,cast(accelerometerData as string) as ACLRTMTR_DATA_RT,cast(horizontalAccuracy as decimal(15, 6)) as HRZNTL_ACCRCY_QTY,cast(degreesLatitude as decimal(18, 10)) as LAT_NB,cast(degreesLongitude as decimal(18, 10)) as LNGTD_NB,cast(throttlePosition as decimal(15, 6)) as THRTL_PSTN_NB,cast(verticalAccuracy as decimal(15, 6)) as VRTCL_ACCRCY_QTY,tps.tripSummaryId AS TRIP_SMRY_KEY,cast(hdop as decimal(18, 10)) as HDOP_QTY,cast(Accel_Longitudinal as decimal(18, 10)) as LNGTDNL_ACCLRTN_RT,cast(Accel_Lateral as decimal(18, 10)) as LATRL_ACCLRTN_RT,cast(Accel_Vertical as decimal(18, 10)) as VRTCL_ACCLRTN_RT,coalesce(load_date, to_date('9999-12-31')) as LOAD_DT,coalesce(sourcesystem, 'NOKEY') AS SRC_SYS_CD,coalesce(tps.load_hour, to_timestamp('9999-12-31')) as LOAD_HR_TS
from(select distinct * from dhf_iot_ims_stageraw_prod.trip_point_missing_trips where etl_load_date=current_date) tps inner join (select distinct tripSummaryId,load_hour,count(*) as tripLen from dhf_iot_ims_stageraw_prod.trip_point_missing_trips where etl_load_date=current_date group by tripSummaryId,load_hour having tripLen<43200) vtps on tps.tripSummaryId = vtps.tripSummaryId and tps.load_hour = vtps.load_hour"""


if (ims_trip_point_missing_trips.count()>0):
  print("trips_missing")
  tables_list= tables_list + ['Trip_point']
  ims_trip_point_missing_trips.withColumn("etl_load_date",current_date()).withColumn("etl_load_time",current_timestamp()).write.format("delta").mode("append").option("mergeSchema","True").saveAsTable("dhf_iot_ims_stageraw_prod.trip_point_missing_trips")
  source_system=source_system+([row[0] for row in (spark.sql("select distinct sourcesystem from  dhf_iot_ims_stageraw_prod.trip_point_missing_trips where etl_load_date=current_date ")).collect()])
  
  spark.sql(f"{chlg_query}").write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_point_chlg")
#   message = f"Few   {source_system}   trips are not processed into harmonize trip_point table for  run date: {audit_check_date}"
#   subject = f"IMS Harmonize Audit check failure: Few trips are not processed for the audit run date  {audit_check_date}"
#   send_email(email_from, email_to, subject, message)


# COMMAND ----------

# trip_event audit check

ims_trip_event_missing_trips=spark.sql(f"""select  a.* from 
(select * from dhf_iot_ims_raw_prod.telemetryevents where load_date>=current_date-{days_end_count} and load_date<=current_date-{days_start_count}) a left anti join
(select * from dhf_iot_harmonized_prod.trip_event where load_dt>=current_date-{days_end_count}-6 and src_sys_cd in ("IMS_SM_5X","IMS_SR_4X","IMS_SR_5X")) b 
on   a.tripsummaryid=b.trip_smry_key  and a.sourcesystem=b.src_sys_cd where tripsummaryid is not null""")

chlg_query="""select distinct cast(row_number() over(order by NULL) +5000+ coalesce((select max(TRIP_EVENT_ID) from dhf_iot_harmonized_prod.trip_event),0) as BIGINT) as TRIP_EVENT_ID, coalesce(tdr.db_load_time, to_timestamp('9999-12-31')) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, cast(tdr.acceleration as decimal(18,5)) as ACLRTN_RT, cast(tdr.secondsOfDriving as decimal(15,6)) as DRVNG_SC_QTY, cast(tdr.telemetryEventSeverityLevel as STRING) as EVNT_SVRTY_CD, case tdr.sourcesystem when ('IMS_SR_4X') THEN coalesce(b.Description, 'NOKEY') else coalesce(tdr.telemetryEventType, 'NOKEY') end EVNT_TP_CD, cast(tdr.headingDegrees as decimal(15,6)) as HEADNG_DEG_QTY, cast(tdr.speed as decimal(18,10)) as SPD_RT, tdr.tripSummaryId AS TRIP_SMRY_KEY, cast(tdr.telemetryEvents__avgSpeed as decimal(18,10)) as AVG_SPD_RT, coalesce(cast(tdr.utcDateTime as TIMESTAMP), to_timestamp('9999-12-31')) as UTC_TS, cast(tdr.Event_Reference_Value as decimal(35,15)) as EVT_RFRNC_VAL, cast(tdr.Event_Timezone_Offset as int) as EVT_TMZN_OFFST_NUM, cast(tdr.degreesLatitude as decimal(18,10)) as LAT_NB, cast(tdr.degreesLongitude as decimal(18,10)) as LNGTD_NB, coalesce(tdr.load_date, to_date('9999-12-31')) as LOAD_DT, coalesce(tdr.sourcesystem, 'NOKEY') AS SRC_SYS_CD, coalesce(tdr.load_hour, to_timestamp('9999-12-31')) as LOAD_HR_TS , null as EVNT_STRT_TS, null as EVNT_END_TS, null as EVNT_DRTN_QTY from  dhf_iot_ims_stageraw_prod.trip_event_missing_trips tdr left join dhf_iot_harmonized_prod.event_code b on tdr.sourcesystem=b.source and tdr.telemetryEventType=b.code where etl_load_date=current_date"""


if (ims_trip_event_missing_trips.count()>0):
  tables_list= tables_list + ['Trip_event']
  print("trips_missing")
  ims_trip_event_missing_trips.withColumn("etl_load_date",current_date()).withColumn("etl_load_time",current_timestamp()).write.format("delta").mode("append").option("mergeSchema","True").saveAsTable("dhf_iot_ims_stageraw_prod.trip_event_missing_trips")
  source_system=source_system+([row[0] for row in (spark.sql("select distinct sourcesystem from  dhf_iot_ims_stageraw_prod.trip_event_missing_trips where etl_load_date=current_date ")).collect()])
  spark.sql(f"{chlg_query}").write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.trip_event_chlg")
#   message = f"Few {source_system} trips are not processed into harmonize trip_event table for  run date: {audit_check_date}"
#   subject = f"IMS Harmonize Audit check failure: Few trips are not processed for the audit run date  {audit_check_date}"
#   send_email(email_from, email_to, subject, message)


# COMMAND ----------

# non_trip_event audit check

ims_non_trip_event_missing_trips=spark.sql(f"""select  a.* from 
(select * from dhf_iot_ims_raw_prod.nontripevent where load_date>=current_date-{days_end_count} and load_date<=current_date-{days_start_count}) a left anti join
(select * from dhf_iot_harmonized_prod.non_trip_event where load_dt>=current_date-{days_end_count}-6 and src_sys_cd in ("IMS_SM_5X","IMS_SR_4X","IMS_SR_5X")) b 
on   a.telemetrySetId=b.NON_TRIP_EVNT_KEY  and a.sourcesystem=b.src_sys_cd where telemetrySetId is not null""")

chlg_query="""select distinct cast(row_number() over(order by NULL) +2000+ coalesce((select max(NON_TRIP_EVENT_ID) from dhf_iot_harmonized_prod.non_trip_event),0) as BIGINT) as NON_TRIP_EVENT_ID, coalesce(tdr.telemetrySetId, 'NOKEY') as NON_TRIP_EVNT_KEY, coalesce(tdr.db_load_time, to_timestamp('9999-12-31')) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, case when coalesce(tdr.deviceIdentifier,tdr.deviceSerialNumber,crc32(tdr.detectedvin)) is null then 'NOKEY' ELSE coalesce(tdr.deviceIdentifier,tdr.deviceSerialNumber,crc32(tdr.detectedvin)) END DEVC_KEY, cast(tdr.acceleration as decimal(18,5)) as ACLRTN_RT, cast(tdr.telemetryEventSeverityLevel as STRING) as EVNT_SVRTY_CD, case tdr.sourcesystem when ('IMS_SR_4X') THEN coalesce(b.Description, 'NOKEY') else coalesce(tdr.telemetryEventType, 'NOKEY') end EVNT_TP_CD, cast(tdr.headingDegrees as decimal(15,6)) as HEADNG_DEG_QTY, cast(tdr.speed as decimal(18,5)) as SPD_MPH_RT, coalesce(cast(tdr.utcDateTime as TIMESTAMP), to_timestamp('9999-12-31')) as UTC_TS, coalesce(tdr.enrolledVin,tdr.detectedVin) AS VEH_KEY, cast(tdr.Event_Reference_Value as decimal(35,15)) as EVT_RFRNC_VAL, cast(tdr.Event_Timezone_Offset as INT) as EVT_TMZN_OFFST_NUM, cast(tdr.degreesLatitude as decimal(18,10)) as LAT_NB, cast(tdr.degreesLongitude as decimal(18,10)) as LNGTD_NB, coalesce(tdr.load_date, to_date('9999-12-31')) as LOAD_DT, coalesce(tdr.sourcesystem, 'NOKEY') AS SRC_SYS_CD, coalesce(tdr.load_hour, to_timestamp('9999-12-31')) as LOAD_HR_TS from dhf_iot_ims_stageraw_prod.non_trip_event_missing_trips tdr left join dhf_iot_harmonized_prod.event_code b on tdr.sourcesystem=b.source and tdr.telemetryEventType=b.code where etl_load_date=current_date"""


if (ims_non_trip_event_missing_trips.count()>0):
  tables_list= tables_list + ['Non_trip_event']
  print("trips_missing")
  ims_non_trip_event_missing_trips.withColumn("etl_load_date",current_date()).withColumn("etl_load_time",current_timestamp()).write.format("delta").mode("append").option("mergeSchema","True").saveAsTable("dhf_iot_ims_stageraw_prod.non_trip_event_missing_trips")
  source_system=source_system+([row[0] for row in (spark.sql("select distinct sourcesystem from  dhf_iot_ims_stageraw_prod.non_trip_event_missing_trips where etl_load_date=current_date ")).collect()])
  
  spark.sql(f"{chlg_query}").write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.non_trip_event_chlg")
#   message = f"Few   {source_system}   trips are not processed into harmonize non_trip_event table for  run date: {audit_check_date}"
#   subject = f"IMS Audit check failure: Few trips are not processed for the audit run date  {audit_check_date}"
#   send_email(email_from, email_to, subject, message)
  


# COMMAND ----------

# vehicle audit check

ims_vehicle_trip_summary_missing_trips=spark.sql(f"""select  distinct a.* from 
(select distinct enrolledVin,detectedVin,load_date,load_hour,db_load_date,db_load_time,sourcesystem from dhf_iot_ims_raw_prod.tripsummary where load_date>=current_date-{days_end_count} and load_date<=current_date-{days_start_count}) a left anti join
(select * from dhf_iot_harmonized_prod.vehicle where src_sys_cd in ("IMS_SM_5X","IMS_SR_4X","IMS_SR_5X")) b 
on   a.enrolledVin=b.enrld_vin_nb  and a.sourcesystem=b.src_sys_cd where enrolledVin is not null""")

ims_non_trip_event_missing_trips=spark.sql(f"""select  distinct a.* from 
(select distinct enrolledVin,detectedVin,load_date,load_hour,db_load_date,db_load_time,sourcesystem from dhf_iot_ims_raw_prod.nontripevent where load_date>=current_date-{days_end_count} and load_date<=current_date-{days_start_count}) a left anti join
(select * from dhf_iot_harmonized_prod.vehicle where  src_sys_cd in ("IMS_SM_5X","IMS_SR_4X","IMS_SR_5X")) b 
on   a.enrolledVin=b.enrld_vin_nb  and a.sourcesystem=b.src_sys_cd where enrolledVin is not null""")

ims_vehicle_missing_vins=ims_vehicle_trip_summary_missing_trips.union(ims_non_trip_event_missing_trips).distinct()

chlg_query="""select distinct cast(row_number() over(order by NULL) +700+ coalesce((select max(VEHICLE_ID) from dhf_iot_harmonized_prod.vehicle),0) as BIGINT) as VEHICLE_ID, COALESCE(enrolledVin,detectedVin,'NOKEY') AS VEH_KEY, coalesce(db_load_time, to_timestamp('9999-12-31')) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, COALESCE(detectedVin,enrolledVin) as DTCTD_VIN_NB, COALESCE(enrolledVin,detectedVin,'NOKEY') as ENRLD_VIN_NB, coalesce(load_date, to_date('9999-12-31')) as LOAD_DT, coalesce(SourceSystem,'NOKEY') AS SRC_SYS_CD, coalesce(load_hour, to_timestamp('9999-12-31')) as LOAD_HR_TS from dhf_iot_ims_stageraw_prod.vehicle_missing_vins where etl_load_date=current_date"""


if (ims_vehicle_missing_vins.count()>0):
  tables_list= tables_list + ['Vehicle']
  print("trips_missing")
  ims_vehicle_missing_vins.withColumn("etl_load_date",current_date()).withColumn("etl_load_time",current_timestamp()).write.format("delta").mode("append").saveAsTable("dhf_iot_ims_stageraw_prod.vehicle_missing_vins")
  source_system=source_system+([row[0] for row in (spark.sql("select distinct sourcesystem from  dhf_iot_ims_stageraw_prod.vehicle_missing_vins where etl_load_date=current_date ")).collect()])
  spark.sql(f"{chlg_query}").write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.vehicle_chlg")
#   message = f"Few {source_system} trips are not processed into harmonize vehicle table for  run date: {audit_check_date}"
#   subject = f"IMS Audit check failure: Few trips are not processed for the date  {audit_check_date}"
#   send_email(email_from, email_to, subject, message)
  

# COMMAND ----------

# device audit check

ims_device_trip_summary_missing_trips=spark.sql(f"""select  distinct a.* from 
(select distinct deviceSerialNumber,deviceIdentifier,load_date,load_hour,db_load_date,db_load_time,sourcesystem,detectedvin,deviceIdentifierType,deviceType from dhf_iot_ims_raw_prod.tripsummary where load_date>=current_date-{days_end_count} and load_date<=current_date-{days_start_count}) a left anti join
(select * from dhf_iot_harmonized_prod.device where   src_sys_cd in ("IMS_SM_5X","IMS_SR_4X","IMS_SR_5X")) b 
on   a.deviceSerialNumber=b.DEVC_SRL_NB  and a.sourcesystem=b.src_sys_cd where deviceSerialNumber is not null""")

ims_device_non_trip_event_missing_trips=spark.sql(f"""select  distinct a.* from 
(select distinct deviceSerialNumber,deviceIdentifier,load_date,load_hour,db_load_date,db_load_time,sourcesystem,detectedvin,deviceIdentifierType,deviceType from dhf_iot_ims_raw_prod.nontripevent where load_date>=current_date-{days_end_count} and load_date<=current_date-{days_start_count}) a left anti join
(select * from dhf_iot_harmonized_prod.device where src_sys_cd in ("IMS_SM_5X","IMS_SR_4X","IMS_SR_5X")) b 
on   a.deviceSerialNumber=b.DEVC_SRL_NB  and a.sourcesystem=b.src_sys_cd where deviceSerialNumber is not null""")

ims_device_missing_vins=ims_device_trip_summary_missing_trips.union(ims_device_non_trip_event_missing_trips).distinct()

chlg_query="""select distinct cast(row_number() over(order by NULL) +1000+ coalesce((select max(DEVICE_ID) from dhf_iot_harmonized_prod.device),0) as BIGINT) as DEVICE_ID, coalesce(deviceIdentifier,deviceSerialNumber,crc32(detectedvin),'NOKEY') AS DEVC_KEY, coalesce(db_load_time, to_timestamp('9999-12-31')) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, deviceIdentifier as DEVC_ID_NB, deviceIdentifierType as DEVC_ID_TP_CD, deviceType as DEVC_TP_CD, deviceSerialNumber as DEVC_SRL_NB, coalesce(load_date, to_date('9999-12-31')) as LOAD_DT, coalesce(SourceSystem, 'NOKEY') AS SRC_SYS_CD, coalesce(load_hour, to_timestamp('9999-12-31')) as LOAD_HR_TS from dhf_iot_ims_stageraw_prod.device_missing_vins where coalesce(deviceIdentifier,deviceSerialNumber,crc32(detectedvin)) is not null and etl_load_date=current_date """

if (ims_device_missing_vins.count()>0):
  tables_list= tables_list + ['Device']
  print("trips_missing")
  ims_device_missing_vins.withColumn("etl_load_date",current_date()).withColumn("etl_load_time",current_timestamp()).write.format("delta").mode("overWrite").option("mergeSchema","True").saveAsTable("dhf_iot_ims_stageraw_prod.device_missing_vins")
  source_system=source_system+([row[0] for row in (spark.sql("select distinct sourcesystem from  dhf_iot_ims_stageraw_prod.device_missing_vins where etl_load_date=current_date ")).collect()])
  spark.sql(f"{chlg_query}").write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.device_chlg")
#   message = f"Few {source_system} trips are not processed into harmonize device table for  run date: {audit_check_date}"
#   subject = f"IMS Audit check failure: Few trips are not processed for the date  {audit_check_date}"
#   send_email(email_from, email_to, subject, message)
  



# COMMAND ----------

# scoring audit check

ims_scoring_missing_trips=spark.sql(f"""select  a.* from 
(select * from dhf_iot_ims_raw_prod.scoring where load_date>=current_date-{days_end_count} and load_date<=current_date-{days_start_count}) a left anti join
(select * from dhf_iot_harmonized_prod.scoring where load_dt>=current_date-{days_end_count}-6 and src_sys_cd in ("IMS_SM_5X","IMS_SR_5X")) b 
on   a.tripsummaryid=b.trip_smry_key  and a.sourcesystem=b.src_sys_cd where tripsummaryid is not null""")

chlg_query="""select distinct cast(row_number() over(order by NULL) +3000+ coalesce((select max(SCORING_ID) from dhf_iot_harmonized_prod.SCORING),0) as BIGINT) as SCORING_ID, coalesce(db_load_time, to_timestamp('9999-12-31')) AS ETL_ROW_EFF_DTS, tripSummaryId AS TRIP_SMRY_KEY, current_timestamp() as ETL_LAST_UPDT_DTS, scoreAlgorithmProvider as SCR_ALGRM_PRVDR_NM, scoreUnit as SCR_UNIT_CD, cast(overallScore as decimal(18,10))as OVRL_SCR_QTY, coalesce(component, 'NOKEY') as INDV_CMPNT_SET_TP_VAL, cast(coalesce(componentScore ,0) as decimal(18,10)) as INDV_CMPNT_SET_SCR_QTY, coalesce(load_date, to_date('9999-12-31')) as LOAD_DT, coalesce(SourceSystem, 'NOKEY') as SRC_SYS_CD, coalesce(load_hour, to_timestamp('9999-12-31')) as LOAD_HR_TS from dhf_iot_ims_stageraw_prod.scoring_missing_trips where etl_load_date=current_date"""


if (ims_scoring_missing_trips.count()>0):
  tables_list= tables_list + ['Scoring']
  print("trips_missing")
  ims_scoring_missing_trips.withColumn("etl_load_date",current_date()).withColumn("etl_load_time",current_timestamp()).write.format("delta").mode("append").option("mergeSchema","True").saveAsTable("dhf_iot_ims_stageraw_prod.scoring_missing_trips")
  source_system=source_system+([row[0] for row in (spark.sql("select distinct sourcesystem from  dhf_iot_ims_stageraw_prod.scoring_missing_trips where etl_load_date=current_date ")).collect()])
  
  spark.sql(f"{chlg_query}").write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.scoring_chlg")
#   message = f"Few {source_system} trips are not processed into harmonize scoring table for  run date: {audit_check_date}"
#   subject = f"IMS Audit check failure: Few trips are not processed for the date  {audit_check_date}"
#   send_email(email_from, email_to, subject, message)



# COMMAND ----------

# histogram_scoring audit check

ims_histogramscoringintervals_missing_trips=spark.sql(f"""select  a.* from 
(select * from dhf_iot_ims_raw_prod.histogramscoringintervals where load_date>=current_date-{days_end_count} and load_date<=current_date-{days_start_count}) a left anti join
(select * from dhf_iot_harmonized_prod.histogram_scoring_interval where load_dt>=current_date-{days_end_count}-6 and src_sys_cd in ("IMS_SM_5X","IMS_SR_5X")) b 
on   a.tripsummaryid=b.trip_smry_key  and a.sourcesystem=b.src_sys_cd where tripsummaryid is not null""")

chlg_query="""select distinct cast(row_number() over(order by NULL) +3000+ coalesce((select max(HISTOGRAM_SCORING_INTERVAL_ID) from dhf_iot_harmonized_prod.HISTOGRAM_SCORING_INTERVAL),0) as BIGINT) as HISTOGRAM_SCORING_INTERVAL_ID, coalesce(db_load_time, to_timestamp('9999-12-31')) AS ETL_ROW_EFF_DTS, current_timestamp() as ETL_LAST_UPDT_DTS, tripSummaryId AS TRIP_SMRY_KEY, coalesce(scoringComponent, 'NOKEY') as SCRG_CMPNT_TP_CD, scoringSubComponent as SCRG_SUB_CMPNT_TP_CD, roadType as ROAD_TP_DSC, cast(thresholdLowerBound as decimal(18,10)) as LOWER_BND_THRSHLD_QTY, cast(thresholdUpperBound as decimal(18,10)) as UPPER_BND_THRSHLD_QTY, scoringComponentUnit as SCRG_CMPNT_UNIT_CD, cast(occurrences as int) as OCRNC_CT, occurrenceUnit as OCRNC_UNIT_CD, coalesce(load_date, to_date('9999-12-31')) as LOAD_DT, coalesce(SourceSystem, 'NOKEY') as SRC_SYS_CD, coalesce(load_hour, to_timestamp('9999-12-31')) as LOAD_HR_TS from dhf_iot_ims_stageraw_prod.histogramscoringintervals_missing_trips where etl_load_date=current_date """


if (ims_histogramscoringintervals_missing_trips.count()>0):
  tables_list= tables_list + ['Histogram_scoring_interval']
  print("trips_missing")
  ims_histogramscoringintervals_missing_trips.withColumn("etl_load_date",current_date()).withColumn("etl_load_time",current_timestamp()).write.format("delta").mode("append").option("mergeSchema","True").saveAsTable("dhf_iot_ims_stageraw_prod.histogramscoringintervals_missing_trips")
  source_system=source_system+([row[0] for row in (spark.sql("select distinct sourcesystem from  dhf_iot_ims_stageraw_prod.histogramscoringintervals_missing_trips where etl_load_date=current_date ")).collect()])
  
  spark.sql(f"{chlg_query}").write.format("delta").mode("append").saveAsTable("dhf_iot_harmonized_prod.histogram_scoring_interval_chlg")
#   message = f"Few {source_system} trips are not processed into harmonize histogramscoringintervals table for  run date: {audit_check_date}"
#   subject = f"IMS Audit check failure: Few trips are not processed for the date  {audit_check_date}"
#   send_email(email_from, email_to, subject, message)
  

# COMMAND ----------

#alert mail
if mail_flag=='true':
  if (len(tables_list)>0) & (len(source_system)>0):
    message = f"Few trips are not processed into harmonized for the follwoing tables: \n{tabels_list}  \n\nsource_system: \n{a} \n\nfor  run date: \n{audit_check_date}    \n\nThanks,\nP&C Data Management "
    subject = f"IMS Audit check failure: Few trips are not processed for the date  {audit_check_date}"
    send_email(email_from, email_to, subject, message)
  else:
    print("No data to send")
else:
  print("mail_flag is false")