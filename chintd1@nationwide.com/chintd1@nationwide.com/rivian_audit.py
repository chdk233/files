# Databricks notebook source
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import current_date
import datetime
import boto3

#deltadatabase   = str(dbutils.widgets.get("deltadatabase"))
#SNSArn = str(dbutils.widgets.get("SNSArn"))
SNSArn="arn:aws:sns:us-east-1:786994105833:dw-cmt-pl-iot-telematics-emailgroup"
current_utcdate = str(datetime.datetime.utcnow())[:10]
path = f"s3://pcds-internal-iot-rivian-telematics-786994105833/rivian_raw/daily/load_date={current_utcdate}/"

#deltadatabase='dhf_iot_rivian_raw_dev'

# COMMAND ----------

def send_mail(sns_arn,subject,message):
  sts_client = boto3.client("sts")
  account_number = sts_client.get_caller_identity()["Account"]
  #print(account_number)
  sns_client = boto3.Session(region_name='us-east-1').client("sns")
  topic_arn=sns_arn
  sns_client.publish(
             TopicArn = topic_arn,
             Message = message,
             Subject = subject)

# COMMAND ----------

def s3_file_availability_check(path):
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create("s3://" + path.split("/")[2]),
        sc._jsc.hadoopConfiguration(),
    )
    sns_arn = SNSArn
    if fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))==False:
      message = f"No files available for current run date: {current_utcdate}"
      subject = f"Rivian Daily File Audit failure: No files available for current run date - {current_utcdate}"
      print(message)
      print(subject)
      send_mail(sns_arn,subject,message)
    else:
      filelist=dbutils.fs.ls(path)
      if len(filelist)<4:
        message = f"{4-len(filelist)} Rivian files not available for current run date: {current_utcdate}"
        subject = f"Rivian Daily File Audit failure: {4-len(filelist)} Daily files not available for current run date - {current_utcdate}"
        print(message)
        print(subject)
        send_mail(sns_arn,subject,message)
      

#path_exists("s3://pcds-internal-iot-rivian-telematics-786994105833/rivian_raw/daily/load_date=20211123/")

# COMMAND ----------


def run_file_availability_check(deltadatabase):
  
  file_missing_previous, file_missing_current = [], []
  
  av_aggregate_previous_day_file_count_chk = spark.sql(f"select case when count(*)>0 then '' else concat('av_aggregate data missing for the date: ',date_sub(current_date(),1)) end as previous_count_av_aggregate from {deltadatabase}.av_aggregate where load_date=cast(date_sub(current_date(),1) as string)").collect()[0]["previous_count_av_aggregate"]
  av_aggregate_current_day_file_count_chk = spark.sql(f"select case when count(*)>0 then '' else concat('av_aggregate data missing for the date: ',current_date()) end as current_count_av_aggregate from {deltadatabase}.av_aggregate where load_date=cast(current_date() as string)").collect()[0]["current_count_av_aggregate"]
  
  if av_aggregate_previous_day_file_count_chk:
    file_missing_previous.append(av_aggregate_previous_day_file_count_chk)
  
  if av_aggregate_current_day_file_count_chk:
    file_missing_current.append(av_aggregate_current_day_file_count_chk)

  trips_previous_day_file_count_chk = spark.sql(f"select case when count(*)>0 then '' else concat('trips data missing for the date: ',date_sub(current_date(),1)) end as previous_count_trips from {deltadatabase}.trips where load_date=cast(date_sub(current_date(),1) as string)").collect()[0]["previous_count_trips"]
  trips_current_day_file_count_chk = spark.sql(f"select case when count(*)>0 then '' else concat('trips data missing for the date: ',current_date()) end as current_count_trips from {deltadatabase}.trips where load_date=cast(current_date() as string)").collect()[0]["current_count_trips"]
  
  if trips_previous_day_file_count_chk:
    file_missing_previous.append(trips_previous_day_file_count_chk)
  
  if trips_current_day_file_count_chk:
    file_missing_current.append(trips_current_day_file_count_chk)
    
  trips_summary_previous_day_file_count_chk = spark.sql(f"select case when count(*)>0 then '' else concat('trips_summary data missing for the date: ',date_sub(current_date(),1)) end as previous_count_trips_summary from {deltadatabase}.trips_summary where load_date=cast(date_sub(current_date(),1) as string)").collect()[0]["previous_count_trips_summary"]
  trips_summary_current_day_file_count_chk = spark.sql(f"select case when count(*)>0 then '' else concat('trips_summary data missing for the date: ',current_date()) end as current_count_trips_summary from {deltadatabase}.trips_summary where load_date=cast(current_date() as string)").collect()[0]["current_count_trips_summary"]
  
  if trips_summary_previous_day_file_count_chk:
    file_missing_previous.append(trips_summary_previous_day_file_count_chk)
  
  if trips_summary_current_day_file_count_chk:
    file_missing_current.append(trips_summary_current_day_file_count_chk)
    
  event_summary_previous_day_file_count_chk = spark.sql(f"select case when count(*)>0 then '' else concat('event_summary data missing for the date: ',date_sub(current_date(),1)) end as previous_count_event_summary from {deltadatabase}.event_summary where load_date=cast(date_sub(current_date(),1) as string)").collect()[0]["previous_count_event_summary"]
  event_summary_current_day_file_count_chk = spark.sql(f"select case when count(*)>0 then '' else concat('event_summary data missing for the date: ',current_date()) end as current_count_event_summary from {deltadatabase}.event_summary where load_date=cast(current_date() as string)").collect()[0]["current_count_event_summary"]
  
  if event_summary_previous_day_file_count_chk:
    file_missing_previous.append(event_summary_previous_day_file_count_chk)
  
  if event_summary_current_day_file_count_chk:
    file_missing_current.append(event_summary_current_day_file_count_chk)
    
  sns_arn = SNSArn
  previous_list_length = len(file_missing_previous)
  current_list_length = len(file_missing_current)
  
  print("Missing files for previous date: ",file_missing_previous)
  print("Missing files for current date: ",file_missing_current)
  
  if(previous_list_length > 0):
    previous_date = file_missing_previous[0].split(": ")[1]
    if(len(file_missing_previous) == 4):
      message = f"No files available for previous run date: {previous_date}"
      subject = f"Rivian Daily File Audit failure: No files loaded for previous run date - {previous_date}"
      print(message)
      print(subject)
      send_mail(sns_arn,subject,message)
    else:
      message = "Following Daily files are unavailable: \n\n"+".\n".join(file_missing_previous)
      subject = f"Rivian Daily File Audit failure: Daily files not loaded for previous date - {previous_date}"
      print(message)
      print(subject)
      send_mail(sns_arn,subject,message)
  
  if(current_list_length > 0):
    current_date = file_missing_current[0].split(": ")[1]  
    if(len(file_missing_current) == 4):
      message = f"No files loaded for current run date: {current_date}"
      subject = f"Rivian Daily File Audit failure: No files loaded for current run date - {current_date}"
      print(message)
      print(subject)
      send_mail(sns_arn,subject,message)
    else:
      message = "Following Daily files are unavailable: \n\n"+".\n".join(file_missing_current)
      subject = f"Rivian Daily File Audit failure: Daily files not loaded for current run date - {current_date}"
      print(message)
      print(subject)
      send_mail(sns_arn,subject,message) 
      
#run_file_availability_check("dhf_iot_rivian_raw_dev")


# COMMAND ----------

def data_availability_check(deltadatabase):
  
  trip_summary_df=spark.read.table(f"{deltadatabase}.trips_summary").where("load_date=current_date()")
  trips_df=spark.read.table(f"{deltadatabase}.trips").where("load_date=current_date()")

  trips_error_df=trips_df.join(trip_summary_df,trips_df.trip_id==trip_summary_df.trip_id,"leftanti").select("trip_id").distinct()
  tripsSummary_error_df=trip_summary_df.join(trips_df,trips_df.trip_id==trip_summary_df.trip_id,"leftanti").select("trip_id").distinct()

  trips_error= [row.trip_id for row in trips_error_df.collect()]

  tripsSummary_error= [row.trip_id for row in tripsSummary_error_df.collect()]
  sns_arn = SNSArn
  if (len(tripsSummary_error)>0):
    message = f"Following trip_id's are not there in trips file {tripsSummary_error}"
    subject = f"Rivian Daily File Audit failure: data check failure for the date - "
    print(message)
    print(subject)
    send_mail(sns_arn,subject,message)
  if (len(trips_error)>0):
    message = f"Following trip_id's are not there in trips_summary file {trips_error}"
    subject = f"Rivian Daily File Audit failure: data check failure for the date - "
    print(message)
    print(subject)
    send_mail(sns_arn,subject,message)
  



#data_availability_check("dhf_iot_rivian_raw_dev")


# COMMAND ----------

s3_file_availability_check(path)
run_file_availability_check("dhf_iot_rivian_raw_dev")
data_availability_check("dhf_iot_rivian_raw_dev")
