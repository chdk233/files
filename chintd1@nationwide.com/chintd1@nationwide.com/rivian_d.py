# Databricks notebook source
#table=str(dbutils.widgets.get("table"))
def run_file_availability_check(database,table_name):

  file_missing_previous, file_missing_current = [], []
  
  rivian_previous_day_file_count_chk = spark.sql(f"select case when count(*)>0 then '' else concat(' {table_name} data missing for the date: ',date_sub(current_date(),1)) end as previous_count_rivian from {database}.{table_name} where load_date=cast(date_sub(current_date(),1) as string)").collect()[0]["previous_count_rivian"]
  rivian_current_day_file_count_chk = spark.sql(f"select case when count(*)>0 then '' else concat(' {table_name} data missing for the date: ',current_date()) end as current_count_rivian from {database}.{table_name} where load_date=cast(current_date() as string)").collect()[0]["current_count_rivian"]
  
  if rivian_previous_day_file_count_chk:
    file_missing_previous.append(rivian_previous_day_file_count_chk)
  
  if rivian_current_day_file_count_chk:
    file_missing_current.append(rivian_current_day_file_count_chk)
  
  
  #sns_arn = sns_topic_arn
  previous_list_length = len(file_missing_previous)
  current_list_length = len(file_missing_current)
  
  print("Missing files for previous date: ",file_missing_previous)
  print("Missing files for current date: ",file_missing_current)
  
  print("Missing files for previous date: ",file_missing_previous)
  print("Missing files for current date: ",file_missing_current)
  
  if(previous_list_length > 0):
    previous_date = file_missing_previous[0].split(": ")[1]
    message = "Following Daily files are unavailable: \n\n"+".\n".join(file_missing_previous)
    subject = f"Rivian Daily File Audit failure: Daily files not available for previous date - {previous_date}"
    #send_mail(sns_arn,subject,message)
    print(message)
    print(subject)
  
  if(current_list_length > 0):
    current_date = file_missing_current[0].split(": ")[1] 
    message = "Following Daily files are unavailable: \n\n"+".\n".join(file_missing_current)
    subject = f"Rivian Daily File Audit failure: Daily files not available for current run date - {current_date}"
    #send_mail(sns_arn,subject,message) 
  
run_file_availability_check("dhf_iot_rivian_raw_dev","trips_summary")
  

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

distance_ch=[]

#trip_summary_df = spark.read.option("header", True).option("inferSchema", True).csv("s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Krishna/Rivian/new/rivian_trips_summary_20211123.00.csv")


trip_summary_df=spark.read.table("dhf_iot_rivian_raw_dev.trips_summary").drop("db_load_time").distinct()
agg_df=spark.read.table("dhf_iot_rivian_raw_dev.av_aggregate").drop("db_load_time").distinct()


tripDF=trip_summary_df.groupBy("vin","load_date").agg(F.round(sum("distance"),3).alias("sum_distance"),F.round(sum("duration"),3).alias("sum_duration"),F.round(sum("av_distance"),3).alias("av_sum_distance"),F.round(sum("av_duration"),3).alias("sum_av_duration"))

#agg_df= spark.read.option("header", True).option("inferSchema", True).csv("s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Krishna/Rivian/new/rivian_av_aggregate_20211123.00.csv")

trip_duration_auditDF=trip_summary_df.select(col("trip_start").cast("timeStamp"),col("trip_end").cast("timeStamp"),col("duration"),col("distance")).withColumn(
    "date_diff_min",  F.round((F.col("trip_end").cast("long") - F.col("trip_start").cast("long"))/60.,3)).where(col("date_diff_min")!=col("duration")).drop("date_diff_min")
#display(df5)

joinDF=agg_df.alias("agg").join(tripDF.alias("trip"),agg_df.vin==trip_summary_df.vin,"inner").where(col("agg.load_date")==col("trip.load_date")).select(col("agg.vin"),col("agg.total_distance"),col("agg.total_duration"),col("agg.av_total_distance"),col("agg.av_total_duration"),col("agg.av_score"),col("trip.sum_duration"),col("trip.sum_distance"),col("trip.sum_av_duration"),col("trip.av_sum_distance")).withColumn("av_score_check",F.round(col("agg.av_total_distance")/col("agg.total_distance"),2))


vin_distance_check_Failed= [row.vin for row in joinDF.select("vin","total_distance","sum_distance").where(col("total_distance")!=col("sum_distance")).drop("total_distance","sum_distance").distinct().collect()]

vin_duration_check_failed= [row.vin for row in joinDF.select("vin","total_duration","sum_duration").where(col("total_duration")!=col("sum_duration")).drop("total_duration","sum_duration").distinct().collect()]

vin_av_distance_check_failed= [row.vin for row in joinDF.select("vin","av_total_distance","av_sum_distance").where(col("av_total_distance")!=col("av_sum_distance")).drop("av_total_distance","av_sum_distance").distinct().collect()]

vin_av_duration_check_failed= [row.vin for row in joinDF.select("vin","total_duration","sum_av_duration").where(col("total_duration")!=col("sum_av_duration")).drop("total_duration","sum_av_duration").distinct().collect()]

vin_av_score_check_failed= [row.vin for row in joinDF.select("vin","av_score","av_score_check").where(col("av_score")!=col("av_score_check")).drop("av_score","av_score_check").distinct().collect()]





print(vin_distance_check_Failed )
#display(distance_check)




#df5=df4.where(("trip_start"-"trip_end")=="duration")

# COMMAND ----------

agg=[row.total_distance for row in agg_df.distinct().sort(col("vin").desc()).collect()]
trip=[row.sum_distance for row in tripDF.distinct().sort(col("vin").desc()).collect()]

print(agg)
print(trip)

# COMMAND ----------

#display(joinDF.where(col("av_total_distance")!=col("av_sum_distance")))
display(trip_duration_auditDF)

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import current_date

#deltadatabase   = str(dbutils.widgets.get("deltadatabase"))

deltadatabase='dhf_iot_rivian_raw_dev'

trip_summary_df=spark.read.table(f"{deltadatabase}.trips_summary").where("load_date=current_date()")
trips_df=spark.read.table(f"{deltadatabase}.trips").where("load_date=current_date()")

trips_error_df=trips_df.join(trip_summary_df,trips_df.trip_id==trip_summary_df.trip_id,"leftanti").select("trip_id").distinct()
tripsSummary_error_df=trip_summary_df.join(trips_df,trips_df.trip_id==trip_summary_df.trip_id,"leftanti").select("trip_id").distinct()

trips_error= [row.trip_id for row in trips_error_df.collect()]

tripsSummary_error= [row.trip_id for row in tripsSummary_error_df.collect()]

if (len(tripsSummary_error)>0):
    message = f"Following trip_id's are not there in trips file {tripsSummary_error}"
    subject = f"Rivian Daily File Audit failure: data check failure for the date - "
    #send_mail(sns_arn,subject,message)
    print(message)
    print(subject)
if (len(trips_error)>0):
    message = f"Following trip_id's are not there in trips_summary file {trips_error}"
    subject = f"Rivian Daily File Audit failure: data check failure for the date - "
    #send_mail(sns_arn,subject,message)
    print(message)
    print(subject)
  






# COMMAND ----------

import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.functions import current_date

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
    
  #sns_arn = sns_topic_arn
  previous_list_length = len(file_missing_previous)
  current_list_length = len(file_missing_current)
  
  print("Missing files for previous date: ",file_missing_previous)
  print("Missing files for current date: ",file_missing_current)
  
  if(previous_list_length > 0):
    previous_date = file_missing_previous[0].split(": ")[1]
    if(len(file_missing_previous) == 4):
      message = f"No files available for previous run date: {previous_date}"
      subject = f"Rivian Daily File Audit failure: No files available for previous run date - {previous_date}"
      print(message)
      print(subject)
      #send_mail(sns_arn,subject,message)
    else:
      message = "Following Daily files are unavailable: \n\n"+".\n".join(file_missing_previous)
      subject = f"Rivian Daily File Audit failure: Daily files not available for previous date - {previous_date}"
      print(message)
      print(subject)
      #send_mail(sns_arn,subject,message)
  
  if(current_list_length > 0):
    current_date = file_missing_current[0].split(": ")[1]  
    if(len(file_missing_current) == 4):
      message = f"No files available for current run date: {current_date}"
      subject = f"Rivian Daily File Audit failure: No files available for current run date - {current_date}"
      print(message)
      print(subject)
      #send_mail(sns_arn,subject,message)
    else:
      message = "Following Daily files are unavailable: \n\n"+".\n".join(file_missing_current)
      subject = f"Rivian Daily File Audit failure: Daily files not available for current run date - {current_date}"
      print(message)
      print(subject)
      #send_mail(sns_arn,subject,message) 
      
run_file_availability_check("dhf_iot_rivian_raw_dev")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_rivian_raw_dev.trips_summary where load_date='2021-12-05'

# COMMAND ----------

import datetime
current_utcdate = str(datetime.datetime.utcnow())[:10]
def path_exists(path):
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create("s3://" + path.split("/")[2]),
        sc._jsc.hadoopConfiguration(),
    )
    return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))
  
path = f"s3://pcds-internal-iot-rivian-telematics-786994105833/rivian_raw/daily/load_date={current_utcdate}/"
pathexists = path_exists(path)
if pathexists==False:
  print("no file")
else:
  filelist=dbutils.fs.ls(path)
  if len(filelist)<4:
    print("some files mssing")



# COMMAND ----------

def s3_file_availability_check(path):
    sc = spark.sparkContext
    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(
        sc._jvm.java.net.URI.create("s3://" + path.split("/")[2]),
        sc._jsc.hadoopConfiguration(),
    )
    if fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))==False:
      message = f"No files available for current run date: {current_date}"
      subject = f"Rivian Daily File Audit failure: No files available for current run date - {current_utcdate}"
      print(message)
      print(subject)
      #send_mail(sns_arn,subject,message)
    else:
      filelist=dbutils.fs.ls(path)
      if len(filelist)<4:
        message = f"{4-len(filelist)} Rivian files not available for current run date: {current_utcdate}"
      subject = f"Rivian Daily File Audit failure: {4-len(filelist)} Daily files not available for current run date - {current_utcdate}"
      print(message)
      print(subject)
      #send_mail(sns_arn,subject,message)
      

#path_exists("s3://pcds-internal-iot-rivian-telematics-786994105833/rivian_raw/daily/load_date=20211123/")

# COMMAND ----------

trip_summary_df=spark.read.table("dhf_iot_rivian_raw_dev.trips_summary").drop("db_load_time")
tripDF=trip_summary_df.groupBy("vin","load_date").agg(F.round(sum("distance"),3).alias("sum_distance"),F.round(sum("duration"),3).alias("sum_duration"),F.round(sum("av_distance"),3).alias("av_sum_distance"),F.round(sum("av_duration"),3).alias("sum_av_duration"))
display%s(tripDF)


# COMMAND ----------

# MAGIC %scala
# MAGIC val df=spark.read.option("header","true").option("inferSchema","true").csv("s3://pcds-internal-iot-rivian-telematics-786994105833/rivian-sourcefiles/daily/load_date=2021-11-23/rivian_trips_summary_20211123.00.csv")
# MAGIC df.printSchema
# MAGIC display(df)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_generic_autoloader_prod.job_config where job_cd in (2001)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from dhf_iot_rivian_raw_prod.av_aggregate ;
# MAGIC -- select * from dhf_iot_rivian_raw_prod.trips ;
# MAGIC -- select *  from dhf_iot_rivian_raw_prod.trips_summary ;
# MAGIC -- select *  from dhf_iot_rivian_raw_prod.event_summary ;
# MAGIC -- select *  from dhf_iot_rivian_raw_prod.trips_odometer ;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in dhf_iot_cmtpl_raw_prod

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct load_date from dhf_iot_cmtpl_raw_prod.trip_detail_realtime order by load_date 
# MAGIC -- select count(*) from dhf_iot_cmtpl_raw_prod.trip_detail_realtime
# MAGIC
# MAGIC
# MAGIC -- 259,826,553,039
# MAGIC -- 2,125,628,507,914
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_harmonized_test.trip_driver_score where TRIP_INSTC_ID='81C309FB-7602-4F80-A8BE-4B83D77C6BBA-1-379' and ANLYTC_TP_CD='driver' and MODL_ACRNYM_TT='NM2'