# Databricks notebook source
# MAGIC %scala
# MAGIC
# MAGIC import spark.implicits._
# MAGIC import org.apache.spark.sql._
# MAGIC import org.apache.spark.sql.functions._
# MAGIC import org.apache.spark.sql.streaming.Trigger
# MAGIC import org.apache.spark.sql.types._
# MAGIC import java.time.LocalDateTime
# MAGIC import scala.collection.mutable.ListBuffer
# MAGIC import scala.util.{Try, Success, Failure} 
# MAGIC import org.apache.spark.sql.streaming._
# MAGIC import org.apache.spark.sql.streaming.StreamingQueryListener._
# MAGIC import scala.util.parsing.json._
# MAGIC  val path1 = Seq("dbfs:/mnt/dw-telematics-prod-read/warehouse/telematics_db/smartride_tripdetail_seconds/source_cd=IMS/batch=201610221700/000247_0","dbfs:/mnt/dw-telematics-prod-read/warehouse/telematics_db/smartride_tripdetail_seconds/source_cd=IMS/batch=201610221700/000247_0")
# MAGIC  val df2 = spark.sparkContext.parallelize(path1).toDF("path1").withColumn("batch",split(split(col("path1"),"/",9)(7),"=",0)(1))
# MAGIC // df2.withcolumn("add",path1.split('.').rsplit('_',1)[1]).show
# MAGIC // df2.select(split($"name","/").as("nameAsArray") ).show
# MAGIC // df2.select("value").path1
# MAGIC //                          .withColumn("source_path_split",split(col("value"),"/",7)(6))
# MAGIC // .withColumn("source_path_s",split(col("value"),"/",4)(3))
# MAGIC //                          .withColumn("target_path",concat(lit("iot/raw/smartride/cmt-pl/realtime/tar/"),col("source_path_split"))).drop("source_path_split","value").show(false)
# MAGIC     display(df2)

# COMMAND ----------

# MAGIC %scala
# MAGIC val df=spark.read.format("csv").option("inferSchema","true").option("header","true").load("s3://pcds-internal-iot-rivian-telematics-786994105833/rivian_raw/daily/load_date=20210817/")
# MAGIC df.show()

# COMMAND ----------

from pyspark.sql.types import StringType

import io
import zipfile
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
import json
import boto3
import time
from pyspark.sql import Row

df3 =spark.createDataFrame(["s3://dw-internal-telematics-786994105833/smartmiles-ims-sourcefiles/zip/load_date=2022-01-16/0500/000c04ac-f8f6-49d7-a2c5-8285eb572afc.zip"], StringType()).toDF("path")
df4=df3.withColumn("load_date",to_date(split(split(col("path"),"/")[5],"=")[1])).withColumn("load_hour",split(col("path"),"/")[6])
display(df4)
#df3.select("path").withColumn("source_path_s",split(col("path"),"/")([3]).show()
 #.withColumn("target_path",concat(lit("iot/raw/smartride/"),col("source_path_split"))

# COMMAND ----------


import io
import zipfile
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
import json
import boto3
import time
from pyspark.sql import Row

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

import boto3

sts = boto3.client('sts')
sts.get_caller_identity()
sourec="CMT/Bala/realtime_testing/load_date=2021-07-01/0800/DS_CMT_7591c60e-effa-46a2-bcf2-ee7246aa4ed0/85F8EF36-5176-4A35-B34A-207E253179B7.tar.gz"
target="iot/kiran/testboto3/85F8EF36-5176-4A35-B34A-207E253179B7.tar.gz"

response = sts.assume_role(RoleArn='arn:aws:iam::786994105833:role/dw-pl-cmt-TelematicS3FileMove-role',RoleSessionName='mycmtsession')

s3 = boto3.resource('s3',
                          aws_access_key_id=response.get('Credentials').get('AccessKeyId'),
                          aws_secret_access_key=response.get('Credentials').get('SecretAccessKey'),
                          aws_session_token=response.get('Credentials').get('SessionToken'))
copy_source = {
    'Bucket': 'dw-internal-pl-cmt-telematics-786994105833',
    'Key': sourec
}

response2 = sts.assume_role(RoleArn='arn:aws:iam::786994105833:role/pcds-databricks-common-access',RoleSessionName='myDhfsession')

s3 = boto3.resource('s3',
                          aws_access_key_id=response2.get('Credentials').get('AccessKeyId'),
                          aws_secret_access_key=response2.get('Credentials').get('SecretAccessKey'),
                          aws_session_token=response2.get('Credentials').get('SessionToken'))

s3.meta.client.copy(copy_source, 'pcds-databricks-common-786994105833', target)


# COMMAND ----------


import io
import zipfile
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
import json
import boto3
import time
from pyspark.sql import Row

d4 =spark.createDataFrame(["s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Krishna/Rivian/new/rivian_trips_summary_20211123.00.csv","s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Krishna/Rivian/new/rivian_trips_summary_20211124.00.csv"], StringType()).toDF("path")
#d4.filter(d4.path.contains('tar')).show()
#d4.select("path").distinct().withColumn("source_path_split",split(col("path"),"/")[0]).show()
d4.withColumn("source_path_s",:split(split(col("path"),"/")[8],"_")[1]).select("source_path_s").show(1,truncate=False)
#.withColumn("target_path",concat(lit("iot/kiran/testboto3/"),col("source_path_split")))\
#.drop("source_path_s","path","target_path")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_rivian_raw_prod.rivian_reporting_av_aggregate

# COMMAND ----------



import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
import boto3

from pyspark.sql import Row


def save_to_s3(dff4):
  
dff5=dff4.select("path").distinct().withColumn("source_path_split",split(col("path"),"/",7)[6])\
.drop("path").rdd.flatMap(lambda x: x).collect()

for line in dff5:
  source=f"CMT/Archive/tar/{line}"
  target=f"iot/raw/smartride/cmt-pl/realtime/tar/{line}"
  print(sourec)
 

  sts = boto3.client('sts')
  sts.get_caller_identity()

  response = sts.assume_role(RoleArn='arn:aws:iam::786994105833:role/dw-pl-cmt-TelematicS3FileMove-role',RoleSessionName='mycmtsession')

  s3 = boto3.resource('s3',
                          aws_access_key_id=response.get('Credentials').get('AccessKeyId'),
                          aws_secret_access_key=response.get('Credentials').get('SecretAccessKey'),
                          aws_session_token=response.get('Credentials').get('SessionToken'))
  copy_source = {
    'Bucket': 'dw-internal-pl-cmt-telematics-786994105833',
    'Key': source
   }

  response2 = sts.assume_role(RoleArn='arn:aws:iam::786994105833:role/pcds-databricks-common-access',RoleSessionName='myDhfsession')

  s3 = boto3.resource('s3',
                          aws_access_key_id=response2.get('Credentials').get('AccessKeyId'),
                          aws_secret_access_key=response2.get('Credentials').get('SecretAccessKey'),
                          aws_session_token=response2.get('Credentials').get('SessionToken'))

  s3.meta.client.copy(copy_source, 'pcds-databricks-common-786994105833',target)


                 

# COMMAND ----------


import io
import zipfile
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
import json
import boto3
import time
from pyspark.sql import Row

dfe4 =spark.createDataFrame(["s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Archive/tar/load_date=2021-07-27/1000/DS_CMT_80cdf2ca-02cc-4d5c-b8f6-af16860209f3/31650A0E-2564-4A8C-3079-A3B571648B2A.tar.gz","s3://dw-internal-pl-cmt-telematics-786994105833/CMT/Archive/tar/load_date=2021-06-23/1800/DS_CMT_5ce21f92-1a55-4698-ae4e-58e4de4573a4/109B5487-22ED-4925-A0B0-9F099417E1CD_3400.tar.gz"], StringType()).toDF("path")
dfe5=dfe4.select("path").distinct().withColumn("source_path_split",split(col("path"),"/",7)[6])\
.drop("path").rdd.collect()

for line in dfe5:
  source=f"CMT/Archive/tar/{line}"
  target=f"iot/raw/smartride/cmt-pl/realtime/tar/{line}"
  print(source)
  

# COMMAND ----------

import datetime
from datetime import datetime
ed="s3://dw-internal-pl-cmt-telematics-786994105833/CMT/krishna/rivian_trips_summary_20210817.00.csv"
#print(f"load_date={ed.split('/')[4].split('.')[0].rsplit('_',1)[1].split('')}")
er=ed.rsplit('_',1)[1].split('.')[0]
date=datetime.strptime(er,'%Y%m%d').strftime('%Y/%m/%d')
print(date)


# COMMAND ----------

filename="trips_summary"
if filename == "av_aggregate":
        schema=1
elif filename == "trips":
        schema=2
elif filename == "trips_summary":
        schema=3
elif filename == "event_summary":
        schema=4
print(schema)
  

# COMMAND ----------

import io
import zipfile
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
import json
import boto3
import time
from pyspark.sql import Row
from datetime import datetime

schema_av_aggregate= StructType([ \
  StructField("car_id",IntegerType(),True), \
  StructField("av_optin_date",StringType(),True), \
  StructField("last_av_optout_date",StringType(),True), \
  StructField("av_elapsed_days",IntegerType(),True), \
  StructField("pol_eff_date",IntegerType(),True), \
  StructField("pol_exp_date",StringType(),True), \
  StructField("total_distance",DoubleType(),True), \
  StructField("total_duration",DoubleType(),True), \
  StructField("av_total_distance",DoubleType(),True), \
  StructField("av_total_duration",DoubleType(),True), \
  StructField("av_score",DoubleType(),True), \
  StructField("av_discount",StringType(),True) \
])

schema_event_summary= StructType([ \
  StructField("car_id",IntegerType(),True), \
  StructField("event_id",StringType(),True), \
  StructField("event_name",StringType(),True), \
  StructField("event_timestamp",StringType(),True), \
  StructField("tzo",IntegerType(),True) \
])

schema_trips= StructType([ \
  StructField("car_id",IntegerType(),True), \
  StructField("trip_id",StringType(),True), \
  StructField("utc_time",StringType(),True), \
  StructField("tzo",IntegerType(),True), \
  StructField("speed",DoubleType(),True) \
])

schema_trips_summary= StructType([ \
  StructField("car_id",IntegerType(),True), \
  StructField("trip_id",StringType(),True), \
  StructField("last_av_optout_date",StringType(),True), \
  StructField("trip_start",StringType(),True), \
  StructField("trip_end",StringType(),True), \
  StructField("tzo",IntegerType(),True), \
  StructField("distance",DoubleType(),True), \
  StructField("duration",DoubleType(),True), \
  StructField("av_distance",DoubleType(),True), \
  StructField("av_duration",DoubleType(),True) \
])
filepath="s3://dw-internal-pl-cmt-telematics-786994105833/CMT/krishna/rivian_trips_summary_20210817.00.csv"
filename=filepath.rsplit('_',1)[0].rsplit('/',1)[1]
date=filepath.rsplit('_',1)[1].split('.')[0]
load_date=datetime.strptime(date,'%Y%m%d').strftime('%Y/%m/%d')
if filename == "rivian_av_aggregate":
        schema=schema_av_aggregate
elif filename == "rivian_trips":
        schema=schema_trips
elif filename == "rivian_trips_summary":
        schema=schema_trips_summary
elif filename == "rivian_event_summary":
        schema=schema_event_summary

df5 = spark.read.option("header", True) \
  .schema(schema) \
  .csv(filepath).withColumn("load_date",lit(load_date))
df5.show(truncate=False)


# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql.types import *
# MAGIC import json
# MAGIC from pyspark.sql.functions import col,from_json, regexp_extract,date_format, explode, slice, size, split, element_at, lit, current_date, current_timestamp, to_date, to_timestamp
# MAGIC # Databricks notebook source
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, DoubleType
# MAGIC from pyspark.sql.functions import *
# MAGIC import datetime
# MAGIC import io
# MAGIC import re
# MAGIC import json
# MAGIC import zipfile
# MAGIC from pyspark.sql.types import *
# MAGIC import boto3
# MAGIC from datetime import datetime as dt
# MAGIC from pyspark.sql import functions as f
# MAGIC import pandas as pd
# MAGIC
# MAGIC from pyspark.sql.functions import col,from_json, regexp_extract,date_format, explode, slice, size, split, element_at, lit, current_date, current_timestamp, to_date, to_timestamp, regexp_replace
# MAGIC json_Df = spark.read.json("s3://pcds-databricks-common-785562577411/iot/raw/smartride/smartmiles/program_enrollment/db_load_date=2022-12-31")
# MAGIC #display(json_Df)
# MAGIC
# MAGIC json_Df.schema.json()
# MAGIC json_Df = json_Df.select((regexp_replace('value', r'(\u0000\u0000\u0000\u0000\[)', '').alias('value')),'topic','partition','offset','timestampType','timestamp')
# MAGIC #json_Df = json.loads(json_Df)
# MAGIC #display(json_Df)
# MAGIC
# MAGIC schemadf = 		StructType([
# MAGIC 				StructField('context', StructType([
# MAGIC 						StructField('id', StringType(), True), 
# MAGIC 						StructField('source', StringType(), True), 
# MAGIC 						StructField('time', StringType(), True), 
# MAGIC 						StructField('type', StringType(), True)
# MAGIC 				])),
# MAGIC 				StructField('smartRideEnrollment', StructType([
# MAGIC 						StructField('transactionType', StringType(), True),
# MAGIC 						StructField('policy', StructType([
# MAGIC 							StructField('policyNumber', StringType(), True),
# MAGIC                             StructField('policyState', StringType(), True),
# MAGIC                             StructField('drivers', ArrayType(StructType([
# MAGIC                               StructField('subjectId', StringType(), True),
# MAGIC                               StructField('firstName', StringType(), True),
# MAGIC                               StructField('lastName', StringType(), True),
# MAGIC                               StructField('middleName', StringType(), True),
# MAGIC                               StructField('suffix', StringType(), True)
# MAGIC                             ])),True),
# MAGIC                             StructField('vehicles', ArrayType(StructType([StructField('subjectId', StringType(), True),
# MAGIC                                                            StructField('vin', StringType(), True),
# MAGIC                                                            StructField('year', StringType(), True),
# MAGIC                                                            StructField('make', StringType(), True),
# MAGIC                                                            StructField('model', StringType(), True)]), True),True),
# MAGIC 							StructField('telematics', StructType([
# MAGIC 								StructField('programs', ArrayType(StructType([
# MAGIC 									StructField('subjectId', StringType(), True),
# MAGIC 									StructField('subjectType', StringType(), True),
# MAGIC 									StructField('enrollmentId', StringType(), True),
# MAGIC 									StructField('programType', StringType(), True),
# MAGIC 									StructField('programStatus', StringType(), True),
# MAGIC                                     StructField('enrollmentEffectiveDate', StringType(), True),
# MAGIC                                     StructField('enrollmentProcessDate', StringType(), True),
# MAGIC                                     StructField('programTermBeginDate', StringType(), True),
# MAGIC                                     StructField('programTermEndDate', StringType(), True),
# MAGIC                                     StructField('programEndDate', StringType(), True),
# MAGIC                                     StructField('dataCollection', StructType([StructField('dataCollectionId', StringType(),True),
# MAGIC                                                                             StructField('vendorAccountId', StringType(), True),
# MAGIC                                                                             StructField('vendorAccountBeginDate', StringType(),True),
# MAGIC                                                                             StructField('vendorAccountEndDate', StringType(),True),
# MAGIC                                                                             StructField('vendor',StringType(),True),
# MAGIC                                                                             StructField('dataCollectionStatus',StringType(),True),
# MAGIC                                                                             StructField('device',StructType([StructField('deviceId', StringType(), True),
# MAGIC                                                                                     StructField('deviceStatus', StringType(), True),
# MAGIC                                                                                     StructField('deviceStatusDate', StringType(), True),
# MAGIC                                                                                     StructField('installGraceEndDate', StringType(), True),
# MAGIC                                                                                     StructField('lastRequestDate', StringType(), True)
# MAGIC                                                                                                             ]),True),
# MAGIC                                                                                                             ])),
# MAGIC                                     StructField('scoreAndDiscount',StructType([StructField('score', StringType(), True),
# MAGIC                                           StructField('scoreType', StringType(), True),
# MAGIC                                           StructField('scoreModel', StringType(), True),
# MAGIC                                           StructField('scoreDate', StringType(), True),
# MAGIC                                           StructField('discountPercent',StringType(),True)]), True),
# MAGIC                                           ]))),
# MAGIC                                 StructField('scoreAndDiscount',StructType([StructField('score', StringType(), True),
# MAGIC                                       StructField('scoreType', StringType(), True),
# MAGIC                                       StructField('scoreModel', StringType(), True),
# MAGIC                                       StructField('scoreDate', StringType(), True),
# MAGIC                                       StructField('discountPercent',StringType(),True)]), True),
# MAGIC                                 ])),
# MAGIC                             ])),
# MAGIC                       ])),
# MAGIC                 StructField('topic', StringType(), True),
# MAGIC                 StructField('partition', StringType(), True),
# MAGIC                 StructField('offset', StringType(), True),
# MAGIC                 StructField('timestampType', StringType(), True), #key
# MAGIC                 StructField('timestamp', StringType(), True)
# MAGIC 				])
# MAGIC
# MAGIC # df2 = json_Df.select('value.offset')
# MAGIC # print('df2')
# MAGIC # display(df2)
# MAGIC parsedDF1 = json_Df.withColumn("parsed_column",from_json(col("value"),schemadf)).drop("value")
# MAGIC display(parsedDF1)
# MAGIC df=parsedDF1.withColumn("program_arry",explode_outer("parsed_column.smartRideEnrollment.policy.telematics.programs")).select("program_arry.dataCollection.device.*")
# MAGIC display(df)

# COMMAND ----------

df=parsedDF1.withColumn("program_arry",explode_outer("parsed_column.smartRideEnrollment.policy.telematics.programs")).select("program_arry.dataCollection.device.*")
display(df)