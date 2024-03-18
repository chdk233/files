# Databricks notebook source
# MAGIC %sql
# MAGIC select  a.* from  dhf_iot_harmonized_prod.TDS_trips  a left anti join dhf_iot_curated_prod.trip_detail b on a.trip_smry_key=b.trip_smry_key

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.trip_detail  where trip_smry_key='00000000105023991665154954'

# COMMAND ----------

# DBTITLE 1,Import Python modules and define job parameters
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import *
import datetime
import io
import re
import json
import zipfile
from pyspark.sql.types import *
import boto3
from datetime import datetime as dt
from pyspark.sql import functions as f

s3_target_file_path = "s3://pcds-databricks-common-786994105833/iot/delta/raw/smartmiles-ims/dhf_iot_sm_ims_raw_dev"
s3_intm_target_file_path= "s3://pcds-databricks-common-786994105833/iot/delta/stageraw/smartmiles-ims/dhf_iot_sm_ims_stageraw_dev"
database_name = "dhf_iot_sm_ims_raw_dev"
target_table_name = "smart_miles"
intmddelta_stage_database="dhf_iot_sm_ims_stageraw_dev"
intm_table_name="smart_miles_intm"
checkpointLocation = "s3://pcds-databricks-common-786994105833/iot/kiran/checkpoint_smartMiles_json_test_krishna/"
maxFilePerTrigger = 50
fileFormat = "binaryFile"
s3_source_file_path = "s3://dw-internal-telematics-786994105833/smartmiles-ims-sourcefiles/zip/"
includeExistingFile = True
cloudFileQueueURL = "https://sqs.us-east-1.amazonaws.com/786994105833/dw-pl-smls-ims-telematics-databricks-queue/"
# sns_topic_arn     = s"arn:aws:sns:us-east-1:786994105833:dw-cmt-pl-iot-telematics-emailgroup"

# COMMAND ----------

# DBTITLE 1,User Defined function to convert Bytes data to String format
def convertBytesToString(bytesdata):
  in_memory_data = io.BytesIO(bytesdata)
  file_obj = zipfile.ZipFile(in_memory_data, "r")
  files = [i for i in file_obj.namelist()]
  record_list = []
  for file in files:
    record_list = [line.replace(b"\n",b"").replace(b"\r",b"").replace(b"\t",b"").decode("utf-8") for line in file_obj.open(file)]
  return "".join(record_list)

# COMMAND ----------

# DBTITLE 1,Databricks Autoloader to load source files
jsonFileStream = spark.readStream.format("cloudFiles")\
.option( "pathGlobFilter", "*.zip")\
.option( "recursiveFileLookup", True)\
.option( "cloudFiles.format", fileFormat)\
.option( "cloudFiles.maxFilesPerTrigger", maxFilePerTrigger)\
.option( "cloudFiles.queueUrl",cloudFileQueueURL)\
.option( "cloudFiles.region", "us-east-1")\
.option("cloudFiles.useNotifications", True)\
.option( "cloudFiles.includeExistingFiles", False)\
.option( "cloudFiles.validateOptions", True)\
.load(s3_source_file_path)

# COMMAND ----------

schema_data = StructType([ 
StructField("accelQuality",BooleanType(),True),StructField("avgSpeed",StringType(),True),StructField("device",StructType([ 
StructField("deviceIdentifier",StringType(),True),StructField("deviceIdentifierType",StringType(),True),StructField("deviceSerialNumber",StringType(),True),StructField("deviceType",StringType(),True)]),True),StructField("drivingDistance",StringType(),True),StructField("externalReferences",ArrayType(StructType([ 
StructField("enterpriseReferenceExtraInfo",StringType(),True),StructField("enterpriseReferenceId",StringType(),True),StructField("type",StringType(),True)]),True),True),StructField("fuelConsumption",StringType(),True),StructField("hdopAverage",StringType(),True),StructField("histogramScoringIntervals",ArrayType(StructType([ 
StructField("occurrenceUnit",StringType(),True),StructField("occurrences",StringType(),True),StructField("roadType",StringType(),True),StructField("scoringComponent",StringType(),True),StructField("scoringComponentUnit",StringType(),True),StructField("scoringSubComponent",StringType(),True),StructField("thresholdLowerBound",StringType(),True),StructField("thresholdUpperBound",StringType(),True)]),True),True),StructField("maxSpeed",StringType(),True),StructField("measurementUnit",StructType([ 
(StructField("system",StringType(),True))]),True),StructField("milStatus",BooleanType(),True),StructField("scoring",StructType([ 
StructField("individualComponentScores",ArrayType(StructType([ 
StructField("component",StringType(),True),StructField("componentScore",DoubleType(),True)]),True),True),
 StructField("overallScore",DoubleType(),True),
 StructField("scoreAlgorithmProvider",StringType(),True),
 StructField("scoreUnit",StringType(),True)]),True),
 StructField("secondsOfIdling",LongType(),True),StructField("telemetryEvents",ArrayType(StructType([ 
StructField("acceleration",StringType(),True),StructField("degreesLatitude",StringType(),True),StructField("degreesLongitude",StringType(),True),StructField("headingDegrees",LongType(),True),StructField("secondsOfDriving",LongType(),True),StructField("speed",StringType(),True),StructField("telemetryEventSeverityLevel",StringType(),True),StructField("telemetryEventType",StringType(),True),StructField("avgSpeed",StringType(),True),StructField("utcDateTime",StringType(),True)]),True),True),StructField("telemetryPoints",ArrayType(StructType([ 
StructField("acceleration",StringType(),True),StructField("barometericPressure",LongType(),True),StructField("ambientTemperature",LongType(),True),StructField("coolantTemperature",LongType(),True),StructField("degreesLatitude",StringType(),True),StructField("degreesLongitude",StringType(),True),StructField("accelerometerData",StringType(),True),StructField("fuelLevel",StringType(),True),StructField("engineRPM",StringType(),True),StructField("hdop",StringType(),True),StructField("headingDegrees",LongType(),True),StructField("speed",StringType(),True),StructField("verticalAccuracy",StringType(),True),StructField("throttlePosition",LongType(),True),StructField("horizontalAccuracy",StringType(),True),StructField("utcDateTime",StringType(),True)]),True),True),StructField("timeZoneOffset",StringType(),True),StructField("totalTripSeconds",LongType(),True),StructField("transportMode",StringType(),True),StructField("transportModeReason",StringType(),True),StructField("tripSummaryId",StringType(),True),StructField("utcEndDateTime",StringType(),True),StructField("utcStartDateTime",StringType(),True),StructField("vehicle",StructType([ 
StructField("detectedVin",StringType(),True),StructField("enrolledVin",StringType(),True)]),True)])

# COMMAND ----------

# DBTITLE 1,Function to process microbatch data and load to Delta table
def handleMicroBatch(microbatchDF, batch_id):
  
  print(f"Running JSON autoloader at {str(datetime.datetime.now())}") 
  batchSize = microbatchDF.count()
  print('batchSize: ',batchSize)
  if(batchSize > 0):
    #Get the number of files to be processed
    filepath_list = [filename["path"] for filename in microbatchDF.select("path").collect()]
    
    #UDF to convert Byte data to String format
    bytesUDF = udf(convertBytesToString)
    schema_dataEVT = """{"telemetrySetId" : "10e8d9a7-82f0-476a-9a1e-ef7fde3cdff5","telemetryEvents" : [ {"degreesLatitude" : null,"degreesLongitude" : null,"headingDegrees" : null,"telemetryEventType" : "DISCONNECT_EVENT","utcDateTime" : "2021-08-18T22:11:14.014+0000","speed" : null,"acceleration" : null,"telemetryEventSeverityLevel" : null}, {"degreesLatitude" : null,"degreesLongitude" : null,"headingDegrees" : null,"telemetryEventType" : "CONNECT_EVENT","utcDateTime" : "2021-08-19T10:44:34.034+0000","speed" : null,"acceleration" : null,"telemetryEventSeverityLevel" : null} ],"device" : {"deviceSerialNumber" : "0101388483","deviceIdentifier" : "868923050628605","deviceIdentifierType" : "IMEI","deviceType" : "SIM_OBD"},"externalReferences" : [ {"enterpriseReferenceId" : "6139V 150272","enterpriseReferenceExtraInfo" : "","type" : "PRIMARY"} ],"vehicle" : {"enrolledVin" : "WBAPH77569NL85014","detectedVin" : null}}"""
    schemadfEVT = spark.read.json(sc.parallelize([schema_dataEVT]))
    batchDFWithoutEVT = microbatchDF.filter(~microbatchDF.path.like('%EVT_%'))
    batchDFWithEVT = microbatchDF.filter(microbatchDF.path.like('%EVT_%'))
    byteParsed = batchDFWithoutEVT.withColumn("path_parsed",bytesUDF("content")).drop("modificationTime","length","content")
    byteParsedEVT = batchDFWithEVT.withColumn("path_parsed",bytesUDF("content")).drop("modificationTime","length","content")
    
    parsedDF = byteParsed.withColumn("parsed_column",from_json("path_parsed",schema=schema_data)).drop("path_parsed")
    parsedDFEVT = byteParsedEVT.withColumn("parsed_column",from_json("path_parsed",schema=schemadfEVT.schema)).drop("path_parsed")

    mainDF = parsedDF.select("path","parsed_column.*" )
    mainDF.write.format("delta")\
      .mode("append")\
      .option("mergeSchema","true")\
      .option("path",f"{s3_intm_target_file_path}/{intm_table_name}_tripSummary/")\
      .saveAsTable(f"{intmddelta_stage_database}.{intm_table_name}_tripSummary")
    
    mainDFEVT = parsedDFEVT.select("path","parsed_column.*" )
    
    telemetryEventsDFEVT = mainDFEVT.withColumn("telemetryEvents_explode",explode_outer("telemetryEvents"))
    finalDFEVT = telemetryEventsDFEVT.withColumn("externalReferences_explode",explode_outer("externalReferences"))
    deltaDFEVT = finalDFEVT.select("telemetrySetId",col("telemetryEvents_explode.degreesLatitude").alias("telemetryEvents_degreesLatitude"),col("telemetryEvents_explode.degreesLongitude").alias("telemetryEvents_degreesLongitude"),col("telemetryEvents_explode.headingDegrees").alias("telemetryEvents_headingDegrees"),col("telemetryEvents_explode.telemetryEventType").alias("telemetryEvents_telemetryEventType"),col("telemetryEvents_explode.utcDateTime").alias("telemetryEvents_utcDateTime"),col("telemetryEvents_explode.speed").alias("telemetryEvents_speed"),col("telemetryEvents_explode.acceleration").alias("telemetryEvents_acceleration"),col("telemetryEvents_explode.telemetryEventSeverityLevel").alias("telemetryEvents_telemetryEventSeverityLevel"),col("device.deviceSerialNumber").alias("device_deviceSerialNumber"),col("device.deviceIdentifier").alias("device_deviceIdentifier"),col("device.deviceIdentifierType").alias("device_deviceIdentifierType"),col("device.deviceType").alias("device_deviceType"),col("externalReferences_explode.enterpriseReferenceId").alias("externalReferences_enterpriseReferenceId"),col("externalReferences_explode.enterpriseReferenceExtraInfo").alias("externalReferences_enterpriseReferenceExtraInfo"),col("externalReferences_explode.type").alias("externalReferences_type"),col("vehicle.enrolledVin").alias("vehicle_enrolledVin"),col("vehicle.detectedVin").alias("vehicle_detectedVin"))
    
    dfDFEVT = deltaDFEVT.withColumn("db_load_time",current_timestamp()).withColumn("db_load_date",current_date())
    
    print("EVT file columns: ", deltaDFEVT.columns)
    dfDFEVT.write.format("delta")\
      .mode("append")\
      .option("mergeSchema","true")\
      .option("path",f"{s3_target_file_path}/{target_table_name}_NonTripEvent/")\
      .saveAsTable(f"{database_name}.{target_table_name}_NonTripEvent")

    
    ### Primary Check: check null timestamps for start and end dates
    ###                Start and end time must both be valid date/timestamps
    ###                Time must always include milliseconds     
    regex = "([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}\+?\-?[0-9]{4})"
    df_dateTime_format = mainDF.select("*", f.when(f.col("utcStartDateTime").rlike(regex), 1).otherwise(0).alias("invalid_startDateTime_format"))
    df_dateTime_format = df_dateTime_format.select("*", f.when(f.col("utcEndDateTime").rlike(regex), 1).otherwise(0).alias("invalid_endDateTime_format"))
    startTime_error = df_dateTime_format.filter(f.col("invalid_startdateTime_format") == 0)
    endTime_error = df_dateTime_format.filter(f.col("invalid_endDateTime_format") == 0)
    
    if startTime_error.count()>0:
      message = f"Start time invalid format"
      subject = f"utcStartDateTime: Invalid Format"
#       send_mail(sns_arn,subject,message)
    if endTime_error.count()>0:
      message = f"End time invalid format"
      subject = f"utcEndDateTime: Invalid Format"
#       send_mail(sns_arn,subject,message)
      
    columns = ['tripSummaryId',  'fuelConsumption',  'milStatus', 'accelQuality', 'transportMode', 'secondsOfIdling', 'transportModeReason', 'hdopAverage', 'utcStartDateTime',  'utcEndDateTime', 'timeZoneOffset', 'drivingDistance', 'maxSpeed', 'avgSpeed', 'totalTripSeconds', 'device.*', 'externalReferences', 'vehicle.*',  'measurementUnit.*', 'path']

    tripSummary = mainDF.select(*columns).withColumn("externalReferences_explode",explode_outer(col("externalReferences")))
    dft = tripSummary.select(*tripSummary.columns,"externalReferences_explode.*").drop(*["externalReferences_explode","externalReferences"]).withColumn("db_load_time",current_timestamp()).withColumn("db_load_date",current_date())
    print("tripSUmmary columns: ", dft.columns)
    dft.write.format("delta")\
      .mode("append")\
      .option("mergeSchema","true")\
      .option("path",f"{s3_target_file_path}/{target_table_name}_tripSummary/")\
      .saveAsTable(f"{database_name}.{target_table_name}_tripSummary")

    histogramScoring = mainDF.select("path", "tripSummaryId",explode_outer(col("histogramScoringIntervals")).alias("histogramScoringIntervals_explode"))
    dfz = histogramScoring.select("tripSummaryId", "histogramScoringIntervals_explode.*").withColumn("db_load_time",current_timestamp()).withColumn("db_load_date",current_date())
    # dfx.display()
    print("histogram columns: ", dfz.columns)
    dfz.write.format("delta")\
      .mode("append")\
      .option("mergeSchema","true")\
      .option("path",f"{s3_target_file_path}/{target_table_name}_histogramScoringIntervals/")\
      .saveAsTable(f"{database_name}.{target_table_name}_histogramScoringIntervals")
    
    telemetryPoints = mainDF.select("tripSummaryId", explode_outer(col("telemetryPoints")).alias("telemetryPoints_explode"))
    dfx = telemetryPoints.select("tripSummaryId", "telemetryPoints_explode.*").withColumn("db_load_time",current_timestamp()).withColumn("db_load_date",current_date())
    print("telemetryPoints columns: ", dfx.columns)
    dfx.write.format("delta")\
      .mode("append")\
      .option("mergeSchema","true")\
      .option("path",f"{s3_target_file_path}/{target_table_name}_telemetryPoints/")\
      .saveAsTable(f"{database_name}.{target_table_name}_telemetryPoints")
    
    
    telemetryEvents = mainDF.select("tripSummaryId", explode_outer(col("telemetryEvents")).alias("telemetryEvents_explode"))
    dfe = telemetryEvents.select("tripSummaryId", "telemetryEvents_explode.*").withColumn("db_load_time",current_timestamp()).withColumn("db_load_date",current_date())
    # dfe.display()
    dfe.write.format("delta")\
      .mode("append")\
      .option("mergeSchema","true")\
      .option("path",f"{s3_target_file_path}/{target_table_name}_telemetryEvents/")\
      .saveAsTable(f"{database_name}.{target_table_name}_telemetryEvents")

    scoring = mainDF.select("tripSummaryId", "scoring.*")
    dfs = mainDF.select("tripSummaryId", "scoring.*").withColumn("individualComponentScores_explode",explode_outer(col("individualComponentScores")))
    dfs_final = dfs.select(*dfs.columns,"individualComponentScores_explode.*").drop(*["individualComponentScores_explode","individualComponentScores"]).withColumn("db_load_time",current_timestamp()).withColumn("db_load_date",current_date())
    # dfs_final.display()
    dfs_final.write.format("delta")\
      .mode("append")\
      .option("mergeSchema","true")\
      .option("path",f"{s3_target_file_path}/{target_table_name}_scoring/")\
      .saveAsTable(f"{database_name}.{target_table_name}_scoring")
    
    deltaDF = mainDF.withColumn("db_load_time",current_timestamp()).withColumn("db_load_date",current_date())
    print(f"Record Count: {deltaDF.count()}")
#     deltaDF.show()
    
    secondary_check(deltaDF)
    
    
  else:
    print('No data to process in current batch')


# COMMAND ----------

# DBTITLE 1,Secondary Audit Checks
def secondary_check(df):
  ### Secondary Check: End date time cannot be less than Start date time
  startTime_greater_endTime_error = df.filter(f.col("utcStartDateTime") > f.col("utcEndDateTime").cast('timestamp'))
  if startTime_greater_endTime_error.count()>0:
    message = f" Please check end date time & Start date time. End date time cannot be less than Start date time"
    subject = f"Error: End time less than start time"
#       send_mail(sns_arn,subject,message)

   ### Secondary Check: Start time and end time cannot be in the future
  startTime_greater_current_error = df.filter(f.col("utcStartDateTime") > f.unix_timestamp().cast('timestamp'))
  message = f" Please check Start date time. Start date time cannot be greater than present date"
  subject = f"Error: Start Date > Present date"
#       send_mail(sns_arn,subject,message)
  endTime_greater_current_error = df.filter(f.col("utcEndDateTime") > f.unix_timestamp().cast('timestamp'))
  message = f" Please check end date time . End date time cannot be greater than present date"
  subject = f"Error: End Date >  Present Date"
#       send_mail(sns_arn,subject,message)

  ### Secondary Check: Total Trip Duration should not be greater than 14 hours
  totalTripDuration = 14*3600
  time_diff = df.withColumn("utcStartDateTime",to_timestamp("utcStartDateTime")).withColumn("utcEndDateTime",to_timestamp("utcEndDateTime"))
  time_diff = time_diff.withColumn("diff", col("utcEndDateTime").cast("long") - col('utcStartDateTime').cast("long"))
  timeDiff_error = time_diff.filter(f.col("diff") > totalTripDuration)
  message = f" Total Trip duration cannot be greater than 14 hours"
  subject = f"Error: Total trip duration > 14"
#       send_mail(sns_arn,subject,message)

  ### Secondary Check: Start time must be within last 2 months
  month_threshold = 2
  month_diff = df.withColumn("monthDiff", months_between( f.unix_timestamp().cast('timestamp'), col("utcStartDateTime")))
  monthDiff_error = month_diff.filter(f.col("monthDiff") > month_threshold)
  message = f" Start time must be within last 2 months"
  subject = f"Error: Start time >2 months"
#       send_mail(sns_arn,subject,message)

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

# DBTITLE 1,Write stream to call the handleMicroBatch function for processing each microbatch
jsonFileStream\
.writeStream\
.trigger(once=True)\
.queryName("jsonFileStream")\
.foreachBatch(handleMicroBatch)\
.start()

# COMMAND ----------

jsonFileStream = spark.readStream.format("cloudFiles")\
.option( "pathGlobFilter", "*.zip")\
.option( "recursiveFileLookup", True)\
.option( "cloudFiles.format", "binaryFile")\
.option( "cloudFiles.queueUrl","dw-fdr-pl-daily-triplabel-kafka-queue")\
.option( "cloudFiles.region", "us-east-1")\
.option("cloudFiles.useNotifications", True)\
.option( "cloudFiles.includeExistingFiles", False)\
.option( "cloudFiles.validateOptions", True)\
.load("s3://dw-internal-pl-cmt-telematics-785562577411/CMT/Archive/zip/*/")

# COMMAND ----------

display(jsonFileStream)