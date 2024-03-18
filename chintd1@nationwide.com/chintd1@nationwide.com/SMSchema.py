# Databricks notebook source
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


schema_data = """{"milStatus": false, "fuelConsumption": "0.159", "accelQuality": true, "transportMode": "DRIVER", "secondsOfIdling": 594, "transportModeReason": "DEFAULT", "histogramScoringIntervals": [{"scoringComponent": "ACCELERATION", "scoringSubComponent": "NONE", "roadType": "DEFAULT", "thresholdLowerBound": "0", "thresholdUpperBound": "0.01", "scoringComponentUnit": "G", "occurrences": "594", "occurrenceUnit": "sec"}], "hdopAverage": "1", "telemetryPoints": [{"utcDateTime": "2021-07-16T09:48:17.000+0000", "speed": "0", "accelerometerData": "", "ambientTemperature": 42, "barometericPressure": 42, "horizontalAccuracy": "7.23", "throttlePosition": 42, "verticalAccuracy": "7.23" }, {"engineRPM": "701.5", "hdop": "1", "utcDateTime": "2021-07-16T09:58:06.000+0000", "degreesLatitude": "36.871318", "degreesLongitude": "-76.448247", "speed": "0", "headingDegrees": 6, "acceleration": "0"}, {"engineRPM": "1366.25", "coolantTemperature": 35, "fuelLevel": "56.47", "hdop": "1", "utcDateTime": "2021-07-16T06:16:36.000+0000", "degreesLatitude": "31.755499", "degreesLongitude": "-106.492754", "speed": "0", "acceleration": "0"}, {"engineRPM": "699.5", "hdop": "1", "utcDateTime": "2021-07-16T09:58:08.000+0000", "degreesLatitude": "36.871318", "degreesLongitude": "-76.448247", "speed": "0", "headingDegrees": 6, "acceleration": "0"}], "tripSummaryId": "00076ae8-a11c-4f0f-9cb0-5abe6a049c00", "utcStartDateTime": "2021-07-16T09:48:17.000+0000", "utcEndDateTime": "2021-07-16T09:58:11.000+0000", "timeZoneOffset": "-0400", "drivingDistance": "0", "maxSpeed": "0", "avgSpeed": "0", "totalTripSeconds": 594, "telemetryEvents": [{"degreesLatitude": "36.871318", "degreesLongitude": "-76.448247", "telemetryEventType": "TRIP_END", "utcDateTime": "2021-07-16T09:58:11.000+0000","acceleration": "-3.218", "avgSpeed": "68.12", "secondsOfDriving": 14, "telemetryEventSeverityLevel": "", "speed": "124.9", "headingDegrees": ""}, {"degreesLatitude": "36.871319", "degreesLongitude": "-76.448248", "telemetryEventType": "TRIP_START", "utcDateTime": "2021-07-16T09:48:17.000+0000"}], "device": {"deviceSerialNumber": "0082688664", "deviceIdentifier": "860112047832607", "deviceIdentifierType": "IMEI", "deviceType": "SIM_OBD"}, "externalReferences": [{"enterpriseReferenceId": "5345J 282031", "enterpriseReferenceExtraInfo": "", "type": "PRIMARY"}, {"enterpriseReferenceId": "5345J 282031", "enterpriseReferenceExtraInfo": "", "type": "PRIMARY"}], "vehicle": {"enrolledVin": "WDDWJ4JB9HF407683", "detectedVin": "WDDWJ4JB9HF407683"}, "scoring": {"scoreAlgorithmProvider": "HISTOGRAM_SCORING", "scoreUnit": "%", "overallScore": 83.00, "individualComponentScores": [{"component": "ACCELERATION", "componentScore": 100.00}, {"component": "BRAKING", "componentScore": 97.50}, {"component": "DISTRACTED_DRIVING", "componentScore": 100.00}]}, "measurementUnit": {"system": "METRIC"}}"""
schemadf = spark.read.json(sc.parallelize([schema_data]))
#print(schemadf.schema)
#display(schemadf)

#schemadf.show()

schemadf.withColumn("db_load_time",current_timestamp()).withColumn("db_load_date",current_date()).write.format("delta")\
.option("path",f"s3://pcds-databricks-common-786994105833/iot/delta/raw/smartmiles-ims/dhf_iot_sm_ims_stageraw_dev/sm_ims_stage_table/")\
.partitionBy("db_load_date")\
.saveAsTable(f"dhf_iot_sm_ims_stageraw_dev.sm_ims_stage_table")
  
telemetryPoints = schemadf.select("tripSummaryId", explode_outer(col("telemetryPoints")).alias("telemetryPoints_explode"))
telemetryPointsdf =  telemetryPoints.select("tripSummaryId", "telemetryPoints_explode.*").withColumn("db_load_time",current_timestamp()).withColumn("db_load_date",current_date())

telemetryPointsdf.write.format("delta")\
.option("path",f"s3://pcds-databricks-common-786994105833/iot/delta/raw/smartmiles-ims/dhf_iot_sm_ims_raw_dev/sm_ims_telemetryPoints/")\
.partitionBy("db_load_date")\
.saveAsTable(f"dhf_iot_sm_ims_raw_dev.sm_ims_telemetryPoints")

histogramScoringIntervals = schemadf.select("tripSummaryId", explode_outer(col("histogramScoringIntervals")).alias("histogramScoringIntervals_explode"))
histogramScoringIntervalsdf =  histogramScoringIntervals.select("tripSummaryId", "histogramScoringIntervals_explode.*").withColumn("db_load_time",current_timestamp()).withColumn("db_load_date",current_date())

histogramScoringIntervalsdf.write.format("delta")\
.option("path",f"s3://pcds-databricks-common-786994105833/iot/delta/raw/smartmiles-ims/dhf_iot_sm_ims_raw_dev/sm_ims_histogramScoring/")\
.partitionBy("db_load_date")\
.saveAsTable(f"dhf_iot_sm_ims_raw_dev.sm_ims_histogramScoring")

telemetryEvents = schemadf.select("tripSummaryId", explode_outer(col("telemetryEvents")).alias("telemetryEvents_explode"))
telemetryEventsdf =  telemetryEvents.select("tripSummaryId", "telemetryEvents_explode.*").withColumn("db_load_time",current_timestamp()).withColumn("db_load_date",current_date())

telemetryEventsdf.write.format("delta")\
.option("path",f"s3://pcds-databricks-common-786994105833/iot/delta/raw/smartmiles-ims/dhf_iot_sm_ims_raw_dev/sm_ims_telemetryEvents/")\
.partitionBy("db_load_date")\
.saveAsTable(f"dhf_iot_sm_ims_raw_dev.sm_ims_telemetryEvents")

scoring = schemadf.select("tripSummaryId", "scoring.*").withColumn("individualComponentScores_explode",explode_outer(col("individualComponentScores")))
scoringdf = scoring.select(*scoring.columns,"individualComponentScores_explode.*").drop(*["individualComponentScores_explode","individualComponentScores"]).withColumn("db_load_time",current_timestamp()).withColumn("db_load_date",current_date())

scoringdf.write.format("delta")\
.option("path",f"s3://pcds-databricks-common-786994105833/iot/delta/raw/smartmiles-ims/dhf_iot_sm_ims_raw_dev/sm_ims_scoring/")\
.partitionBy("db_load_date")\
.saveAsTable(f"dhf_iot_sm_ims_raw_dev.sm_ims_scoring")

columns = ['tripSummaryId',  'fuelConsumption',  'milStatus', 'accelQuality', 'transportMode', 'secondsOfIdling', 'transportModeReason', 'hdopAverage', 'utcStartDateTime',  'utcEndDateTime', 'timeZoneOffset', 'drivingDistance', 'maxSpeed', 'avgSpeed', 'totalTripSeconds', 'device.*', 'externalReferences', 'vehicle.*',  'measurementUnit.*']

tripSummary = schemadf.select(*columns).withColumn("externalReferences_explode",explode_outer(col("externalReferences")))
tripSummarydf = tripSummary.select(*tripSummary.columns,"externalReferences_explode.*").drop(*["externalReferences_explode","externalReferences"]).withColumn("db_load_time",current_timestamp()).withColumn("db_load_date",current_date())

tripSummarydf.write.format("delta")\
.option("path",f"s3://pcds-databricks-common-786994105833/iot/delta/raw/smartmiles-ims/dhf_iot_sm_ims_raw_dev/sm_ims_tripSummary/")\
.partitionBy("db_load_date")\
.saveAsTable(f"dhf_iot_sm_ims_raw_dev.sm_ims_tripSummary")

display(histogramScoringIntervalsdf)  
display(telemetryEventsdf) 
display(scoringdf) 
display(tripSummarydf) 



schema_dataEVT = """{"telemetrySetId" : "10e8d9a7-82f0-476a-9a1e-ef7fde3cdff5","telemetryEvents" : [ {"degreesLatitude" : null,"degreesLongitude" : null,"headingDegrees" : null,"telemetryEventType" : "DISCONNECT_EVENT","utcDateTime" : "2021-08-18T22:11:14.014+0000","speed" : null,"acceleration" : null,"telemetryEventSeverityLevel" : null}, {"degreesLatitude" : null,"degreesLongitude" : null,"headingDegrees" : null,"telemetryEventType" : "CONNECT_EVENT","utcDateTime" : "2021-08-19T10:44:34.034+0000","speed" : null,"acceleration" : null,"telemetryEventSeverityLevel" : null} ],"device" : {"deviceSerialNumber" : "0101388483","deviceIdentifier" : "868923050628605","deviceIdentifierType" : "IMEI","deviceType" : "SIM_OBD"},"externalReferences" : [ {"enterpriseReferenceId" : "6139V 150272","enterpriseReferenceExtraInfo" : "","type" : "PRIMARY"} ],"vehicle" : {"enrolledVin" : "WBAPH77569NL85014","detectedVin" : null}}"""


schemadfEVT = spark.read.json(sc.parallelize([schema_dataEVT]))

telemetryEventsDFEVT = schemadfEVT.withColumn("telemetryEvents_explode",explode_outer("telemetryEvents"))
finalDFEVT = telemetryEventsDFEVT.withColumn("externalReferences_explode",explode_outer("externalReferences"))
deltaDFEVT = finalDFEVT.select("telemetrySetId",col("telemetryEvents_explode.degreesLatitude").alias("telemetryEvents__degreesLatitude"),col("telemetryEvents_explode.degreesLongitude").alias("telemetryEvents__degreesLongitude"),col("telemetryEvents_explode.headingDegrees").alias("telemetryEvents__headingDegrees"),col("telemetryEvents_explode.telemetryEventType").alias("telemetryEvents__telemetryEventType"),col("telemetryEvents_explode.utcDateTime").alias("telemetryEvents__utcDateTime"),col("telemetryEvents_explode.speed").alias("telemetryEvents__speed"),col("telemetryEvents_explode.acceleration").alias("telemetryEvents__acceleration"),col("telemetryEvents_explode.telemetryEventSeverityLevel").alias("telemetryEvents__telemetryEventSeverityLevel"),col("device.deviceSerialNumber").alias("device__deviceSerialNumber"),col("device.deviceIdentifier").alias("device__deviceIdentifier"),col("device.deviceIdentifierType").alias("device__deviceIdentifierType"),col("device.deviceType").alias("device__deviceType"),col("externalReferences_explode.enterpriseReferenceId").alias("externalReferences__enterpriseReferenceId"),col("externalReferences_explode.enterpriseReferenceExtraInfo").alias("externalReferences__enterpriseReferenceExtraInfo"),col("externalReferences_explode.type").alias("externalReferences__type"),col("vehicle.enrolledVin").alias("vehicle__enrolledVin"),col("vehicle.detectedVin").alias("vehicle__detectedVin"))
    
dfDFEVT = deltaDFEVT.withColumn("db_load_time",current_timestamp()).withColumn("db_load_date",current_date())

dfDFEVT.write.format("delta")\
.option("path",f"s3://pcds-databricks-common-786994105833/iot/delta/raw/smartmiles-ims/dhf_iot_sm_ims_raw_dev/sm_ims_nontripevent/")\
.partitionBy("db_load_date")\
.saveAsTable(f"dhf_iot_sm_ims_raw_dev.sm_ims_nontripevent")

display(dfDFEVT)

