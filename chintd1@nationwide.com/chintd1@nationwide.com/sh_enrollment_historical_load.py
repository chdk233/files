# Databricks notebook source
import json
from pyspark.sql.functions import regexp_extract, col,from_json, explode_outer,current_timestamp,current_date, when, lit, length, lpad, to_date


spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")   
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
jsonDF = spark.read.json("s3://dw-internal-pl-notion-smarthome-telematics-785562577411/SHP/ReceivedFiles/ProcessedFiles/*")


schema_data1 = """{"context": {"id":"CgAAj7dRXlzLIIv0QHA1ce04BSoLPEglEMhW7xbFLOrPHs1uAAB24g..","source":"SmartHomeProcessing","subject":"","time":1676120987079,"type":"com.nationwide.pls.telematics.home.programs.enrollment.created"},"smartHomeEnrollment":{"enrollmentId":"2167","event_id":"","policyNumber":"9234EA002851","enrollmentEffectiveDate":"2023-08-15","enrollmentRemovalReason":"","communicationType":"","partnerMemberId":"eda1eed8ef2745a90ff9f1265f86834e309de4398145e58f3af71384","categories":[{"deviceCategory":"SmartHome","deviceDiscountType":"Participating"}]}}"""
  

json_rdd = sc.parallelize([schema_data1])

schemadf = spark.read.json(json_rdd)

   
cleansedDF = jsonDF.withColumn("cleansed_value",regexp_extract(col('value').cast('string'), r'.*?(\{.*\})',1)).drop("value")
  
parsedDF = cleansedDF.withColumn("parsed_column",from_json(col("cleansed_value"),schema=schemadf.schema))

parsedDF.display()

# COMMAND ----------

mainDF = parsedDF.selectExpr("topic","partition","offset","timestamp","timestampType","parsed_column.context.*",
                                  "parsed_column.smartHomeEnrollment.*"
                                  ) \
                        .withColumn("categories_arry",explode_outer(col("categories"))) \
                        .selectExpr("*","categories_arry.*") \
                        .drop("smartHome","categories","categories_arry")


# COMMAND ----------


deltaDF = (mainDF.withColumn("db_load_time",current_timestamp())
           .withColumn("db_load_date",current_date())
           .withColumn("partition",col("partition").cast("int"))
           .withColumn("timestampType",col("timestampType").cast("int"))
           .drop("subject","event_id")
           .withColumn("timestamp",(col("time")/1000).cast('timestamp'))
           .withColumn("dataCollectionId", when(length(col("partnerMemberId")) < 55, lpad(col("partnerMemberId"), 55, "0")).otherwise(col("partnerMemberId")))
           .drop("partnerMemberId")
           .withColumn("src_sys_cd",lit("SHP_PL"))
           .withColumn("load_hr_ts",(col("time")/1000).cast('timestamp'))
           .withColumn("load_dt",to_date(col("load_hr_ts")))
          ) 


deltaDF.display()

# COMMAND ----------

(deltaDF.coalesce(1).write
  .format("delta")
  .mode("append")
  .option("path","s3://pc-iot-raw-785562577411/smarthome/tables/dhf_iot_raw_prod/sh_enrollment/")
  .saveAsTable("dhf_iot_raw_prod.sh_enrollment"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC optimize dhf_iot_raw_prod.sh_enrollment

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dhf_iot_raw_prod.sh_enrollment limit 10