# Databricks notebook source
driver = "com.teradata.jdbc.TeraDriver"
url = "jdbc:teradata://nidbc.nwie.net/LOGMECH=LDAP"
user = "chintd1"
password = "Accen#072023"






# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
schema = StructType([
  StructField('audit_check', StringType(), True),
  StructField('no_of_errortables', StringType(), True),
  StructField('previous_week_error_tables', StringType(), True),
  StructField('load_dt', StringType(), True)
  ])
# df=spark.createDataFrame([('3','2','3','3')],schema=schema)
current_list_length=4
previous_list_length=3
today_date=spark.sql("select cast(date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date)) as string) as date   ").collect()[0]["date"] 
# df=spark.createDataFrame([('3','2','3','StringType')],schema=schema)
# df2=df.selectExpr("'run_file_availability_check' as audit_check")
# df2=df.withColumn("audit_check",lit("run_file_availability_check"))\
#              .withColumn("no_of_errortables",lit(current_list_length))\
#              .withColumn("previous_week_error_tables",lit(previous_list_length))\
#              .withColumn("load_dt",lit(today_date).cast(DateType()))
row=[("run_file_availability_check",current_list_length,0,today_date)]
df=spark.createDataFrame(data=row,schema=schema)
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date))
# MAGIC         

# COMMAND ----------

# MAGIC %scala
# MAGIC val jsonDF=spark.sql("select * from dhf_iot_cmt_raw_prod.rewards_raw_kafka order by db_load_time")
# MAGIC val convertedudf = udf((unicodestring: String) => unicodestring.replace("\u0000\u0000\u0000\u0000\ufffd","").replace("\u0000\u0000\u0000\u00025","").replace("\u0000\u0000\u0000\u0001\u0000","").replace("\u0000\u0000\u0000\u0001\"",""))
# MAGIC   val cleansedDF = jsonDF.withColumn("cleansed_value",convertedudf(jsonDF("value"))).drop("value")
# MAGIC   display(cleansedDF)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_cmt_raw_prod.rewards order by db_load_time desc
# MAGIC -- select * from dhf_iot_cmt_raw_prod.rewards_raw_kafka order by db_load_time

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_generic_autoloader_prod.job_config where JOB_CD =3001

# COMMAND ----------

# MAGIC %sql
# MAGIC -- alter table  dhf_iot_cmt_raw_prod.rewards add columns (age string,driverFinalScore string);
# MAGIC -- alter table  dhf_iot_harmonized_prod.rewards add columns (DRIVR_AGE int,DRIVR_FNL_SCR_QTY decimal(35,12))

# COMMAND ----------

# MAGIC %sql
# MAGIC desc dhf_iot_rivian_raw_prod.av_aggregate

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_generic_autoloader_prod.job_config where JOB_CD=6001

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct vendor from dhf_iot_raw_prod.program_enrollment where eventtype='com.nationwide.pls.telematics.auto.programs.v3.enrollment.scored' and program_scoreType='Estimated' --and program_score is null

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history dhf_iot_harmonized_prod.trip_summary_chlg 

# COMMAND ----------

# MAGIC %sql
# MAGIC select *  from dhf_iot_harmonized_prod.trip_summary_chlg where trip_smry_key='9119ddf1-3f58-4145-bb44-77ec3bf148a7' --and load_date = '2023-06-08' and SourceSystem = 'IMS_SR_5X' and enrolledVin in ('1GT49YEY2RF126021')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dhf_iot_curated_prod.device_status

# COMMAND ----------

driver = "com.teradata.jdbc.TeraDriver"
url = "jdbc:teradata://nidbc.nwie.net/LOGMECH=LDAP"
# user1 = "chintd1"
# password1 = "Accen#072023"
user1 = dbutils.secrets.get(scope = "iot_system", key = "username")
password1 = dbutils.secrets.get(scope = "iot_system", key = "password")

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table SHP_Json_Processed_Quote

# COMMAND ----------


sql="""
SELECT *
FROM stg_dqpc_db.PROP_NFCBASIC_BASIC_INFO_REC
"""


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SHP_Jsolive_load_daten_Processed_Quote

# COMMAND ----------

table1 = spark.read.format("jdbc")\
  .option("driver", driver)\
  .option("url", url)\
  .option("dbtable", '({sql}) as src'.format(sql=sql))\
  .option("user", user)\
  .option("password", password)\
  .load()

table1.createOrReplaceTempView("PROP_NFCCNTL1_CTRL_SEG_REC_1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select (*) from PROP_NFCCNTL1_CTRL_SEG_REC_1 where QUOTE_POL_NUM like '%9234%'