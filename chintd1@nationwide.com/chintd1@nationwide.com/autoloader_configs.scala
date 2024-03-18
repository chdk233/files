// Databricks notebook source
6000,6001,6002,6003--SM 5X
7002,7003,7004,7006--SR OCTO
8000,8001,8002,8003--SR 4X
9000,9001,9002,9003--SR 5X
9004--FMC
9005,9006--TIMS

// COMMAND ----------

dbutils.fs.ls("/mnt/pcds-iot-extracts-prod/rivian/") 

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_generic_autoloader_prod.job_config where job_cd in (6001) 