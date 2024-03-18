# Databricks notebook source
#audit_noteBook_path=str(dbutils.widgets.get("audit_noteBook_path"))
#deltadatabase=str(dbutils.widgets.get("deltadatabase"))
audit_noteBook_path="Repos/iotsid@nationwide.com/pcds-iot-${enviroment}/Ingestion/Rivian/rivian_audit"
deltadatabase="dhf_iot_rivian_raw_dev"
SNSArn="test"

# COMMAND ----------

dbutils.notebook.run(f"{audit_noteBook_path}",60,{"deltadatabase":f"{deltadatabase}","SNSArn":f"{SNSArn}"})

# COMMAND ----------

from datetime import date, timedelta

start_date = date(2019, 1, 1)
end_date = date(2020, 1, 1)
delta = timedelta(days=1)
while start_date <= end_date:
    print(start_date.strftime("%Y-%m-%d"))
    start_date += delta

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.rm("s3://pcds-databricks-common-785562577411/iot/delta/curated/dhf_iot_curated_prod/DEVICE_STATUS/SRC_SYS_CD=IMS_SR_4X/",True)