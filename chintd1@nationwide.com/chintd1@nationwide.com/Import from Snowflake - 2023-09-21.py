# Databricks notebook source
# Use dbutils secrets to get Snowflake credentials.
user = dbutils.secrets.get("data-warehouse", "<snowflake-user>")
password = dbutils.secrets.get("data-warehouse", "<snowflake-password>")

options = {
  "sfUrl": "<snowflake-url>",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "<snowflake-database>",
  "sfSchema": "<snowflake-schema>",
  "sfWarehouse": "<snowflake-cluster>"
}

# COMMAND ----------

# Generate a simple dataset containing five values and write the dataset to Snowflake.
spark.range(5).write \
  .format("snowflake") \
  .options(**options) \
  .option("dbtable", "table_name") \
  .save()

# COMMAND ----------

# Read the data written by the previous cell back.
df = spark.read \
  .format("snowflake") \
  .options(options) \
  .option("dbtable", "table_name") \
  .load()

display(df)

# COMMAND ----------

# Write the data to a Delta table

df.write.format("delta").saveAsTable("sf_ingest_table")