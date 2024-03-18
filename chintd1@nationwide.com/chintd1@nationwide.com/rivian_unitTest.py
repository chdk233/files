# Databricks notebook source
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F

trip_summary_df=spark.read.table("dhf_iot_rivian_raw_dev.trips_summary").drop("db_load_time").distinct()
agg_df=spark.read.table("dhf_iot_rivian_raw_dev.av_aggregate").drop("db_load_time").distinct()
trips_df=spark.read.table("dhf_iot_rivian_raw_dev.trips").drop("db_load_time").distinct()
event_summary_df=spark.read.table("dhf_iot_rivian_raw_dev.event_summary").drop("db_load_time").distinct()



trip_agg_df=trip_summary_df.groupBy("vin","load_date").agg(F.round(sum("distance"),3).alias("sum_distance"),F.round(sum("duration"),3).alias("sum_duration"),F.round(sum("av_distance"),3).alias("sum_av_distance"),F.round(sum("av_duration"),3).alias("sum_av_duration"))


trip_duration_df=trip_summary_df.select(col("trip_start").cast("timeStamp"),col("trip_end").cast("timeStamp"),col("duration"),col("distance")).withColumn("date_diff_min",  F.round((F.col("trip_end").cast("long") - F.col("trip_start").cast("long"))/60.,3)).where(col("date_diff_min")!=col("duration")).drop("date_diff_min")


joinDF=agg_df.alias("agg").join(trip_agg_df.alias("trip"),agg_df.vin==trip_summary_df.vin,"inner").where(col("agg.load_date")==col("trip.load_date")).select(col("agg.vin"),col("agg.total_distance"),col("agg.total_duration"),col("agg.av_total_distance"),col("agg.av_total_duration"),col("agg.av_score"),col("trip.sum_duration"),col("trip.sum_distance"),col("trip.sum_av_duration"),col("trip.sum_av_distance")).withColumn("av_score_check",F.round(col("agg.av_total_distance")/col("agg.total_distance"),2))





# COMMAND ----------

display(agg_df)

# COMMAND ----------

from pyspark.sql.types import _parse_datatype_string
assert agg_df.schema == _parse_datatype_string("vin string,av_optin_date	string,last_av_optout_date	string,av_elapsed_days	int	,pol_eff_date	string	,pol_exp_date	string	,total_distance	double	,total_duration	double	,av_total_distance	double	,av_total_duration	double	,av_score	double	,av_discount	string	,load_date	date"),"Schemas do not match"
print("Assertion passed.")

# COMMAND ----------

assert trip_summary_df.schema == _parse_datatype_string("vin	string	,trip_id	string	,trip_start	string	,trip_end	string	,tzo	int	,distance	double	,duration	double	,av_distance	double	,av_duration	double	,load_date	date"),"Schemas do not match"
print("Assertion passed.")

# COMMAND ----------

assert trips_df.schema == _parse_datatype_string("vin	string	,trip_id	string	,utc_time	string	,tzo	int	,speed	double	,load_date	date"),"Schemas do not match"
print("Assertion passed.")

# COMMAND ----------

assert event_summary_df.schema == _parse_datatype_string("vin	string	,event_id	string	,event_name	string	,event_timestamp	string	,tzo	int	,load_date	date"),"Schemas do not match"
print("Assertion passed.")

# COMMAND ----------

assert (joinDF.where(col("total_distance")!=col("sum_distance")).count()==0),"total distance not matching"
print("assertion passed")

# COMMAND ----------

assert (joinDF.where(col("total_duration")!=col("sum_duration")).count()==0),"total duration not matching"
print("assertion passed")

# COMMAND ----------

assert (joinDF.where(col("av_total_distance")!=col("sum_av_distance")).count()==0),"total av_distance not matching"
print("assertion passed")

# COMMAND ----------

assert (joinDF.where(col("av_total_duration")!=col("sum_av_duration")).count()==0),"total av_duration not matching"
print("assertion passed")

# COMMAND ----------

trips_error_df=trips_df.join(trip_summary_df,[trips_df.trip_id==trip_summary_df.trip_id,trips_df.load_date==trip_summary_df.load_date],"leftanti").distinct()
tripsSummary_error_df=trip_summary_df.join(trips_df,[trips_df.trip_id==trip_summary_df.trip_id,trips_df.load_date==trip_summary_df.load_date],"leftanti").distinct()



# COMMAND ----------

assert (trips_error_df.count()==0),"trips not matching with trips_summary file"
print("assertion passed")

# COMMAND ----------

assert (tripsSummary_error_df.count()==0),"trips in summary file not matching with trips file"
print("assertion passed ")