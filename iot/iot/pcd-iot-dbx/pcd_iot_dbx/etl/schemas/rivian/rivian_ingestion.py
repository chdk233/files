"""
Source File schemas for Rivan
"""

from pyspark.sql.types import *

av_aggregate = StructType(
	[
		StructField('vin', StringType(), True), 
		StructField('av_optin_date', StringType(), True), 
		StructField('last_av_optout_date', StringType(), True), 
		StructField('av_elapsed_days', IntegerType(), True), 
		StructField('pol_eff_date', StringType(), True), 
		StructField('pol_exp_date', StringType(), True), 
		StructField('total_distance', DoubleType(), True), 
		StructField('total_duration', DoubleType(), True), 
		StructField('av_total_distance', DoubleType(), True), 
		StructField('av_total_duration', DoubleType(), True), 
		StructField('av_score', DoubleType(), True), 
		StructField('av_discount', StringType(), True), 
		StructField('src_sys_cd', StringType(), True), 
		StructField('db_load_time', TimestampType(), True), 
		StructField('db_load_date', DateType(), True), 
		StructField('load_dt', DateType(), True), 
		StructField('load_hr_ts', TimestampType(), True)
	]
)


event_summary = StructType(
	[
		StructField('vin', StringType(), True), 
		StructField('event_id', StringType(), True), 
		StructField('event_name', StringType(), True), 
		StructField('event_timestamp', StringType(), True), 
		StructField('tzo', IntegerType(), True), 
		StructField('src_sys_cd', StringType(), True), 
		StructField('db_load_time', TimestampType(), True), 
		StructField('db_load_date', DateType(), True), 
		StructField('load_dt', DateType(), True), 
		StructField('load_hr_ts', TimestampType(), True)
	]
)


trips = StructType(
	[
		StructField('vin', StringType(), True), 
		StructField('trip_id', StringType(), True), 
		StructField('utc_time', StringType(), True),  
		StructField('tzo', IntegerType(), True), 
		StructField('speed', DoubleType(), True),
		StructField('src_sys_cd', StringType(), True), 
		StructField('db_load_time', TimestampType(), True), 
		StructField('db_load_date', DateType(), True), 
		StructField('load_dt', DateType(), True), 
		StructField('load_hr_ts', TimestampType(), True)
	]
)


trips_odometer = StructType(
	[
		StructField('vin', StringType(), True), 
		StructField('trip_id', StringType(), True), 
		StructField('utc_time', StringType(), True), 
		StructField('tzo', IntegerType(), True), 
		StructField('mileage_delta', StringType(), True), 
		StructField('src_sys_cd', StringType(), True), 
		StructField('db_load_time', TimestampType(), True), 
		StructField('db_load_date', DateType(), True), 
		StructField('load_dt', DateType(), True), 
		StructField('load_hr_ts', TimestampType(), True)
	]
)


trips_summary = StructType(
	[
		StructField('vin', StringType(), True), 
		StructField('trip_id', StringType(), True), 
		StructField('trip_start', StringType(), True), 
		StructField('trip_end', StringType(), True), 
		StructField('tzo', IntegerType(), True), 
		StructField('distance', DoubleType(), True), 
		StructField('duration', DoubleType(), True), 
		StructField('av_distance', DoubleType(), True), 
		StructField('av_duration', DoubleType(), True), 
		StructField('src_sys_cd', StringType(), True), 
		StructField('db_load_time', TimestampType(), True), 
		StructField('db_load_date', DateType(), True), 
		StructField('load_dt', DateType(), True), 
		StructField('load_hr_ts', TimestampType(), True)
	]
)