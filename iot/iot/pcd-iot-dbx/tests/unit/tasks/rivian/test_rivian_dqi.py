from pcd_iot_dbx.tasks.rivian.rivian_dqi import rivianDQI
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    TimestampType
)
import pyspark
import great_expectations as ge
import dbldatagen as dg
import pytest
from pathlib import Path
import shutil


@pytest.fixture(scope="session")
def input_details(spark: SparkSession):

    table='rivian_audit'
    avagg_test_data_spec = (
        dg.DataGenerator(spark, name="test_data_set1", rows=1, partitions=1)
        .withIdOutput()
        .withColumn("trip_id",  StringType(), values=["T1"])
        .withColumn("vin",  StringType(), values=["V1"])
        .withColumn("av_optin_date",  StringType(), values=["V1"])
        .withColumn("last_av_optout_date",  StringType(), values=["V1"])
        .withColumn("av_elapsed_days",  StringType(), values=["V1"])
        .withColumn("pol_eff_date",  StringType(), values=["V1"])
        .withColumn("pol_exp_date",  StringType(), values=["V1"])
        .withColumn("total_distance",  StringType(), values=["V1"])
        .withColumn("total_duration",  StringType(), values=["V1"])
        .withColumn("av_total_distance",  StringType(), values=["V1"])
        .withColumn("av_total_duration",  StringType(), values=["V1"])
        .withColumn("av_score",  StringType(), values=["V1"])
        .withColumn("av_discount",  StringType(), values=["V1"])
        .withColumn("load_dt", TimestampType(), values=["2023-11-03"])
    )

    avagg_df_test_data = avagg_test_data_spec.build()

    avagg_df_test_data.write.mode("append").saveAsTable("default.av_aggregate")

    #trips

    tr_test_data_spec = (
        dg.DataGenerator(spark, name="test_data_set1", rows=1, partitions=1)
        .withIdOutput()
        .withColumn("trip_id",  StringType(), values=["T2"])
        .withColumn("vin",  StringType(), values=["V2"])
        .withColumn("utc_time",  StringType(), values=["V2"])
        .withColumn("speed",  StringType(), values=["V2"])
        .withColumn("tzo",  StringType(), values=["V2"])
        .withColumn("load_dt", TimestampType(), values=["2023-11-03"])
    )

    tr_df_test_data = tr_test_data_spec.build()

    tr_df_test_data.write.mode("append").saveAsTable("default.trips")

    #events

    evt_test_data_spec = (
        dg.DataGenerator(spark, name="test_data_set1", rows=1, partitions=1)
        .withIdOutput()
        .withColumn("trip_id",  StringType(), values=["T3"])
        .withColumn("vin",  StringType(), values=["V3"])
        .withColumn("event_id",  StringType(), values=["V3"])
        .withColumn("event_name",  StringType(), values=["V3"])
        .withColumn("event_timestamp",  StringType(), values=["V3"])
        .withColumn("tzo",  StringType(), values=["V3"])
        .withColumn("load_dt", TimestampType(), values=["2023-11-06"])

    )

    evt_df_test_data = evt_test_data_spec.build()

    evt_df_test_data.write.mode("append").saveAsTable("default.event_summary")

    #trips_summary

    ts_test_data_spec = (
        dg.DataGenerator(spark, name="test_data_set1", rows=1, partitions=1)
        .withIdOutput()
        .withColumn("trip_id",  StringType(), values=["T4"])
        .withColumn("vin",  StringType(), values=["V4"])
        .withColumn("trip_start",  StringType(), values=["T4"])
        .withColumn("trip_end",  StringType(), values=["T4"])
        .withColumn("tzo",  StringType(), values=["T4"])
        .withColumn("distance",  StringType(), values=["T4"])
        .withColumn("duration",  StringType(), values=["T4"])
        .withColumn("av_distance",  StringType(), values=["T4"])
        .withColumn("av_duration",  StringType(), values=["T4"])
        .withColumn("load_dt", TimestampType(), values=["2023-11-03"])
    )

    ts_df_test_data = ts_test_data_spec.build()

    ts_df_test_data.write.mode("append").saveAsTable("default.trips_summary")

    #trips_odometer

    odo_test_data_spec = (
        dg.DataGenerator(spark, name="test_data_set1", rows=2, partitions=1)
        .withIdOutput()
        .withColumn("trip_id",  StringType(), values=["T5"])
        .withColumn("vin",  StringType(), values=["V5"])
        .withColumn("utc_time",  StringType(), values=["V5"])
        .withColumn("tzo",  StringType(), values=["V5"])
        .withColumn("mileage_delta",  StringType(), values=["V5"])
        .withColumn("load_dt", TimestampType(), values=["2023-10-27","2023-11-03"])
    )

    odo_df_test_data = odo_test_data_spec.build()

    odo_df_test_data.write.mode("append").saveAsTable("default.trips_odometer") 
    yield {'table' :table}

def test_rivian_dqi(spark: SparkSession,input_details):

    

    conf = {
        'deltadatabase' : 'default',
        'email_from' : 'do-not-reply@nationwide.com',
        'email_to': 'chintd1@nationwide.com'}

    task = rivianDQI(init_conf=conf)

    
    table=input_details.get('table')

    task.launch()
    
    df=spark.read.table(f'default.{table}')

    ge_df = ge.dataset.SparkDFDataset(df)
    

    assert ge_df.expect_column_to_exist('audit_check').get('success')
    assert ge_df.expect_column_to_exist('no_of_errortables').get('success')
    assert ge_df.expect_column_to_exist('previous_week_error_tables').get('success')
    assert ge_df.expect_column_to_exist('load_dt').get('success')
    assert ge_df.expect_column_values_to_not_be_null('audit_check').get('success')
    assert ge_df.expect_column_values_to_not_be_null('no_of_errortables').get('success')
    assert ge_df.expect_column_values_to_not_be_null('previous_week_error_tables').get('success')
    assert ge_df.expect_column_values_to_not_be_null('load_dt').get('success')
    assert len(df.columns) == 4
    assert ge_df.expect_table_row_count_to_equal(4).get('success')


