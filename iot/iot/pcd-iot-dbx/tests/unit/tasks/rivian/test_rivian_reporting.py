from pcd_iot_dbx.tasks.rivian.rivian_reporting import rivianReporting
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    TimestampType,
    IntegerType,
    DoubleType
)
import great_expectations as ge
import dbldatagen as dg
import pytest
import os


@pytest.fixture(scope="session")
def agg_input_details(spark: SparkSession):

    agg_report_table='rivian_reporting_av_aggregate'
    avagg_test_data_spec = (
        dg.DataGenerator(spark, name="test_data_set1", rows=2, partitions=1)
        .withIdOutput()
        .withColumn("trip_id",  StringType(), values=["T1"])
        .withColumn("vin",  StringType(), values=["V1"])
        .withColumn("av_optin_date",  StringType(), values=["V1"])
        .withColumn("last_av_optout_date",  StringType(), values=["V1"])
        .withColumn("av_elapsed_days",  IntegerType(), values=[1])
        .withColumn("pol_eff_date",  StringType(), values=["V1"])
        .withColumn("pol_exp_date",  StringType(), values=["V1"])
        .withColumn("total_distance",  DoubleType(), values=[1])
        .withColumn("total_duration",  DoubleType(), values=[1])
        .withColumn("av_total_distance",  DoubleType(), values=[1])
        .withColumn("av_total_duration",  DoubleType(), values=[1])
        .withColumn("av_score",  DoubleType(), values=[1])
        .withColumn("av_discount",  StringType(), values=["V1"])
        .withColumn("load_dt", TimestampType(), values=["2023-11-03","2023-11-04"])
    )

    avagg_df_test_data = avagg_test_data_spec.build()

    avagg_df_test_data.write.mode("append").saveAsTable("default.av_aggregate")

    yield {'agg_report_table' :agg_report_table}

@pytest.fixture(scope="session")    
def ts_input_details(spark: SparkSession):
    #trips_summary
    
    ts_report_table='rivian_reporting_trips_summary'
    ts_test_data_spec = (
        dg.DataGenerator(spark, name="test_data_set1", rows=2, partitions=1)
        .withIdOutput()
        .withColumn("trip_id",  StringType(), values=["T4"])
        .withColumn("vin",  StringType(), values=["V4"])
        .withColumn("trip_start",  StringType(), values=["T4"])
        .withColumn("trip_end",  StringType(), values=["T4"])
        .withColumn("tzo",  IntegerType(), values=[1])
        .withColumn("distance",  DoubleType(), values=[1])
        .withColumn("duration",  DoubleType(), values=[1])
        .withColumn("av_distance",  DoubleType(), values=[1])
        .withColumn("av_duration",  DoubleType(), values=[1])
        .withColumn("load_dt", TimestampType(), values=["2023-11-03","2023-11-04"])
    )

    ts_df_test_data = ts_test_data_spec.build()

    ts_df_test_data.write.mode("append").saveAsTable("default.trips_summary")

     
    yield {'ts_report_table' :ts_report_table}

def test_rivian_reporting(spark: SparkSession,agg_input_details,ts_input_details):

    

    conf = {
        'environment' : 'dev',
        'deltadatabase' : 'default',
        'path':'./data/rivian/reporting/',
        'email_from' : 'do-not-reply@nationwide.com',
        'email_to': 'chintd1@nationwide.com'}
    if not os.path.exists('./data/rivian/reporting/'):
        os.mkdir('./data/rivian/reporting/')
    task = rivianReporting(init_conf=conf)

    
    agg_report_table=agg_input_details.get('agg_report_table')
    ts_report_table=ts_input_details.get('ts_report_table')

    task.launch()
    
    agg_report_df=spark.read.table(f'default.{agg_report_table}')
    ts_report_df=spark.read.table(f'default.{ts_report_table}')

    agg_ge_df = ge.dataset.SparkDFDataset(agg_report_df)
    ts_ge_df = ge.dataset.SparkDFDataset(ts_report_df)
    agg_report_df.printSchema()
    ts_report_df.printSchema()

    assert agg_ge_df.expect_column_to_exist('vin').get('success')
    assert agg_ge_df.expect_column_to_exist('av_optin_date').get('success')
    assert agg_ge_df.expect_column_to_exist('last_av_optout_date').get('success')
    assert agg_ge_df.expect_column_to_exist('av_elapsed_days').get('success')
    assert agg_ge_df.expect_column_to_exist('pol_eff_date').get('success')
    assert agg_ge_df.expect_column_to_exist('pol_exp_date').get('success')
    assert agg_ge_df.expect_column_to_exist('total_distance').get('success')
    assert agg_ge_df.expect_column_to_exist('total_duration').get('success')
    assert agg_ge_df.expect_column_to_exist('av_total_distance').get('success')
    assert agg_ge_df.expect_column_to_exist('av_total_duration').get('success')
    assert agg_ge_df.expect_column_to_exist('av_score').get('success')
    assert agg_ge_df.expect_column_to_exist('av_discount').get('success')
    assert agg_ge_df.expect_column_values_to_not_be_null('vin').get('success')
    assert agg_ge_df.expect_column_values_to_not_be_null('av_optin_date').get('success')
    assert agg_ge_df.expect_column_values_to_not_be_null('last_av_optout_date').get('success')
    assert agg_ge_df.expect_column_values_to_not_be_null('av_elapsed_days').get('success')
    assert agg_ge_df.expect_column_values_to_not_be_null('pol_eff_date').get('success')
    assert agg_ge_df.expect_column_values_to_not_be_null('pol_exp_date').get('success')
    assert agg_ge_df.expect_column_values_to_not_be_null('total_distance').get('success')
    assert agg_ge_df.expect_column_values_to_not_be_null('total_duration').get('success')
    assert agg_ge_df.expect_column_values_to_not_be_null('av_total_distance').get('success')
    assert agg_ge_df.expect_column_values_to_not_be_null('av_total_duration').get('success')
    assert agg_ge_df.expect_column_values_to_not_be_null('av_score').get('success')
    assert agg_ge_df.expect_column_values_to_not_be_null('av_discount').get('success')
    assert len(agg_report_df.columns) == 17
    assert agg_ge_df.expect_table_row_count_to_equal(1).get('success')

    assert ts_ge_df.expect_column_to_exist('vin').get('success')
    assert ts_ge_df.expect_column_to_exist('trip_id').get('success')
    assert ts_ge_df.expect_column_to_exist('trip_start').get('success')
    assert ts_ge_df.expect_column_to_exist('trip_end').get('success')
    assert ts_ge_df.expect_column_to_exist('tzo').get('success')
    assert ts_ge_df.expect_column_to_exist('distance').get('success')
    assert ts_ge_df.expect_column_to_exist('duration').get('success')
    assert ts_ge_df.expect_column_to_exist('av_distance').get('success')
    assert ts_ge_df.expect_column_to_exist('av_duration').get('success')
    assert ts_ge_df.expect_column_values_to_not_be_null('vin').get('success')
    assert ts_ge_df.expect_column_values_to_not_be_null('trip_id').get('success')
    assert ts_ge_df.expect_column_values_to_not_be_null('trip_start').get('success')
    assert ts_ge_df.expect_column_values_to_not_be_null('trip_end').get('success')
    assert ts_ge_df.expect_column_values_to_not_be_null('tzo').get('success')
    assert ts_ge_df.expect_column_values_to_not_be_null('distance').get('success')
    assert ts_ge_df.expect_column_values_to_not_be_null('duration').get('success')
    assert ts_ge_df.expect_column_values_to_not_be_null('av_distance').get('success')
    assert ts_ge_df.expect_column_values_to_not_be_null('av_duration').get('success')
    assert len(ts_report_df.columns) == 13
    assert ts_ge_df.expect_table_row_count_to_equal(1).get('success')




