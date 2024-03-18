from pcd_iot_dbx.etl.transformations.smarthome.notion import events_weekly as events_weekly_tf
from pcd_iot_dbx.etl.schemas.smarthome.notion import events_weekly as events_weekly_schema
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
)
from pyspark.sql.functions import collect_list, sum
import great_expectations as ge
import dbldatagen as dg
import pytest
from pathlib import Path
import shutil


@pytest.fixture(scope="session")
def event_weekly(spark: SparkSession):
    event_weekly_path = './data/event_weekly_test.json'
    row_count = 10
    test_data_spec = (
            dg.DataGenerator(spark, name="test_event_weekly", rows=row_count, partitions=1)
            .withColumn(colName='number_of_records',
                        colType=IntegerType(),
                        values=[1]
                        )
            .withColumn(colName='partner_member_id',
                        colType=StringType(),
                        random=True,
                        omit=True,
                        template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                        )
            .withColumn(colName='sensor_hardware_id',
                        colType=StringType(),
                        random=True,
                        omit=True,
                        template=r'0\xkkkkkkkkkkkkkkkk',
                        baseColumn='partner_member_id'
                        )
            .withColumn(colName='timestamp_utc',
                        colType=TimestampType(),
                        random=True,
                        omit=True,
                        nullable=True,
                        minValue=0.0,
                        maxValue=100.0
                        )
            .withColumn(colName='task_name',
                        colType=StringType(),
                        random=True,
                        omit=True,
                        values=["Other, Sliding","Other, Vertical Hinged","Other, Water Leak","Other, Temperature, Vertical Hinged, Water Leak"]
                        )
            .withColumn(colName='event',
                        colType=StringType(),
                        random=True,
                        omit=True,
                        values=["motion","critical","no_alarm","low","above","below","alarm","inside","leak","high","medium","closed","no_leak","open","calibration_initiated","calibration_complete"]
                        )
            .withColumn(colName='alertable',
                        colType=IntegerType(),
                        omit=True,
                        values=[1,0]
                        )
            .withColumn(
                colName="records", 
                colType=StructType([StructField("partner_member_id", StringType()),
                                    StructField("sensor_hardware_id", StringType()),
                                    StructField("timestamp_utc", TimestampType()),
                                    StructField("task_name", StringType()),
                                    StructField("event", StringType()),
                                    StructField("alertable", IntegerType())
                                    ]
                                    ),
                expr="named_struct('partner_member_id', partner_member_id," + \
                      "'sensor_hardware_id', sensor_hardware_id," +\
                      "'timestamp_utc', timestamp_utc," +\
                      "'task_name', task_name," +\
                      "'event', event," +\
                      "'alertable', alertable)",
                baseColumn=["partner_member_id","sensor_hardware_id","timestamp_utc","task_name","event","alertable"]
            )
        )

    df_test_data = test_data_spec.build()

    df_test_data = df_test_data \
        .groupBy() \
            .agg(sum('number_of_records').alias('number_of_records'),
                 collect_list('records').alias('records')) \
             .drop('identifier')

    df_test_data.write.mode("overwrite").json(event_weekly_path) 

    yield event_weekly_path

    if Path(event_weekly_path).exists():
        shutil.rmtree(event_weekly_path)

def test_event_weekly(spark,event_weekly: str):

    event_weekly_df = spark.read.json(event_weekly,schema=events_weekly_schema)

    df = events_weekly_tf(event_weekly_df)

    ge_df = ge.dataset.SparkDFDataset(df)

    assert ge_df.expect_column_to_exist('partner_member_id').get('success')
    assert ge_df.expect_column_to_exist('sensor_hardware_id').get('success')
    assert ge_df.expect_column_to_exist('timestamp_utc').get('success')
    assert ge_df.expect_column_to_exist('task_name').get('success')
    assert ge_df.expect_column_to_exist('event').get('success')
    assert ge_df.expect_column_to_exist('alertable').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='partner_member_id',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='sensor_hardware_id',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='timestamp_utc',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='task_name',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='event',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='alertable',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_not_be_null('partner_member_id').get('success')
    assert ge_df.expect_column_values_to_not_be_null('sensor_hardware_id').get('success')
    assert ge_df.expect_column_values_to_not_be_null('timestamp_utc').get('success')
    assert ge_df.expect_column_values_to_not_be_null('task_name').get('success')
    assert ge_df.expect_column_values_to_not_be_null('event').get('success')
    assert ge_df.expect_column_values_to_not_be_null('alertable').get('success')
    assert ge_df.expect_table_column_count_to_equal(6)
    assert ge_df.expect_table_row_count_to_equal(10)
