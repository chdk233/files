from pcd_iot_dbx.etl.transformations.smarthome.notion import system_health_daily as system_health_daily_tf
from pcd_iot_dbx.etl.schemas.smarthome.notion import system_health_daily as system_health_daily_schema
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
)
from pyspark.sql.functions import collect_list, sum
import great_expectations as ge
import dbldatagen as dg
import pytest
from pathlib import Path
import shutil


@pytest.fixture(scope="session")
def system_health_daily(spark: SparkSession):
    system_health_daily_path = './data/system_health_daily_test.json'
    row_count = 10
    test_data_spec = (
            dg.DataGenerator(spark, name="test_system_health_daily", rows=row_count, partitions=1)
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
            .withColumn(colName='hardware_id',
                        colType=StringType(),
                        random=True,
                        omit=True,
                        template=r'0\xkkkkkkkkkkkkkkkk',
                        baseColumn='partner_member_id'
                        )
            .withColumn(colName='hardware_type',
                        colType=StringType(),
                        random=True,
                        omit=True,
                        values=["sensor","bridge"]
                        )
            .withColumn(colName='battery_level',
                        colType=StringType(),
                        random=True,
                        omit=True,
                        values=["high","medium","critical"]
                        )
            .withColumn(colName='uptime',
                        colType=FloatType(),
                        nullable=True,
                        random=True,
                        omit=True,
                        minValue=0.0,
                        maxValue=100.0
                        )
            .withColumn(colName='current_status',
                        colType=StringType(),
                        random=True,
                        omit=True,
                        values=["active","abandoned","dormant","churned"]
                        )
            .withColumn(colName='disconnect_count',
                        colType=IntegerType(),
                        random=True,
                        omit=True
                        )
            .withColumn(
                colName="records", 
                colType=StructType([StructField("partner_member_id", StringType()),
                                    StructField("hardware_id", StringType()),
                                    StructField("hardware_type", StringType()),
                                    StructField("battery_level", StringType()),
                                    StructField("uptime", FloatType()),
                                    StructField("current_status", StringType()),
                                    StructField("disconnect_count", IntegerType())
                                    ]
                                    ),
                expr="named_struct('partner_member_id', partner_member_id," + \
                      "'hardware_id', hardware_id," +\
                      "'hardware_type', hardware_type," +\
                      "'battery_level', battery_level," +\
                      "'uptime', uptime," +\
                      "'current_status', current_status," +\
                      "'disconnect_count', disconnect_count)",
                baseColumn=["partner_member_id","hardware_id","hardware_type","battery_level","uptime","current_status","disconnect_count"]
            )
        )

    df_test_data = test_data_spec.build()

    df_test_data = df_test_data \
        .groupBy() \
            .agg(sum('number_of_records').alias('number_of_records'),
                 collect_list('records').alias('records')) \
             .drop('identifier')

    df_test_data.write.mode("overwrite").json(system_health_daily_path) 

    yield system_health_daily_path

    if Path(system_health_daily_path).exists():
        shutil.rmtree(system_health_daily_path)

def test_system_health_daily(spark,system_health_daily: str):

    system_health_daily_df = spark.read.json(system_health_daily,schema=system_health_daily_schema)

    df = system_health_daily_tf(system_health_daily_df)

    ge_df = ge.dataset.SparkDFDataset(df)

    assert ge_df.expect_column_to_exist('partner_member_id').get('success')
    assert ge_df.expect_column_to_exist('hardware_id').get('success')
    assert ge_df.expect_column_to_exist('hardware_type').get('success')
    assert ge_df.expect_column_to_exist('uptime').get('success')
    assert ge_df.expect_column_to_exist('current_status').get('success')
    assert ge_df.expect_column_to_exist('battery_level').get('success')
    assert ge_df.expect_column_to_exist('disconnect_count').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='partner_member_id',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='hardware_id',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='hardware_type',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='uptime',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='current_status',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='battery_level',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='disconnect_count',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_not_be_null('partner_member_id').get('success')
    assert ge_df.expect_column_values_to_not_be_null('hardware_id').get('success')
    assert ge_df.expect_column_values_to_not_be_null('hardware_type').get('success')
    assert ge_df.expect_column_values_to_not_be_null('uptime').get('success')
    assert ge_df.expect_column_values_to_not_be_null('current_status').get('success')
    assert ge_df.expect_column_values_to_not_be_null('battery_level').get('success')
    assert ge_df.expect_column_values_to_not_be_null('disconnect_count').get('success')
    assert ge_df.expect_table_column_count_to_equal(7)
    assert ge_df.expect_table_row_count_to_equal(10)
