from pcd_iot_dbx.etl.transformations.smarthome.notion import sensor_monthly as sensor_monthly_tf
from pcd_iot_dbx.etl.schemas.smarthome.notion import sensor_monthly as sensor_monthly_schema
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
def sensor_monthly(spark: SparkSession):
    sensor_monthly_path = './data/sensor_monthly_test.json'
    row_count = 10
    test_data_spec = (
            dg.DataGenerator(spark, name="test_sensor_monthly", rows=row_count, partitions=1)
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
            .withColumn(colName='task_names',
                        colType=StringType(),
                        random=True,
                        omit=True,
                        values=["Other, Sliding","Other, Vertical Hinged","Other, Water Leak","Other, Temperature, Vertical Hinged, Water Leak"]
                        )
            .withColumn(colName='location',
                        colType=StringType(),
                        random=True,
                        omit=True,
                        values=["Garage","Laundry Room","Kitchen","Office","Master bathroom","Dishwasher"]
                        )
            .withColumn(colName='installed_at',
                        colType=TimestampType(),
                        nullable=True,
                        random=True,
                        omit=True,
                        minValue=0.0,
                        maxValue=100.0
                        )
            .withColumn(
                colName="records", 
                colType=StructType([StructField("partner_member_id", StringType()),
                                    StructField("sensor_hardware_id", StringType()),
                                    StructField("task_names", StringType()),
                                    StructField("location", StringType()),
                                    StructField("installed_at", TimestampType())
                                    ]
                                    ),
                expr="named_struct('partner_member_id', partner_member_id," + \
                      "'sensor_hardware_id', sensor_hardware_id," +\
                      "'task_names', task_names," +\
                      "'location', location," +\
                      "'installed_at', installed_at)",
                baseColumn=["partner_member_id","sensor_hardware_id","task_names","location","installed_at"]
            )
        )

    df_test_data = test_data_spec.build()

    df_test_data = df_test_data \
        .groupBy() \
            .agg(sum('number_of_records').alias('number_of_records'),
                 collect_list('records').alias('records')) 

    df_test_data.write.mode("overwrite").json(sensor_monthly_path) 

    yield sensor_monthly_path

    if Path(sensor_monthly_path).exists():
        shutil.rmtree(sensor_monthly_path)

def test_sensor_monthly(spark,sensor_monthly: str):

    sensor_monthly_df = spark.read.json(sensor_monthly,schema=sensor_monthly_schema)

    df = sensor_monthly_tf(sensor_monthly_df)

    ge_df = ge.dataset.SparkDFDataset(df)

    assert ge_df.expect_column_to_exist('partner_member_id').get('success')
    assert ge_df.expect_column_to_exist('sensor_hardware_id').get('success')
    assert ge_df.expect_column_to_exist('task_names').get('success')
    assert ge_df.expect_column_to_exist('location').get('success')
    assert ge_df.expect_column_to_exist('installed_at').get('success')

    assert ge_df.expect_column_values_to_be_of_type(column='partner_member_id',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='sensor_hardware_id',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='task_names',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='location',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='installed_at',
                                                    type_='StringType').get('success')
    
    assert ge_df.expect_column_values_to_not_be_null('partner_member_id').get('success')
    assert ge_df.expect_column_values_to_not_be_null('sensor_hardware_id').get('success')
    assert ge_df.expect_column_values_to_not_be_null('task_names').get('success')
    assert ge_df.expect_column_values_to_not_be_null('location').get('success')
    assert ge_df.expect_column_values_to_not_be_null('installed_at').get('success')

    assert ge_df.expect_table_column_count_to_equal(5)
    assert ge_df.expect_table_row_count_to_equal(10)
