from pcd_iot_dbx.etl.transformations.smarthome.notion import shipped_installed_daily as shipped_installed_daily_tf
from pcd_iot_dbx.etl.schemas.smarthome.notion import shipped_installed_daily as shipped_installed_daily_schema
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
def shipped_installed_daily(spark: SparkSession):
    shipped_installed_daily_path = './data/shipped_installed_daily_test.json'
    row_count = 10
    test_data_spec = (
            dg.DataGenerator(spark, name="test_shipped_installed_daily", rows=row_count, partitions=1)
            .withColumn(colName='number_of_records',
                        colType=IntegerType(),
                        values=[1]
                        )
            .withColumn(colName='order_number',
                        colType=StringType(),
                        random=True,
                        omit=True,
                        template=r'AAAADDDD'
                        )
            .withColumn(colName='partner_member_id',
                        colType=StringType(),
                        random=True,
                        omit=True,
                        template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk',
                        baseColumn='order_number'
                        )
            .withColumn(colName='shipping_status',
                        colType=StringType(),
                        random=True,
                        omit=True,
                        values=["shipped","ORDER_COMPLETED"]
                        )
            .withColumn(colName='shipped_date',
                        colType=TimestampType(),
                        nullable=True,
                        random=True,
                        omit=True,
                        minValue=0.0,
                        maxValue=100.0
                        )
            .withColumn(colName='activation_date',
                        colType=TimestampType(),
                        nullable=True,
                        random=True,
                        omit=True,
                        minValue=0.0,
                        maxValue=100.0
                        )
            .withColumn(colName='carrier',
                        colType=StringType(),
                        random=True,
                        omit=True,
                        values=["fedex","ups","Endicia - DHL GM Parcel Plus Ground","pizza guy?"]
                        )
            .withColumn(colName='tracking_code',
                        colType=StringType(),
                        random=True,
                        omit=True,
                        template=r'DDDDDDDDDDDDDDDDDDDDDD'
                        )
            .withColumn(colName='changed_flag',
                        colType=IntegerType(),
                        values=[1]
                        )
            .withColumn(
                colName="records", 
                colType=StructType([StructField("order_number", StringType()),
                                    StructField("partner_member_id", StringType()),
                                    StructField("shipping_status", StringType()),
                                    StructField("shipped_date", TimestampType()),
                                    StructField("activation_date", TimestampType()),
                                    StructField("carrier", StringType()),
                                    StructField("tracking_code", StringType()),
                                    StructField("changed_flag", IntegerType())
                                    ]
                                    ),
                expr="named_struct('order_number', order_number," + \
                      "'partner_member_id', partner_member_id," +\
                      "'shipping_status', shipping_status," +\
                      "'shipped_date', shipped_date," +\
                      "'activation_date', activation_date," +\
                      "'carrier', carrier," +\
                      "'tracking_code', tracking_code," +\
                      "'changed_flag', changed_flag)",
                baseColumn=["order_number","partner_member_id","shipping_status","shipped_date","activation_date","carrier","tracking_code","changed_flag"]
            ).withColumn(
                colName="_metadata", 
                colType=StructType([StructField("file_path", StringType())]),
                expr="named_struct('file_path', 's3://internal-bucket-123456789110/Notion_Raw/Period/DS_NTN_prd_type_2022_11_01.json')"
            )
        )

    df_test_data = test_data_spec.build()

    df_test_data = df_test_data \
        .groupBy("_metadata") \
            .agg(sum('number_of_records').alias('number_of_records'),
                 collect_list('records').alias('records')) \
             .drop('identifier')

    df_test_data.write.mode("overwrite").json(shipped_installed_daily_path) 

    yield {'path' : shipped_installed_daily_path,
           '_metadata_field' : StructField('_metadata', StructType([StructField('file_path', StringType(), True)]), False)
            }

    if Path(shipped_installed_daily_path).exists():
        shutil.rmtree(shipped_installed_daily_path)

def test_shipped_installed_daily(spark,shipped_installed_daily: str):

    json_path = shipped_installed_daily.get('path')

    _metadata_field = shipped_installed_daily.get('_metadata_field')

    shipped_installed_daily_df = spark.read.json(json_path,
                                                 schema=shipped_installed_daily_schema.add(_metadata_field))

    df = shipped_installed_daily_tf(shipped_installed_daily_df)

    ge_df = ge.dataset.SparkDFDataset(df)

    assert ge_df.expect_column_to_exist('order_number').get('success')
    assert ge_df.expect_column_to_exist('partner_member_id').get('success')
    assert ge_df.expect_column_to_exist('shipping_status').get('success')
    assert ge_df.expect_column_to_exist('shipped_date').get('success')
    assert ge_df.expect_column_to_exist('activation_date').get('success')
    assert ge_df.expect_column_to_exist('carrier').get('success')
    assert ge_df.expect_column_to_exist('tracking_code').get('success')
    assert ge_df.expect_column_to_exist('changed_flag').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='order_number',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='partner_member_id',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='shipping_status',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='shipped_date',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='activation_date',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='carrier',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='tracking_code',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='changed_flag',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_not_be_null('order_number').get('success')
    assert ge_df.expect_column_values_to_not_be_null('partner_member_id').get('success')
    assert ge_df.expect_column_values_to_not_be_null('shipping_status').get('success')
    assert ge_df.expect_column_values_to_not_be_null('shipped_date').get('success')
    assert ge_df.expect_column_values_to_not_be_null('activation_date').get('success')
    assert ge_df.expect_column_values_to_not_be_null('carrier').get('success')
    assert ge_df.expect_column_values_to_not_be_null('tracking_code').get('success')
    assert ge_df.expect_column_values_to_not_be_null('changed_flag').get('success')
    assert len(df.columns) == 13
    assert ge_df.expect_table_row_count_to_equal(10).get('success')
