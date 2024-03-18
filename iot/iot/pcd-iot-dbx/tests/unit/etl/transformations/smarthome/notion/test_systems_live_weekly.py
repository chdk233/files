from pcd_iot_dbx.etl.transformations.smarthome.notion import systems_live_weekly as systems_live_weekly_tf
from pcd_iot_dbx.etl.schemas.smarthome.notion import systems_live_weekly as systems_live_weekly_schema
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)
from pyspark.sql.functions import collect_list, sum
import great_expectations as ge
import dbldatagen as dg
import pytest
from pathlib import Path
import shutil

@pytest.fixture(scope="session")
def systems_live_weekly(spark: SparkSession):
    systems_live_weekly_path = './data/systems_live_weekly_test.json'
    row_count = 10
    test_data_spec = (
            dg.DataGenerator(spark, name="test_systems_live_weekly", rows=row_count, partitions=1)
            .withColumn(colName='identifier',
                        colType=IntegerType(),
                        values=[1,2]
                        )
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
            .withColumn(colName='live_status',
                        colType=IntegerType(),
                        omit=True,
                        values=[1,0],
                        weights=[9,1]
                        )
            .withColumn(colName='subscriptions',
                        colType=StringType(),
                        omit=True,
                        values=["","pro"],
                        weights=[9,1]
                        )
            .withColumn(
                colName="records", 
                colType=StructType([StructField("partner_member_id", StringType()),
                                    StructField("live_status", IntegerType()),
                                    StructField("subscriptions", StringType())
                                    ]
                                    ),
                expr="named_struct('partner_member_id', partner_member_id, 'live_status', live_status, 'subscriptions', subscriptions)",
                baseColumn=["partner_member_id","live_status","subscriptions"]
            ).withColumn(
                colName="_metadata", 
                colType=StructType([StructField("file_path", StringType())]),
                expr="named_struct('file_path', 's3://internal-bucket-123456789110/Notion_Raw/Period/DS_NTN_prd_type_2022_11_01.json')"
            )
        )

    df_test_data = test_data_spec.build()

    df_test_data = df_test_data \
        .groupBy('identifier','_metadata') \
            .agg(sum('number_of_records').alias('number_of_records'),
                 collect_list('records').alias('records')) \
             .drop('identifier')

    df_test_data.write.mode("overwrite").json(systems_live_weekly_path) 

    yield {'path' : systems_live_weekly_path,
           '_metadata_field' : StructField('_metadata', StructType([StructField('file_path', StringType(), True)]), False)
            }

    if Path(systems_live_weekly_path).exists():
        shutil.rmtree(systems_live_weekly_path)

def test_systems_live_weekly(spark,systems_live_weekly: str):

    json_path = systems_live_weekly.get('path')

    _metadata_field = systems_live_weekly.get('_metadata_field')

    systems_live_weekly_df = spark.read.json(json_path,
                                             schema=systems_live_weekly_schema.add(_metadata_field))

    df = systems_live_weekly_tf(systems_live_weekly_df)

    ge_df = ge.dataset.SparkDFDataset(df)

    assert ge_df.expect_column_to_exist('partner_member_id').get('success')
    assert ge_df.expect_column_to_exist('live_status').get('success')
    assert ge_df.expect_column_to_exist('subscriptions').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='partner_member_id',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='live_status',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='subscriptions',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_not_be_null('partner_member_id').get('success')
    assert ge_df.expect_column_values_to_not_be_null('live_status').get('success')
    assert ge_df.expect_column_values_to_not_be_null('subscriptions').get('success')
    assert len(df.columns) == 8
    assert ge_df.expect_table_row_count_to_equal(10).get('success')
