from pcd_iot_dbx.etl.transformations.rivian.rivian_ingestion import event_summary as event_summary_tf
from pcd_iot_dbx.etl.schemas.rivian.rivian_ingestion import event_summary as event_summary_schema
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DoubleType,
)
from pyspark.sql.functions import collect_list, sum,lit,struct
import great_expectations as ge
import dbldatagen as dg
import pytest
from pathlib import Path
import shutil

@pytest.fixture(scope="session")
def event_summary(spark: SparkSession):
    event_summary_path = './data/rivian/event_summary_test.csv'
    row_count = 10
    test_data_spec = (
            dg.DataGenerator(spark, name="test_event_summary", rows=row_count, partitions=1)
            .withColumn(colName='vin',
                        colType=StringType(),
                        nullable=False,
                        random=True,
                        template=r'\Pdddddddd'
                        )
            .withColumn(colName='event_id',
                        colType=StringType(),
                        nullable=False,
                        random=True,
                        template=r'\Pdddddddd'
                        )
            .withColumn(colName='event_name',
                        colType=StringType(),
                        nullable=False,
                        random=True,
                        template=r'\Pdddddddd'
                        )
            .withColumn(colName='event_timestamp',
                        colType="timestamp", #looks like SQL timestamp data type, UTC
                        nullable=False,
                        random=True,
                        begin="2023-09-01 01:00:00",
                        end="2023-10-01 23:59:00",
                        interval="1 second"
                        )
            .withColumn(colName='tzo',
                        colType=IntegerType(),
                        values=[1]
                        )
        )

    df_test_data = test_data_spec.build()


    df_test_data.write.mode("overwrite").csv(event_summary_path)

    yield {'path' : event_summary_path}


    if Path(event_summary_path).exists():
        shutil.rmtree(event_summary_path)

def test_event_summary(spark,event_summary: str):

    csv_path = event_summary.get('path')

    # _metadata_field = event_summary.get('_metadata_field')

    event_summary_df = spark.read.csv(csv_path,
                                      schema=event_summary_schema).withColumn("_metadata",struct( lit('s3://internal-bucket-123456789110/rivian/daily/av_aggregate_2022_11_01.csv').alias("file_path")))

    df = event_summary_tf(event_summary_df)

    ge_df = ge.dataset.SparkDFDataset(df)
    
    assert ge_df.expect_column_to_exist('vin').get('success')
    assert ge_df.expect_column_to_exist('event_id').get('success')
    assert ge_df.expect_column_to_exist('event_name').get('success')
    assert ge_df.expect_column_to_exist('event_timestamp').get('success')
    assert ge_df.expect_column_to_exist('tzo').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='vin',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='event_id',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='event_name',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='event_timestamp',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='tzo',
                                                    type_='IntegerType').get('success')
    assert ge_df.expect_column_values_to_not_be_null('vin').get('success')
    assert ge_df.expect_column_values_to_not_be_null('event_id').get('success')
    assert ge_df.expect_column_values_to_not_be_null('event_name').get('success')
    assert ge_df.expect_column_values_to_not_be_null('event_timestamp').get('success')
    assert ge_df.expect_column_values_to_not_be_null('tzo').get('success')
    assert len(df.columns) == 10
    assert ge_df.expect_table_row_count_to_equal(10).get('success')
