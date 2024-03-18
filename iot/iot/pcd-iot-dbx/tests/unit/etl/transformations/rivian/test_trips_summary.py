from pcd_iot_dbx.etl.transformations.rivian.rivian_ingestion import trips_summary as trips_summary_tf
from pcd_iot_dbx.etl.schemas.rivian.rivian_ingestion import trips_summary as trips_summary_schema
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
def trips_summary(spark: SparkSession):
    trips_summary_path = './data/rivian/trips_summary_test.csv'
    row_count = 10
    test_data_spec = (
            dg.DataGenerator(spark, name="test_trips_summary", rows=row_count, partitions=1)
            .withColumn(colName='vin',
                        colType=StringType(),
                        random=True,
                        nullable=True,
                        template=r'0\xkkkkkkkkkkkkkkkk'
                        )
            .withColumn(colName='trip_id',
                        colType=StringType(),
                        random=True,
                        nullable=True,
                        template=r'0\xkkkkkkkkkkkkkkkk'
                        )
            .withColumn(colName='trip_start',
                        colType="timestamp", #looks like SQL timestamp data type, UTC
                        nullable=False,
                        random=True,
                        begin="2023-09-01 01:00:00",
                        end="2023-10-01 23:59:00",
                        interval="1 second"
                        )
            .withColumn(colName='trip_end',
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
            .withColumn(colName='distance',
                        colType=DoubleType(),
                        nullable=True,
                        random=True,
                        minValue=0.0,
                        maxValue=100.0
                        )
            .withColumn(colName='duration',
                        colType=DoubleType(),
                        nullable=True,
                        random=True,
                        minValue=0.0,
                        maxValue=100.0
                        )
            .withColumn(colName='av_distance',
                        colType=DoubleType(),
                        nullable=True,
                        random=True,
                        minValue=0.0,
                        maxValue=100.0
                        )
            .withColumn(colName='av_duration',
                        colType=DoubleType(),
                        nullable=True,
                        random=True,
                        minValue=0.0,
                        maxValue=100.0
                        )
        )

    df_test_data = test_data_spec.build()


    df_test_data.write.mode("overwrite").csv(trips_summary_path)

    yield {'path' : trips_summary_path}


    if Path(trips_summary_path).exists():
        shutil.rmtree(trips_summary_path)

def test_trips_summary(spark,trips_summary: str):

    csv_path = trips_summary.get('path')

    # _metadata_field = trips_summary.get('_metadata_field')

    trips_summary_df = spark.read.csv(csv_path,
                                      schema=trips_summary_schema).withColumn("_metadata",struct( lit('s3://internal-bucket-123456789110/rivian/daily/av_aggregate_2022_11_01.csv').alias("file_path")))
    

    df = trips_summary_tf(trips_summary_df)

    ge_df = ge.dataset.SparkDFDataset(df)
    
    assert ge_df.expect_column_to_exist('vin').get('success')
    assert ge_df.expect_column_to_exist('trip_id').get('success')
    assert ge_df.expect_column_to_exist('trip_start').get('success')
    assert ge_df.expect_column_to_exist('trip_end').get('success')
    assert ge_df.expect_column_to_exist('tzo').get('success')
    assert ge_df.expect_column_to_exist('distance').get('success')
    assert ge_df.expect_column_to_exist('duration').get('success')
    assert ge_df.expect_column_to_exist('av_distance').get('success')
    assert ge_df.expect_column_to_exist('av_duration').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='vin',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='trip_id',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='trip_start',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='trip_end',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='tzo',
                                                    type_='IntegerType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='distance',
                                                    type_='DoubleType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='duration',
                                                    type_='DoubleType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='av_distance',
                                                    type_='DoubleType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='av_duration',
                                                    type_='DoubleType').get('success')
    assert ge_df.expect_column_values_to_not_be_null('vin').get('success')
    assert ge_df.expect_column_values_to_not_be_null('trip_id').get('success')
    assert ge_df.expect_column_values_to_not_be_null('trip_start').get('success')
    assert ge_df.expect_column_values_to_not_be_null('trip_end').get('success')
    assert ge_df.expect_column_values_to_not_be_null('tzo').get('success')
    assert ge_df.expect_column_values_to_not_be_null('distance').get('success')
    assert ge_df.expect_column_values_to_not_be_null('duration').get('success')
    assert ge_df.expect_column_values_to_not_be_null('av_distance').get('success')
    assert ge_df.expect_column_values_to_not_be_null('av_duration').get('success')
    assert len(df.columns) == 14
    assert ge_df.expect_table_row_count_to_equal(10).get('success')
