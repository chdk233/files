from pcd_iot_dbx.etl.transformations.rivian.rivian_ingestion import av_aggregate as av_aggregate_tf
from pcd_iot_dbx.etl.schemas.rivian.rivian_ingestion import av_aggregate as av_aggregate_schema
from pyspark.sql import SparkSession
from pyspark.sql.types import (
   IntegerType,
    StringType,
    DoubleType,
)
from pyspark.sql.functions import collect_list, sum,lit,struct
import great_expectations as ge
import dbldatagen as dg
import pytest
from pathlib import Path
import shutil

@pytest.fixture(scope="session")
def av_aggregate(spark: SparkSession):
    av_aggregate_path = './data/rivian/av_aggregate_test.csv'
    row_count = 10
    test_data_spec = (
            dg.DataGenerator(spark, name="test_av_aggregate", rows=row_count, partitions=1)
            .withColumn(colName='vin',
                        colType=StringType(),
                        nullable=False,
                        random=True,
                        template=r'\Pdddddddd'
                        )
            .withColumn(colName='av_optin_date',
                        colType="timestamp", #looks like SQL timestamp data type, UTC
                        nullable=False,
                        random=True,
                        begin="2023-09-01 01:00:00",
                        end="2023-10-01 23:59:00",
                        interval="1 second"
                        )
            .withColumn(colName='last_av_optout_date',
                        colType="timestamp", #looks like SQL timestamp data type, UTC
                        nullable=False,
                        random=True,
                        begin="2023-09-01 01:00:00",
                        end="2023-10-01 23:59:00",
                        interval="1 second"
                        )
            .withColumn(colName='av_elapsed_days',
                        colType=IntegerType(),
                        values=[1]
                        )
            .withColumn(colName='pol_eff_date',
                        colType="timestamp", #looks like SQL timestamp data type, UTC
                        nullable=False,
                        random=True,
                        begin="2023-09-01 01:00:00",
                        end="2023-10-01 23:59:00",
                        interval="1 second"
                        )
            .withColumn(colName='pol_exp_date',
                        colType="timestamp", #looks like SQL timestamp data type, UTC
                        nullable=False,
                        random=True,
                        begin="2023-09-01 01:00:00",
                        end="2023-10-01 23:59:00",
                        interval="1 second"
                        )
            .withColumn(colName='total_distance',
                        colType=DoubleType(),
                        nullable=True,
                        random=True,
                        minValue=0.0,
                        maxValue=100.0
                        )
            .withColumn(colName='total_duration',
                        colType=DoubleType(),
                        nullable=True,
                        random=True,
                        minValue=0.0,
                        maxValue=100.0
                        )
            .withColumn(colName='av_total_distance',
                        colType=DoubleType(),
                        nullable=True,
                        random=True,
                        minValue=0.0,
                        maxValue=100.0
                        )
            .withColumn(colName='av_total_duration',
                        colType=DoubleType(),
                        nullable=True,
                        random=True,
                        minValue=0.0,
                        maxValue=100.0
                        )
            .withColumn(colName='av_score',
                        colType=DoubleType(),
                        nullable=True,
                        random=True,
                        minValue=0.0,
                        maxValue=100.0
                        )
            .withColumn(colName='av_discount',
                        colType=DoubleType(),
                        nullable=True,
                        random=True,
                        minValue=0.0,
                        maxValue=100.0
                        )
        )

    df_test_data = test_data_spec.build()

    df_test_data.write.mode("overwrite").csv(av_aggregate_path)

    yield {'path' : av_aggregate_path}


    if Path(av_aggregate_path).exists():
        shutil.rmtree(av_aggregate_path)

def test_av_aggregate(spark,av_aggregate: str):

    csv_path = av_aggregate.get('path')

    # _metadata_field = av_aggregate.get('_metadata_field')

    av_aggregate_df = spark.read.csv(csv_path,
                                      schema=av_aggregate_schema).withColumn("_metadata",struct( lit('s3://internal-bucket-123456789110/rivian/daily/av_aggregate_2022_11_01.csv').alias("file_path")))
    
    df = av_aggregate_tf(av_aggregate_df)

    ge_df = ge.dataset.SparkDFDataset(df)
    
    assert ge_df.expect_column_to_exist('vin').get('success')
    assert ge_df.expect_column_to_exist('av_optin_date').get('success')
    assert ge_df.expect_column_to_exist('last_av_optout_date').get('success')
    assert ge_df.expect_column_to_exist('av_elapsed_days').get('success')
    assert ge_df.expect_column_to_exist('pol_eff_date').get('success')
    assert ge_df.expect_column_to_exist('pol_exp_date').get('success')
    assert ge_df.expect_column_to_exist('total_distance').get('success')
    assert ge_df.expect_column_to_exist('total_duration').get('success')
    assert ge_df.expect_column_to_exist('av_total_distance').get('success')
    assert ge_df.expect_column_to_exist('av_total_duration').get('success')
    assert ge_df.expect_column_to_exist('av_score').get('success')
    assert ge_df.expect_column_to_exist('av_discount').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='vin',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='av_optin_date',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='last_av_optout_date',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='av_elapsed_days',
                                                    type_='IntegerType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='pol_eff_date',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='pol_exp_date',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='total_distance',
                                                    type_='DoubleType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='total_duration',
                                                    type_='DoubleType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='av_total_distance',
                                                    type_='DoubleType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='av_total_duration',
                                                    type_='DoubleType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='av_score',
                                                    type_='DoubleType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='av_discount',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_not_be_null('vin').get('success')
    assert ge_df.expect_column_values_to_not_be_null('av_optin_date').get('success')
    assert ge_df.expect_column_values_to_not_be_null('last_av_optout_date').get('success')
    assert ge_df.expect_column_values_to_not_be_null('av_elapsed_days').get('success')
    assert ge_df.expect_column_values_to_not_be_null('pol_eff_date').get('success')
    assert ge_df.expect_column_values_to_not_be_null('pol_exp_date').get('success')
    assert ge_df.expect_column_values_to_not_be_null('total_distance').get('success')
    assert ge_df.expect_column_values_to_not_be_null('total_duration').get('success')
    assert ge_df.expect_column_values_to_not_be_null('av_total_distance').get('success')
    assert ge_df.expect_column_values_to_not_be_null('av_total_duration').get('success')
    assert ge_df.expect_column_values_to_not_be_null('av_score').get('success')
    assert ge_df.expect_column_values_to_not_be_null('av_discount').get('success')
    assert len(df.columns) == 17
    assert ge_df.expect_table_row_count_to_equal(10).get('success')
