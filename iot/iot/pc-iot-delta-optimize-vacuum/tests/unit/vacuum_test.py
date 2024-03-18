from pc_iot_delta_optimize_vacuum.tasks.vacuum import VacuumTask
from pyspark.sql import SparkSession
import pytest
import logging

from os import listdir
import dbldatagen as dg
from pyspark.sql.types import FloatType, IntegerType, StringType

from delta.tables import DeltaTable


@pytest.fixture(scope="session", autouse=True)
def prepare_data(spark: SparkSession, constants: dict):

    logging.info("Preparing test data set")
    row_count = 1000
    column_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="test_data_set1", rows=row_count, partitions=4)
        .withIdOutput()
        .withColumn(
            "r",
            FloatType(),
            expr="floor(rand() * 350) * (86400 + 3600)",
            numColumns=column_count,
        )
        .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
        .withColumn("code2", "integer", minValue=0, maxValue=10, random=True)
        .withColumn("code3", StringType(), values=["online", "offline", "unknown"])
        .withColumn(
            "code4", StringType(), values=["a", "b", "c"], random=True, percentNulls=0.05
        )
        .withColumn(
            "code5", "string", values=["a", "b", "c"], random=True, weights=[9, 1, 1]
        )
    )

    df_test_data = test_data_spec.build()

    df_test_data.write.saveAsTable('mock_table_vacuum', format='delta')

    dt = DeltaTable.forName(spark,tableOrViewName='mock_table_vacuum')

    dt.optimize().executeCompaction()

def test_vacuum(spark: SparkSession, constants: dict):
    logging.info("Testing the vacuum job")
    MOCK_TABLE_NAME = 'mock_table_vacuum'
    initial_row_count = spark.read.table(MOCK_TABLE_NAME).count()
    initial_file_count = len(listdir(f"{constants.get('data_path')}/{MOCK_TABLE_NAME}"))
    spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled','false')
    etl_job = VacuumTask(spark,{'table_specs' : {'table' : MOCK_TABLE_NAME,
                                                 'schema': 'default',
                                                 'retention_hours' : 0.0}})
    etl_job.launch()
    after_vacuum_row_count = spark.read.table(MOCK_TABLE_NAME).count()
    after_vacuum_file_count = len(listdir(f"{constants.get('data_path')}/{MOCK_TABLE_NAME}"))
    assert initial_row_count == after_vacuum_row_count
    assert initial_file_count > after_vacuum_file_count
