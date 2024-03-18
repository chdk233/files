from pcd_iot_dbx.tasks.dqi_ingest_check import DQITask

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType
)
import great_expectations as ge
import dbldatagen as dg
import pytest

from datetime import date


@pytest.fixture(scope="function")
def exception_table(spark: SparkSession):

    table = 'default.dqi_ingest_exceptions'

    spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table}
(
table string,
where_clause string,
record_count integer,
exception_reason string,
exception_timestamp timestamp
)
USING PARQUET
"""
    )

    yield table

    spark.sql(f"drop table {table}")


@pytest.fixture(scope="session")
def records_loaded_table(spark: SparkSession):

    table = 'ingest_table'

    row_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="test_data_set1", rows=row_count, partitions=4)
        .withIdOutput()
        .withColumn("load_dt", DateType(), values = [date.today().strftime('%Y-%m-%d')])

    )

    df_test_data = test_data_spec.build()

    df_test_data.write.mode("append").saveAsTable(table) 

    yield table

@pytest.fixture(scope="session")
def no_records_loaded_table(spark: SparkSession):

    table = 'ingest_table_two'

    row_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="test_data_set1", rows=row_count, partitions=4)
        .withIdOutput()
        .withColumn("load_dt", DateType(), values = ['2023-10-01'])

    )

    df_test_data = test_data_spec.build()

    df_test_data.write.mode("append").saveAsTable(table) 

    yield table


def test_dqi_ingest_check_pass(spark,records_loaded_table,exception_table):

    
    task = DQITask(spark=spark,init_conf={
        'name' : 'dqi_ingest_check',
        'read_stream' : {
            'table_source' : records_loaded_table
        },
        'dqi_conf' : {
            'target_table' : exception_table,
            'mode' : 'append',
            'source_partition_column' : 'load_dt'
        },
        'alert_conf': {
            'email_to' : '56a3a54a.nationwide.com@amer.teams.ms',
            'email_from' : 'iot_ut@nationwide.com',
            'email_subject' : 'DQI UT'
        }
        })
    
    task.launch()

    assert spark.read.table(exception_table).count() == 0

def test_dqi_ingest_check_fail(spark,no_records_loaded_table,exception_table):

    
    task = DQITask(spark=spark,init_conf={
        'name' : 'dqi_ingest_check',
        'read_stream' : {
            'table_source' : no_records_loaded_table
        },
        'dqi_conf' : {
            'target_table' : exception_table,
            'mode' : 'append',
            'source_partition_column' : 'load_dt'
        },
        'alert_conf': {
            'email_to' : '56a3a54a.nationwide.com@amer.teams.ms',
            'email_from' : 'iot_ut@nationwide.com',
            'email_subject' : 'DQI UT'
        }
        })
    
    task.launch()

    assert spark.read.table(exception_table).count() > 0


