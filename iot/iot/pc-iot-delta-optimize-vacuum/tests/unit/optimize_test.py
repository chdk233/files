from pc_iot_delta_optimize_vacuum.tasks.optimize import OptimizeTask
from pc_iot_delta_optimize_vacuum.common import PartitionSpecs
from pyspark.sql import SparkSession
import pytest
import logging
from datetime import date, timedelta

from os import listdir
import dbldatagen as dg
from pyspark.sql.types import FloatType, IntegerType, StringType
from pyspark.sql.functions import lit

from delta.tables import DeltaTable

from pydantic  import ValidationError


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

    df_test_data.persist()

    df_test_data.write.save(f"{constants.get('data_path')}/mock_path",format="delta",mode="overwrite")

    df_test_data.write.saveAsTable('mock_table', format='delta')

    (df_test_data
        .withColumn('load_dt', lit('2023-05-12'))
        .write
        .partitionBy('load_dt')
        .saveAsTable('mock_table_partitioned', 
                    format='delta',
                    mode='append'))

    (df_test_data
        .withColumn('load_dt', lit('2023-05-13'))
        .write
        .partitionBy('load_dt')
        .saveAsTable('mock_table_partitioned', 
                    format='delta',
                    mode='append'))
    
    df_test_data.write.saveAsTable('mock_table_zorder', format='delta')

def test_validate_job_specs(spark):

    with pytest.raises(ValidationError):
      OptimizeTask(spark,{})._validate_job_specs()

    with pytest.raises(ValidationError):
      OptimizeTask(spark, 
                   {'table_specs' : {'path': 'test','table':'test'}}
                   )._validate_job_specs()
      
    with pytest.raises(ValidationError):
      OptimizeTask(spark, 
                   {'table_specs' : {'table':'test'}}
                   )._validate_job_specs()
    
    with pytest.raises(ValidationError):
      OptimizeTask(spark, 
                   {'table_specs' : {'schema':'test'}}
                   )._validate_job_specs()


def test_optimize_path(spark: SparkSession, constants: dict):
    logging.info("Testing the optimize job")
    MOCK_TABLE_PATH = f"{constants.get('data_path')}/mock_path"
    initial_row_count = spark.read.format("delta").load(MOCK_TABLE_PATH).count()
    initial_file_count = len(listdir(MOCK_TABLE_PATH))
    etl_job = OptimizeTask(spark,{'table_specs' : {'path' : MOCK_TABLE_PATH}})
    etl_job.launch()
    after_optimize_row_count = spark.read.format("delta").load(MOCK_TABLE_PATH).count()
    after_optimize_file_count = len(listdir(MOCK_TABLE_PATH))
    assert initial_row_count == after_optimize_row_count
    assert initial_file_count < after_optimize_file_count

def test_optimize_table(spark: SparkSession, constants: dict):
    logging.info("Testing the optimize job")
    MOCK_TABLE_NAME = 'mock_table'
    initial_row_count = spark.read.table(MOCK_TABLE_NAME).count()
    initial_file_count = len(listdir(f"{constants.get('data_path')}/{MOCK_TABLE_NAME}"))
    etl_job = OptimizeTask(spark,{'table_specs' : {'table' : MOCK_TABLE_NAME, 'schema': 'default'}})
    etl_job.launch()
    after_optimize_row_count = spark.read.table(MOCK_TABLE_NAME).count()
    after_optimize_file_count = len(listdir(f"{constants.get('data_path')}/{MOCK_TABLE_NAME}"))
    assert initial_row_count == after_optimize_row_count
    assert initial_file_count < after_optimize_file_count

def test_optimize_table_partition(spark: SparkSession, constants: dict):
    logging.info("Testing the optimize job")
    MOCK_TABLE_NAME = 'mock_table_partitioned'
    initial_row_count = spark.read.table(MOCK_TABLE_NAME).count()
    initial_file_count_non_opt = len(listdir(f"{constants.get('data_path')}/{MOCK_TABLE_NAME}/load_dt=2023-05-12"))
    initial_file_count_opt = len(listdir(f"{constants.get('data_path')}/{MOCK_TABLE_NAME}/load_dt=2023-05-13"))
    etl_job = OptimizeTask(spark,{'table_specs' : {'table' : MOCK_TABLE_NAME, 
                                                   'schema': 'default',
                                                   'partition' : {"expression" : "load_dt='2023-05-13'" }}})
    etl_job.launch()
    after_optimize_row_count = spark.read.table(MOCK_TABLE_NAME).count()
    after_optimize_file_count_non_opt = len(listdir(f"{constants.get('data_path')}/{MOCK_TABLE_NAME}/load_dt=2023-05-12"))
    after_optimize_file_count_opt = len(listdir(f"{constants.get('data_path')}/{MOCK_TABLE_NAME}/load_dt=2023-05-13"))
    assert initial_row_count == after_optimize_row_count
    assert initial_file_count_non_opt == after_optimize_file_count_non_opt
    assert initial_file_count_opt < after_optimize_file_count_opt

def test_zorder(spark: SparkSession, constants: dict):
    MOCK_TABLE_NAME = 'mock_table_zorder'
    etl_job = OptimizeTask(spark,{'table_specs' : {'table' : MOCK_TABLE_NAME,
                                                   'schema': 'default',
                                                   'zorder' : ['id']}})
    etl_job.launch()
    dt = DeltaTable.forName(spark,MOCK_TABLE_NAME)
    zorder = dt.history().collect()[0].operationParameters.get('zOrderBy')
    assert '["id"]' == zorder

def test_format_partition_spec_static():
    static_partition_spec_conf = {
                                "static": [
                                    {
                                        "source_cd": "SOURCE_SYS_A"
                                    },
                                    {
                                        "region_cd": [
                                            "REGION_A",
                                            "REGION_B"
                                        ]
                                    }
                                ]
                            }

    static_partition_spec = PartitionSpecs(**static_partition_spec_conf)
    
    partition_spec = OptimizeTask.format_partition_specs(static_partition_spec)

    assert partition_spec == \
        "source_cd = 'SOURCE_SYS_A' AND region_cd IN ('REGION_A','REGION_B')"
    
def test_format_partition_spec_expression():
    static_partition_spec_conf = {
                                "expression": "my_partition_field = 'some value'"
    }

    static_partition_spec = PartitionSpecs(**static_partition_spec_conf)
    
    partition_spec = OptimizeTask.format_partition_specs(static_partition_spec)

    assert partition_spec == \
        "my_partition_field = 'some value'"

def test_format_partition_spec_relative_date_days():
    dynamic_partition_spec_conf = \
                            {
                                "relative_date": 
                                    {
                                        'load_dt' : {
                                            'days' : -1
                                        }
                                    }
                            }
    
    dynamic_partition_spec = PartitionSpecs(**dynamic_partition_spec_conf)
    
    partition_spec = OptimizeTask.format_partition_specs(dynamic_partition_spec)

    assert partition_spec == \
        f"load_dt = '{(date.today() + timedelta(days=-1)).strftime('%Y-%m-%d')}'"
    
def test_format_partition_spec_relative_date_weeks():
    dynamic_partition_spec_conf = \
                            {
                                "relative_date": 
                                    {
                                        'load_dt' : {
                                            'weeks' : -2
                                        }
                                    }
                            }
    
    dynamic_partition_spec = PartitionSpecs(**dynamic_partition_spec_conf)
    
    partition_spec = OptimizeTask.format_partition_specs(dynamic_partition_spec)

    assert partition_spec == \
        f"load_dt = '{(date.today() + timedelta(weeks=-2)).strftime('%Y-%m-%d')}'"
    
def test_format_partition_spec_relative_date_days_and_weeks():
    dynamic_partition_spec_conf = \
                            {
                                "relative_date": 
                                    {
                                        'load_dt' : {
                                            'weeks' : -2,
                                            'days' : 1
                                        }
                                    }
                            }
    
    dynamic_partition_spec = PartitionSpecs(**dynamic_partition_spec_conf)
    
    partition_spec = OptimizeTask.format_partition_specs(dynamic_partition_spec)

    assert partition_spec == \
        f"load_dt = '{(date.today() + timedelta(weeks=-2, days=1)).strftime('%Y-%m-%d')}'"