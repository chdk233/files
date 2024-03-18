from pcd_iot_dbx.tasks.kafka_ingest import KafkaIngest
from pcd_iot_dbx.etl.transformations.common import straight_move
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType
)
import great_expectations as ge
import dbldatagen as dg
import pytest
from pathlib import Path
import shutil



@pytest.fixture(scope="session")
def input_details(spark: SparkSession):
    test_kafka_ingestion_path = './data/test_kafka_ingestion.json'
    checkpoint_path = './data/checkpoints/test_auto_loader_ingest'
    row_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="test_kafka_dataset", rows=row_count, partitions=4)
        .withIdOutput()
        .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
        .withColumn("code2", "integer", minValue=0, maxValue=10, random=True)
        .withColumn("code3", StringType(), values=["online", "offline", "unknown"])
    )

    df_test_data = test_data_spec.build()

    df_test_data.write.mode("overwrite").json(test_kafka_ingestion_path) 

    yield {'ingest_path' : test_kafka_ingestion_path, 
           'checkpoint_path' : checkpoint_path,
           'schema' : df_test_data.schema}
    
    if Path(test_kafka_ingestion_path).exists():
        shutil.rmtree(test_kafka_ingestion_path)

    if Path(checkpoint_path).exists():
        shutil.rmtree(checkpoint_path)


def test_kafka_ingest(spark: SparkSession,input_details: dict):

    target_table = 'test_kafka_ingestion_output'

    target_schema = 'default'

    conf = {
        'program' : 'test',
        'name' : 'auto_loader_test',
        'src_sys_cd' : 'TEST_AUTO_LOADER',
        'read_stream' : {
            'format' : 'json',
            'options' : {
                'pathGlobFilter' : '*.json'
            },
            'source_s3_location' : input_details.get('ingest_path') + "/*"
        },
        'write_stream' : {
            'trigger' : {
                'availableNow' : True
            },
            'options' : {
                'checkpointLocation' : input_details.get('checkpoint_path')
            }
        },
        'micro_batch' : {
             'mode' : 'append',
             'save_as' : {
                 'table' : target_table,
                 'schema' : target_schema
             }
        }
    }

    task = KafkaIngest(init_conf=conf)

    task.launch(schema=input_details.get('schema'),
                transformation_func=straight_move)
    
    df = spark.read.table(target_schema + '.' + target_table)

    ge_df = ge.dataset.SparkDFDataset(df)

    assert ge_df.expect_column_to_exist('id').get('success')
    assert ge_df.expect_column_to_exist('code1').get('success')
    assert ge_df.expect_column_to_exist('code2').get('success')
    assert ge_df.expect_column_to_exist('code3').get('success')
    assert ge_df.expect_column_to_exist('db_load_time').get('success')
    assert ge_df.expect_column_to_exist('db_load_date').get('success')
    assert ge_df.expect_column_values_to_not_be_null('id').get('success')
    assert ge_df.expect_column_values_to_not_be_null('code1').get('success')
    assert ge_df.expect_column_values_to_not_be_null('code2').get('success')
    assert ge_df.expect_column_values_to_not_be_null('code3').get('success')
    assert ge_df.expect_column_values_to_not_be_null('db_load_time').get('success')
    assert ge_df.expect_column_values_to_not_be_null('db_load_date').get('success')
    assert ge_df.expect_table_column_count_to_equal(6)
    assert ge_df.expect_table_row_count_to_equal(10)


