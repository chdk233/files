from pcd_iot_dbx.etl.transformations.common import set_etl_fields_using_file_metadata
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
)
import great_expectations as ge
import pyspark
import dbldatagen as dg
import pytest
from datetime import date

@pytest.fixture(scope="session")
def sample_dataframe(spark: SparkSession):
    row_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="test_metadata_dataset", rows=row_count, partitions=1)
        .withIdOutput()
        .withColumn(
            colName="_metadata", 
            colType=StructType([StructField("file_path", StringType())]),
            expr="named_struct('file_path', 's3://internal-bucket-123456789110/Notion_Raw/Period/DS_NTN_prd_type_2022_11_01.json')"
        )
    )

    df_test_data = test_data_spec.build()

    yield df_test_data


def test_set_etl_fields_using_file_metadata(sample_dataframe: pyspark.sql.DataFrame):

    today = date.today().strftime('%Y-%m-%d')

    df = set_etl_fields_using_file_metadata(sample_dataframe, "TEST")

    ge_df = ge.dataset.SparkDFDataset(df)

    assert ge_df.expect_column_to_exist('src_sys_cd').get('success')
    assert ge_df.expect_column_values_to_not_be_null('src_sys_cd').get('success')
    assert ge_df.expect_column_values_to_be_in_set('src_sys_cd',['TEST']).get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='src_sys_cd',
                                                    type_='StringType').get('success')

    assert ge_df.expect_column_to_exist('load_dt').get('success')
    assert ge_df.expect_column_values_to_not_be_null('load_dt').get('success')
    assert ge_df.expect_column_values_to_be_in_set('load_dt',['2022-11-01']).get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='load_dt',
                                                    type_='DateType').get('success')

    assert ge_df.expect_column_to_exist('load_hr_ts').get('success')
    assert ge_df.expect_column_values_to_not_be_null('load_hr_ts').get('success')
    assert ge_df.expect_column_values_to_be_in_set('load_hr_ts',['2022-11-01 00:00:00']).get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='load_hr_ts',
                                                    type_='TimestampType').get('success')

    assert ge_df.expect_column_to_exist('db_load_time').get('success')
    assert ge_df.expect_column_values_to_not_be_null('db_load_time').get('success')
    assert ge_df.expect_column_values_to_match_regex('db_load_time',today + ' \d\d:\d\d:\d\d').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='db_load_time',
                                                    type_='TimestampType').get('success')

    assert ge_df.expect_column_to_exist('db_load_date').get('success')
    assert ge_df.expect_column_values_to_not_be_null('db_load_date').get('success')
    assert ge_df.expect_column_values_to_be_in_set('db_load_date',[today]).get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='db_load_date',
                                                    type_='DateType').get('success')
