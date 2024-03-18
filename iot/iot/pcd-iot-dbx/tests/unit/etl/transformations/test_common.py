from pcd_iot_dbx.etl.transformations.common import (
    set_load_dt_hr_frm_date_in_file_nm, 
    set_src_sys_cd, 
    set_current_datetime_fields, 
    remove_metadata_field,
    set_load_dt_hr_frm_kafka_msg_ts,
    set_key,
    set_key_id,
    set_row_hash,
    set_table_id,
    set_key_fields)
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    TimestampType,
    LongType
)
from pyspark.sql.functions import lit
import great_expectations as ge
import pyspark
import dbldatagen as dg
import pytest
from datetime import date
from itertools import product

LONG_MIN = -9223372036854775808
LONG_MAX = 9223372036854775807

@pytest.fixture(scope="session")
def sample_dataframe_with_metadata(spark: SparkSession):
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
    

@pytest.fixture(scope="session")
def sample_dataframe_with_timestamp(spark: SparkSession):
    row_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="test_metadata_dataset", rows=row_count, partitions=1)
        .withIdOutput()
        .withColumn(
            colName="timestamp", 
            colType=TimestampType(),
            values=["2023-01-01 10:00:00"]
        )
    )

    df_test_data = test_data_spec.build()

    yield df_test_data

@pytest.fixture(scope="session")
def sample_dataframe_keys(spark: SparkSession):
    row_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="test_keys", rows=row_count, partitions=1)
        .withIdOutput()
        .withColumn(
            colName='attr1',
            colType=StringType(),
            values=["blue",
                    "green",
                    "yellow",
                    "red"],
            random=True
        )
        .withColumn(
            colName='attr2',
            colType=StringType(),
            values=["cat",
                    "dog",
                    "mouse",
                    "fox"],
            random=True            
        )
        .withColumn(
            colName='attr3',
            colType=StringType(),
            values=["tree",
                    "grass",
                    "bush",
                    "flower"],
            random=True            
        )
        .withColumn(
            colName='attr4',
            colType=StringType(),
            values=["car",
                    "bike",
                    "train",
                    "truck"],
            random=True            
        )
    )
    df_test_data = test_data_spec.build()

    yield df_test_data

def test_set_src_sys_cd(sample_dataframe_with_metadata: pyspark.sql.DataFrame):

    @set_src_sys_cd(src_sys_cd='TEST')
    def mock_transformation(df: pyspark.sql.DataFrame):
        return df.withColumn('additional_column',lit('value'))
    
    df = mock_transformation(sample_dataframe_with_metadata)

    ge_df = ge.dataset.SparkDFDataset(df)

    assert ge_df.expect_column_to_exist('src_sys_cd').get('success')
    assert ge_df.expect_column_values_to_not_be_null('src_sys_cd').get('success')
    assert ge_df.expect_column_values_to_be_in_set('src_sys_cd',['TEST']).get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='src_sys_cd',
                                                    type_='StringType').get('success')
    assert len(df.columns) == 4


def test_set_current_datetime_fields(sample_dataframe_with_metadata: pyspark.sql.DataFrame):

    @set_current_datetime_fields
    def mock_transformation(df: pyspark.sql.DataFrame):
        return df.withColumn('additional_column',lit('value'))
    
    df = mock_transformation(sample_dataframe_with_metadata)

    ge_df = ge.dataset.SparkDFDataset(df)

    assert ge_df.expect_column_to_exist('db_load_date').get('success')
    assert ge_df.expect_column_values_to_not_be_null('db_load_date').get('success')
    assert ge_df.expect_column_values_to_be_in_set('db_load_date',[date.today()]).get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='db_load_date',
                                                    type_='DateType').get('success')
    assert ge_df.expect_column_to_exist('db_load_time').get('success')
    assert ge_df.expect_column_values_to_not_be_null('db_load_time').get('success')
    assert ge_df.expect_column_values_to_match_regex('db_load_time',date.today().strftime('%Y-%m-%d') + \
                                                      ' \d\d:\d\d:\d\d').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='db_load_time',
                                                    type_='TimestampType').get('success')
    assert len(df.columns) == 5



def test_set_load_dt_hr_frm_date_in_file_nm(sample_dataframe_with_metadata: pyspark.sql.DataFrame):


    @set_load_dt_hr_frm_date_in_file_nm(regex=r"(\d\d\d\d)_(\d\d)_(\d\d).json")
    def mock_transformation(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        return df.withColumn('additional_column',lit('value'))

    df = mock_transformation(sample_dataframe_with_metadata)

    ge_df = ge.dataset.SparkDFDataset(df)
    

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
    assert len(df.columns) == 5

def test_remove_metadata_field(sample_dataframe_with_metadata: pyspark.sql.DataFrame):

    @remove_metadata_field
    def mock_transformation(df: pyspark.sql.DataFrame):
        return df.withColumn('additional_column',lit('value'))
    
    df = mock_transformation(sample_dataframe_with_metadata)

    assert len(df.columns) == 2

def test_set_load_dt_hr_frm_kafka_msg_ts(sample_dataframe_with_timestamp: pyspark.sql.DataFrame):

    @set_load_dt_hr_frm_kafka_msg_ts
    def mock_transformation(df: pyspark.sql.DataFrame):
        return df

    df = mock_transformation(sample_dataframe_with_timestamp)

    ge_df = ge.dataset.SparkDFDataset(df)

    assert ge_df.expect_column_to_exist('id').get('success')
    assert ge_df.expect_column_to_exist('timestamp').get('success')
    assert ge_df.expect_column_to_exist('load_dt').get('success')
    assert ge_df.expect_column_to_exist('load_hr_ts').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='load_dt',
                                                    type_='DateType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='load_hr_ts',
                                                    type_='TimestampType').get('success')
    assert ge_df.expect_column_values_to_be_in_set(
        "load_dt",
        ["2023-01-01"],
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "load_hr_ts",
        ["2023-01-01 10:00:00"],
    ).get("success")
    assert len(df.columns) == 4


def test_decorator_chaining(sample_dataframe_with_metadata):

    @remove_metadata_field
    @set_current_datetime_fields
    @set_load_dt_hr_frm_date_in_file_nm(regex=r"(\d\d\d\d)_(\d\d)_(\d\d).json")
    @set_src_sys_cd(src_sys_cd='TEST')
    def mock_transformation(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        return df.withColumn('additional_column',lit('value'))

    df = mock_transformation(sample_dataframe_with_metadata)

    assert len(df.columns) == 7


def test_set_key(sample_dataframe_keys):
    table_name = 'TEST_TABLE'
    attributes_list = [f"attr{num}" for num in range(1,5)]
    df = set_key(sample_dataframe_keys, attributes_list, table_name)

    ge_df = ge.dataset.SparkDFDataset(df)
    
    attr1 = ["blue", "green", "yellow", "red"]
    attr2 = ["cat", "dog", "mouse", "fox"]
    attr3 = ["tree", "grass", "bush", "flower"]
    attr4 = ["car", "bike", "train", "truck"]

    attr_combinations = list(product(attr1, attr2, attr3, attr4))
    
    attr_combinations_joined = ["".join(combination) for combination in attr_combinations]

    assert ge_df.expect_column_to_exist('id').get('success')
    assert ge_df.expect_column_to_exist('attr1').get('success')
    assert ge_df.expect_column_to_exist('attr2').get('success')
    assert ge_df.expect_column_to_exist('attr3').get('success')
    assert ge_df.expect_column_to_exist('attr4').get('success')
    assert ge_df.expect_column_to_exist(f'{table_name}_key').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr1',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr2',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr3',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr4',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column=f'{table_name}_key',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_in_set(
        "attr1",
        ["blue", "green", "yellow", "red"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "attr2",
        ["cat", "dog", "mouse", "fox"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "attr3",
        ["tree", "grass", "bush", "flower"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "attr4",
        ["car", "bike", "train", "truck"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        f'{table_name}_key',
        attr_combinations_joined
    ).get("success")
    assert len(df.columns) == 6

def test_set_key_id(sample_dataframe_keys):
    table_name = "TEST_TABLE"
    attributes_list = [f"attr{num}" for num in range(1,5)]
    df = set_key(sample_dataframe_keys, attributes_list, table_name)
    df = set_key_id(df, table_name)

    ge_df = ge.dataset.SparkDFDataset(df)

    assert ge_df.expect_column_to_exist('id').get('success')
    assert ge_df.expect_column_to_exist('attr1').get('success')
    assert ge_df.expect_column_to_exist('attr2').get('success')
    assert ge_df.expect_column_to_exist('attr3').get('success')
    assert ge_df.expect_column_to_exist('attr4').get('success')
    assert ge_df.expect_column_to_exist(f'{table_name}_key').get('success')
    assert ge_df.expect_column_to_exist(f'{table_name}_key_id').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr1',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr2',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr3',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr4',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column=f'{table_name}_key',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column=f'{table_name}_key_id',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_in_set(
        "attr1",
        ["blue", "green", "yellow", "red"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "attr2",
        ["cat", "dog", "mouse", "fox"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "attr3",
        ["tree", "grass", "bush", "flower"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "attr4",
        ["car", "bike", "train", "truck"]
    ).get("success") 
    assert len(df.columns) == 7

    # Convert to long type to test values of hash to ensure validity
    df_long = df.withColumn(f'{table_name}_key_id', df[f'{table_name}_key_id'].cast(LongType()))
    ge_df_long = ge.dataset.SparkDFDataset(df_long)

    assert ge_df_long.expect_column_values_to_be_of_type(column=f'{table_name}_key_id',
                                                    type_='LongType').get('success')
    assert ge_df_long.expect_column_values_to_be_between(f'{table_name}_key_id', min_value=LONG_MIN, max_value=LONG_MAX)

def test_set_row_hash(sample_dataframe_keys):
    attributes_list = [f"attr{num}" for num in range(1,5)]
    row_attributes_list = attributes_list[:3]

    df = set_row_hash(sample_dataframe_keys, row_attributes_list)

    ge_df = ge.dataset.SparkDFDataset(df)

    assert ge_df.expect_column_to_exist('id').get('success')
    assert ge_df.expect_column_to_exist('attr1').get('success')
    assert ge_df.expect_column_to_exist('attr2').get('success')
    assert ge_df.expect_column_to_exist('attr3').get('success')
    assert ge_df.expect_column_to_exist('attr4').get('success')
    assert ge_df.expect_column_to_exist('row_hash').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr1',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr2',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr3',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr4',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='row_hash',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_in_set(
        "attr1",
        ["blue", "green", "yellow", "red"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "attr2",
        ["cat", "dog", "mouse", "fox"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "attr3",
        ["tree", "grass", "bush", "flower"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "attr4",
        ["car", "bike", "train", "truck"]
    ).get("success") 
    assert len(df.columns) == 6

    # Convert to long type to test values of hash to ensure validity
    df_long = df.withColumn("row_hash", df['row_hash'].cast(LongType()))
    ge_df_long = ge.dataset.SparkDFDataset(df_long)

    assert ge_df_long.expect_column_values_to_be_of_type(column='row_hash',
                                                    type_='LongType').get('success')
    assert ge_df_long.expect_column_values_to_be_between("row_hash", min_value=LONG_MIN, max_value=LONG_MAX)

def test_set_table_id(sample_dataframe_keys):
    table_name = "TEST_TABLE"

    df = set_table_id(sample_dataframe_keys, table_name)

    ge_df = ge.dataset.SparkDFDataset(df)

    assert ge_df.expect_column_to_exist('id').get('success')
    assert ge_df.expect_column_to_exist('attr1').get('success')
    assert ge_df.expect_column_to_exist('attr2').get('success')
    assert ge_df.expect_column_to_exist('attr3').get('success')
    assert ge_df.expect_column_to_exist('attr4').get('success')
    assert ge_df.expect_column_to_exist(f'{table_name}_id').get('success')

    assert ge_df.expect_column_values_to_be_of_type(column='attr1',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr2',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr3',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr4',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column=f'{table_name}_ID',
                                                    type_='StringType').get('success')
    
    assert ge_df.expect_column_values_to_be_in_set(
        "attr1",
        ["blue", "green", "yellow", "red"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "attr2",
        ["cat", "dog", "mouse", "fox"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "attr3",
        ["tree", "grass", "bush", "flower"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "attr4",
        ["car", "bike", "train", "truck"]
    ).get("success")

    df_long = df.withColumn(f'{table_name}_id', df[f'{table_name}_id'].cast(LongType()))
    ge_df_long = ge.dataset.SparkDFDataset(df_long)

    # Convert to long type to test values of hash to ensure validity
    assert ge_df_long.expect_column_values_to_be_of_type(column=f'{table_name}_id',
                                                    type_='LongType').get('success')
    assert ge_df_long.expect_column_values_to_be_between(f'{table_name}_id', min_value=LONG_MIN, max_value=LONG_MAX)

def test_set_key_fields(sample_dataframe_keys):
    table_name = "TEST_TABLE"
    attributes_list = [f"attr{num}" for num in range(1,5)]
    row_attributes_list = attributes_list[1:4]

    attr1 = ["blue", "green", "yellow", "red"]
    attr2 = ["cat", "dog", "mouse", "fox"]
    attr3 = ["tree", "grass", "bush", "flower"]
    attr4 = ["car", "bike", "train", "truck"]

    attr_combinations = list(product(attr1, attr2, attr3, attr4))
    
    attr_combinations_joined = ["".join(combination) for combination in attr_combinations]

    @set_key_fields(attributes=attributes_list, row_attributes=row_attributes_list, table_name=table_name)
    def mock_transformation(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        return df
    
    df = mock_transformation(sample_dataframe_keys)

    ge_df = ge.dataset.SparkDFDataset(df)

    assert ge_df.expect_column_to_exist('id').get('success')
    assert ge_df.expect_column_to_exist('attr1').get('success')
    assert ge_df.expect_column_to_exist('attr2').get('success')
    assert ge_df.expect_column_to_exist('attr3').get('success')
    assert ge_df.expect_column_to_exist('attr4').get('success')
    assert ge_df.expect_column_to_exist(f'{table_name}_key').get('success')
    assert ge_df.expect_column_to_exist(f'{table_name}_key').get('success')
    assert ge_df.expect_column_to_exist('row_hash').get('success')
    assert ge_df.expect_column_to_exist(f'{table_name}_id').get('success')

    assert ge_df.expect_column_values_to_be_of_type(column='attr1',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr2',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr3',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='attr4',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column=f'{table_name}_key',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column=f'{table_name}_key_id',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column='row_hash',
                                                    type_='StringType').get('success')
    assert ge_df.expect_column_values_to_be_of_type(column=f'{table_name}_id',
                                                    type_='StringType').get('success')

    assert ge_df.expect_column_values_to_be_in_set(
        "attr1",
        ["blue", "green", "yellow", "red"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "attr2",
        ["cat", "dog", "mouse", "fox"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "attr3",
        ["tree", "grass", "bush", "flower"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "attr4",
        ["car", "bike", "train", "truck"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        f'{table_name}_key',
        attr_combinations_joined
    ).get("success")

    # Convert to long type to test values of hash to ensure validity
    df_long = df.withColumn(f'{table_name}_key_id', df[f'{table_name}_key_id'].cast(LongType())) \
                .withColumn("row_hash", df['row_hash'].cast(LongType())) \
                .withColumn(f'{table_name}_id', df[f'{table_name}_id'].cast(LongType()))
    ge_df_long = ge.dataset.SparkDFDataset(df_long)

    assert ge_df_long.expect_column_values_to_be_of_type(column=f'{table_name}_key_id',
                                                    type_='LongType').get('success')
    assert ge_df_long.expect_column_values_to_be_of_type(column='row_hash',
                                                    type_='LongType').get('success')
    assert ge_df_long.expect_column_values_to_be_of_type(column=f'{table_name}_id',
                                                    type_='LongType').get('success')
    
    assert ge_df_long.expect_column_values_to_be_between(f'{table_name}_key_id', min_value=LONG_MIN, max_value=LONG_MAX)

    assert ge_df_long.expect_column_values_to_be_between("row_hash", min_value=LONG_MIN, max_value=LONG_MAX)
    assert ge_df_long.expect_column_values_to_be_between(f'{table_name}_id', min_value=LONG_MIN, max_value=LONG_MAX)
    assert len(df.columns) == 9