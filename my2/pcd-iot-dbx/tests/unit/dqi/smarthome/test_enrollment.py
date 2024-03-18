from pcd_iot_dbx.dqi.smarthome.enrollment import Enrollment
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, TimestampType
import great_expectations as ge
import dbldatagen as dg
import pytest


@pytest.fixture(scope="session")
def created_data_collection_id_null(spark: SparkSession):
    row_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="data_collection_id_null", rows=row_count, partitions=1)
        .withColumn(
            colName="dataCollectionId",
            colType=StringType(),
            percentNulls=0.1,
            template=r"kkkkkkkk-kkkk-kkkk-kkkk-kkkkkkkkkkkk",
        )
        .withColumn(
            colName="type",
            colType=StringType(),
            values=["com.nationwide.pls.telematics.home.programs.enrollment.created"],
        )
    )

    df_test_data = test_data_spec.build()

    yield df_test_data


@pytest.fixture(scope="session")
def created_data_collection_id_not_null(spark: SparkSession):
    row_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="data_collection_id_not_null", rows=row_count, partitions=1)
        .withColumn(
            colName="dataCollectionId",
            colType=StringType(),
            percentNulls=0.0,
            template=r"kkkkkkkk-kkkk-kkkk-kkkk-kkkkkkkkkkkk",
        )
        .withColumn(
            colName="type",
            colType=StringType(),
            values=["com.nationwide.pls.telematics.home.programs.enrollment.created"],
        )
    )

    df_test_data = test_data_spec.build()

    yield df_test_data


@pytest.fixture(scope="session")
def policy_state_null(spark: SparkSession):
    row_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="policy_state_null", rows=row_count, partitions=1)
        .withColumn(
            colName="policyState", colType=StringType(), percentNulls=0.1, template=r"AA"
        )
        .withColumn(
            colName="policyNumber", colType=StringType(), percentNulls=0.0, template=r"DDDDAADDDDDD"
        )
        .withColumn(
            colName="type",
            colType=StringType(),
            values=["com.nationwide.pls.telematics.home.programs.enrollment.created"],
        )
    )

    df_test_data = test_data_spec.build()

    yield df_test_data

@pytest.fixture(scope="session")
def policy_number_null(spark: SparkSession):
    row_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="policy_number_null", rows=row_count, partitions=1)
        .withColumn(
            colName="policyState", colType=StringType(), percentNulls=0.0, template=r"AA"
        )
        .withColumn(
            colName="policyNumber", colType=StringType(), percentNulls=0.2, template=r"DDDDAADDDDDD"
        )
        .withColumn(
            colName="type",
            colType=StringType(),
            values=["com.nationwide.pls.telematics.home.programs.enrollment.created"],
        )
    )

    df_test_data = test_data_spec.build()

    yield df_test_data

@pytest.fixture(scope="session")
def policy_state_policy_number_null(spark: SparkSession):
    row_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="policy_state_policy_number_null", rows=row_count, partitions=1)
        .withColumn(
            colName="policyState", colType=StringType(), percentNulls=0.1, template=r"AA"
        ).withColumn(
            colName="policyNumberOmit", colType=StringType(), template=r"DDDDAADDDDDD",omit=True
        )
        .withColumn(
            colName="policyNumber", colType=StringType(),baseColumn=['policyNumberOmit'],
            expr="case when policyState is null then null else policyNumberOmit end"
        )
        .withColumn(
            colName="type",
            colType=StringType(),
            values=["com.nationwide.pls.telematics.home.programs.enrollment.created"],
        )
    )

    df_test_data = test_data_spec.build()

    yield df_test_data

@pytest.fixture(scope="session")
def policy_state_policy_number_not_null(spark: SparkSession):
    row_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="policy_state_policy_number_not_null", rows=row_count, partitions=1)
        .withColumn(
            colName="policyState", colType=StringType(), percentNulls=0.0, template=r"AA"
        )
        .withColumn(
            colName="policyNumber", colType=StringType(), percentNulls=0.0, template=r"DDDDAADDDDDD"
        )
        .withColumn(
            colName="type",
            colType=StringType(),
            values=["com.nationwide.pls.telematics.home.programs.enrollment.created"],
        )
    )

    df_test_data = test_data_spec.build()

    yield df_test_data

@pytest.fixture(scope="session")
def time_null(spark: SparkSession):
    row_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="time_null", rows=row_count, partitions=1)
        .withColumn(
            colName="time", colType=TimestampType(), percentNulls=0.1, template=r"kkkkkkkk-kkkk-kkkk-kkkk-kkkkkkkkkkkk"
        )
        .withColumn(
            colName="type",
            colType=StringType(),
            values=["com.nationwide.pls.telematics.home.programs.enrollment.created"],
        )
    )

    df_test_data = test_data_spec.build()

    yield df_test_data


@pytest.fixture(scope="session")
def time_not_null(spark: SparkSession):
    row_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="time_not_null", rows=row_count, partitions=1)
        .withColumn(
            colName="time", colType=TimestampType(), percentNulls=0.0, template=r"kkkkkkkk-kkkk-kkkk-kkkk-kkkkkkkkkkkk"
        )
        .withColumn(
            colName="type",
            colType=StringType(),
            values=["com.nationwide.pls.telematics.home.programs.enrollment.created"],
        )
    )

    df_test_data = test_data_spec.build()

    yield df_test_data


@pytest.fixture(scope="session")
def dataframe(spark: SparkSession):
    row_count = 10
    test_data_spec = (
        dg.DataGenerator(spark, name="dataframe", rows=row_count, partitions=1)
        .withColumn(
            colName="policyState", colType=StringType(), percentNulls=0.1, template=r"AA"
        )
        .withColumn(
            colName="policyNumber", colType=StringType(), percentNulls=0.2, template=r"DDDDAADDDDDD"
        )
        .withColumn(
            colName="dataCollectionId",
            colType=StringType(),
            percentNulls=0.1,
            template=r"kkkkkkkk-kkkk-kkkk-kkkk-kkkkkkkkkkkk",
        )
        .withColumn(
            colName="time", colType=TimestampType(), percentNulls=0.2, template=r"kkkkkkkk-kkkk-kkkk-kkkk-kkkkkkkkkkkk"
        )
        .withColumn(
            colName="type",
            colType=StringType(),
            values=["com.nationwide.pls.telematics.home.programs.enrollment.created"],
        )
    )

    df_test_data = test_data_spec.build()

    yield df_test_data

@pytest.fixture(scope="session")
def enrollment():
    EMAIL_CONF = {
        "email_to": "56a3a54a.nationwide.com@amer.teams.ms",
        "email_from": "iot_dev@nationwide.com",
        "email_subject": "dqi_enrollment_unit_test",
        "alert_cooldown": 1,
    }
    yield Enrollment(exception_table="dqi_enrollment_unit_test", 
                     alert_conf=EMAIL_CONF)


def test_data_collection_id_cannot_be_null_with_nulls(
    created_data_collection_id_null: DataFrame, enrollment: Enrollment
):
    dqi_check_result = enrollment.data_collection_id_cannot_be_null(created_data_collection_id_null)
    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_column_to_exist("dataCollectionId").get("success")
    assert ge_df.expect_column_to_exist("type").get("success")
    assert ge_df.expect_column_to_exist("exception_reason").get("success")
    assert ge_df.expect_column_to_exist("exception_timestamp").get("success")
    assert ge_df.expect_table_column_count_to_equal(4).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "exception_reason",
        ["dataCollectionId cannot be null when type is com.nationwide.pls.telematics.home.programs.enrollment.created"],
    ).get("success")
    assert ge_df.expect_column_values_to_be_of_type(column="exception_timestamp", type_="TimestampType").get("success")


def test_datacollectionid_cannot_be_null_without_nulls(
    created_data_collection_id_not_null: DataFrame, enrollment: Enrollment
):
    assert not enrollment.data_collection_id_cannot_be_null(created_data_collection_id_not_null)


def test_policy_state_cannot_be_null_with_nulls(
    policy_state_null: DataFrame, enrollment: Enrollment
):
    dqi_check_result = enrollment.policy_state_policy_number_cannot_be_null(policy_state_null)
    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_column_to_exist("policyState").get("success")
    assert ge_df.expect_column_to_exist("policyNumber").get("success")
    assert ge_df.expect_column_to_exist("type").get("success")
    assert ge_df.expect_column_to_exist("exception_reason").get("success")
    assert ge_df.expect_column_to_exist("exception_timestamp").get("success")
    assert ge_df.expect_table_column_count_to_equal(5).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "exception_reason",
        ["policyState cannot be null when type is com.nationwide.pls.telematics.home.programs.enrollment.created"],
    ).get("success")
    assert ge_df.expect_column_values_to_be_of_type(column="exception_timestamp", type_="TimestampType").get(
        "success"
    )

def test_policy_number_cannot_be_null_with_nulls(
    policy_number_null: DataFrame, enrollment: Enrollment
):
    dqi_check_result = enrollment.policy_state_policy_number_cannot_be_null(policy_number_null)
    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_column_to_exist("policyState").get("success")
    assert ge_df.expect_column_to_exist("policyNumber").get("success")
    assert ge_df.expect_column_to_exist("type").get("success")
    assert ge_df.expect_column_to_exist("exception_reason").get("success")
    assert ge_df.expect_column_to_exist("exception_timestamp").get("success")
    assert ge_df.expect_table_column_count_to_equal(5).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "exception_reason",
        ["policyNumber cannot be null when type is com.nationwide.pls.telematics.home.programs.enrollment.created"],
    ).get("success")
    assert ge_df.expect_column_values_to_be_of_type(column="exception_timestamp", type_="TimestampType").get(
        "success"
    )

def test_policy_state_policy_number_cannot_be_null_with_nulls(
    policy_state_policy_number_null: DataFrame, enrollment: Enrollment
):
    dqi_check_result = enrollment.policy_state_policy_number_cannot_be_null(policy_state_policy_number_null)
    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_column_to_exist("policyState").get("success")
    assert ge_df.expect_column_to_exist("policyNumber").get("success")
    assert ge_df.expect_column_to_exist("type").get("success")
    assert ge_df.expect_column_to_exist("exception_reason").get("success")
    assert ge_df.expect_column_to_exist("exception_timestamp").get("success")
    assert ge_df.expect_table_column_count_to_equal(5).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "exception_reason",
        ["policyNumber and policyState cannot be null when type is com.nationwide.pls.telematics.home.programs.enrollment.created"],
    ).get("success")
    assert ge_df.expect_column_values_to_be_of_type(column="exception_timestamp", type_="TimestampType").get(
        "success"
    )

def test_policy_state_policy_number_cannot_be_null_without_nulls(
    policy_state_policy_number_not_null: DataFrame, enrollment: Enrollment
):
    assert not enrollment.policy_state_policy_number_cannot_be_null(policy_state_policy_number_not_null)


def test_time_cannot_be_null_with_nulls(time_null: DataFrame, enrollment: Enrollment):
    dqi_check_result = enrollment.time_cannot_be_null(time_null)
    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_column_to_exist("time").get("success")
    assert ge_df.expect_column_to_exist("type").get("success")
    assert ge_df.expect_column_to_exist("exception_reason").get("success")
    assert ge_df.expect_column_to_exist("exception_timestamp").get("success")
    assert ge_df.expect_table_column_count_to_equal(4).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "exception_reason", ["time cannot be null as it used for sequencing events"]
    ).get("success")
    assert ge_df.expect_column_values_to_be_of_type(column="exception_timestamp", type_="TimestampType").get("success")


def test_time_cannot_be_null_without_nulls(time_not_null: DataFrame, enrollment: Enrollment):
    assert not enrollment.time_cannot_be_null(time_not_null)

def test_execute_dqi_checks(dataframe: DataFrame, enrollment: Enrollment):

    enrollment.set_dataframe(dataframe=dataframe)
    enrollment.execute_dqi_checks()

    ge_df = ge.dataset.SparkDFDataset(enrollment.dqi_check_dfs[0])
    assert ge_df.expect_table_row_count_to_be_between(1)
    ge_df = ge.dataset.SparkDFDataset(enrollment.dqi_check_dfs[1])
    assert ge_df.expect_table_row_count_to_be_between(1)
    ge_df = ge.dataset.SparkDFDataset(enrollment.dqi_check_dfs[2])
    assert ge_df.expect_table_row_count_to_be_between(1)