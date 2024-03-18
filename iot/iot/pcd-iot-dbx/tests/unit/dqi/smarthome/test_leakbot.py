from pcd_iot_dbx.dqi.smarthome.leakbot import Events
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, DateType,IntegerType
import great_expectations as ge
import dbldatagen as dg
import pytest

EMAIL_CONF = {
    "email_to": "56a3a54a.nationwide.com@amer.teams.ms",
    "email_from": "iot_dev@nationwide.com",
    "email_subject": "dqi_leakbot_unittest",
    "alert_cooldown": 1,
}

CONF = {
    'alert_conf' : EMAIL_CONF,
    'read_stream' : {
        'table_source' : 'dqi_leakbot_unittest'
    },
    'dqi_conf' : {
        'mode' : 'append',
        'target_table' : 'dqi_leakbot_unittest_exceptions',
        'source_partition_column' : 'load_dt'
    }
}

@pytest.fixture(scope="session")
def events_dfs(spark: SparkSession):
    row_count = 100
    pr_null = (
        dg.DataGenerator(spark, name="partner_reference_null", rows=row_count, partitions=1)
        .withColumn(colName = 'partner_reference', 
                    colType=StringType(),
                    percentNulls=0.10,
                    nullable=True,
                    random=True,
                    template=r'\Pdddddddd')
        .withColumn(colName='leakbot_id',
                    colType=StringType(),
                    random=True,
                    template=r'ddddd')
        .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True)
    )

    lb_id_null = (
        dg.DataGenerator(spark, name="leakbot_id_null", rows=row_count, partitions=1)
        .withColumn(colName = 'partner_reference', 
                    colType=StringType(),
                    random=True,
                    template=r'\Pdddddddd')
        .withColumn(colName='leakbot_id',
                    percentNulls=0.15,
                    colType=StringType(),
                    random=True,
                    template=r'ddddd')
        .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True)

    )

    no_null = (
        dg.DataGenerator(spark, name="no_null", rows=row_count, partitions=1)
        .withColumn(colName = 'partner_reference', 
                    colType=StringType(),
                    random=True,
                    template=r'\Pdddddddd')
        .withColumn(colName='leakbot_id',
                    colType=StringType(),
                    random=True,
                    template=r'ddddd')
        .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True)

    )

    df_pr_null = pr_null.build()
    df_lb_id_null = lb_id_null.build()
    df_no_null = no_null.build()

    yield {
        'pr_null' : df_pr_null,
        'lb_id_null' : df_lb_id_null,
        'no_null' : df_no_null
    }

@pytest.fixture(scope="session")
def message_null_device_removed(spark: SparkSession):
    row_count = 50
    test_data_spec = (
        dg.DataGenerator(spark, name='message_null_device_removed', rows=row_count, partitions=1)
        .withColumn(colName='event_type',
                    colType=StringType(),
                    nullable=False,
                    random=True,
                    values=["Registered",
                            "WaitingOnPipe",
                            "OnPipe",
                            "LeakTrue",
                            "LeakFalse",
                            "HighFlow",
                            "LowBattery",
                            "NoBattery",
                            "NoSignal",
                            "HasSignal",
                            "LowSignal",
                            "LostSignal",
                            "HotPipe",
                            "HardwareProblem",
                            "VacantProperty",
                            "OccupiedProperty",
                            "DeviceRemoved"])
        .withColumn(colName='message_type',
                    colType=StringType(),
                    random=True,
                    nullable=True,
                    percentNulls=0.15,
                    values=["Open", "Close"])
        .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True)
            
    )

    df_test_data = test_data_spec.build()

    yield df_test_data



@pytest.fixture(scope="session")
def dataframe(spark: SparkSession):
    row_count = 50
    test_data_spec = (
        dg.DataGenerator(spark, name="dataframe", rows=row_count, partitions=1)
        .withColumn(colName = 'partner_reference', 
                    colType=StringType(),
                    percentNulls=0.10,
                    nullable=True,
                    random=True,
                    template=r'\Pdddddddd')
        .withColumn(colName='leakbot_id',
                    colType=StringType(),
                    random=True,
                    nullable=True,
                    percentNulls=0.15,
                    template=r'ddddd')
        .withColumn(colName='event_type',
                    colType=StringType(),
                    nullable=False,
                    random=True,
                    values=["Registered",
                            "WaitingOnPipe",
                            "OnPipe",
                            "LeakTrue",
                            "LeakFalse",
                            "HighFlow",
                            "LowBattery",
                            "NoBattery",
                            "NoSignal",
                            "HasSignal",
                            "LowSignal",
                            "LostSignal",
                            "HotPipe",
                            "HardwareProblem",
                            "VacantProperty",
                            "OccupiedProperty",
                            "DeviceRemoved"])
        .withColumn(colName='message_type',
                    colType=StringType(),
                    random=True,
                    nullable=True,
                    percentNulls=0.15,
                    values=["Open", "Close"])
        .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True)
    )

    df_test_data = test_data_spec.build()
    yield df_test_data

def test_events_dqi(
        events_dfs: dict, spark: SparkSession
):
    events_dqi = Events(spark=spark, init_conf=CONF)
    dqi_check_result_pr = events_dqi.null_check(events_dfs.get('pr_null'))

    ge_df_pr = ge.dataset.SparkDFDataset(dqi_check_result_pr)

    """
    Check to ensure partner_reference is not null
    """

    assert ge_df_pr.expect_column_to_exist("table").get("success")
    assert ge_df_pr.expect_column_to_exist("where_clause").get("success")
    assert ge_df_pr.expect_column_to_exist("record_count").get("success")
    assert ge_df_pr.expect_column_to_exist("exception_reason").get("success")
    assert ge_df_pr.expect_column_to_exist("exception_timestamp").get("success")
    assert ge_df_pr.expect_table_column_count_to_equal(5).get("success")
    assert ge_df_pr.expect_column_values_to_be_in_set(
        "exception_reason",
        ["Events - null fields"],
    ).get("success")
    assert ge_df_pr.expect_column_values_to_be_of_type(column="exception_timestamp", type_="TimestampType").get("success")
    assert ge_df_pr.expect_table_row_count_to_be_between(1).get('success')

    """
    Check to ensure leakbot_id is not null
    """

    dqi_check_result_lb_id = events_dqi.null_check(events_dfs.get('lb_id_null'))
    ge_df_lb_id = ge.dataset.SparkDFDataset(dqi_check_result_lb_id)

    assert ge_df_lb_id.expect_table_row_count_to_be_between(1).get('success')

    """
    Check to ensure no critical fields are null
    """
    assert not events_dqi.null_check(events_dfs.get('no_null'))


def test_message_type_null_device_removed(
        message_null_device_removed: DataFrame, spark: SparkSession       
):
    events_dqi = Events(spark=spark, init_conf=CONF)

    dqi_check_result = events_dqi.message_type_null_device_removed(message_null_device_removed)

    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_column_to_exist("table").get("success")
    assert ge_df.expect_column_to_exist("where_clause").get("success")
    assert ge_df.expect_column_to_exist("record_count").get("success")
    assert ge_df.expect_column_to_exist("exception_reason").get("success")
    assert ge_df.expect_column_to_exist("exception_timestamp").get("success")
    assert ge_df.expect_table_column_count_to_equal(5).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "exception_reason",
        ["message_type can only be null when the event_type is DeviceRemoved"],
    ).get("success")
    assert ge_df.expect_column_values_to_be_of_type(column="exception_timestamp", type_="TimestampType").get(
        "success"
    )
    assert ge_df.expect_table_row_count_to_be_between(1).get('success')


def test_execute_dqi_checks(
        dataframe:  DataFrame, spark: SparkSession    
):
    events_dqi = Events(spark=spark, init_conf=CONF)

    events_dqi.set_dataframe(dataframe=dataframe)
    events_dqi.execute_dqi_checks()

    ge_df = ge.dataset.SparkDFDataset(events_dqi.dqi_check_dfs[0])
    assert ge_df.expect_table_row_count_to_be_between(1).get('success')
    ge_df = ge.dataset.SparkDFDataset(events_dqi.dqi_check_dfs[1])
    assert ge_df.expect_table_row_count_to_be_between(1).get('success')
