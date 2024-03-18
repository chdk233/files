from pcd_iot_dbx.dqi.smarthome.notion import (
    DailyShippedInstalled,
    WeeklyEvents,
    MonthlySensor,
    DailySystemHealth, 
    WeeklySystemsLive)
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DateType,IntegerType
import great_expectations as ge
import dbldatagen as dg
import pytest


EMAIL_CONF = {
    "email_to": "56a3a54a.nationwide.com@amer.teams.ms",
    "email_from": "iot_dev@nationwide.com",
    "email_subject": "dqi_notion_unittest",
    "alert_cooldown": 1,
}

CONF = {
    'alert_conf' : EMAIL_CONF,
    'read_stream' : {
        'table_source' : 'dqi_notion_unittest'
    },
    'dqi_conf' : {
        'mode' : 'append',
        'target_table' : 'dqi_notion_unittest_exceptions',
        'source_partition_column' : 'load_dt'
    }
}


@pytest.fixture(scope="session")
def dly_shp_instl_dfs(spark: SparkSession):
    row_count = 10
    pmi_null = (
        dg.DataGenerator(spark, name="partner_member_id_null", rows=row_count, partitions=1)
        .withColumn(colName='order_number',
                    colType=StringType(),
                    template=r'AAAADDDD'
                    )
        .withColumn(colName='partner_member_id',
                    percentNulls=0.15,
                    colType=StringType(),
                    random=True,
                    template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                    )
       .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True
                    )
    )

    ordr_nb_null = (
        dg.DataGenerator(spark, name="order_number_null", rows=row_count, partitions=1)
        .withColumn(colName='order_number',
                    percentNulls=0.15,
                    colType=StringType(),
                    random=True,
                    template=r'AAAADDDD'
                    )
        .withColumn(colName='partner_member_id',
                    colType=StringType(),
                    random=True,
                    template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                    )
       .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True
                    )
    )

    no_null = (
        dg.DataGenerator(spark, name="no_null", rows=row_count, partitions=1)
        .withColumn(colName='order_number',
                    colType=StringType(),
                    random=True,
                    template=r'AAAADDDD'
                    )
        .withColumn(colName='partner_member_id',
                    colType=StringType(),
                    random=True,
                    template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                    )
       .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True
                    )
    )


    df_pmi_null = pmi_null.build()
    df_order_nb_null = ordr_nb_null.build()
    df_no_null = no_null.build()

    yield {
        'pmi_null' : df_pmi_null,
        'order_nb_null' : df_order_nb_null,
        'no_null' : df_no_null

    }

@pytest.fixture(scope="session")
def weekly_events_dfs(spark: SparkSession):
    row_count = 10
    pmi_null = (
        dg.DataGenerator(spark, name="partner_member_id_null", rows=row_count, partitions=1)
        .withColumn(colName='sensor_hardware_id',
                    colType=StringType(),
                    random=True,
                    template=r'0\xkkkkkkkkkkkkkkkk'
                    )
        .withColumn(colName='partner_member_id',
                    percentNulls=0.15,
                    colType=StringType(),
                    random=True,
                    template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                    )
       .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True
                    )
    )

    sns_hrdwr_id_null = (
        dg.DataGenerator(spark, name="sensor_hardware_id_null", rows=row_count, partitions=1)
        .withColumn(colName='sensor_hardware_id',
                    percentNulls=0.15,
                    colType=StringType(),
                    random=True,
                    template=r'0\xkkkkkkkkkkkkkkkk'
                    )
        .withColumn(colName='partner_member_id',
                    colType=StringType(),
                    random=True,
                    template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                    )
       .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True
                    )
    )

    no_null = (
        dg.DataGenerator(spark, name="partner_member_id_null", rows=row_count, partitions=1)
        .withColumn(colName='sensor_hardware_id',
                    colType=StringType(),
                    random=True,
                    template=r'0\xkkkkkkkkkkkkkkkk'
                    )
        .withColumn(colName='partner_member_id',
                    colType=StringType(),
                    random=True,
                    template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                    )
       .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True
                    )
    )


    df_pmi_null = pmi_null.build()
    df_sns_hrdwr_id_null = sns_hrdwr_id_null.build()
    df_no_null = no_null.build()

    yield {
        'pmi_null' : df_pmi_null,
        'sns_hrdwr_id_null' : df_sns_hrdwr_id_null,
        'no_null' : df_no_null

    }


@pytest.fixture(scope="session")
def mnthly_snsr_dfs(spark: SparkSession):
    row_count = 10
    pmi_null = (
        dg.DataGenerator(spark, name="partner_member_id_null", rows=row_count, partitions=1)
        .withColumn(colName='sensor_hardware_id',
                    colType=StringType(),
                    random=True,
                    template=r'0\xkkkkkkkkkkkkkkkk'
                    )
        .withColumn(colName='partner_member_id',
                    percentNulls=0.15,
                    colType=StringType(),
                    random=True,
                    template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                    )
       .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True
                    )
    )

    sns_hrdwr_id_null = (
        dg.DataGenerator(spark, name="sensor_hardware_id_null", rows=row_count, partitions=1)
        .withColumn(colName='sensor_hardware_id',
                    percentNulls=0.15,
                    colType=StringType(),
                    random=True,
                    template=r'0\xkkkkkkkkkkkkkkkk'
                    )
        .withColumn(colName='partner_member_id',
                    colType=StringType(),
                    random=True,
                    template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                    )
       .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True
                    )
    )

    no_null = (
        dg.DataGenerator(spark, name="partner_member_id_null", rows=row_count, partitions=1)
        .withColumn(colName='sensor_hardware_id',
                    colType=StringType(),
                    random=True,
                    template=r'0\xkkkkkkkkkkkkkkkk'
                    )
        .withColumn(colName='partner_member_id',
                    colType=StringType(),
                    random=True,
                    template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                    )
       .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True
                    )
    )


    df_pmi_null = pmi_null.build()
    df_sns_hrdwr_id_null = sns_hrdwr_id_null.build()
    df_no_null = no_null.build()

    yield {
        'pmi_null' : df_pmi_null,
        'sns_hrdwr_id_null' : df_sns_hrdwr_id_null,
        'no_null' : df_no_null
    }

@pytest.fixture(scope="session")
def dly_sys_hlth_dfs(spark: SparkSession):
    row_count = 10
    pmi_null = (
        dg.DataGenerator(spark, name="partner_member_id_null", rows=row_count, partitions=1)
        .withColumn(colName='hardware_id',
                    colType=StringType(),
                    random=True,
                    template=r'0\xkkkkkkkkkkkkkkkk'
                    )
        .withColumn(colName='partner_member_id',
                    percentNulls=0.15,
                    colType=StringType(),
                    random=True,
                    template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                    )
       .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True
                    )
    )

    hrdwr_id_null = (
        dg.DataGenerator(spark, name="hardware_id_null", rows=row_count, partitions=1)
        .withColumn(colName='hardware_id',
                    percentNulls=0.15,
                    colType=StringType(),
                    random=True,
                    template=r'0\xkkkkkkkkkkkkkkkk'
                    )
        .withColumn(colName='partner_member_id',
                    colType=StringType(),
                    random=True,
                    template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                    )
       .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True
                    )
    )

    no_null = (
        dg.DataGenerator(spark, name="partner_member_id_null", rows=row_count, partitions=1)
        .withColumn(colName='hardware_id',
                    colType=StringType(),
                    random=True,
                    template=r'0\xkkkkkkkkkkkkkkkk'
                    )
        .withColumn(colName='partner_member_id',
                    colType=StringType(),
                    random=True,
                    template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                    )
       .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True
                    )
    )


    df_pmi_null = pmi_null.build()
    df_hrdwr_id_null = hrdwr_id_null.build()
    df_no_null = no_null.build()

    yield {
        'pmi_null' : df_pmi_null,
        'hrdwr_id_null' : df_hrdwr_id_null,
        'no_null' : df_no_null
    }


@pytest.fixture(scope="session")
def wkly_sys_live_dfs(spark: SparkSession):
    row_count = 10
    pmi_null = (
        dg.DataGenerator(spark, name="partner_member_id_null", rows=row_count, partitions=1)
        .withColumn(colName='live_status',
                    colType=IntegerType(),
                    values=[1,0],
                    weights=[9,1]
                    )
        .withColumn(colName='partner_member_id',
                    percentNulls=0.15,
                    colType=StringType(),
                    random=True,
                    template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                    )
       .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True
                    )
    )

    live_status_null = (
        dg.DataGenerator(spark, name="live_status_null", rows=row_count, partitions=1)
        .withColumn(colName='live_status',
                    percentNulls=0.15,
                    colType=IntegerType(),
                    values=[1,0],
                    weights=[9,1]
                    )
        .withColumn(colName='partner_member_id',
                    colType=StringType(),
                    random=True,
                    template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                    )
       .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True
                    )
    )

    no_null = (
        dg.DataGenerator(spark, name="no_null", rows=row_count, partitions=1)
        .withColumn(colName='live_status',
                    colType=IntegerType(),
                    values=[1,0],
                    weights=[9,1]
                    )
        .withColumn(colName='partner_member_id',
                    colType=StringType(),
                    random=True,
                    template=r'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'
                    )
       .withColumn(colName='load_dt',
                    colType=DateType(),
                    random=True
                    )
    )


    df_pmi_null = pmi_null.build()
    df_live_status_null = live_status_null.build()
    df_no_null = no_null.build()

    yield {
        'pmi_null' : df_pmi_null,
        'live_status_null' : df_live_status_null,
        'no_null' : df_no_null
    }



def test_dly_shp_instl(
    dly_shp_instl_dfs: dict, spark: SparkSession
):

    dly_shp_instl = DailyShippedInstalled(spark=spark,init_conf=CONF)
    dqi_check_result = dly_shp_instl.null_check(dly_shp_instl_dfs.get('pmi_null'))

    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_column_to_exist("table").get("success")
    assert ge_df.expect_column_to_exist("where_clause").get("success")
    assert ge_df.expect_column_to_exist("record_count").get("success")
    assert ge_df.expect_column_to_exist("exception_reason").get("success")
    assert ge_df.expect_column_to_exist("exception_timestamp").get("success")
    assert ge_df.expect_table_column_count_to_equal(5).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "exception_reason",
        ["DailyShippedInstalled - null fields"],
    ).get("success")
    assert ge_df.expect_column_values_to_be_of_type(column="exception_timestamp", type_="TimestampType").get("success")
    assert ge_df.expect_table_row_count_to_be_between(1).get('success')

    dqi_check_result = dly_shp_instl.null_check(dly_shp_instl_dfs.get('order_nb_null'))

    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_table_row_count_to_be_between(1).get('success')

    assert not dly_shp_instl.null_check(dly_shp_instl_dfs.get('no_null'))


def test_wkly_evnts(
    weekly_events_dfs: dict, spark: SparkSession
):

    wkly_evnts_dqi = WeeklyEvents(spark=spark,init_conf=CONF)
    dqi_check_result = wkly_evnts_dqi.null_check(weekly_events_dfs.get('pmi_null'))

    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_column_to_exist("table").get("success")
    assert ge_df.expect_column_to_exist("where_clause").get("success")
    assert ge_df.expect_column_to_exist("record_count").get("success")
    assert ge_df.expect_column_to_exist("exception_reason").get("success")
    assert ge_df.expect_column_to_exist("exception_timestamp").get("success")
    assert ge_df.expect_table_column_count_to_equal(5).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "exception_reason",
        ["WeeklyEvents - null fields"],
    ).get("success")
    assert ge_df.expect_column_values_to_be_of_type(column="exception_timestamp", type_="TimestampType").get("success")

    dqi_check_result = wkly_evnts_dqi.null_check(weekly_events_dfs.get('sns_hrdwr_id_null'))

    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_table_row_count_to_be_between(1).get('success')

    assert not wkly_evnts_dqi.null_check(weekly_events_dfs.get('no_null'))

def test_mnthly_snsr(
    mnthly_snsr_dfs: dict, spark: SparkSession
):

    dqi = MonthlySensor(spark=spark,init_conf=CONF)
    dqi_check_result = dqi.null_check(mnthly_snsr_dfs.get('pmi_null'))

    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_column_to_exist("table").get("success")
    assert ge_df.expect_column_to_exist("where_clause").get("success")
    assert ge_df.expect_column_to_exist("record_count").get("success")
    assert ge_df.expect_column_to_exist("exception_reason").get("success")
    assert ge_df.expect_column_to_exist("exception_timestamp").get("success")
    assert ge_df.expect_table_column_count_to_equal(5).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "exception_reason",
        ["MonthlySensor - null fields"],
    ).get("success")
    assert ge_df.expect_column_values_to_be_of_type(column="exception_timestamp", type_="TimestampType").get("success")

    dqi_check_result = dqi.null_check(mnthly_snsr_dfs.get('sns_hrdwr_id_null'))

    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_table_row_count_to_be_between(1).get('success')

    assert not dqi.null_check(mnthly_snsr_dfs.get('no_null'))

def test_dly_sys_health(
    dly_sys_hlth_dfs: dict, spark: SparkSession
):

    dqi = DailySystemHealth(spark=spark,init_conf=CONF)
    dqi_check_result = dqi.null_check(dly_sys_hlth_dfs.get('pmi_null'))

    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_column_to_exist("table").get("success")
    assert ge_df.expect_column_to_exist("where_clause").get("success")
    assert ge_df.expect_column_to_exist("record_count").get("success")
    assert ge_df.expect_column_to_exist("exception_reason").get("success")
    assert ge_df.expect_column_to_exist("exception_timestamp").get("success")
    assert ge_df.expect_table_column_count_to_equal(5).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "exception_reason",
        ["DailySystemHealth - null fields"],
    ).get("success")
    assert ge_df.expect_column_values_to_be_of_type(column="exception_timestamp", type_="TimestampType").get("success")

    dqi_check_result = dqi.null_check(dly_sys_hlth_dfs.get('hrdwr_id_null'))

    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_table_row_count_to_be_between(1).get('success')

    assert not dqi.null_check(dly_sys_hlth_dfs.get('no_null'))

def test_wkly_sys_live(
    wkly_sys_live_dfs: dict, spark: SparkSession
):

    dqi = WeeklySystemsLive(spark=spark,init_conf=CONF)
    dqi_check_result = dqi.null_check(wkly_sys_live_dfs.get('pmi_null'))

    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_column_to_exist("table").get("success")
    assert ge_df.expect_column_to_exist("where_clause").get("success")
    assert ge_df.expect_column_to_exist("record_count").get("success")
    assert ge_df.expect_column_to_exist("exception_reason").get("success")
    assert ge_df.expect_column_to_exist("exception_timestamp").get("success")
    assert ge_df.expect_table_column_count_to_equal(5).get("success")
    assert ge_df.expect_column_values_to_be_in_set(
        "exception_reason",
        ["WeeklySystemsLive - null fields"],
    ).get("success")
    assert ge_df.expect_column_values_to_be_of_type(column="exception_timestamp", type_="TimestampType").get("success")

    dqi_check_result = dqi.null_check(wkly_sys_live_dfs.get('live_status_null'))

    ge_df = ge.dataset.SparkDFDataset(dqi_check_result)

    assert ge_df.expect_table_row_count_to_be_between(1).get('success')

    assert not dqi.null_check(wkly_sys_live_dfs.get('no_null'))