from pcd_iot_dbx.dqi import DQI
from pcd_iot_dbx.exceptions import MissingAlertConfigurations
import pytest
from datetime import datetime, timedelta, date
from pyspark.sql import SparkSession

EMAIL_CONF = {'email_to' : '56a3a54a.nationwide.com@amer.teams.ms',
              'email_from' : 'iot_dev@nationwide.com',
              'email_subject' : 'dqi_unit_test',
              'alert_cooldown': 1}

CONF = {
    'alert_conf' : EMAIL_CONF,
    'dqi_conf' : {
        'source_partition_column' : 'test_field',
        'mode' : 'append',
        'target_table' : 'dqi_test_table'
    }
}

CONF2 = {
    'alert_conf' : EMAIL_CONF,
    'dqi_conf' : {
        'mode' : 'append',
        'target_table' : 'dqi_test_table_2'
    }
}


NOW = datetime.now()

class DQI_TEST(DQI):

    def __init__(self,spark=None,init_conf=None):
        super().__init__(spark=spark,init_conf=init_conf)

    def execute_dqi_checks(self):
        return super().execute_dqi_checks()

@pytest.fixture(scope="session")
def dqi(spark: SparkSession):
    yield DQI_TEST(spark=spark,init_conf=CONF)

@pytest.fixture(scope="session")
def dqi_with_df(spark: SparkSession):
    dqi = DQI_TEST(spark=spark,init_conf=CONF2)
    df = spark.createDataFrame([['test']])
    dqi.set_dataframe(df)
    yield dqi

@pytest.fixture(scope="session")
def dqi_with_df_two(spark: SparkSession):
    dqi = DQI_TEST(spark=spark,init_conf=CONF2)
    df = spark.createDataFrame([['test','test2']],schema=['field1','field2'])
    df2 = spark.createDataFrame([['test3','test4']],schema=['field1','field2'])
    dqi.dqi_check_dfs.append(df)
    dqi.dqi_check_dfs.append(df2)
    yield dqi

def test_validate_missing_alert_conf():

    with pytest.raises(MissingAlertConfigurations) as e:
        assert DQI.validate_alert_conf({})

def test_validate_missing_subject_alert_conf():

    with pytest.raises(MissingAlertConfigurations) as e:
        assert DQI.validate_alert_conf({'email_from' : 'test',
                                        'email_to' : 'test'})

def test_validate_missing_email_to_alert_conf():

    with pytest.raises(MissingAlertConfigurations) as e:
        assert DQI.validate_alert_conf({'email_from' : 'test',
                                        'email_subject' : 'test'})
        
def test_validate_missing_email_from_alert_conf():

    with pytest.raises(MissingAlertConfigurations) as e:
        assert DQI.validate_alert_conf({'email_to' : 'test',
                                        'email_subject' : 'test'})
        
def test_dqi_init(spark):
    with pytest.raises(MissingAlertConfigurations) as e:
        assert DQI_TEST(spark=spark)
        
def test_check_if_alert_needed_true(dqi: DQI_TEST):
    dqi.latest_alert_timestamp = NOW
    dqi.exception_timestamp = NOW + timedelta(seconds=1)

    assert dqi._check_if_alert_needed()

def test_check_if_alert_needed_false(dqi: DQI_TEST):
    dqi.latest_alert_timestamp = NOW
    dqi.exception_timestamp = NOW

    assert dqi._check_if_alert_needed() == False

def test_set_dataframe(dqi: DQI_TEST):
    df = dqi.spark.createDataFrame([['test']])
    dqi.set_dataframe(df)
    assert df == dqi.dataframe

def test_save_dqi_results_and_cleanup(dqi: DQI_TEST):
    dqi.set_dataframe(dqi.spark.createDataFrame([['test']]))
    dqi.dqi_check_dfs.append(dqi.dataframe)
    dqi._union_dqi_results()
    dqi._save_dqi_results()
    target_table = dqi.conf.get('dqi_conf').get('target_table')
    assert dqi.spark.read.table(target_table).count() == 1
    assert len(dqi.dqi_check_dfs) == 1
    dqi._clean_up()
    assert len(dqi.dqi_check_dfs) == 0

def test_send_alert(dqi_with_df_two: DQI_TEST):
    dqi_with_df_two._union_dqi_results()
    dqi_with_df_two._send_alert()
    assert isinstance(dqi_with_df_two.latest_alert_timestamp, datetime)

def test_check_dqi_results(dqi_with_df: DQI_TEST):
    dqi_with_df.dqi_check_dfs.append(dqi_with_df.dataframe)
    dqi_with_df.check_dqi_results()
    target_table = dqi_with_df.conf.get('dqi_conf').get('target_table')
    assert dqi_with_df.spark.read.table(target_table).count() == 1
    assert isinstance(dqi_with_df.latest_alert_timestamp, datetime)
    assert len(dqi_with_df.dqi_check_dfs) == 0

def test_collect_problem_date_partitions_for_where_clause(dqi: DQI_TEST):
    df = dqi.spark.createDataFrame([{'test_field' : date(2023,1,1)}])
    str_result = dqi._collect_problem_date_partitions_for_where_clause(df)
    assert " and test_field in ('2023-01-01')" == str_result