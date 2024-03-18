from pcd_iot_dbx.dqi import DQI, MissingAlertConfigurations
import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

EMAIL_CONF = {'email_to' : '56a3a54a.nationwide.com@amer.teams.ms',
              'email_from' : 'iot_dev@nationwide.com',
              'email_subject' : 'dqi_unit_test',
              'alert_cooldown': 1}

NOW = datetime.now()

class DQI_TEST(DQI):

    def execute_dqi_checks(self):
        return super().execute_dqi_checks()

@pytest.fixture
def dqi():
    yield DQI_TEST(exception_table='dqi_test_table',
                   alert_conf=EMAIL_CONF)

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
        
def test_dqi_init():
    with pytest.raises(MissingAlertConfigurations) as e:
        assert DQI_TEST(exception_table='test_table',
                        alert_conf={})
        
def test_check_if_alert_needed_true(dqi: DQI_TEST):
    dqi.latest_alert_timestamp = NOW
    dqi.exception_timestamp = NOW + timedelta(seconds=1)

    assert dqi.check_if_alert_needed()

def test_check_if_alert_needed_false(dqi: DQI_TEST):
    dqi.latest_alert_timestamp = NOW
    dqi.exception_timestamp = NOW

    assert dqi.check_if_alert_needed() == False

def test_set_dataframe(spark: SparkSession,dqi: DQI_TEST):
    df = spark.createDataFrame([['test']])
    dqi.set_dataframe(df)
    assert df == dqi.dataframe

def test_save_dqi_results_and_cleanup(spark: SparkSession,dqi: DQI_TEST):
    dqi.dqi_check_dfs.append(spark.createDataFrame([['test']]))
    dqi.save_dqi_results()
    assert spark.read.table(dqi.exception_table).count() == 1
    assert len(dqi.dqi_check_dfs) == 1
    dqi.clean_up()
    assert len(dqi.dqi_check_dfs) == 0

def test_send_alert(dqi: DQI_TEST):
    dqi.send_alert()
    assert isinstance(dqi.latest_alert_timestamp, datetime)

def test_dqi_results(spark: SparkSession):
    dqi = DQI_TEST(exception_table='dqi_test_table_e2e',
                   alert_conf=EMAIL_CONF)
    dqi.dqi_check_dfs.append(spark.createDataFrame([['test']]))
    dqi.check_dqi_results()
    assert spark.read.table(dqi.exception_table).count() == 1
    assert isinstance(dqi.latest_alert_timestamp, datetime)
    assert len(dqi.dqi_check_dfs) == 0