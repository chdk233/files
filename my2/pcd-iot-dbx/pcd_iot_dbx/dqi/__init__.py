

from abc import ABC, abstractmethod
from datetime import datetime
from pcd_iot_dbx.util.alert import send_email
import pyspark

class DQI(ABC):

    def __init__(self,exception_table,alert_conf,dqi_conf=None):
        self.validate_alert_conf(alert_conf)
        self.exception_table = exception_table
        self.dqi_conf = dqi_conf
        self.alert_conf = alert_conf
        self.dqi_check_dfs = []
        self.latest_alert_timestamp = None

    def set_dataframe(self,dataframe: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        self.dataframe = dataframe

    def check_dqi_results(self):
        self.dqi_check_dfs = list(filter(lambda x: x != None, self.dqi_check_dfs))
        if len(self.dqi_check_dfs) > 0:
            self.exception_timestamp = datetime.now()
            self.save_dqi_results()
            if self.check_if_alert_needed():
                self.send_alert()

        self.clean_up()

    def save_dqi_results(self):
        for df in self.dqi_check_dfs:
            df.write.mode('append').saveAsTable(self.exception_table)

    def check_if_alert_needed(self) -> bool:
        return not self.latest_alert_timestamp or \
            ((self.exception_timestamp - self.latest_alert_timestamp).seconds >= \
                self.alert_conf.get('alert_cooldown',0))

    def send_alert(self):
        self.latest_alert_timestamp = datetime.now()
        send_email(email_from=self.alert_conf.get('email_from'),
                   email_to=self.alert_conf.get('email_to'),
                   email_subject=self.alert_conf.get('email_subject'),
                   email_body='test')

    def clean_up(self):
        self.dqi_check_dfs = []

    @staticmethod
    def validate_alert_conf(conf: dict):
        required_keys = ['email_from', 'email_to','email_subject']
        missing_keys = []
        for key in required_keys:
            if key not in conf.keys():
                missing_keys.append(key)
        if missing_keys:
            raise MissingAlertConfigurations(missing_keys)

    @abstractmethod
    def execute_dqi_checks(self):
        pass


class MissingAlertConfigurations(Exception):
    pass