from pcd_iot_dbx.util.alert import send_email
from pcd_iot_dbx.util import email_templates
from pcd_iot_dbx.common import Task
from pcd_iot_dbx.exceptions import MissingAlertConfigurations

from abc import ABC, abstractmethod
from datetime import datetime

import pyspark
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField,StringType, IntegerType


class DQI(Task, ABC):
    """This class helps run DQI checks, saves results and sends an alert. The
    following methods need to be ran in sequence to invoke the dqi checks

    1. set_dataframe
    2. execute_dqi_checks
    3. check_dqi_results

    Note: It is possible to skip step 2 as long as your dqi results are stored in
    the class instance list dqi_checks_df

    Required Conf:


    """

    def __init__(self, spark, init_conf=None):
        super().__init__(spark=spark, init_conf=init_conf)
        self.alert_conf = self.conf.get("alert_conf")
        self.dqi_conf = self.conf.get("dqi_conf")
        self.validate_alert_conf(self.alert_conf)
        self.dqi_check_dfs = []
        self.latest_alert_timestamp = None

    def set_dataframe(self, dataframe: pyspark.sql.DataFrame):
        """Sets dataframe to run dqi checks on."""
        self.dataframe = dataframe
        self.dataframe.persist()

    def check_dqi_results(self):
        """Chains some of the methods together to save results and send an alert
        if needed based on dataframes added to dqi_check_dfs list
        """
        self.dqi_check_dfs = list(filter(lambda x: x != None, self.dqi_check_dfs))
        if len(self.dqi_check_dfs) > 0:
            self.exception_timestamp = datetime.now()
            self._union_dqi_results()
            self._save_dqi_results()
            if self._check_if_alert_needed():
                self._send_alert()

        self._clean_up()

    @abstractmethod
    def execute_dqi_checks(self):
        """Abstract method to invoke all dqi checks. It's intent is to save all
        dqi results to the list dqi_check_dfs
        """
        pass

    def _union_dqi_results(self):
        """Helper to union any dataframes stored in dqi_check_dfs"""

        self.unioned_df = self.dqi_check_dfs[0]
        for df in self.dqi_check_dfs[1:]:
            self.unioned_df = self.unioned_df.union(df)

    def _save_dqi_results(self):
        """Saves data quality check results to provided exception table.

        Required Conf:
            dqi_conf.target_table
        """
        self.logger.warn(f"Writing audit results to {self.dqi_conf.get('target_table')}")
        self.unioned_df.write.mode(self.dqi_conf.get("mode")).saveAsTable(self.dqi_conf.get("target_table"))

    def _check_if_alert_needed(self) -> bool:
        """Checks to see if an alert is warranted by comparing last exception time
        with the cool down specified in alert_conf.alert_cooldown. Default is 0
        """
        return not self.latest_alert_timestamp or (
            (self.exception_timestamp - self.latest_alert_timestamp).seconds >= self.alert_conf.get("alert_cooldown", 0)
        )

    def _send_alert(self):
        """Sends an alert email to recipients

        Rerquired Conf:
            alert_conf.email_from
            alert_conf.email_to
            alert_conf.email_subject
        """
        self.logger.warn(f"Sending alert to {self.alert_conf.get('email_to')}")
        self.latest_alert_timestamp = datetime.now()
        body = email_templates.render_template(email_templates.ALERT,
                                               {'exception_table' : self.dqi_conf.get("target_table"),
                                                'rows' : self.unioned_df.collect(),
                                                'columns': self.unioned_df.columns} )
        send_email(
            email_from=self.alert_conf.get("email_from"),
            email_to=self.alert_conf.get("email_to"),
            email_subject=self.alert_conf.get("email_subject"),
            email_body=body,
        )

    def _clean_up(self):
        """Removes stored dataframes to get ready for next run if class is still
        instantiated.
        """
        self.dqi_check_dfs = []
        self.dataframe.unpersist()

    def _collect_problem_date_partitions_for_where_clause(self, df: pyspark.sql.DataFrame):
        """Given a dataframe and dqi_conf.source_partition_column in conf
        will return a string to append to an existing where clause. This is designed
        to work with a date field.
        """
        partition_field = self.dqi_conf.get("source_partition_column")
        dates = df.select(partition_field).select(partition_field).distinct().collect()
        dates_str = ",".join([f"'{d[0].strftime('%Y-%m-%d')}'" for d in dates])
        return f" and {partition_field} in ({dates_str})"

    def set_exception_dataframe(self, exception_df, where_clause, row_count, exception_reason,collect_dates=True):
        if collect_dates:
            where_clause = where_clause + self._collect_problem_date_partitions_for_where_clause(exception_df)
        return self.spark.createDataFrame(
            [
                [
                    self.conf.get("read_stream").get("table_source"),
                    where_clause,
                    row_count,
                    exception_reason,
                ]
            ],
            schema=StructType([
                    StructField('table',StringType(),False),
                    StructField('where_clause',StringType(),False),
                    StructField('record_count',IntegerType(),False),
                    StructField('exception_reason',StringType(),False)
                    ]
                )
        ).withColumn("exception_timestamp", current_timestamp())

    def launch(self):
        return super().launch()

    @staticmethod
    def validate_alert_conf(conf: dict):
        """Validates that required alert configuration properties found
        in configuration"""
        if conf:
            required_keys = ["email_from", "email_to", "email_subject"]
            missing_keys = []
            for key in required_keys:
                if key not in conf.keys():
                    missing_keys.append(key)
            if missing_keys:
                raise MissingAlertConfigurations(missing_keys)
        else:
            raise MissingAlertConfigurations("Missing alert configuration")
        

