from pcd_iot_dbx.common import Task
from pcd_iot_dbx.dqi import DQI

from datetime import date


class DQITask(Task):


    class IngestCheck(DQI):

        def __init__(self,spark,init_conf=None):
            super().__init__(spark=spark,init_conf=init_conf)
            self.CLS_NM = self.__class__.__name__

        def execute_dqi_checks(self):
            self.dqi_check_dfs.append(self.record_count_check())

        def record_count_check(self):

            exception_reason = f"{self.CLS_NM} - zero records"
            self.logger.info(f"Executing DQI check: {exception_reason}")

            date_col = self.conf.get('dqi_conf').get('source_partition_column')

            today = date.today().strftime('%Y-%m-%d')

            where_clause = f"{date_col} = '{today}'"

            df = self.dataframe.where(where_clause)
            
            row_count = df.count()

            if row_count > 0:
                self.logger.info(f"DQI check {exception_reason} PASSED")
            else:
                self.logger.warn(f"DQI check {exception_reason} FAILED")

                return \
                    self.set_exception_dataframe(exception_df=df,
                                                where_clause=where_clause,
                                                row_count=row_count,
                                                exception_reason=exception_reason,
                                                collect_dates=False)


    def launch(self):
        self.logger.info(f'Launching dqi check task for {self.conf.get("name")}')

        self.dqi = self.IngestCheck(spark=self.spark,init_conf=self.conf)

        source_table = self.conf.get('read_stream').get('table_source')

        dataframe_to_check = self.spark.read.table(source_table)

        self.dqi.set_dataframe(dataframe_to_check)

        self.dqi.execute_dqi_checks()

        self.dqi.check_dqi_results()

        self.logger.info(f'DQI task for {self.conf.get("name")} has finished!')


# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    task = DQITask()

    task.launch()


# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == "__main__":
    entrypoint()
