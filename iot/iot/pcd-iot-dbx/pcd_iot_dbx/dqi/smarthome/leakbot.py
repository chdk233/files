"""
DQI Checks for SmartHome Leakbot
"""
from pcd_iot_dbx.dqi import DQI
import pyspark



class Events(DQI):

    def __init__(self,spark,init_conf=None):
        super().__init__(spark=spark,init_conf=init_conf)
        self.CLS_NM = self.__class__.__name__

    def execute_dqi_checks(self):
        for dqi_check in [self.null_check, self.message_type_null_device_removed]:
            self.dqi_check_dfs.append(dqi_check(self.dataframe))

    def null_check(self, df: pyspark.sql.DataFrame):

        exception_reason = f"{self.CLS_NM} - null fields"
        self.logger.info(f"Executing DQI check: {exception_reason}")
        where_clause = \
"""leakbot_id is null or
partner_reference is null
"""
        df_filtered = df.where(where_clause)

        row_count = df_filtered.count()

        if row_count == 0:
            self.logger.info(f"DQI check {exception_reason} PASSED")
        else:
            self.logger.warn(f"DQI check {exception_reason} FAILED")

            return \
                self.set_exception_dataframe(exception_df=df_filtered,
                                             where_clause=where_clause,
                                             row_count=row_count,
                                             exception_reason=exception_reason)

    def message_type_null_device_removed(self, df: pyspark.sql.DataFrame):
        exception_reason = "message_type can only be null when the event_type is DeviceRemoved"
        self.logger.info(f"Executing DQI check: {exception_reason}")

        where_clause = \
        """
        event_type='DeviceRemoved' 
        and message_type is null       
        """
                               
        df_filtered = df.where(where_clause)

        row_count = df_filtered.count()

        null_count_message_type = df.where(
            """
            message_type is null"""
        ).count()
        count_device_removed = df.where(
            """
            event_type='DeviceRemoved' """
        ).count()

        if row_count == null_count_message_type == count_device_removed:
            self.logger.info(f"DQI check {exception_reason} PASSED")
        else:
            self.logger.warn(f"DQI check {exception_reason} FAILED")

            return \
                self.set_exception_dataframe(exception_df=df_filtered,
                                             where_clause=where_clause,
                                             row_count=row_count,
                                             exception_reason=exception_reason)
        

