"""
DQI Checks for SmartHome Notion
"""
from pcd_iot_dbx.dqi import DQI
import pyspark




class DailyShippedInstalled(DQI):

    def __init__(self,spark,init_conf=None):
        super().__init__(spark=spark,init_conf=init_conf)
        self.CLS_NM = self.__class__.__name__

    def execute_dqi_checks(self):
        for dqi_check in [self.null_check]:
            self.dqi_check_dfs.append(dqi_check(self.dataframe))

    def null_check(self,df: pyspark.sql.DataFrame):

        exception_reason = f"{self.CLS_NM} - null fields"
        self.logger.info(f"Executing DQI check: {exception_reason}")
        where_clause = \
"""order_number is null or
partner_member_id is null
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

class WeeklyEvents(DQI):
    def __init__(self,spark,init_conf=None):
        super().__init__(spark=spark,init_conf=init_conf)
        self.CLS_NM = self.__class__.__name__

    def execute_dqi_checks(self):
        for dqi_check in [self.null_check]:
            self.dqi_check_dfs.append(dqi_check(self.dataframe))

    def null_check(self,df: pyspark.sql.DataFrame):
        exception_reason = f"{self.CLS_NM} - null fields"
        self.logger.info(f"Executing DQI check: {exception_reason}")
        where_clause = \
"""
partner_member_id is null or
sensor_hardware_id is null
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
        
        
class MonthlySensor(DQI):
    def __init__(self,spark,init_conf=None):
        super().__init__(spark=spark,init_conf=init_conf)
        self.CLS_NM = self.__class__.__name__

    def execute_dqi_checks(self):
        for dqi_check in [self.null_check]:
            self.dqi_check_dfs.append(dqi_check(self.dataframe))

    def null_check(self,df: pyspark.sql.DataFrame):
        exception_reason = f"{self.CLS_NM} - null fields"
        self.logger.info(f"Executing DQI check: {exception_reason}")
        where_clause = \
"""
partner_member_id is null or
sensor_hardware_id is null
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
        
class DailySystemHealth(DQI):

    def __init__(self,spark,init_conf=None):
        super().__init__(spark=spark,init_conf=init_conf)
        self.CLS_NM = self.__class__.__name__

    def execute_dqi_checks(self):
        for dqi_check in [self.null_check]:
            self.dqi_check_dfs.append(dqi_check(self.dataframe))

    def null_check(self,df: pyspark.sql.DataFrame):

        exception_reason = f"{self.CLS_NM} - null fields"
        self.logger.info(f"Executing DQI check: {exception_reason}")
        where_clause = \
"""
partner_member_id is null or
hardware_id is null
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
        
class WeeklySystemsLive(DQI):

    def __init__(self,spark,init_conf=None):
        super().__init__(spark=spark,init_conf=init_conf)
        self.CLS_NM = self.__class__.__name__

    def execute_dqi_checks(self):
        for dqi_check in [self.null_check]:
            self.dqi_check_dfs.append(dqi_check(self.dataframe))

    def null_check(self,df: pyspark.sql.DataFrame):

        exception_reason = f"{self.CLS_NM} - null fields"
        self.logger.info(f"Executing DQI check: {exception_reason}")
        where_clause = \
"""
partner_member_id is null or
live_status is null
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



