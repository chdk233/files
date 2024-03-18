"""
DQI Checks for SmartHome Enrollment
"""
from pcd_iot_dbx.dqi import DQI
import pyspark
from pyspark.sql.functions import lit, current_timestamp, when, col, collect_list


class Enrollment(DQI):

    def __init__(self,spark,init_conf=None):
        super().__init__(spark=spark,init_conf=init_conf)

    def execute_dqi_checks(self):
        for dqi_check in [
            self.data_collection_id_cannot_be_null,
            self.policy_state_policy_number_cannot_be_null,
            self.time_cannot_be_null,
        ]:
            self.dqi_check_dfs.append(dqi_check(self.dataframe))


    def data_collection_id_cannot_be_null(self,df: pyspark.sql.DataFrame):
        exception_reason = "dataCollectionId cannot be null when type is com.nationwide.pls.telematics.home.programs.enrollment.created"
        self.logger.info(f"Executing DQI check: {exception_reason}")

        df_filtered = df.where(
            """
            type='com.nationwide.pls.telematics.home.programs.enrollment.created' 
            and dataCollectionId is null"""
        )

        if df_filtered.count() == 0:
            self.logger.info(f"DQI check PASSED: {exception_reason}")
            return
        else:
            self.logger.warn(f"DQI check FAILED: {exception_reason}")
            return df_filtered.withColumn(
                "exception_reason", lit(exception_reason)
            ).withColumn("exception_timestamp", current_timestamp())

    def policy_state_policy_number_cannot_be_null(self,df: pyspark.sql.DataFrame):
        exception_reason = "policy_state and/or policy_number cannot be null type is com.nationwide.pls.telematics.home.programs.enrollment.created"
        self.logger.info(f"Executing DQI check: {exception_reason}")
        df_filtered = df.where(
        """    
        type='com.nationwide.pls.telematics.home.programs.enrollment.created'
        and (policyState is null or policyNumber is null)"""
        )

        if df_filtered.count() == 0:
            self.logger.info(f"DQI check PASSED: {exception_reason}")
            return
        else:
            self.logger.warn(f"DQI check FAILED: {exception_reason}. Please see table for specific error message")
            return df_filtered.withColumn(
                "exception_reason",
                when(
                    (col("policyNumber").isNull() & (col("policyState").isNull())),
                    lit("policyNumber and policyState cannot be null when type is com.nationwide.pls.telematics.home.programs.enrollment.created"),
                )
                .when(
                    col("policyNumber").isNull(), lit("policyNumber cannot be null when type is com.nationwide.pls.telematics.home.programs.enrollment.created")
                )
                .when(col("policyState").isNull(), lit("policyState cannot be null when type is com.nationwide.pls.telematics.home.programs.enrollment.created")),
            ).withColumn("exception_timestamp", current_timestamp())

    def time_cannot_be_null(self,df: pyspark.sql.DataFrame):
        exception_reason = "time cannot be null as it used for sequencing events"
        self.logger.info(f"Executing DQI check: {exception_reason}")

        df_filtered = df.where(
            """
        time is null
        """
        )

        if df_filtered.count() == 0:
            self.logger.info(f"DQI check PASSED: {exception_reason}")
            return
        else:
            self.logger.warn(f"DQI check FAILED: {exception_reason}")
            return df_filtered.withColumn(
                "exception_reason", lit(exception_reason)
            ).withColumn("exception_timestamp", current_timestamp())
