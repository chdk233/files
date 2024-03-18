"""
Transformations for source file dqi checks
"""
from pcd_iot_dbx.dqi import DQI
import pyspark
from pyspark.sql.functions import lit, current_timestamp, when, col, collect_list


class Enrollment(DQI):
    def execute_dqi_checks(self):
        for dqi_check in [
            self.data_collection_id_cannot_be_null,
            self.policy_state_policy_number_cannot_be_null,
            self.time_cannot_be_null,
        ]:
            self.dqi_check_dfs.append(dqi_check(self.dataframe))

    @staticmethod
    def data_collection_id_cannot_be_null(df: pyspark.sql.DataFrame):
        df_filtered = df.where(
            """
            type='com.nationwide.pls.telematics.home.programs.enrollment.created' 
            and dataCollectionId is null"""
        )

        if df_filtered.count() == 0:
            return
        else:
            return df_filtered.withColumn(
                "exception_reason", lit("dataCollectionId cannot be null when type is com.nationwide.pls.telematics.home.programs.enrollment.created")
            ).withColumn("exception_timestamp", current_timestamp())

    @staticmethod
    def policy_state_policy_number_cannot_be_null(df: pyspark.sql.DataFrame):
        df_filtered = df.where(
        """    
        type='com.nationwide.pls.telematics.home.programs.enrollment.created'
        and (policyState is null or policyNumber is null)"""
        )

        if df_filtered.count() == 0:
            return
        else:
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

    @staticmethod
    def time_cannot_be_null(df: pyspark.sql.DataFrame):
        df_filtered = df.where(
            """
        time is null
        """
        )

        if df_filtered.count() == 0:
            return
        else:
            return df_filtered.withColumn(
                "exception_reason", lit("time cannot be null as it used for sequencing events")
            ).withColumn("exception_timestamp", current_timestamp())
