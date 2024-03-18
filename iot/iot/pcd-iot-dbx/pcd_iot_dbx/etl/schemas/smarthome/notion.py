"""
Source files schemas
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    ArrayType
)

shipped_installed_daily = StructType(
    [
        StructField("number_of_records", LongType(), True),
        StructField(
            "records",
            ArrayType(
                StructType(
                    [
                        StructField("order_number", StringType(), True),
                        StructField("partner_member_id", StringType(), True),
                        StructField("shipping_status", StringType(), True),
                        StructField("shipped_date", StringType(), True),
                        StructField("activation_date", StringType(), True),
                        StructField("carrier", StringType(), True),
                        StructField("tracking_code", StringType(), True),
                        StructField("changed_flag", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("report_date_created", StringType(), True),
    ]
)

events_weekly = StructType(
    [
        StructField("number_of_records", LongType(), True),
        StructField(
            "records",
            ArrayType(
                StructType(
                    [
                        StructField("partner_member_id", StringType(), True),
                        StructField("sensor_hardware_id", StringType(), True),
                        StructField("timestamp_utc", StringType(), True),
                        StructField("task_name", StringType(), True),
                        StructField("event", StringType(), True),
                        StructField("alertable", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("report_date_created", StringType(), True),
    ]
)

sensor_monthly = StructType(
    [
        StructField("number_of_records", LongType(), True),
        StructField(
            "records",
            ArrayType(
                StructType(
                    [
                        StructField("partner_member_id", StringType(), True),
                        StructField("sensor_hardware_id", StringType(), True),
                        StructField("task_names", StringType(), True),
                        StructField("location", StringType(), True),
                        StructField("installed_at", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("report_date_created", StringType(), True),
    ]
)

system_health_daily = StructType(
    [
        StructField("number_of_records", LongType(), True),
        StructField(
            "records",
            ArrayType(
                StructType(
                    [
                        StructField("partner_member_id", StringType(), True),
                        StructField("hardware_id", StringType(), True),
                        StructField("hardware_type", StringType(), True),
                        StructField("uptime", StringType(), True),
                        StructField("current_status", StringType(), True),
                        StructField("battery_level", StringType(), True),
                        StructField("disconnect_count", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("report_date_created", StringType(), True),
    ]
)

systems_live_weekly = StructType(
    [
        StructField("number_of_records", LongType(), True),
        StructField(
            "records",
            ArrayType(
                StructType(
                    [
                        StructField("partner_member_id", StringType(), True),
                        StructField("live_status", StringType(), True),
                        StructField("subscriptions", StringType(), True),
                    ]
                ),
                True,
            ),
            True,
        ),
        StructField("report_date_created", StringType(), True),
    ]
)