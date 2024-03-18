"""
Source File schemas for LeakBot
"""

from pyspark.sql.types import *
"""
Schema for events table
"""

events = StructType(
    [
        StructField('sent_ts', StringType(), True),
        StructField('partner_reference', StringType(), True),
        StructField('user_id', StringType(), True),
        StructField('event_id', StringType(), True),
        StructField('event_ts', StringType(), True),
        StructField('event_type', StringType(), True),
        StructField('leakbot_id', StringType(), True),
        StructField('message_type', StringType(), True),
    ]
)