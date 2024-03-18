"""
Source files schemas
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    IntegerType,
    TimestampType,
    DateType
)

sh_enrollment = StructType(
    [
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("key", StringType(), True),
        StructField("offset", LongType(), True),
        StructField("timestamp", LongType(), True),
        StructField("timestampType", IntegerType(), True),
        StructField("id", StringType(), True),
        StructField("source", StringType(), True),
        StructField("time", LongType(), True),
        StructField("type", StringType(), True),
        StructField("transactionType", StringType(), True),
        StructField("policyNumber", StringType(), True),
        StructField("policyState", StringType, True),
        StructField("policyType", StringType(), True),
        StructField("enrollmentEffectiveDate", StringType(), True),
        StructField("enrollmentId", StringType(), True),
        StructField("enrollmentProcessDate", StringType(), True),
        StructField("enrollmentRemovalReason", StringType(), True),
        StructField("communicationType", StringType(), True),
        StructField("programStatus", StringType(), True),
        StructField("programType", StringType(), True),
        StructField("dataCollectionBeginDate", StringType(), True),
        StructField("dataCollectionEndDate", StringType(), True),
        StructField("dataCollectionId", StringType(), True),
        StructField("dataCollectionStatus", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("deviceStatus", StringType(), True),
        StructField("deviceStatusDate", StringType(), True),
        StructField("deviceType", StringType(), True),
        StructField("checkoutType", StringType(), True),
        StructField("vendorAccountId", StringType(), True),
        StructField("vendorName", StringType(), True),
        StructField("vendorUserId", StringType(), True),
        StructField("deviceCategory", StringType(), True),
        StructField("deviceDiscountType", StringType(), True),
        StructField("db_load_time", TimestampType(), True),
        StructField("db_load_date", DateType(), True),
        StructField("partnerMemberId", StringType(), True),
        StructField("src_sys_cd", StringType(), True),
    ]
)