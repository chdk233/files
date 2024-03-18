"""
Source File schemas for LeakBot
"""

from pyspark.sql.types import *

"""
Schema for events table
"""


categories = StructField(
    "categories",
    StructType(
        [
            StructField("deviceCategory", StringType(), True),
            StructField("deviceDiscountType", StringType(), True),
        ]
    ),
    True,
)

device = StructField(
    "device",
    StructType(
        [
            StructField("deviceId", StringType(), True),
            StructField("deviceType", StringType(), True),
            StructField("checkOutType", StringType(), True),
            StructField("deviceStatus", StringType(), True),
            StructField("deviceStatusDate", StringType(), True),
            categories,
        ]
    ),
    True,
)

vendor =  StructField(
    "vendor",
    StructType(
        [
            StructField("vendorName", StringType(), True),
            StructField("vendorUserId", StringType(), True),
        ]
    ),
    True,
)

data_collection = StructField(
    "dataCollection",
    StructType(
        [
            StructField("dataCollectionId", StringType(), True),
            StructField("dataCollectionStatus", StringType(), True),
            StructField("dataCollectionBeginDate", StringType(), True),
            StructField("dataCollectionEndDate", StringType(), True),
            vendor,
            device,
        ]
    ),
    True,
)


enrollment = StructType(
    [
        StructField(
            "context",
            StructType(
                [
                    StructField("id", StringType(), True),
                    StructField("source", StringType(), True),
                    StructField("time", LongType(), True),
                    StructField("type", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "smartHomeEnrollment",
            StructType(
                [
                    StructField("transactionType", StringType(), True),
                    StructField(
                        "policy",
                        StructType(
                            [
                                StructField("policyNumber", StringType(), True),
                                StructField("policyState", StringType(), True),
                                StructField("policyType", StringType(), True),
                                StructField(
                                    "smartHome",
                                    StructType(
                                        [
                                            StructField(
                                                "program",
                                                StructType(
                                                    [
                                                        StructField("enrollmentId", StringType(), True),
                                                        StructField("enrollmentEffectiveDate", StringType(), True),
                                                        StructField("enrollmentProcessDate", StringType(), True),
                                                        StructField("enrollmentRemovalReason", StringType(), True),
                                                        StructField("communicationType", StringType(), True),
                                                        StructField("programType", StringType(), True),
                                                        StructField("programStatus", StringType(), True),
                                                        data_collection,
                                                    ]
                                                ),
                                                True,
                                            )
                                        ]
                                    ),
                                    True,
                                ),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)

