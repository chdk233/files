from pyspark.sql.types import (
    StructType, 
    StructField, 
    BinaryType, 
    StringType, 
    IntegerType, 
    LongType,
    TimestampType,
    ArrayType
)

kafka_topic = StructType(
    [
        StructField("key", BinaryType(), True),
        StructField("value", BinaryType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("timestampType", IntegerType(), True),
        StructField("headers", ArrayType(StringType()), True),
    ]
)
