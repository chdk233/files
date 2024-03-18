""""Contains common transformations. Decorators can be chained. The order is top most
last. For example

@decoratorC
@decoratorB
@decoratorA
def myfunct():
    pass
    
In this case decoratorA runs first and decoratorC runs last. This is important if you 
want your columns to be in a certain order.
"""
from pyspark.sql.functions import current_date, current_timestamp, regexp_extract, lit, concat_ws, concat, col, xxhash64
import pyspark
from functools import wraps

def set_key(df: pyspark.sql.DataFrame, attributes: list, table_name: str):
    concat_attr = concat(*attributes)
    df = df.withColumn(f"{table_name}_key", concat_attr)
    return df

def set_key_id(df: pyspark.sql.DataFrame, table_name: str):
    df = df.withColumn(f"{table_name}_key_id", xxhash64(df[f'{table_name}_key']).cast("string"))
    return df

def set_row_hash(df: pyspark.sql.DataFrame, row_attributes: list):
    df = df.withColumn("row_hash", xxhash64(*row_attributes).cast("string"))
    return df

def set_table_id(df: pyspark.sql.DataFrame, table_name: str):
    df = df.withColumn(f"{table_name}_id", xxhash64(*(df.columns)).cast("string"))
    return df

def set_key_fields(attributes: list, row_attributes: list, table_name: str):
    """Decorator to add KEY, KEY_ID, ROW_HASH and <TABLE>_ID values. Relies on attributes for KEY being passed, row attributes for
    ROW_HASH being passed and the table name for <TABLE>_ID being passed
    """
    def decorator(f):
        @wraps(f)
        def wrapper(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
            df = f(df)
            df = set_key(df, attributes, table_name)
            df = set_key_id(df, table_name)
            df = set_row_hash(df, row_attributes)
            df = set_table_id(df, table_name)
            return df
        return wrapper
    return decorator

def set_load_dt_hr_frm_date_in_file_nm(regex: str):
    """Decorator to add load_dt and load_hr_ts to a dataframe transformation function.
    It is dependent on the presence of the _metadata field and relies on the file
    path containing a date. The regex used to extract the date must be passed.
    """
    def decorator(f):
        @wraps(f)
        def wrapper(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
            """Runs decorated transformation and then adds etl fields"""
            df = f(df)

            column_expression = (
                concat_ws(
                    "-",
                    regexp_extract("_metadata.file_path", regex,1),
                    regexp_extract("_metadata.file_path", regex,2),
                    regexp_extract("_metadata.file_path", regex,3)
                )
            ).cast('date')

            df = df.withColumn("load_dt", column_expression) \
                    .withColumn("load_hr_ts", column_expression.cast('timestamp'))

            return df
        return wrapper
    return decorator

def set_load_dt_hr_frm_path(regex: str):
    """Decorator to add load_dt and load_hr_ts to a dataframe transformation function.
    It is dependent on the presence of the _metadata field and relies on the file
    path containing a date, and load hour. The regex used to extract the date must be passed.
    Used for leakbot load_dt and load_hr_ts
    """
    def decorator(f):
        @wraps(f)
        def wrapper(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
            """Runs decorated transformation and then adds etl fields"""
            df = f(df)

            column_expression = (
                concat_ws(
                    "-",
                    regexp_extract("_metadata.file_path", regex,1),
                    regexp_extract("_metadata.file_path", regex,2),
                    regexp_extract("_metadata.file_path", regex,3)
                )
            )
            
            hour_col = regexp_extract("_metadata.file_path", regex,4).substr(1,2)

            hour_m_s_col = concat_ws(":", hour_col, lit("00"), lit("00"))

            load_hr_ts_expression = concat_ws(" ", column_expression, hour_m_s_col)


            df = df.withColumn("load_dt", column_expression.cast("date")) \
                    .withColumn("load_hr_ts", load_hr_ts_expression.cast("timestamp"))

            return df
        return wrapper
    return decorator

def set_load_dt_hr_frm_kafka_msg_ts(f):
    """Decorator to add load_dt and load_hr_ts to a dataframe transformation function.
    It is dependent on the presence of the timestamp field when reading from a kafka
    topic.
    """
    @wraps(f)
    def wrapper(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        """Runs decorated transformation and then adds etl fields"""
        return f(df).withColumn("load_dt", col('timestamp').cast("date")) \
                .withColumn("load_hr_ts", col('timestamp'))
    return wrapper


def set_src_sys_cd(src_sys_cd: str):
    """Decorator to add src_sys_cd column to a dataframe transformation function
    """
    def decorator(f):
        @wraps(f)
        def wrapper(df: pyspark.sql.DataFrame):
            return f(df).withColumn("src_sys_cd", lit(src_sys_cd))
        return wrapper
    return decorator


def set_current_datetime_fields(f):
    """Decorator to add db_load_time and db_load_date to a dataframe transformation
    function"""
    @wraps(f)
    def wrapper(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        return f(df).withColumn("db_load_time", current_timestamp()) \
                .withColumn("db_load_date", current_date())
    return wrapper

def remove_metadata_field(f):
    """Decorator to clean up _metadata field. Should be top most decorator
    if chaining mulitple decorators that use the metadata field"""
    @wraps(f)
    def wrapper(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
        return f(df).drop("_metadata")
    return wrapper


def straight_move(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """Useful when no transformations are needed"""
    return df