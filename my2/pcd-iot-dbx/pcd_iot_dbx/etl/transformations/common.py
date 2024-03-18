from pyspark.sql.functions import current_date, current_timestamp, regexp_extract, lit, concat_ws
import pyspark

def set_etl_fields_using_file_metadata(df: pyspark.sql.DataFrame,src_sys_cd: str) -> pyspark.sql.DataFrame:
    """
    This function is dependent on _metadata.file_path field. It is designed to
    extract a date in the format of yyyy_mm_dd.json.
    """
    column_expression = (
        concat_ws(
            "-",
            regexp_extract("_metadata.file_path", r"(\d\d\d\d)_(\d\d)_(\d\d).json",1),
            regexp_extract("_metadata.file_path", r"(\d\d\d\d)_(\d\d)_(\d\d).json",2),
            regexp_extract("_metadata.file_path", r"(\d\d\d\d)_(\d\d)_(\d\d).json",3)
        )
    ).cast('date')

    df = df.withColumn("src_sys_cd", lit(src_sys_cd)) \
            .withColumn("load_dt", column_expression) \
            .withColumn("load_hr_ts", column_expression.cast('timestamp'))

    return set_current_datetime_fields(df).drop("_metadata")


def set_current_datetime_fields(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return df.withColumn("db_load_time", current_timestamp()) \
            .withColumn("db_load_date", current_date())


def straight_move(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    return df