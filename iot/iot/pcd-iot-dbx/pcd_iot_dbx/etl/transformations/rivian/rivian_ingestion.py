"""
Transformations for each source file. For Notion the records list is exploded
into individual rows.
"""
from pcd_iot_dbx.etl.transformations.common import (
    set_current_datetime_fields,  
    set_src_sys_cd,
    set_load_dt_hr_frm_date_in_file_nm,
    remove_metadata_field,
    straight_move)
import pyspark

SRC_SYS_CD = 'RIVIAN_PL_ADAS'
# LOAD_DT_HR_REGEX = r"(\d\d\d\d)-(\d\d)-(\d\d)/"

@remove_metadata_field
@set_current_datetime_fields
@set_load_dt_hr_frm_date_in_file_nm(regex=r'load_date=(\d\d\d\d)-(\d\d)-(\d\d)/')
@set_src_sys_cd(src_sys_cd=SRC_SYS_CD)

def av_aggregate(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    straight_move(df)
    return df

@remove_metadata_field
@set_current_datetime_fields
@set_load_dt_hr_frm_date_in_file_nm(regex=r'load_date=(\d\d\d\d)-(\d\d)-(\d\d)/')
@set_src_sys_cd(src_sys_cd=SRC_SYS_CD)
def event_summary(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    straight_move(df)
    return df

@remove_metadata_field
@set_current_datetime_fields
@set_load_dt_hr_frm_date_in_file_nm(regex=r'load_date=(\d\d\d\d)-(\d\d)-(\d\d)/')
@set_src_sys_cd(src_sys_cd=SRC_SYS_CD)
def trips(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    straight_move(df)
    return df

@remove_metadata_field
@set_current_datetime_fields
@set_load_dt_hr_frm_date_in_file_nm(regex=r'load_date=(\d\d\d\d)-(\d\d)-(\d\d)/')
@set_src_sys_cd(src_sys_cd=SRC_SYS_CD)
def trips_odometer(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    straight_move(df)
    return df

@remove_metadata_field
@set_current_datetime_fields
@set_load_dt_hr_frm_date_in_file_nm(regex=r'load_date=(\d\d\d\d)-(\d\d)-(\d\d)/')
@set_src_sys_cd(src_sys_cd=SRC_SYS_CD)
def trips_summary(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    straight_move(df)
    return df

