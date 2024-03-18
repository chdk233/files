"""
Transformations for each source file
"""
import pyspark
from pcd_iot_dbx.etl.transformations.common import (
    set_current_datetime_fields, 
    set_load_dt_hr_frm_path, 
    set_src_sys_cd,
    remove_metadata_field,
    straight_move)

SRC_SYS_CD = 'LKBT_SH_PL'
LOAD_DT_HR_REGEX = r"(\d\d\d\d)-(\d\d)-(\d\d)\/(\d{0,4})"

"""
Transformation for events straight move, no transformation)
"""
@remove_metadata_field
@set_current_datetime_fields
@set_load_dt_hr_frm_path(regex=LOAD_DT_HR_REGEX)
@set_src_sys_cd(src_sys_cd=SRC_SYS_CD)
def events(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    straight_move(df)
    return df