"""
Transformations for smarthome enrollment
"""
import pyspark
from pyspark.sql.functions import (
    regexp_extract,
    col,
    from_json
)
from pcd_iot_dbx.etl.transformations.common import (
    set_load_dt_hr_frm_kafka_msg_ts, 
    set_src_sys_cd,
    set_current_datetime_fields)

from pcd_iot_dbx.etl.schemas.smarthome.enrollment import enrollment as enrollment_schema

SRC_SYS_CD = 'SHP_PL'


"""
Transformation for events straight move, no transformation)
"""

@set_current_datetime_fields
@set_load_dt_hr_frm_kafka_msg_ts
@set_src_sys_cd(src_sys_cd=SRC_SYS_CD)
def enrollment(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    
    df = \
        df.select(
            col('key').cast('string'),
            regexp_extract(col('value').cast('string'), r'.*?(\{.*\})',1)
                           .alias('value'),
            'topic',
            'partition',
            'offset',
            'timestamp',
            'timestampType'
        )
    
    df = \
        df.select(
            '*',
            from_json('value',enrollment_schema).alias('parsed_value')
        )

    return \
        df.select(
            '*',
            'parsed_value.context.id',
            'parsed_value.context.time',
            'parsed_value.context.source',
            'parsed_value.context.type',
            'parsed_value.smartHomeEnrollment.transactionType',
            'parsed_value.smartHomeEnrollment.policy.policyNumber',
            'parsed_value.smartHomeEnrollment.policy.policyState',
            'parsed_value.smartHomeEnrollment.policy.policyType',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.enrollmentId',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.enrollmentEffectiveDate',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.enrollmentProcessDate',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.enrollmentRemovalReason',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.communicationType',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.programType',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.programStatus',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.dataCollection.dataCollectionId',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.dataCollection.dataCollectionStatus',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.dataCollection.dataCollectionBeginDate',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.dataCollection.dataCollectionEndDate',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.dataCollection.vendor.vendorName',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.dataCollection.vendor.vendorUserId',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.dataCollection.device.deviceId',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.dataCollection.device.deviceType',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.dataCollection.device.checkOutType',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.dataCollection.device.deviceStatus',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.dataCollection.device.deviceStatusDate',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.dataCollection.device.categories.deviceCategory',
            'parsed_value.smartHomeEnrollment.policy.smartHome.program.dataCollection.device.categories.deviceDiscountType',

        ).drop('parsed_value')
