"""
Transformations for each source file
"""
import pyspark
from pyspark.sql.functions import explode, col


def shipped_installed_daily(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Required Source Fields:

        order_number: string
        partner_member_id: string
        shipping_status: string
        shipped_date: string
        activation_date: string
        carrier: string
        tracking_code: string
        changed_flag: string
    """
    return (
        df.withColumn("exploded_records", explode(col("records")))
            .withColumn('order_number', col("exploded_records.order_number"))
            .withColumn('partner_member_id', col("exploded_records.partner_member_id"))
            .withColumn('shipping_status', col("exploded_records.shipping_status"))
            .withColumn('shipped_date', col("exploded_records.shipped_date"))
            .withColumn('activation_date', col("exploded_records.activation_date"))
            .withColumn('carrier', col("exploded_records.carrier"))
            .withColumn('tracking_code', col("exploded_records.tracking_code"))
            .withColumn('changed_flag', col("exploded_records.changed_flag"))
            .drop("records")
            .drop("number_of_records")
            .drop("report_date_created")
            .drop("exploded_records")
    )

def events_weekly(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Required Source Fields:

        partner_member_id: string
        sensor_hardware_id: string
        timestamp_utc: string
        task_name: string
        event: string
        alertable: string

    """
    return (
        df.withColumn("exploded_records", explode(col("records")))
            .withColumn('partner_member_id', col("exploded_records.partner_member_id"))
            .withColumn('sensor_hardware_id', col("exploded_records.sensor_hardware_id"))
            .withColumn('timestamp_utc', col("exploded_records.timestamp_utc"))
            .withColumn('task_name', col("exploded_records.task_name"))
            .withColumn('event', col("exploded_records.event"))
            .withColumn('alertable', col("exploded_records.alertable"))
            .drop("records")
            .drop("number_of_records")
            .drop("report_date_created")
            .drop("exploded_records")
    )

def sensor_monthly(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Required Source Fields:

        partner_member_id: string
        sensor_hardware_id: string
        task_names: string
        location: string
        installed_at: string 
    """
    return (
        df.withColumn("exploded_records", explode(col("records")))
            .withColumn('partner_member_id', col("exploded_records.partner_member_id"))
            .withColumn('sensor_hardware_id', col("exploded_records.sensor_hardware_id"))
            .withColumn('task_names', col("exploded_records.task_names"))
            .withColumn('location', col("exploded_records.location"))    
            .withColumn('installed_at', col("exploded_records.installed_at"))
            .drop("records")
            .drop("number_of_records")
            .drop("report_date_created")
            .drop("exploded_records")
    )


def system_health_daily(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Required Source Fields:

        partner_member_id: string
        hardware_id: string
        hardware_type: string
        uptime: string
        current_status: string
        battery_level: string
        disconnect_count: string
    """
    return (
        df.withColumn("exploded_records", explode(col("records")))
            .withColumn('partner_member_id', col("exploded_records.partner_member_id"))    
            .withColumn('hardware_id', col("exploded_records.hardware_id"))    
            .withColumn('hardware_type', col("exploded_records.hardware_type"))    
            .withColumn('uptime', col("exploded_records.uptime"))    
            .withColumn('current_status', col("exploded_records.current_status"))    
            .withColumn('battery_level', col("exploded_records.battery_level"))    
            .withColumn('disconnect_count', col("exploded_records.disconnect_count"))
            .drop("records")    
            .drop("number_of_records")
            .drop("report_date_created")
            .drop("exploded_records")
    )

def systems_live_weekly(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Required Source Fields:

        partner_member_id: string
        live_status: string
        subscriptions: string
    """
    return (
        df.withColumn("exploded_records", explode(col("records")))
            .withColumn('partner_member_id', col("exploded_records.partner_member_id"))    
            .withColumn('live_status', col("exploded_records.live_status"))    
            .withColumn('subscriptions', col("exploded_records.subscriptions"))
            .drop("records")    
            .drop("number_of_records")
            .drop("report_date_created")
            .drop("exploded_records")
        )

