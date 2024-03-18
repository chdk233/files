"""
schema and transformation mappings for smarthome notion
"""
from pcd_iot_dbx.etl.schemas.smarthome import notion as notion_schemas
from pcd_iot_dbx.etl.transformations.smarthome import notion as notion_tfs

schemas_dict = {
    "sensor_monthly": notion_schemas.sensor_monthly,
    "shipped_installed_daily": notion_schemas.shipped_installed_daily,
    "events_weekly": notion_schemas.events_weekly,
    "system_health_daily": notion_schemas.system_health_daily,
    "systems_live_weekly": notion_schemas.systems_live_weekly,
}

transformations_dict = {
    "sensor_monthly": notion_tfs.sensor_monthly,
    "shipped_installed_daily": notion_tfs.shipped_installed_daily,
    "events_weekly": notion_tfs.events_weekly,
    "system_health_daily": notion_tfs.system_health_daily,
    "systems_live_weekly": notion_tfs.systems_live_weekly,
}