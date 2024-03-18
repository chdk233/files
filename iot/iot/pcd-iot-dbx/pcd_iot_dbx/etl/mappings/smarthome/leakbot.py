"""
schema and transformation mappings for smarthome leakbot
"""

from pcd_iot_dbx.etl.schemas.smarthome import leakbot as leakbot_schema
from pcd_iot_dbx.etl.transformations.smarthome import leakbot as leakbot_tfs

schemas_dict = {
    "events": leakbot_schema.events
}
transformations_dict = {
    "events": leakbot_tfs.events
}