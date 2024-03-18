"""
schema and transformation mappings for smarthome enrollment
"""
from pcd_iot_dbx.etl.schemas.common import kafka_topic
from pcd_iot_dbx.etl.transformations.common import straight_move

schemas_dict = {
    "enrollment": kafka_topic
}

transformations_dict = {
    "enrollment": straight_move
}