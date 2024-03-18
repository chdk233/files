"""
schema and transformation mappings for smarthome enrollment
"""
from pcd_iot_dbx.etl.transformations.smarthome.enrollment import enrollment

schemas_dict = {
    "enrollment": None
}

transformations_dict = {
    "enrollment": enrollment
}