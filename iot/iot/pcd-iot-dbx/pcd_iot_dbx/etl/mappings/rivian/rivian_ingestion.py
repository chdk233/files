"""
schema and transformation mappings for rivian
"""
from pcd_iot_dbx.etl.schemas.rivian import rivian_ingestion as rivian_schemas
from pcd_iot_dbx.etl.transformations.rivian import rivian_ingestion as rivian_tfs

schemas_dict = {
    "av_aggregate": rivian_schemas.av_aggregate,
    "event_summary": rivian_schemas.event_summary,
    "trips": rivian_schemas.trips,
    "trips_odometer": rivian_schemas.trips_odometer,
    "trips_summary": rivian_schemas.trips_summary,
}

transformations_dict = {
    "av_aggregate": rivian_tfs.av_aggregate,
    "event_summary": rivian_tfs.event_summary,
    "trips": rivian_tfs.trips,
    "trips_odometer": rivian_tfs.trips_odometer,
    "trips_summary": rivian_tfs.trips_summary,
}