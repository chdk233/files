from jinja2 import Environment, FileSystemLoader

env = Environment(loader = FileSystemLoader('./conf/templates'))

template = env.get_template('etl_config_template.yml')

tables = [
    'dhf_iot_ims_raw.tripsummary',
    'dhf_iot_ims_raw.telemetrypoints',
    'dhf_iot_ims_raw.nontripevent',
    'dhf_iot_ims_raw.telemetryevents',
    'dhf_iot_harmonized.trip_point',
    'dhf_iot_harmonized.trip_summary',
    'dhf_iot_harmonized.trip_detail_seconds',
    'dhf_iot_harmonized.trip_event',
]

for table in tables:
    with open(f"./conf/tasks/{table}.yml", 'w',newline='\n') as f:
        f.write(template.render(table=table.split('.')[-1]))