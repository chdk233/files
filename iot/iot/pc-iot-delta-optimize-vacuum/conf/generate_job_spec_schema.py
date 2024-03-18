from pc_iot_delta_optimize_vacuum.common import JobSpecs

with open('./conf/templates/job_specs_schema.json','w+') as f:
    f.write(JobSpecs.schema_json())