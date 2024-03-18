from pcd_iot_dbx.etl.mappings.smarthome import notion, enrollment

class ETL:

    MAPPING_DICT = {
        'smarthome_notion' : notion,
        'smarthome_enrollment' : enrollment
    }

    def __init__(self, program,
                  name,
                  schema=None,
                  transformation_func=None,
                  mapping=MAPPING_DICT):
        self.program = program
        self.name = name
        self.schema = schema
        self.transformation_func = transformation_func
        self.mapping_dict=mapping
        if not self.transformation_func:
            self.set_transformation_dict()
            self.set_transformation_func()
        if not self.schema:
            self.set_schema_dict()
            self.set_schema()

    def set_schema_dict(self):
        self.schemas_dict = self.mapping_dict.get(self.program).schemas_dict

    def set_transformation_dict(self):
        self.transformations_dict = self.mapping_dict.get(self.program).transformations_dict

    def set_schema(self):
        self.schema = self.schemas_dict.get(self.name)

    def get_schema(self):
        return self.schema

    def set_transformation_func(self):
        self.transformation_func = self.transformations_dict.get(self.name)

    def get_transformation_func(self):
        return self.transformation_func
    
