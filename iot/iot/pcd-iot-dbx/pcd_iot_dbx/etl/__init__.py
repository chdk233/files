from pcd_iot_dbx.etl.mappings.smarthome import notion, leakbot, enrollment
from pcd_iot_dbx.etl.mappings.rivian import rivian_ingestion
from pcd_iot_dbx.etl.transformations.common import straight_move
from pcd_iot_dbx.exceptions import (
    MissingNameException,
    MissingProgramException
)

class ETL:
    """Sets source schema and transformation logic to target table. I guess it 
    should really just be called ET.

    `MAPPING_DICT` is used to retrieve schema and transformations given the `program`
    and `name` defined in the config file. The program identifies the python 
    module containing the schema and transformations. The name is used to retrieve 
    the specific schema and transformation function.
    """
    MAPPING_DICT = {
        'smarthome_notion' : notion,
        'smarthome' : enrollment,
        'smarthome_leakbot': leakbot,
        'rivian': rivian_ingestion
    }

    def __init__(self, program,
                  name,
                  schema=None,
                  transformation_func=None,
                  mapping=MAPPING_DICT):
        self.program = program
        # Check to make sure program is not set to None. 
        if not self.program:
            raise MissingProgramException("Must supply `program` config entry. " +
                                          "It is currently undefined.")
        self.name = name
        # Check to make sure name is not set to None. 
        if not self.name:
            raise MissingNameException("Must supply `name` config entry. " +
                                          "It is currently undefined.")
        self.schema = schema
        self.transformation_func = transformation_func
        self.mapping_dict=mapping
        # If transformation function set to None, then set transformation using 
        # program, name and MAPPING_DICT
        if not self.transformation_func:
            self.set_transformation_dict()
            self.set_transformation_func()
        # If schema set to None, then set schema using program, name
        # and MAPPING_DICT
        if not self.schema:
            self.set_schema_dict()
            self.set_schema()

    def set_schema_dict(self):
        """Sets the dictionary used to find schemas given a `program` name.
        """
        module = self.mapping_dict.get(self.program)
        if module:
            self.schemas_dict = module.schemas_dict
        else:
            self.schemas_dict = {}

    def set_transformation_dict(self):
        """Sets the dictionary used to find transformations given a `program` name
        """
        module = self.mapping_dict.get(self.program)
        if module:
            self.transformations_dict = module.transformations_dict
        else:
            self.transformations_dict = {}

    def set_schema(self):
        """Assigns the schema given the `name` provided in the config file.
        """
        self.schema = self.schemas_dict.get(self.name,None)


    def get_schema(self):
        """Retrieves the assigned schema
        """
        return self.schema

    def set_transformation_func(self):
        """Assigns the transformation function given the `name` provided in the
        config file.
        """
        self.transformation_func = self.transformations_dict.get(self.name, straight_move)

    def get_transformation_func(self):
        """Retrieves the assigned transformation schema
        """
        return self.transformation_func
    
