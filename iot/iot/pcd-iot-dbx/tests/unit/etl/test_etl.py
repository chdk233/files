from pcd_iot_dbx.etl import ETL
from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
)
import pytest
from pcd_iot_dbx.exceptions import (
    MissingNameException,
    MissingProgramException,
)

SCHEMA = StructType([StructField('test_field',StringType(),True)])
FUNCTION = lambda x:x

class mapping:

    schemas_dict = {
        'test_name' : SCHEMA
    }

    transformations_dict = {
        'test_name' : FUNCTION
    }

MAPPING = {
    "test_program" : mapping
}

def test_etl_passed_schema_and_function():

    etl = ETL(program='test_program',
              name='test_name',
              schema=SCHEMA,
              transformation_func=FUNCTION)

    assert SCHEMA == etl.get_schema()
    assert FUNCTION == etl.get_transformation_func()

def test_etl_mappings():

    etl = ETL(program='test_program',
              name='test_name',
              mapping=MAPPING)
    
    assert SCHEMA == etl.get_schema()
    assert FUNCTION == etl.get_transformation_func()

def test_missing_program():
        
    with pytest.raises(MissingProgramException):
        ETL(program=None,
            name='test_name',
            mapping=MAPPING)
        
def test_missing_name():
        
    with pytest.raises(MissingNameException):
        ETL(program='test_program',
            name=None,
            mapping=MAPPING)