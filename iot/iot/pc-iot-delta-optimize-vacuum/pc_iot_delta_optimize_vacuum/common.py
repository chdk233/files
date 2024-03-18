from abc import ABC, abstractmethod
from argparse import ArgumentParser
from typing import Dict, Any
import yaml
import pathlib
from pyspark.sql import SparkSession
import sys
import os

from pydantic import BaseModel, Field, validator, root_validator
from typing import Optional, Union


def get_dbutils(
    spark: SparkSession,
):  # please note that this function is used in mocking by its name
    try:
        from pyspark.dbutils import DBUtils  # noqa

        if "dbutils" not in locals():
            utils = DBUtils(spark)
            return utils
        else:
            return locals().get("dbutils")
    except ImportError:
        return None


class Task(ABC):
    """
    This is an abstract class that provides handy interfaces to implement workloads (e.g. jobs or job tasks).
    Create a child from this class and implement the abstract launch method.
    Class provides access to the following useful objects:
    * self.spark is a SparkSession
    * self.dbutils provides access to the DBUtils
    * self.logger provides access to the Spark-compatible logger
    * self.conf provides access to the parsed configuration of the job
    """

    def __init__(self, spark=None, init_conf=None):
        self.spark = self._prepare_spark(spark)
        self.logger = self._prepare_logger()
        self.dbutils = self.get_dbutils()
        if init_conf:
            self.conf = init_conf
        else:
            self.conf = self._provide_config()
        self._overide_conf()
        self._log_conf()

    @staticmethod
    def _prepare_spark(spark) -> SparkSession:
        if not spark:
            return SparkSession.builder.getOrCreate()
        else:
            return spark

    def get_dbutils(self):
        utils = get_dbutils(self.spark)

        if not utils:
            self.logger.warn("No DBUtils defined in the runtime")
        else:
            self.logger.info("DBUtils class initialized")

        return utils

    def _provide_config(self):
        self.logger.info("Reading configuration from --conf-file job option")
        conf_file = self._get_conf_file()
        if not conf_file:
            self.logger.info(
                "No conf file was provided, setting configuration to empty dict."
                "Please override configuration in subclass init method"
            )
            return {}
        else:
            self.logger.info(f"Conf file was provided, reading configuration from {conf_file}")
            return self._read_config(conf_file)

    def _overide_conf(self):
        """use environment variables to append/overwrite self.confwhen prefixed
         with  DBX_
        """
        parms = [ v for v in os.environ if v.startswith('DBX_')]
        pass

    @staticmethod
    def _get_conf_file():
        p = ArgumentParser()
        p.add_argument("--conf-file", required=False, type=str)
        namespace = p.parse_known_args(sys.argv[1:])[0]
        return namespace.conf_file

    @staticmethod
    def _read_config(conf_file) -> Dict[str, Any]:
        config = yaml.safe_load(pathlib.Path(conf_file).read_text())
        return config

    def _prepare_logger(self):
        log4j_logger = self.spark._jvm.org.apache.log4j  # noqa
        return log4j_logger.LogManager.getLogger(self.__class__.__name__)

    def _log_conf(self):
        # log parameters
        self.logger.info("Launching job with configuration parameters:")
        for key, item in self.conf.items():
            self.logger.info("\t Parameter: %-30s with value => %-30s" % (key, item))

    @abstractmethod
    def launch(self):
        """
        Main method of the job.
        :return:
        """
        pass

    def _validate_job_specs(self):

        return JobSpecs(**self.conf)

class DateAttributes(BaseModel):
    weeks: Optional[int]
    days: Optional[int]
    hours: Optional[int]
    minutes: Optional[int]
    seconds: Optional[int]

    @root_validator
    def at_least_one_argument(cls,v):
        if not any(v.values()):
            raise ValueError('one of weeks, days, hours, minutes or seconds must have a value')
        return v

class PartitionSpecs(BaseModel):
    static: Optional[list[dict[str,Union[str,int,list[Union[str,int]]]]]]
    relative_date: Optional[dict[str,DateAttributes]]
    expression: Optional[str]

    @root_validator
    def at_least_one_argument(cls,v):
        if not any(v.values()):
            raise ValueError('one of static, relative_date or expression must have a value')
        return v

class TableSpecs(BaseModel):
    path: Optional[str] = Field(None, description="Path to table to optimize") 
    table: Optional[str] = Field(None, description="Table name to optimize") 
    schema_name: Optional[str] = Field(None, alias='schema', description="Schema name containing the table to optimize") 
    partition: Optional[PartitionSpecs] = Field(None, description="Partition specifications if wanting ot optimize a specific partition")
    zorder: Optional[list[str]] = Field(None, description="Takes a list of columns to perform zorder on")
    retention_hours: Optional[float] = Field(None, description="Retention hours for vacuum")

    @validator('table',always=True)
    def must_contain_table_or_path(cls, v, values):
        if not values.get('path') and not v:
            raise ValueError('Must contain path or table')
        if values.get('path') and v:
            raise ValueError('Only provide path or table, not both')
        return v
    
    @validator('schema_name',always=True)
    def must_contain_schema_when_table_given(cls, v, values):
        if values.get('table') and not v:
            raise ValueError('Must contain schema when table given')
        if not values.get('table') and v:
            raise ValueError('Must provide table if schema given')
        return v

class JobSpecs(BaseModel):
    table_specs: TableSpecs


