from pc_iot_delta_optimize_vacuum.common import *
from delta.tables import DeltaTable
from argparse import ArgumentParser
from datetime import datetime,timedelta
from pyspark.sql import DataFrame
from pyspark.sql.functions import to_json

class OptimizeTask(Task):
    """
    init_conf required to launch
    -----

    table_specs: dict

        path: str (conditional vs table/schema)

        table: str (conditional vs path)

        schema: str (conditional vs path)

        partition: str (optional)

        zorder: list (optional) 
    """

    def optimize(self,table_specs: TableSpecs) -> DataFrame:
        """Executes delta table optimization given a path or table/view. Can 
        optionally provide provide partition specifications as well as columns
        for z-ordering
        """
        if table_specs.path:
            delta_table_func = DeltaTable.forPath
            delta_table_args = {'path' : table_specs.path }
        else:
            delta_table_func = DeltaTable.forName
            delta_table_args = {'tableOrViewName' : table_specs.schema_name + 
                                '.' + table_specs.table }

        dt = delta_table_func(sparkSession=self.spark,
                              **delta_table_args)
        
        optimize_builder = dt.optimize()

        if table_specs.partition:
            formatted_partition_specs = self.format_partition_specs(table_specs.partition)
            optimize_builder =  optimize_builder.where(formatted_partition_specs)

        if table_specs.zorder:
            return optimize_builder.executeZOrderBy(table_specs.zorder)
        else:
            return optimize_builder.executeCompaction()

    @staticmethod
    def format_partition_specs(partition_specs: PartitionSpecs) -> str:
        """
        partition_specs:

        Should include either static partitions, relative or both. Can also 
        just provide an expression, which will be used by default if others
        are provided as well.

        Example 1:

            config:

                {
                    "static": [
                        {
                            "source_cd": "SOURCE_SYS_A"
                        },
                        {
                            "region_cd": [
                                "REGION_A",
                                "REGION_B"
                            ]
                        }
                    ],
                    "relative_date": {
                        "load_dt": {
                            "day": -1
                        }
                    }
                }

            result:

                source_cd = 'SOURCE_SYS_A' 
                and region_cd in ('REGION_A','REGION_B') 
                and load_dt = '2023-05-24'

        Example 2:

            config:

                {
                    "expression" : 
                        "load_dt='2023-05-24' and source_cd='SOURCE_SYS_A'"
                }

            result:

                load_dt='2023-05-24' and source_cd='SOURCE_SYS_A'

        """

        # An expression will take precedence
        if partition_specs.expression:
            return partition_specs.expression

        partition_expr = ''

        partition_specs_expr_list = []

        if partition_specs.static:
            for static_spec in partition_specs.static:
                for k,v in static_spec.items():
                    if isinstance(v,str):
                        partition_specs_expr_list.append(f"{k} = '{v}'")
                    if isinstance(v,list):
                        value_list = ','.join(list(map(lambda x: f"'{x}'",v)))
                        partition_specs_expr_list.append(f"{k} IN ({value_list})")

        if partition_specs.relative_date:
            
            now = datetime.now()
            # iterate over the possible DateAttributes and add to partition
            # expression list
            for k,v in partition_specs.relative_date.items():
                date_attributes = v.dict()
                timedelta_specs = {
                    atr_key : atr_value
                    for atr_key, atr_value
                    in date_attributes.items()
                    if atr_value
                }
                # creating a timedelta object given relative_date specs
                td = timedelta(**timedelta_specs)
                date_value = (now + td).strftime('%Y-%m-%d')
                partition_specs_expr_list.append(f"{k} = '{date_value}'")

        # concatenate expressions
        if partition_specs_expr_list:
            partition_expr = ' AND '.join(partition_specs_expr_list)
    
        return partition_expr


    def launch(self):
        self.logger.info("Launching optimizer")

        job_specs = self._validate_job_specs()
        self.logger.info(f"Table Specs: {job_specs}")

        opt_results = self.optimize(job_specs.table_specs)
        self.logger.info(f"Optimization results: {opt_results.select(to_json('metrics')).collect()[0][0]}")

        self.logger.info(f"Done optimizing {job_specs}")

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    
    p = ArgumentParser()
    p.add_argument("--schema", required=False, type=str)
    p.add_argument("--full", required=False,action='store_true')
    namespace = p.parse_known_args(sys.argv[1:])[0]

    task = OptimizeTask()

    if namespace.schema:
        task.conf['table_specs']['schema'] = namespace.schema

    if namespace.full:
        if task.conf.get('table_specs').get('partition'):
            del task.conf['table_specs']['partition']

    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()
