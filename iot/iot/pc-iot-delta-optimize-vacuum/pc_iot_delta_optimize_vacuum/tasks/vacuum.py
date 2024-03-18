from pc_iot_delta_optimize_vacuum.common import *
from delta.tables import DeltaTable
from argparse import ArgumentParser

class VacuumTask(Task):
    """
    init_conf required to launch
    -----

    table_specs: dict

        path: str (conditional vs table/schema)

        table: str (conditional vs path)

        schema: str (conditional vs path)

        retention_hours: float (optional)
    """

    def vacuum(self,table_specs: TableSpecs):



        if table_specs.path:

            delta_table_func = DeltaTable.forPath
            delta_table_args = {'path' : table_specs.path }
        else:
            delta_table_func = DeltaTable.forName
            delta_table_args = {'tableOrViewName' :  table_specs.schema_name + '.' + 
                                table_specs.table }


        dt = delta_table_func(sparkSession=self.spark,
                              **delta_table_args)

        retention_hours = table_specs.retention_hours

        if isinstance(retention_hours,float):
            return dt.vacuum(retention_hours)
        else: 
            return  dt.vacuum()



    def launch(self):
        self.logger.info("Launching vacuum")

        job_specs = self._validate_job_specs()
        self.logger.info(f"Table Specs: {job_specs}")

        self.vacuum(job_specs.table_specs)

        self.logger.info(f"Done vacuuming {job_specs}")

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    
    p = ArgumentParser()
    p.add_argument("--schema", required=False, type=str)
    namespace = p.parse_known_args(sys.argv[1:])[0]

    task = VacuumTask()

    if namespace.schema:
        task.conf['table_specs']['schema'] = namespace.schema

    task.launch()

# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == '__main__':
    entrypoint()
