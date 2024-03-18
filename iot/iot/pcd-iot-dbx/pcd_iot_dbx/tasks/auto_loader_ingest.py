from pcd_iot_dbx.common import Task
from pcd_iot_dbx.etl.transformations.common import set_load_dt_hr_frm_date_in_file_nm
from pcd_iot_dbx.etl import ETL

from argparse import ArgumentParser
import sys


class AutoLoaderIngest(Task):
    def _read_stream(self, schema, options, source_s3_location):
        return (
            self.spark.readStream.format(self.conf.get('read_stream').get('format'))
            .options(**options)
            .schema(schema)
            .load(source_s3_location)
            .selectExpr("*", "_metadata")
        )

    def handle_microbatch(self, micro_batch_df, batch_id):
        micro_batch_options = self.conf.get("micro_batch")

        fq_table_name = (
            micro_batch_options.get("save_as").get("schema") + "." + micro_batch_options.get("save_as").get("table")
        )

        self.logger.info(f"entering microbatch batch_id {batch_id} for {fq_table_name}")

        transformed_df = self.etl.get_transformation_func()(df=micro_batch_df)

        transformed_df.write.mode(micro_batch_options.get('mode')).saveAsTable(fq_table_name)

    def _write_stream(self, read_stream, options ,trigger):
        return (
            read_stream.writeStream.queryName(f"AutoLoader-{self.conf.get('name')}")
            .options(**options)
            .foreachBatch(self.handle_microbatch)
            .trigger(**trigger)
            .start()
        )

    def launch(self,schema=None,transformation_func=None):
        self.logger.info(f"Launching auto loader for {self.conf.get('name')}")

        self.etl = ETL(program=self.conf.get('program'),
                       name=self.conf.get("name"),
                       schema=schema,
                       transformation_func=transformation_func)

        rs_properties = self.conf.get("read_stream")

        stream = self._read_stream(
            schema=self.etl.get_schema(),
            options=rs_properties.get("options"),
            source_s3_location=rs_properties.get("source_s3_location")
        )

        ws_properties = self.conf.get("write_stream")

        query = self._write_stream(
            read_stream=stream, 
            options=ws_properties.get("options"),
            trigger=ws_properties.get('trigger')
        )

        query.awaitTermination()

        self.logger.info(f"Auto loader task for {self.conf.get('name')} has finished!")


# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    p = ArgumentParser()
    p.add_argument("--schema", required=False, type=str)

    namespace = p.parse_known_args(sys.argv[1:])[0]

    task = AutoLoaderIngest()

    if namespace.schema:
        task.conf["options"]["micro_batch"]["save_as"]["schema"] = namespace.schema

    task.launch()


# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == "__main__":
    entrypoint()
