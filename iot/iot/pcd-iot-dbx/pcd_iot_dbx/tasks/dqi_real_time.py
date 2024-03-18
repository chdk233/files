from pcd_iot_dbx.common import Task
from pcd_iot_dbx.dqi.smarthome import NOTION_NAME_TO_DQI_CHECK, LEAKBOT_NAME_TO_DQI_CHECK


class DQITask(Task):
    def _get_dqi_checks(self,program:str, name:str):
        mappings = {"smarthome_notion": NOTION_NAME_TO_DQI_CHECK,
                    "smarthome_leakbot": LEAKBOT_NAME_TO_DQI_CHECK}

        return mappings.get(program).get(name)

    def _read_stream(self, options):
        read_stream_format = self.conf.get("read_stream").get("format")

        read_stream = (
            self.spark.readStream.format(read_stream_format)
            .options(**options)
            .table(self.conf.get("read_stream").get("table_source"))
        )

        return read_stream

    def handle_microbatch(self, micro_batch_df, batch_id):
        
        self.dqi.set_dataframe(micro_batch_df)

        self.dqi.execute_dqi_checks()

        self.dqi.check_dqi_results()

    def _write_stream(self, read_stream, options, trigger):
        return (
            read_stream.writeStream.queryName(f"DQI-{self.conf.get('name')}")
            .options(**options)
            .foreachBatch(self.handle_microbatch)
            .trigger(**trigger)
            .start()
        )

    def launch(self):
        self.logger.info(f'Launching dqi check task for {self.conf.get("name")}')

        dqi_check_cls = self._get_dqi_checks(program=self.conf.get('program'),
                                           name=self.conf.get('name'))

        self.dqi = dqi_check_cls(spark=self.spark,init_conf=self.conf)

        rs_properties = self.conf.get("read_stream")

        stream = self._read_stream(options=rs_properties.get("options"))

        ws_properties = self.conf.get("write_stream")

        query = self._write_stream(
            read_stream=stream, options=ws_properties.get("options"), trigger=ws_properties.get("trigger")
        )

        query.awaitTermination()

        self.logger.info(f'DQI task for {self.conf.get("name")} has finished!')


# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    task = DQITask()

    task.launch()


# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == "__main__":
    entrypoint()
