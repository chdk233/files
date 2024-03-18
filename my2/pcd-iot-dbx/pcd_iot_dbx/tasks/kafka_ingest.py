from pcd_iot_dbx.common import Task
from pcd_iot_dbx.etl.transformations.common import set_current_datetime_fields, straight_move
from pcd_iot_dbx.util.secrets import retrieve_ssl_password
from pcd_iot_dbx.etl.schemas.common import kafka_topic
from pcd_iot_dbx.etl import ETL
from pcd_iot_dbx import certs

class KafkaIngest(Task):
    def _read_stream(self, options):

        read_stream_format = self.conf.get('read_stream').get('format','kafka')

        if read_stream_format == 'kafka':

            options['kafka.ssl.truststore.password'] = retrieve_ssl_password(self,options.get('kafka.ssl.truststore.password'))
            options['kafka.ssl.keystore.password'] = retrieve_ssl_password(self,options.get('kafka.ssl.keystore.password'))
            options['kafka.ssl.key.password'] = retrieve_ssl_password(self,options.get('kafka.ssl.key.password'))
            options['kafka.ssl.keystore.location'] = certs.where(options.get('kafka.ssl.keystore.location'))
            options['kafka.ssl.truststore.location'] = certs.where(options.get('kafka.ssl.truststore.location'))

            read_stream = (
                self.spark.readStream.format(read_stream_format)
                .options(**options)
                .schema(self.etl.get_schema())
                .load(self.conf.get('read_stream').get('source_s3_location'))
            )

        else:
            #Used for unit testing in lieu of ephemeral kafka topic
            read_stream = (
                self.spark.readStream.format(read_stream_format)
                .options(**options)
                .schema(self.etl.get_schema())
                .load(self.conf.get('read_stream').get('source_s3_location'))
            )

        return read_stream

    def handle_microbatch(self, micro_batch_df, batch_id):
        micro_batch_options = self.conf.get("micro_batch")

        fq_table_name = (
            micro_batch_options.get("save_as").get("schema") + "." + micro_batch_options.get("save_as").get("table")
        )

        self.logger.info(f"entering microbatch batch_id {batch_id} for {fq_table_name}")

        transformed_df = self.etl.get_transformation_func()(df=micro_batch_df)

        transformed_df_with_etl_fields = \
            set_current_datetime_fields(df=transformed_df)

        transformed_df_with_etl_fields.write.mode(micro_batch_options.get('mode')).saveAsTable(fq_table_name)

    def _write_stream(self, read_stream, options ,trigger):
        return (
            read_stream.writeStream.queryName(f"Kafka-{self.conf.get('name')}")
            .options(**options)
            .foreachBatch(self.handle_microbatch)
            .trigger(**trigger)
            .start()
        )

    def launch(self,schema=None,transformation_func=None):
        self.logger.info(f'Launching kafka ingest task for {self.conf.get("name")}')

        self.etl = ETL(program=self.conf.get('program'),
                       name=self.conf.get("name"),
                       schema=schema,
                       transformation_func=transformation_func)

        rs_properties = self.conf.get("read_stream")

        stream = self._read_stream(options=rs_properties.get("options"))

        ws_properties = self.conf.get("write_stream")

        query = self._write_stream(
            read_stream=stream, 
            options=ws_properties.get("options"),
            trigger=ws_properties.get('trigger')
        )

        query.awaitTermination()

        self.logger.info(f'Kafka ingest task for {self.conf.get("name")} has finished!')


# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover

    task = KafkaIngest()

    task.launch()


# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == "__main__":
    entrypoint()
