from pcd_iot_dbx.common import Task
from argparse import ArgumentParser
import sys

class device_summary_view_Task(Task):
    

    def device_summary_view(self):

        tables=self.conf.get('device_summary_view')

        src_table_name = (
            self.conf.get("schema") + "." + tables.get("source")
        )
        
        trgt_table_name = (
            self.conf.get("schema") + "." + tables.get("target")
        )

        SmartHome_Device_Summary_VW=self.spark.sql(f"""CREATE OR REPLACE VIEW {trgt_table_name} AS
                                                SELECT 
                                                NULL as offset
                                                ,NULL as partition
                                                ,CONTEXT_TIME as timestamp_value 
                                                ,NULL as timestampType
                                                ,CONTEXT_TOPIC as topic
                                                ,CONTEXT_ID as id
                                                ,CONTEXT_SOURCE as source
                                                ,CONTEXT_TIME as time
                                                ,CONTEXT_TYPE as type
                                                ,PRGRM_ENROLLMENT_ID as enrollmentId
                                                ,POLICY_KEY as policyNumber
                                                ,ENRLMNT_EFF_DT as enrollmentEffectiveDate 
                                                ,Case 
                                                when PARTNER_MEMBER_ID is null then DATA_COLLECTION_ID
                                                when DATA_COLLECTION_ID is null then PARTNER_MEMBER_ID
                                                End as partnerMemberId 
                                                ,DEVC_CATEGORY as categories 
                                                ,LOAD_DT as load_date 
                                                ,NULL as enrollmentRemovalReason
                                                ,DEVC_SHIPPED_DT as shipped_date
                                                ,ACTVTN_DT as Activation_date 
                                                ,LIVE_STATUS_IN as Live_status 
                                                ,LIVE_LOAD_DT as live_load_date 
                                                ,QT_USER_ID as quote_user_id 
                                                ,QT_SRC_IDFR as quote_source_idfr 
                                                ,PLCY_TP as  policy_type_001 
                                                ,PRDCR_OFFC as PDCR_OFFC_NAME_L102 
                                                ,PRDCR_TP as PDCR_TYPE_L102 
                                                ,PLCY_TERM as POL_Term_01 
                                                ,PROTCTV_DEVC_DISC_AMT as PROT_DEV_DISC_AMT_11 
                                                ,HOME_CAR_DISC_AMT as HOME_CAR_DISC_AMT_11 
                                                ,PLCY_DESC_TYPE as PLCY_DESC_TYPE
                                                from {src_table_name}
                                                """)
                                              
        # SmartHome_Device_Summary_VW.write.mode("overWrite").saveAsTable(trgt_table_name)



    def launch(self):
        self.logger.info("Launching smarthome device_summary_view task")


        self.device_summary_view()

        self.logger.info(f"smarthome device_summary_view task has finished")

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover
    
    task = device_summary_Task()
    p = ArgumentParser()
    p.add_argument("--schema", required=False, type=str)

    namespace = p.parse_known_args(sys.argv[1:])[0]

    task = device_summary_view_Task()
    
    if namespace.schema:
        task.conf["schema"] = namespace.schema

    task.launch()


# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == "__main__":
    entrypoint()
