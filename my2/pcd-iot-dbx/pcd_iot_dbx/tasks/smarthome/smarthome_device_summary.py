from pcd_iot_dbx.common import Task
from argparse import ArgumentParser
import sys

class device_summary_Task(Task):
    

    def device_summary(self):

        tables=self.conf.get('device_summary')

        src_table_name = (
            self.conf.get("schema") + "." + tables.get("source")
        )
        
        trgt_table_name = (
            self.conf.get("schema") + "." + tables.get("target")
        )

        devc_summary_DF=self.spark.sql(f"""select 
                                      cast(row_number() over(order by NULL) as BIGINT) as SMARTHOME_DEVICE_SUMMARY_ID,
                                      a.DEVICE_ID,
                                      a.POLICY_KEY,
                                      a.POLICY_KEY_ID,
                                      a.PLCY_RT_ST_CD,
                                      a.ENRLMNT_EFF_DT,
                                      a.PARTNER_MEMBER_ID,
                                      a.DATA_COLLECTION_ID,
                                      d.ACTVTN_DT,
                                      a.DEVC_FIRST_STTS_DT,
                                      a.DEVC_LST_STTS_DT,
                                      a.DEVC_STTS_CD,
                                      case when a.DEVC_STTS_CD='ACTIVE' then '1' else '0' end DEVC_CNCT_IN,
                                      case when a.DEVC_STTS_CD='INACTIVE' then '1' else '0' end DEVC_UNAVL_IN,
                                      a.QT_USER_ID,
                                      a.QT_SRC_IDFR,
                                      a.PLCY_TP,
                                      a.PRDCR_OFFC,
                                      a.PRDCR_TP,
                                      a.PLCY_TERM,
                                      a.PROTCTV_DEVC_DISC_AMT,
                                      a.HOME_CAR_DISC_AMT,
                                      a.ACQUISITION_CHANNEL_CD,
                                      a.SERVICING_CHANNEL_CD,
                                      a.CONTEXT_TOPIC,
                                      a.CONTEXT_ID,
                                      a.CONTEXT_SOURCE,
                                      a.CONTEXT_TIME,
                                      a.CONTEXT_TYPE,
                                      a.PRGRM_ENROLLMENT_ID,
                                      a.DEVC_CATEGORY,
                                      a.PLCY_DESC_TYPE,
                                      a.MEMBER_TYPE,
                                      LIVE_STATUS_IN,
                                      LIVE_LOAD_DT,
                                      b.DEVC_SHIPPED_DT,
                                      c.INSTALLED_DATE,
                                      SRC_SYS_CD,
                                      LOAD_DT,
                                      LOAD_HR_TS,
                                      ETL_ROW_EFF_DTS,
                                      current_timestamp() as ETL_LAST_UPDT_DTS
                                      FROM (select *,row_number() over (partition by DEVICE_ID,POLICY_KEY,PARTNER_MEMBER_ID,DATA_COLLECTION_ID order by DEVC_LST_STTS_DT desc) row_nb from
                                      {src_table_name}) a
                                      left join
                                        (select DEVICE_ID,POLICY_KEY,PARTNER_MEMBER_ID,DATA_COLLECTION_ID,min(DEVC_LST_STTS_DT) as DEVC_SHIPPED_DT from {src_table_name} where DEVC_STTS_CD='SHIPPED' group by DEVICE_ID,POLICY_KEY,PARTNER_MEMBER_ID,DATA_COLLECTION_ID) b 
                                            on a.DEVICE_ID=b.DEVICE_ID and a.POLICY_KEY=b.POLICY_KEY and (a.PARTNER_MEMBER_ID=b.PARTNER_MEMBER_ID or (a.PARTNER_MEMBER_ID is null and b.PARTNER_MEMBER_ID is null)) and (a.DATA_COLLECTION_ID=b.DATA_COLLECTION_ID or (a.DATA_COLLECTION_ID is null and b.DATA_COLLECTION_ID is null))
                                      left join
                                        (select DEVICE_ID,POLICY_KEY,PARTNER_MEMBER_ID,DATA_COLLECTION_ID,min(DEVC_LST_STTS_DT) as INSTALLED_DATE from {src_table_name} where DEVC_STTS_CD='INSTALLED' group by DEVICE_ID,POLICY_KEY,PARTNER_MEMBER_ID,DATA_COLLECTION_ID) c 
                                            on a.DEVICE_ID=c.DEVICE_ID and a.POLICY_KEY=c.POLICY_KEY and (a.PARTNER_MEMBER_ID=c.PARTNER_MEMBER_ID or (a.PARTNER_MEMBER_ID is null and c.PARTNER_MEMBER_ID is null)) and (a.DATA_COLLECTION_ID=c.DATA_COLLECTION_ID or (a.DATA_COLLECTION_ID is null and c.DATA_COLLECTION_ID is null)) 
                                      left join
                                        (select DEVICE_ID,POLICY_KEY,PARTNER_MEMBER_ID,DATA_COLLECTION_ID,min(DEVC_LST_STTS_DT) as ACTVTN_DT from {src_table_name} where DEVC_STTS_CD='ACTIVE' group by DEVICE_ID,POLICY_KEY,PARTNER_MEMBER_ID,DATA_COLLECTION_ID) d 
                                            on a.DEVICE_ID=d.DEVICE_ID and a.POLICY_KEY=d.POLICY_KEY and (a.PARTNER_MEMBER_ID=d.PARTNER_MEMBER_ID or (a.PARTNER_MEMBER_ID is null and d.PARTNER_MEMBER_ID is null)) and (a.DATA_COLLECTION_ID=d.DATA_COLLECTION_ID or (a.DATA_COLLECTION_ID is null and d.DATA_COLLECTION_ID is null))
                                        where row_nb=1""")

        devc_summary_DF.write.mode("overWrite").saveAsTable(trgt_table_name)



    def launch(self):
        self.logger.info("Launching smarthome device_summary task")


        self.device_summary()

        self.logger.info(f"smarthome device_summary task has finished")

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  # pragma: no cover

    task = device_summary_Task()
    p = ArgumentParser()
    p.add_argument("--schema", required=False, type=str)

    namespace = p.parse_known_args(sys.argv[1:])[0]

    task = device_summary_Task()

    if namespace.schema:
        task.conf["schema"] = namespace.schema

    task.launch()


# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == "__main__":
    entrypoint()
