from pcd_iot_dbx.common import Task
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime
import smtplib
# import email
from email.utils import COMMASPACE
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email import encoders



class rivianReporting(Task):
    def send_email(self, sender, recipients, subject, body, av_aggregate_file_name, trips_summary_file_name,  file_path):
        #build email structure
        # MIME_ATTACHMENT = MIMEBase('application','vnd.ms-excel')
        recipients = [x.strip() for x in recipients.split(',')]
        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = COMMASPACE.join(recipients)
        msg.preamble = subject
        msg.attach(MIMEText(body,'plain'))
        
        #attach csv
        attachment = open(file_path + av_aggregate_file_name, 'rb') 
        p = MIMEBase('application', 'octet-stream')
        p.set_payload(attachment.read())
        p.add_header('Content-Disposition', 'attachment', filename=av_aggregate_file_name)
        encoders.encode_base64(p)
        msg.attach(p)
        
        attachment2 = open(file_path + trips_summary_file_name, 'rb') 
        p2 = MIMEBase('application', 'octet-stream')
        p2.set_payload(attachment2.read())
        p2.add_header('Content-Disposition', 'attachment', filename=trips_summary_file_name)
        encoders.encode_base64(p2)
        msg.attach(p2)
        
        #send to recipients
        s = smtplib.SMTP('mail-gw.ent.nwie.net')
        s.sendmail(sender, recipients, msg.as_string())
        s.quit()
    
    
      
    def data_load(self,deltadatabase,path,av_aggregate_file_name,trips_summary_file_name):  
        

        #save as delta table
        av_aggregate_df=self.spark.sql(f"""select * from (Select *
                                            ,load_dt as Data_as_of_dt
                                            ,row_number() Over(PARTITION by Vin ORDER BY load_dt desc, pol_exp_date  ) as Rnk
                                            from {deltadatabase}.av_aggregate ) where rnk=1""").drop("rnk")

        av_aggregate_df.withColumn("ETL_ROW_EFF_DTS",current_timestamp())\
            .write.format("delta").mode("overWrite").saveAsTable(f"{deltadatabase}.rivian_reporting_av_aggregate")
       
        trips_summary_df=self.spark.sql(f"""select * from (Select *
                                        ,load_dt as Data_as_of_dt
                                        ,row_number() Over(PARTITION by trip_id ORDER BY load_dt  desc) as Rnk
                                        from {deltadatabase}.trips_summary   ) where rnk=1""").drop("rnk")
  
        trips_summary_df.withColumn("ETL_ROW_EFF_DTS",current_timestamp())\
            .write.format("delta").mode("overWrite").saveAsTable(f"{deltadatabase}.rivian_reporting_trips_summary")
        
    
        #save as csv files
        av_aggregate_df.drop("load_dt","load_hr_ts","db_load_dt","db_load_time","ETL_ROW_EFF_DTS")\
            .toPandas().to_csv(path + av_aggregate_file_name, index=False)
        trips_summary_df.drop("load_dt","load_hr_ts","db_load_dt","db_load_time","ETL_ROW_EFF_DTS")\
            .toPandas().to_csv(path + trips_summary_file_name, index=False)
    
    def launch(self):
        self.logger.info(f"running rivian reporting notebook")

        environment=self.conf.get('environment')
        deltadatabase=self.conf.get('deltadatabase')
        email_from=self.conf.get('email_from')
        email_to=self.conf.get('email_to')

        delta = datetime.timedelta(hours=-5)
        est = datetime.timezone(delta, name="EST")
        today = datetime.datetime.today().astimezone(est).strftime('%Y-%m-%d')
        av_aggregate_file_name = f'rivian_av_aggregate_weekly_report_{today}.csv'
        trips_summary_file_name = f'rivian_trips_summary_weekly_report_{today}.csv'
        path = self.conf.get('path')
        email_subject = f'{environment}: RIVIAN Weekly Report  ' +  today
        email_body = 'Databricks job: https://nationwide-pcds-prod-virginia.cloud.databricks.com/?o=3828446292145485#job/1090147371975878 \n\nThanks,\nP&C Data Management'

        self.data_load(deltadatabase, path, av_aggregate_file_name, trips_summary_file_name)
        self.send_email(email_from, email_to, email_subject, email_body,  av_aggregate_file_name, trips_summary_file_name, path)
        
        self.logger.info(f"reporting task for rivian has finished!")
        
    

# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  
    task = rivianReporting()


    task.launch()


# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == "__main__":
    entrypoint()
