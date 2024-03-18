from pcd_iot_dbx.common import Task
from pyspark.sql.types import *
from pyspark.sql.functions import *
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib



class rivianDQI(Task):

    def send_mail(self,email_from, email_to, email_subject, email_body):
        recipients = [x.strip() for x in email_to.split(',')]
        msg = MIMEMultipart()
        msg['Subject'] = email_subject
        msg['From'] = email_from
        msg['To'] = ','.join(recipients)
        msg.preamble = email_subject

        body = MIMEText(email_body, 'html')
        msg.attach(body)

        s = smtplib.SMTP('mail-gw.ent.nwie.net')
        s.sendmail(email_from, recipients, msg.as_string())
        s.quit()
    
    def insert_audit_data(self,row,deltadatabase):
        schema = StructType([
                    StructField('audit_check', StringType(), True),
                    StructField('no_of_errortables', StringType(), True),
                    StructField('previous_week_error_tables', StringType(), True),
                    StructField('load_dt', StringType(), True)
                    ])
        df=self.spark.createDataFrame(data=row,schema=schema).withColumn("load_dt",col("load_dt").cast(DateType()))
        df.write.mode("append").saveAsTable(f"{deltadatabase}.rivian_audit")

    def run_file_availability_check(self,email_from, email_to,deltadatabase):
        
        today_date=self.spark.sql("select cast(date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date)) as string) as date   ").collect()[0]["date"] 
        file_missing_previous, file_missing_current = [], []
        av_aggregate_previous_week_file_count_chk = self.spark.sql(f"""select case when count(*)>0 then '' else concat('av_aggregate data missing for the date: ',date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date-7)%7),current_date-7))) end as previous_count_av_aggregate from {deltadatabase}.av_aggregate where load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date-7)%7),current_date-7))""").collect()[0]["previous_count_av_aggregate"]
        av_aggregate_current_day_file_count_chk = self.spark.sql(f"select case when count(*)>0 then '' else concat('av_aggregate data missing for the date: ',current_date()) end as current_count_av_aggregate from {deltadatabase}.av_aggregate where load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date))").collect()[0]["current_count_av_aggregate"]
        
        if av_aggregate_previous_week_file_count_chk:
            file_missing_previous.append(av_aggregate_previous_week_file_count_chk)
        
        if av_aggregate_current_day_file_count_chk:
            file_missing_current.append(av_aggregate_current_day_file_count_chk)

        trips_previous_week_file_count_chk = self.spark.sql(f"select case when count(*)>0 then '' else concat('trips data missing for the date: ',date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date-7)%7),current_date-7))) end as previous_count_trips from {deltadatabase}.trips where load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date-7)%7),current_date-7))").collect()[0]["previous_count_trips"]
        trips_current_day_file_count_chk = self.spark.sql(f"select case when count(*)>0 then '' else concat('trips data missing for the date: ',current_date()) end as current_count_trips from {deltadatabase}.trips where load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date))").collect()[0]["current_count_trips"]
        
        if trips_previous_week_file_count_chk:
            file_missing_previous.append(trips_previous_week_file_count_chk)
        
        if trips_current_day_file_count_chk:
            file_missing_current.append(trips_current_day_file_count_chk)
            
        trips_summary_previous_week_file_count_chk = self.spark.sql(f"select case when count(*)>0 then '' else concat('trips_summary data missing for the date: ',date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date-7)%7),current_date-7))) end as previous_count_trips_summary from {deltadatabase}.trips_summary where load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date-7)%7),current_date-7))").collect()[0]["previous_count_trips_summary"]
        trips_summary_current_day_file_count_chk = self.spark.sql(f"select case when count(*)>0 then '' else concat('trips_summary data missing for the date: ',current_date()) end as current_count_trips_summary from {deltadatabase}.trips_summary where load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date))").collect()[0]["current_count_trips_summary"]
        
        if trips_summary_previous_week_file_count_chk:
            file_missing_previous.append(trips_summary_previous_week_file_count_chk)
        
        if trips_summary_current_day_file_count_chk:
            file_missing_current.append(trips_summary_current_day_file_count_chk)
                
        event_summary_previous_week_file_count_chk = self.spark.sql(f"select case when count(*)>0 then '' else concat('event_summary data missing for the date: ',date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date-7)%7),current_date-7))) end as previous_count_event_summary from {deltadatabase}.event_summary where load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date-7)%7),current_date-7))").collect()[0]["previous_count_event_summary"]
        event_summary_current_day_file_count_chk = self.spark.sql(f"select case when count(*)>0 then '' else concat('event_summary data missing for the date: ',current_date()) end as current_count_event_summary from {deltadatabase}.event_summary where load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date))").collect()[0]["current_count_event_summary"]
        
        if event_summary_previous_week_file_count_chk:
            file_missing_previous.append(event_summary_previous_week_file_count_chk)
        
        if event_summary_current_day_file_count_chk:
            file_missing_current.append(event_summary_current_day_file_count_chk)
        
        trips_odometer_previous_week_file_count_chk = self.spark.sql(f"select case when count(*)>0 then '' else concat('trips_odometer data missing for the date: ',date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date-7)%7),current_date-7))) end as previous_count_trips_odometer from {deltadatabase}.trips_odometer where load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date-7)%7),current_date-7))").collect()[0]["previous_count_trips_odometer"]
        trips_odometer_current_day_file_count_chk = self.spark.sql(f"select case when count(*)>0 then '' else concat('trips_odometer data missing for the date: ',current_date()) end as current_count_trips_odometer from {deltadatabase}.trips_odometer where load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date))").collect()[0]["current_count_trips_odometer"]
        
        if trips_odometer_previous_week_file_count_chk:
            file_missing_previous.append(trips_odometer_previous_week_file_count_chk)
        
        if trips_odometer_current_day_file_count_chk:
            file_missing_current.append(trips_odometer_current_day_file_count_chk)
            
        #   email_from, email_to = SNSArn
        previous_list_length = len(file_missing_previous)
        current_list_length = len(file_missing_current)
        row=[("run_file_availability_check",current_list_length,previous_list_length,today_date)]
        self.insert_audit_data(row,deltadatabase)

        print("Missing files for previous date: ",file_missing_previous)
        print("Missing files for current date: ",file_missing_current)
        
        if(previous_list_length > 0):
            previous_week = file_missing_previous[0].split(": ")[1]
            if(len(file_missing_previous) == 5):
                message = f"No tables loaded for previous run date: {previous_week}"
                subject = f"Rivian Daily File Audit failure: No Data loaded for previous run date - {previous_week}"
                print(message)
                print(subject)
                self.send_mail(email_from, email_to,subject,message)
            else:
                message = "Following Daily Tables are not loaded: \n\n"+".\n".join(file_missing_previous)
                subject = f"Rivian Daily File Audit failure: Daily tables not loaded for previous date - {previous_week}"
                print(message)
                print(subject)
                self.send_mail(email_from, email_to,subject,message)
        
        if(current_list_length > 0):
            current_date = file_missing_current[0].split(": ")[1]  
            if(len(file_missing_current) == 5):
                message = f"No tables loaded for current run date: {current_date}"
                subject = f"Rivian Daily File Audit failure: No tables loaded for current run date - {current_date}"
                print(message)
                print(subject)
                self.send_mail(email_from, email_to,subject,message)
            else:
                message = "Following Daily tables are not loaded: \n\n"+".\n".join(file_missing_current)
                subject = f"Rivian Daily File Audit failure: Daily tables not loaded for current run date - {current_date}"
                print(message)
                print(subject)
                self.send_mail(email_from, email_to,subject,message) 
        # return current_list_length,previous_list_length

    def data_availability_check(self,email_from, email_to,deltadatabase):
	  
        today_date=self.spark.sql("select cast(date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date)) as string) as date   ").collect()[0]["date"] 
        trip_summary_df=self.spark.read.table(f"{deltadatabase}.trips_summary").where("load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date))")
        trips_df=self.spark.read.table(f"{deltadatabase}.trips").where("load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date))")
        av_aggregate_df=self.spark.read.table(f"{deltadatabase}.av_aggregate").where("load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date))")
        
        trips_error_ts_df=trips_df.join(trip_summary_df,trips_df.trip_id==trip_summary_df.trip_id,"leftanti").select("trip_id").distinct()
        av_aggregate_error_ts_df=trip_summary_df.join(av_aggregate_df,trip_summary_df.vin==av_aggregate_df.vin,"leftanti").select("vin").distinct()
        av_aggregate_error_trips_df=trips_df.join(av_aggregate_df,trips_df.vin==av_aggregate_df.vin,"leftanti").select("vin").distinct()
	  

        trips_error= [row.trip_id for row in trips_error_ts_df.select("trip_id").distinct().collect()]
        av_aggregate_error_ts= [row.vin for row in av_aggregate_error_ts_df.select("vin").distinct().collect()]
        av_aggregate_error_trips= [row.vin for row in av_aggregate_error_trips_df.select("vin").distinct().collect()]
        today_date= self.spark.sql("select cast(date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date)) as string) as date   ").collect()[0]["date"] 
        count=0
        if (len(trips_error)>0):
            count=count+1
            message = f"Following trip_id's are not there in trips_summary table {trips_error}"
            subject = f"Rivian Daily File Audit failure: data check failure for the date - {today_date}"
            print(message)
            print(subject)
            self.send_mail(email_from, email_to,subject,message)
        if (len(av_aggregate_error_ts)>0):
            count=count+1
            message = f"Following trip_summary vin's are not there in av_aggregate table {av_aggregate_error_ts}"
            subject = f"Rivian Daily File Audit failure: data check failure for the date -{today_date} "
            print(message)
            print(subject)
            self.send_mail(email_from, email_to,subject,message)
        if (len(av_aggregate_error_trips)>0):
            count=count+1
            message = f"Following trips vin's are not there in av_aggregate table {av_aggregate_error_trips}"
            subject = f"Rivian Daily File Audit failure: data check failure for the date - {today_date}"
            print(message)
            print(subject)
            self.send_mail(email_from, email_to,subject,message)

        row=[("data_availability_check",count,0,today_date)]
        self.insert_audit_data(row,deltadatabase)
        # return len(trips_error),len(av_aggregate_error_ts),len(av_aggregate_error_trips)

    def data_duplicates_check(self,email_from, email_to,deltadatabase):
        tables_list=[]
        today_date=self.spark.sql("select cast(date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date)) as string) as date   ").collect()[0]["date"] 
        trip_summary_dups_df=self.spark.sql(f"""select * from (select vin,trip_id,trip_start,trip_end,tzo,distance,duration,av_distance,av_duration,max(load_dt) as max_load_date,min(load_dt) as min_load_date from {deltadatabase}.trips_summary  group by vin,trip_id,trip_start,trip_end,tzo,distance,duration,av_distance,av_duration having count(*)>1) where max_load_date!=min_load_date and max_load_date=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date)) """)
        if (trip_summary_dups_df.count()>0):
            tables_list.append("duplicates found in trips_summary when compared to previous load")
            
        trips_dups_df=self.spark.sql(f"""select * from (select vin,trip_id,utc_time,speed,tzo,max(load_dt) as max_load_date,min(load_dt) as min_load_date from {deltadatabase}.trips  group by vin,trip_id,utc_time,speed,tzo having count(*)>1) where max_load_date!=min_load_date and max_load_date=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date)) """)
        if (trips_dups_df.count()>0):
            tables_list.append("duplicates found in trips when compared to previous load")
        
        event_summary_dups_df=self.spark.sql(f"""select * from (select vin,event_id,event_name,event_timestamp,tzo,max(load_dt) as max_load_date,min(load_dt) as min_load_date from {deltadatabase}.event_summary  group by vin,event_id,event_name,event_timestamp,tzo having count(*)>1) where max_load_date!=min_load_date and max_load_date=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date)) """)
        if (event_summary_dups_df.count()>0):
            tables_list.append("duplicates found in event_summary when compared to previous load")
        
        trips_odometer_dups_df=self.spark.sql(f"""select * from (select vin,trip_id,utc_time,tzo,mileage_delta,max(load_dt) as max_load_date,min(load_dt) as min_load_date from {deltadatabase}.trips_odometer  group by vin,trip_id,utc_time,tzo,mileage_delta having count(*)>1) where max_load_date!=min_load_date and max_load_date=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date)) """)
        if (trips_odometer_dups_df.count()>0):
            tables_list.append("duplicates found in trips_odometer when compared to previous load")
            
        row=[("data_duplicates_check",len(tables_list),0,today_date)]
        self.insert_audit_data(row,deltadatabase)
        
        if(len(tables_list) > 0):
            if(len(tables_list) == 5):
                message = f"Duplicates found when compared to previous load run date"
                subject = f"Rivian Daily-Audit failure:Duplicates found when compared to previous load for run date- {today_date}"
                print(message)
                print(subject)
                self.send_mail(email_from, email_to,subject,message)
            else:
                message = "Data check failed for following Daily Tables : \n\n"+".\n".join(tables_list)
                subject = f"Rivian Daily-Audit failure:Duplicates found when compared to previous load for run date- {today_date}"
                print(message)
                print(subject)
                self.send_mail(email_from, email_to,subject,message)
        # return len(tables_list)  
    
    def fields_validation(self,email_from, email_to,deltadatabase):
	  
        today_date=self.spark.sql("select cast(date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date)) as string) as date   ").collect()[0]["date"] 
        trip_summary_null_df=self.spark.sql(f"select * from {deltadatabase}.trips_summary where load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date)) and (trip_id is null or vin is null or load_dt is null)")
        av_aggregate_null_df=self.spark.sql(f"select * from {deltadatabase}.av_aggregate where load_dt=date(DATEADD(DAY,-(DATEDIFF(DAY,'1900-01-05',current_date)%7),current_date)) and (vin is null or load_dt is null or pol_exp_date is null)")
        count=0
        if (trip_summary_null_df.count()>0):
            count=count+1
            message = f"Null constraints check failed for the trip_summary table \n\nPlease Validate the trip_id,vin,load_dt fields"
            subject = f"Rivian Daily File Audit failure: data check failure for the date - {today_date}"
            print(message)
            print(subject)
            self.send_mail(email_from, email_to,subject,message)
        if (av_aggregate_null_df.count()>0):
            count=count+1
            message = f"Null constraints check failed for the trip_summary table \n\nPlease Validate the vin,pol_exp_date,load_dt fields"
            subject = f"Rivian Daily File Audit failure: data check failure for the date - {today_date}"
            print(message)
            print(subject)
            self.send_mail(email_from, email_to,subject,message)

        row=[("fields_validation",count,0,today_date)]
        self.insert_audit_data(row,deltadatabase)
        # return trip_summary_null_df,av_aggregate_null_df
      
    def launch(self):
        self.logger.info(f"running audit checks for rivian")
        deltadatabase=self.conf.get('deltadatabase')
        email_from=self.conf.get('email_from')
        email_to=self.conf.get('email_to')
        
        self.run_file_availability_check(email_from, email_to,deltadatabase)
        self.data_availability_check(email_from, email_to,deltadatabase)
        self.data_duplicates_check(email_from, email_to,deltadatabase)
        self.fields_validation(email_from, email_to,deltadatabase)

        self.logger.info(f"Audit task for rivian has finished!")


# if you're using python_wheel_task, you'll need the entrypoint function to be used in setup.py
def entrypoint():  
    task = rivianDQI()


    task.launch()


# if you're using spark_python_task, you'll need the __main__ block to start the code execution
if __name__ == "__main__":
    entrypoint()
