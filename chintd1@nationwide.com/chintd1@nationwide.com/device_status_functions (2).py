# Databricks notebook source
from datetime import datetime, timedelta
from operator import itemgetter
from pyspark.sql import Row
from pyspark.sql.types import StructType,StructField, TimestampType, StringType, LongType, IntegerType, DateType
from pyspark.sql.functions import  when, col, lit, trim
from pytz import timezone
import hashlib

HIGH_DATE = datetime(9999, 1, 1)
GRACE_PERIOD = 8 

EVENTCODE_HEARTBEAT = 'HEARTBEAT'
EVENTCODE_INSTALL = 'CONNECT_EVENT'
EVENTCODE_UNINSTALL = 'DISCONNECT_EVENT'

CONNECT_STATUS = "1"
DISCONNECT_STATUS = "0"

tz = timezone('US/Eastern')
ds_schema = StructType([StructField('PRGRM_INSTC_ID', LongType(), True),
                       StructField('DATA_CLCTN_ID', StringType(), True),
                       StructField('DEVC_KEY', StringType(), False),
                       StructField('SRC_SYS_CD', StringType(), False),
                       StructField('ETL_ROW_EFF_DTS', TimestampType(), False),
                       StructField('ETL_LAST_UPDT_DTS', TimestampType(), True), 
                       StructField('ENRLD_VIN_NB', StringType(), False),
                       StructField('CNCTD_STTS_FLAG', StringType(), False),
                       StructField('STTS_EFCTV_TS', TimestampType(), False),
                       StructField('STTS_EXPRTN_TS', TimestampType(), True),
                       StructField('LAST_DEVC_ACTVTY_TS', TimestampType(), False),
                       StructField('DEVC_UNAVLBL_FLAG', StringType(), True),
                       StructField('LOAD_DT', DateType(), False),
                       StructField('LOAD_HR_TS', TimestampType(), False),
                       StructField('MD5_HASH', StringType(), False)])



# COMMAND ----------

def make_dictionary_existing(row):
  return dict(
    PRGRM_INSTC_ID = row['PRGRM_INSTC_ID'],
    DATA_CLCTN_ID = row['DATA_CLCTN_ID'],
    DEVC_KEY = row['DEVC_KEY'],
    SRC_SYS_CD = row['SRC_SYS_CD'],
    ETL_ROW_EFF_DTS =row['ETL_ROW_EFF_DTS'],
    ETL_LAST_UPDT_DTS = row['ETL_LAST_UPDT_DTS'],
    ENRLD_VIN_NB = row['ENRLD_VIN_NB'],
    CNCTD_STTS_FLAG = row['CNCTD_STTS_FLAG'],
    STTS_EFCTV_TS = row['STTS_EFCTV_TS'],
    STTS_EXPRTN_TS = row['STTS_EXPRTN_TS'],
    LAST_DEVC_ACTVTY_TS = row['LAST_DEVC_ACTVTY_TS'],
    DEVC_UNAVLBL_FLAG = row['DEVC_UNAVLBL_FLAG'],
    LOAD_DT = row['LOAD_DT'],
    LOAD_HR_TS = row['LOAD_HR_TS'],
    MD5_HASH = row['MD5_HASH']
  )
  
def check_enrollment_period(row, start):
  if row['src_sys_cd'] == 'IMS_SM_5X':
    nullable_field = row['data_clctn_id']
  else:
    nullable_field = row['prog_inst_id']
    
  if nullable_field is None or start is None:
    return None, None
  else:
    if row['active_start_dt'] is None or row['active_end_dt'] is None:
      return None, None
    elif row['active_start_dt'] <= start and start < row['active_end_dt']:
      return row['data_clctn_id'], row['prog_inst_id']
    else:
      return None, None
  
### Current logic

def make_dictionary_instances(row):
  now = datetime.now()
  dci, piid = check_enrollment_period(row, row['event_start_ts'])
  instance = dict(
  PRGRM_INSTC_ID = piid,
  DATA_CLCTN_ID = dci,
  DEVC_KEY = row['device_nbr'] if row['device_nbr'] is not None else 'NOKEY',
  SRC_SYS_CD = row['src_sys_cd'],
  ETL_ROW_EFF_DTS = now,
  ETL_LAST_UPDT_DTS = now,
  ENRLD_VIN_NB = row['veh_key'],
  CNCTD_STTS_FLAG = DISCONNECT_STATUS if row['event_key'] == EVENTCODE_UNINSTALL else CONNECT_STATUS,
  STTS_EFCTV_TS = row['event_start_ts'],
  STTS_EXPRTN_TS = row['event_end_ts'],
  LAST_DEVC_ACTVTY_TS = row['event_end_ts'],
  DEVC_UNAVLBL_FLAG = 0,
  LOAD_DT = row['load_dt'],
  LOAD_HR_TS = row['load_hr_ts'],
  MD5_HASH = row['md5_hash']
  )
  return instance

def split_records(status, event):
  new_statuses = []
  now = datetime.now()
  status['ETL_LAST_UPDT_DTS'] = now
  status['STTS_EXPRTN_TS'] = event['STTS_EFCTV_TS']
  status['DEVC_UNAVLBL_FLAG'] = is_lost(event['LAST_DEVC_ACTVTY_TS'], status['LAST_DEVC_ACTVTY_TS'])


  event['ETL_LAST_UPDT_DTS'] = now
  event['STTS_EXPRTN_TS'] = status['STTS_EXPRTN_TS']
  event['DEVC_UNAVLBL_FLAG'] = is_lost( event['STTS_EXPRTN_TS'], event['STTS_EFCTV_TS'])
  
  return status, event
    
def is_lost(old_lastactivity, lastactivity):
  return "1" if old_lastactivity != None and old_lastactivity - lastactivity > timedelta(days=GRACE_PERIOD) else "0"

def chain_events(prevEvent, currEvent):
  now = datetime.now()
  prevEvent['STTS_EXPRTN_TS'] = currEvent['STTS_EFCTV_TS']
  prevEvent['ETL_LAST_UPDT_DTS'] = now
  prevEvent['DEVC_UNAVLBL_FLAG'] = is_lost( currEvent['LAST_DEVC_ACTVTY_TS'], prevEvent['LAST_DEVC_ACTVTY_TS'])
  
  currEvent['STTS_EXPRTN_TS'] = HIGH_DATE
  currEvent['DEVC_UNAVLBL_FLAG'] = is_lost(now, prevEvent['LAST_DEVC_ACTVTY_TS'])
  currEvent['ETL_LAST_UPDT_DTS'] = now
  return prevEvent, currEvent 

def process_last_event(last_inst):
  now = datetime.now()
  last_inst['STTS_EXPRTN_TS'] = HIGH_DATE
  last_inst['DEVC_UNAVLBL_FLAG'] = is_lost(now, last_inst['LAST_DEVC_ACTVTY_TS'])
  last_inst['ETL_LAST_UPDT_DTS'] = now
  return last_inst


def prioritize_simultaneous_events(cEvent, dEvent):
  now = datetime.now()
  dEvent['ETL_LAST_UPDT_DTS'] = now
  cEvent['ETL_LAST_UPDT_DTS'] = now
  cEvent['DEVC_UNAVLBL_FLAG'] = is_lost(now, cEvent['LAST_DEVC_ACTVTY_TS'])
  
  return dEvent, cEvent

# COMMAND ----------

def load_device_status3(partitionData):
  data_by_vin = {}
  for row in partitionData:
    if row["veh_key"] not in data_by_vin:
        data_by_vin[row["veh_key"]] = []
    data_by_vin[row["veh_key"]].append(row)
  return_data = []
  for vin in data_by_vin:
    vin_data =data_by_vin[vin]
    m_update_list=[]
    m_device_status_list=[]
    lower_boundary_dt = HIGH_DATE
    prevMD5s_chlg = []
    prevMD5s_src = []
    z = 0
    for u_l in vin_data:
      vinput = u_l['veh_key']
      if vinput is None:
        return None
      if vinput.strip(' ') == vin:
        dict_instnc = make_dictionary_instances(u_l)
        if z == 0:
          prevMD5s_chlg.append(dict_instnc['MD5_HASH'])
          m_update_list.append(dict_instnc)
          lower_boundary_dt = dict_instnc['STTS_EFCTV_TS'] if dict_instnc['STTS_EFCTV_TS'] < lower_boundary_dt else lower_boundary_dt
        else:
          if dict_instnc['MD5_HASH'] not in prevMD5s_chlg:
            m_update_list.append(dict_instnc)
            lower_boundary_dt = dict_instnc['STTS_EFCTV_TS'] if dict_instnc['STTS_EFCTV_TS'] < lower_boundary_dt else lower_boundary_dt
            prevMD5s_chlg.append(dict_instnc['MD5_HASH'])
        z = z + 1
    print('Loop 1 Done :' + str(datetime.now()))
    z = 0 
    for u_l in vin_data:
      if u_l['ENRLD_VIN_NB'] is not None and u_l['ENRLD_VIN_NB'].strip(' ')==vin and (lower_boundary_dt <= u_l['STTS_EFCTV_TS'] or u_l['STTS_EXPRTN_TS'] == HIGH_DATE):
        existing_rec_dict = make_dictionary_existing(u_l)
        if z == 0:
          prevMD5s_src.append(existing_rec_dict['MD5_HASH'])
          m_device_status_list.append(existing_rec_dict)
        else:
          if existing_rec_dict['MD5_HASH'] not in prevMD5s_src:
            m_device_status_list.append(existing_rec_dict)
            prevMD5s_src.append(existing_rec_dict['MD5_HASH'])
        z = z + 1
    print('Loop 2 Done : ' + str(datetime.now()))
    event_data = m_update_list
    event_count = len(m_update_list)
    prevMD5s_chlg = []
    prevMD5s_src = []
    i = 0
    instances = []
    new_statuses = []
    if event_count == 0:
      return new_statuses
    
    dupHash = False
    device_events = sorted((m_update_list+m_device_status_list), key=itemgetter('STTS_EFCTV_TS'))
    print('Events sorted : ' + str(datetime.now()))
    new_status = device_events.pop(0)
    for event in device_events:
      new_statuses.append(new_status)
      last_rec = new_statuses.pop(len(new_statuses)-1)
      if last_rec is None:
        return new_statuses.append(process_last_event(last_rec))
      else:
        if last_rec['MD5_HASH'] != event['MD5_HASH']:
          if last_rec['STTS_EFCTV_TS'] < event['STTS_EFCTV_TS'] and event['STTS_EXPRTN_TS'] < last_rec['STTS_EXPRTN_TS']:
            front_status, new_status = split_records(last_rec, event)
            new_statuses.append(front_status)
          elif last_rec['STTS_EFCTV_TS'] == event['STTS_EFCTV_TS'] and last_rec['LAST_DEVC_ACTVTY_TS'] == event['STTS_EFCTV_TS']:
            if last_rec['CNCTD_STTS_FLAG'] == CONNECT_STATUS and event['CNCTD_STTS_FLAG'] == DISCONNECT_STATUS:
              deprioritized_disconnect, new_status = prioritize_simultaneous_events(last_rec, event)
              new_statuses.append(deprioritized_disconnect)
            else:
              updated_status, new_status = chain_events(last_rec, event)
              new_statuses.append(updated_status)
          elif last_rec['STTS_EXPRTN_TS'] <= event['STTS_EFCTV_TS']:
            updated_status, new_status = chain_events(last_rec, event)
            new_statuses.append(updated_status)
          else:
            print('none matched')
        else: # should only happen when updating record
          print('matched hash')
          new_status = event if event['ETL_LAST_UPDT_DTS'] > last_rec['ETL_LAST_UPDT_DTS'] else last_rec
          dupHash = True


    new_statuses.append(process_last_event(new_status))      
    print('Loop 3 Done : ' + str(datetime.now()))   
    return_data.extend(new_statuses)
  return iter(return_data)

# COMMAND ----------

from delta.tables import *
def make_device_status(microBatchDF,  harmonizedDB, curatedDB, target_table):
  curated_table = curatedDB+"."+target_table
  microBatchDF.createOrReplaceGlobalTempView('microBatchViewName')
  device_status = spark.sql(f"select distinct *  from ((select distinct * from (select *,row_number() over (partition by vin_nbr,SRC_SYS_CD,prog_inst_id,DATA_CLCTN_ID,MD5_HASH ,device_nbr order by load_hr_ts desc) as row_nb from global_temp.microBatchViewName) where row_nb=1) chlg inner join (select min(event_start_ts) as min_start, veh_key as veh_key2 from global_temp.microBatchViewName group by veh_key) ms on chlg.veh_key = ms.veh_key2) chlg2 left join (select distinct * from {curated_table}) ds on chlg2.veh_key = ds.ENRLD_VIN_NB and (ds.STTS_EXPRTN_TS = to_timestamp('9999-01-01') or ds.STTS_EFCTV_TS >= chlg2.min_start)")
  vins = spark.sql(f"select count(distinct veh_key) as vins from global_temp.microBatchViewName").collect()[0]['vins']
  print(f"processing {vins} vins")
  
  if vins == 0:
    pass
  else:
    print('partitioning data')
    partitioned_df = device_status.repartition(320, "veh_key").sortWithinPartitions('veh_key', 'event_end_ts', 'STTS_EXPRTN_TS', 'STTS_EFCTV_TS')
    ds_rdd = partitioned_df.rdd.mapPartitions(load_device_status3, preservesPartitioning=True)
    df=spark.createDataFrame(data=ds_rdd, schema=ds_schema).createOrReplaceTempView('new_device_status')
    print('merging into table')
#     display(df)
    spark.sql(f"merge into {curated_table} target using (select distinct *, cast(row_number() over (order by NULL) + coalesce((select max(DEVICE_STATUS_ID) from {curated_table}),0) as BIGINT) as DEVICE_STATUS_ID from new_device_status) updates on target.ENRLD_VIN_NB = updates.ENRLD_VIN_NB and target.SRC_SYS_CD = updates.SRC_SYS_CD and ((target.PRGRM_INSTC_ID = updates.PRGRM_INSTC_ID or target.DATA_CLCTN_ID = updates.DATA_CLCTN_ID) or ((target.PRGRM_INSTC_ID is null and updates.PRGRM_INSTC_ID is null) and (target.DATA_CLCTN_ID is null and updates.DATA_CLCTN_ID is null))) and target.MD5_HASH = updates.MD5_HASH and target.DEVC_KEY = updates.DEVC_KEY when matched then update set target.ETL_LAST_UPDT_DTS = current_timestamp(), target.STTS_EFCTV_TS = updates.STTS_EFCTV_TS, target.STTS_EXPRTN_TS = updates.STTS_EXPRTN_TS, target.CNCTD_STTS_FLAG = updates.CNCTD_STTS_FLAG, target.LAST_DEVC_ACTVTY_TS = updates.LAST_DEVC_ACTVTY_TS, target.DEVC_UNAVLBL_FLAG = updates.DEVC_UNAVLBL_FLAG when not matched then insert *")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct *  from (
# MAGIC
# MAGIC (select distinct * from 
# MAGIC (select *,row_number() over (partition by vin_nbr,SRC_SYS_CD,prog_inst_id,DATA_CLCTN_ID,MD5_HASH ,device_nbr order by load_hr_ts desc) as row_nb from dhf_iot_curated_prod.device_status_chlg where veh_key in ('WBA3A5C53EP601218') and load_dt>'2022-03-01') 
# MAGIC where row_nb=1) chlg inner join 
# MAGIC (select min(event_start_ts) as min_start, veh_key as veh_key2 from dhf_iot_curated_prod.device_status_chlg where veh_key in ('WBA3A5C53EP601218') and load_dt>'2022-03-01' group by veh_key) ms 
# MAGIC on chlg.veh_key = ms.veh_key2) chlg2 
# MAGIC
# MAGIC left join (select distinct * from dhf_iot_curated_prod.ds_test) ds on chlg2.veh_key = ds.ENRLD_VIN_NB and (ds.STTS_EXPRTN_TS = to_timestamp('9999-01-01') or ds.STTS_EFCTV_TS >= chlg2.min_start)
# MAGIC -- 2022-01-01  9999
# MAGIC -- 2020-01-01  2021-01-01
# MAGIC -- 2021-01-01

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dhf_iot_curated_prod.device_status_chlg --where veh_key in ('JTEBU4BF3AK097878')  and load_dt='2022-04-25'
# MAGIC
# MAGIC -- select * from dhf_iot_harmonized_prod.trip_summary where load_dt='2022-04-25'

# COMMAND ----------

# microBatchDF=spark.sql("select * from dhf_iot_curated_prod.device_status_chlg where veh_key in ('WBA3A5C53EP601218') and load_dt<'2021-01-01'")
microBatchDF=spark.sql("select * from dhf_iot_curated_prod.device_status_chlg where veh_key in ('WBA3A5C53EP601218')  and load_dt>='2022-01-01'")
# microBatchDF=spark.sql("select * from dhf_iot_curated_prod.device_status_chlg where veh_key in ('WBA3A5C53EP601218') and load_dt>='2021-01-01' and load_dt<'2022-01-01'")
make_device_status(microBatchDF,  "dhf_iot_harmonized_prod", "dhf_iot_curated_prod", "Ds_test")

# COMMAND ----------

# MAGIC %sql
# MAGIC delete  from dhf_iot_curated_prod.Ds_test
# MAGIC

# COMMAND ----------

