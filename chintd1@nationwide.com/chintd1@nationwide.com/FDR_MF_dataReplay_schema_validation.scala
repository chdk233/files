// Databricks notebook source
print("schema validation starts")

// COMMAND ----------

// DBTITLE 1,schema check for program enrollment

 val schema_data="""{"entrp_cust_nb":" ","plcy_nb":"6341J 133848","data_clctn_id":"075670d6-beab-4991-8b81-abe289967bc8","driver_key":"424719663","last_rqst_dt":"2023-06-12","plcy_st_cd":"TN","prgrm_term_beg_dt":"2023-02-27","prgrm_term_end_dt":"2023-08-26","prgrm_stts_cd":"Active","data_clctn_stts_cd":"Active","devc_stts_dt":"2022-10-31","prgrm_tp_cd":"SmartRideMobileContinuous","prgrm_cd":"SmartRideMobileContinuous","src_sys_cd":"CMT_PL_SRMCM","sbjt_id":"1","sbjt_tp_cd":"DRIVER","vndr_cd":"CMT","devc_id":"9012320381","devc_stts_cd":"Registered","instl_grc_end_dt":" "}"""
 val schemadf = spark.read.json(Seq(schema_data).toDS)
val df1=spark.sql("select programEnrollment from dhf_iot_cmt_raw_prod.replay_kafka_trips union select programEnrollment from dhf_iot_cmt_raw_prod.kafka_replay_tripLabel")
val df2=df1.withColumn("json",from_json(df1("programEnrollment"),schema=schemadf.schema))
val enrollment=df2.selectExpr("json.*","programEnrollment").filter("entrp_cust_nb is null or plcy_nb is null or data_clctn_id is null or driver_key is null or last_rqst_dt is null or plcy_st_cd is null or prgrm_term_beg_dt is null or prgrm_term_end_dt is null or prgrm_stts_cd is null or data_clctn_stts_cd is null or devc_stts_dt is null or prgrm_tp_cd is null or prgrm_cd is null or src_sys_cd is null or sbjt_id is null or sbjt_tp_cd is null or vndr_cd is null or devc_id is null or devc_stts_cd is null or instl_grc_end_dt is null")
assert(enrollment.count()==0)


// COMMAND ----------

// DBTITLE 1,schema check for transaction

 val schema_data="""{"trnsctn_id":"3A6F5701-F1FF-4A6B-B363-7488C0A5D216_20230520_1600","trnsctn_tp_cd":"Trip","src_sys_cd":"CMT_PL_SRMCM"}"""
 val schemadf = spark.read.json(Seq(schema_data).toDS)
val df1=spark.sql("select transaction from dhf_iot_cmt_raw_prod.replay_kafka_trips union select transaction from dhf_iot_cmt_raw_prod.kafka_replay_tripLabel")
val df2=df1.withColumn("json",from_json(df1("transaction"),schema=schemadf.schema))
val transaction=df2.selectExpr("json.*","transaction").filter("trnsctn_id is null or trnsctn_tp_cd is null or src_sys_cd is null")
assert(transaction.count()==0)

// COMMAND ----------

// DBTITLE 1,schema check for tripdetail

 val schema_data="""{"filepath":"s3://pcds-databricks-common-785562577411/iot/raw/smartride/cmt-pl/realtime/tar/load_date=2023-05-20/1600/DS_CMT_03f92ff1-d4c4-4a65-b86d-7270ab24c31c/3A6F5701-F1FF-4A6B-B363-7488C0A5D216.tar.gz","trip_detail_count":25265}"""
 val schemadf = spark.read.json(Seq(schema_data).toDS)
val df1=spark.sql("select tripDetail from dhf_iot_cmt_raw_prod.replay_kafka_trips ")
val df2=df1.withColumn("json",from_json(df1("tripDetail"),schema=schemadf.schema))
val tripdetail=df2.selectExpr("json.*","tripDetail").filter("filepath is null or trip_detail_count is null")
assert(tripdetail.count()==0)

// COMMAND ----------

// DBTITLE 1,schema check for eventcount

 val schema_data="""{"raw":{"deprecated_ct":0,"harsh_braking_ct":0,"harsh_cornering_ct":0,"harsh_acceleration_ct":0,"speeding_ct":0,"phone_motion_ct":1,"phone_call_ct":0,"handheld_ct":0,"handsfree_ct":0,"tapping_ct":0,"idle_time_ct":4},"derived":{"phone_handling_ct":1}}"""
 val schemadf = spark.read.json(Seq(schema_data).toDS)
val df1=spark.sql("select eventsCount from dhf_iot_cmt_raw_prod.replay_kafka_trips")
val df2=df1.withColumn("json",from_json(df1("eventsCount"),schema=schemadf.schema))
val evencount=df2.selectExpr("json.*","eventsCount","json.raw.*","json.derived.*").filter("deprecated_ct is null or harsh_braking_ct is null or harsh_cornering_ct is null or harsh_acceleration_ct is null or speeding_ct is null or phone_motion_ct is null or phone_call_ct is null or handheld_ct is null or handsfree_ct is null or tapping_ct is null or idle_time_ct is null or phone_handling_ct is null or raw  is null or  derived  is null ")
assert(evencount.count()==0)

// COMMAND ----------

// DBTITLE 1,schema check for tripLabelUpdate

 val schema_data="""{"trip_smry_key":"000AE7DA-F29D-4DF7-8188-D0C7C539C593","devc_key":"83332D35-205F-4793-9C38-443D48A98859","user_lbl_nm":"DRIVER_NO_DISTRACTION","user_lbl_dt":"2023-06-12 19:15:23.291472","plcy_st_cd":"MN","prgrm_cd":"SmartRideMobileContinuous"}"""
 val schemadf = spark.read.json(Seq(schema_data).toDS)
val df1=spark.sql("select tripLabelUpdate from dhf_iot_cmt_raw_prod.kafka_replay_tripLabel")
val df2=df1.withColumn("json",from_json(df1("tripLabelUpdate"),schema=schemadf.schema))
val triplable=df2.selectExpr("json.*","tripLabelUpdate").filter("trip_smry_key is null or devc_key is null or user_lbl_nm is null or user_lbl_dt is null or plcy_st_cd is null or prgrm_cd is null ")
assert(triplable.count==0)

// COMMAND ----------

// DBTITLE 1,schema check for trip_smry_json

 val schema_data="""{"account_id":"03f92ff1-d4c4-4a65-b86d-7270ab24c31c","classification":"car","deviceid":"5EC8AB9A-FC88-4B72-9E82-D112CA770FA8","distance_km":19.959347376141572,"driveid":"3A6F5701-F1FF-4A6B-B363-7488C0A5D216","driving":true,"end":{"lat":"45.148775","lon":"-122.4134","ts":"2023-05-17T16:32:09.000Z"},"events":[{"event_type":6,"ts":"2023-05-17T16:11:50.000Z","lat":45.147922,"lon":-122.580654,"speed_kmh":47.2,"duration_sec":31.0,"displayed":"app"},{"event_type":6,"ts":"2023-05-17T16:16:49.000Z","lat":45.169866,"lon":-122.523306,"speed_kmh":74.94,"duration_sec":23.0,"displayed":"app"},{"event_type":6,"ts":"2023-05-17T16:30:23.000Z","lat":45.160517,"lon":-122.414344,"speed_kmh":89.13,"duration_sec":22.0,"displayed":"app"},{"event_type":6,"ts":"2023-05-17T16:20:32.000Z","lat":45.169195,"lon":-122.47337,"speed_kmh":90.04,"duration_sec":20.0,"displayed":"app"},{"event_type":6,"ts":"2023-05-17T16:22:54.000Z","lat":45.172089,"lon":-122.435472,"speed_kmh":16.06,"duration_sec":16.0,"displayed":"app"},{"event_type":6,"ts":"2023-05-17T16:14:56.000Z","lat":45.156327,"lon":-122.550717,"speed_kmh":102.64,"duration_sec":14.0,"displayed":"app"},{"event_type":9,"ts":"2023-05-17T16:27:26.000Z","lat":45.172089,"lon":-122.435355,"speed_kmh":0.98,"duration_sec":29.0,"displayed":"app"}],"id":816742964,"idle_sec":28,"nightdriving_sec":0,"start":{"lat":"45.150835","lon":"-122.574533","ts":"2023-05-17T16:04:05.000Z"},"utc_offset":"-07:00:00","waypoints":[{"lat":45.150835,"lon":-122.574533,"ts":"2023-05-17T16:04:03.000Z","avg_moving_speed_kmh":0.0,"max_speed_kmh":0.0,"speed_limit_kmh":0.0,"link_id":5291823,"display_code":[-1,0],"prepended":true},{"lat":45.150764,"lon":-122.574547,"ts":"2023-05-17T16:04:07.000Z","avg_moving_speed_kmh":0.0,"max_speed_kmh":0.0,"speed_limit_kmh":0.0,"link_id":5291823,"display_code":[-1,0],"prepended":true},{"lat":45.14877,"lon":-122.413627,"ts":"2023-05-17T16:31:48.000Z","avg_moving_speed_kmh":17.837,"max_speed_kmh":17.91,"speed_limit_kmh":0.0,"link_id":0,"display_code":[0,0]}],"user_state":" ","program_id":" ","file_src_cd":"SRMCM"}"""
 val schemadf = spark.read.json(Seq(schema_data).toDS)
val df1=spark.sql("select trip_smry_json_tt from dhf_iot_cmt_raw_prod.replay_kafka_trips")
val df2=df1.withColumn("json",from_json(df1("trip_smry_json_tt"),schema=schemadf.schema))
val trip_smry=df2.selectExpr("json.*","trip_smry_json_tt").filter("account_id is null or classification is null or deviceid is null or distance_km is null or driveid is null or driving is null or end is null or events is null or id is null or idle_sec is null or nightdriving_sec is null or start is null or utc_offset is null or waypoints is null or user_state is null or program_id is null or file_src_cd is null")
assert(trip_smry.count==0)

// COMMAND ----------

print("Schema validation sucessfull")