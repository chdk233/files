# Databricks notebook source
# environment = str(dbutils.widgets.get('environment'))
# account_id = str(dbutils.widgets.get('account_id'))
account_id="785562577411"
environment="prod"

# COMMAND ----------

spark.sql(f""" CREATE DATABASE  IF NOT EXISTS dhf_iot_analytics_raw LOCATION 's3://pcds-databricks-common-{account_id}/iot/delta/raw/analytics/dhf_iot_analytics_raw';""")

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS dhf_iot_analytics_raw.sip_srp (
POLICY_NB	string	,
PLCY_RENWL_DT	string	,
PLCY_EXPR_DT	date	,
PLCY_TERM_IND	string	,
PLCY_RATD_ST_CD	string	,
VIN	string	,
SR_PGM_ID	decimal(18,0)	,
TELEMATICS_PROGRAM	string	,
SR_ENRLMNT_DT	date	,
CANCEL_DATE	date	,
INSTL_DT	date	,
ACT_INSTL_DAY_CT	int	,
INSTALLED_IN	string	,
DAYS_OF_DATA	int	,
ENROLLMENT_MONTH	string	,
DAY46_INCLUDE_IN	string	,
BAD_DATA_IN	string	,
DEV_ID_NBR	string	,
SCRNG_MDL_CD	string	,
ACT_PGM_COMP_DT	date	,
EST_PGM_COMP_DT	date	,
MOST_RECENT_SCORE_DATE	date	,
ACTL_SCOR_QT	int	,
ANNL_MILEAGE_QT	int	,
MOST_RECENT_DISCOUNT_DATE	date	,
DSCNTTYPE_DS	string	,
DSCNT_PC	decimal(5,2)	,
DATE_OF_QUERY	date	,
MODEL_YR_NBR	string	,
MAKE_DS	string	,
MODEL_DS	string	,
CAR_DS	string	,
VNDR_ID_CD	string	,
SRP_ERROR_IN	string	,
VEHICLES	int	,
SHIP_DT date,
DELIVER_DT date,
T_DEV_ID_NBR string,
FIRST_HEARTBEAT_DT date,
LAST_HEARTBEAT_DT date,
SR_VHCL_STAT_CD string,
DEV_STAT_IN string,
LOAD_DT	date,
LOAD_HR_TS timestamp
)
USING delta
PARTITIONED BY (LOAD_DT)
LOCATION 's3://pcds-databricks-common-{account_id}/iot/delta/raw/analytics/dhf_iot_analytics_raw/sip_srp';""")


# COMMAND ----------

driver = "com.ibm.db2.jcc.DB2Driver"
url = "jdbc:db2://nwiegateway-desktop-prod.nwie.net:50000/NW_DBG2"
user = dbutils.secrets.get(scope = "db2scope", key = "db2username") 
password = dbutils.secrets.get(scope = "db2scope", key = "db2password") 

# COMMAND ----------

sql_srp="""SELECT DISTINCT 																
	CASE WHEN substring(j1.FULL_PLCY_NBR,12,1,CODEUNITS32) = ' ' THEN substring(j1.FULL_PLCY_NBR,1,4,CODEUNITS32)||substring(j1.FULL_PLCY_NBR,11,1,CODEUNITS32)||' '||substring(j1.FULL_PLCY_NBR,8,3,CODEUNITS32)||substring(j1.FULL_PLCY_NBR,5,3,CODEUNITS32) 															
			 ELSE trim(BOTH FROM j1.FULL_PLCY_NBR) END AS Policy_NB,													
																
	j1.PLCY_RENWL_DT,															
	j1.PLCY_EXPR_DT,															
	j1.PLCY_TERM_IND,															
	j1.PLCY_RATD_ST_CD,															
	trim(CAST(COALESCE(j1.VHCL_ID_NBR,j2.VHCL_ID_NBR) AS VarChar(17))) VIN,															
	j1.SR_PGM_ID,															
	CASE WHEN j1.SR_PGM_TYPE ='SM' THEN 'SmartMiles' 															
		 WHEN j1.SR_PGM_TYPE ='MP' THEN 'SmartRide Device'														
		 WHEN j1.SR_PGM_TYPE ='MO' THEN 'SmartRide Device - Mileage Only (CA)'														
		 WHEN j1.SR_PGM_TYPE ='TC' THEN 'SmartRide Device - Connected Car'														
		 ELSE 'Unknown' END Telematics_Program,														
	COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT) SR_ENRLMNT_DT,															
	CASE WHEN j2.SR_DISCNT_CD = 'R' THEN CAST(j2.TRANS_TS_VHCL AS date) END Cancel_Date,															
	j2.INSTL_DT,															
	j2.ACT_INSTL_DAY_CT,															
	CASE WHEN j2.ACT_INSTL_DAY_CT < 0 OR j2.ACT_INSTL_DAY_CT IS null THEN 'Bad Data' 															
		 WHEN j2.ACT_INSTL_DAY_CT = 0 THEN 'Not Installed' ELSE 'Installed' END Installed_In,														
	CASE WHEN j2.SR_DISCNT_CD = 'R' THEN (DAYS(CAST(j2.TRANS_TS_VHCL AS date)) - DAYS(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)))															
		 ELSE (DAYS(CURRENT_DATE) - DAYS(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))) END Days_of_Data,														
	CASE WHEN month(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)) = 1 THEN year(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))||'-01 JAN'															
		 WHEN month(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)) = 2 THEN year(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))||'-02 FEB'														
		 WHEN month(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)) = 3 THEN year(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))||'-03 MAR'														
		 WHEN month(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)) = 4 THEN year(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))||'-04 APR'														
		 WHEN month(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)) = 5 THEN year(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))||'-05 MAY'														
		 WHEN month(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)) = 6 THEN year(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))||'-06 JUN'														
		 WHEN month(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)) = 7 THEN year(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))||'-07 JUL'														
		 WHEN month(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)) = 8 THEN year(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))||'-08 AUG'														
		 WHEN month(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)) = 9 THEN year(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))||'-09 SEP'														
		 WHEN month(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)) = 10 THEN year(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))||'-10 OCT'														
		 WHEN month(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)) = 11 THEN year(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))||'-11 NOV'														
		 WHEN month(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)) = 12 THEN year(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))||'-12 DEC'														
		 ELSE 'Issue: Look at Query' END  Enrollment_Month,	 													
	CASE WHEN 															
 		 CASE WHEN j2.SR_DISCNT_CD = 'R' THEN (DAYS(CAST(j2.TRANS_TS_VHCL AS date)) - DAYS(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)))														
		 ELSE (DAYS(CURRENT_DATE) - DAYS(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))) END >= 46 THEN 'Include' ELSE 'Remove' END AS Day46_Include_In, 														
	CASE WHEN j2.ACT_INSTL_DAY_CT < 0 OR 															
			  j2.ACT_INSTL_DAY_CT IS NULL OR													
			  CASE WHEN j2.SR_DISCNT_CD = 'R' THEN (DAYS(CAST(j2.TRANS_TS_VHCL AS date)) - DAYS(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)))													
		 		   ELSE (DAYS(CURRENT_DATE) - DAYS(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))) END < (j2.ACT_INSTL_DAY_CT) OR 
			  CASE WHEN j2.SR_DISCNT_CD = 'R' THEN (DAYS(CAST(j2.TRANS_TS_VHCL AS date)) - DAYS(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT)))													
		  		   ELSE (DAYS(CURRENT_DATE) - DAYS(COALESCE(j1.SR_ENRLMNT_DT, j2.SR_ENRLMNT_DT))) END < 0 	
		 THEN 'Bad Data' ELSE 'Good Data' END Bad_Data_In,														
	j1.DEV_ID_NBR,															
	j2.SCRNG_MDL_CD,															
	j2.ACT_PGM_COMP_DT,															
	j2.EST_PGM_COMP_DT,															
																
	j3.Most_Recent_Score_Date,															
	j3.ACTL_SCOR_QT,															
	j3.ANNL_MILEAGE_QT,															
	j3.Most_Recent_Discount_Date,															
	j3.DSCNTTYPE_DS, 															
	j3.DSCNT_PC,															
																
	current_date AS Date_of_Query,														
	j2.MODEL_YR_NBR,															
	j2.MAKE_DS,															
	j2.MODEL_DS,															
	j2.Car_Ds,															
	j2.VNDR_ID_CD,															
	CASE WHEN j1.FULL_PLCY_NBR IS NULL OR j2.INSTL_DT IS NULL THEN 'Error in SRP Join' ELSE 'Good' END SRP_Error_In,															
	1 vehicles
    ,track.SHIP_DT
   ,track.DELIVER_DT
   ,track.DEV_ID_NBR T_DEV_ID_NBR
   ,hb.FIRST_HEARTBEAT_DT
   ,hb.LAST_HEARTBEAT_DT,j2.SR_VHCL_STAT_CD,j2.DEV_STAT_IN
																
From																
																
(SELECT DISTINCT 																
	a.FULL_PLCY_NBR,															
	b.SR_PLCY_ID,															
	b.PLCY_RENWL_DT,															
	b.PLCY_EXPR_DT,															
	b.PLCY_TERM_IND,															
	b.PLCY_RATD_ST_CD,															
	a.VHCL_ID_NBR,															
	a.SR_ENRLMNT_DT,															
	a.DEV_ID_NBR,															
	a.SR_PGM_ID,															
	a.SR_PGM_TYPE															
FROM DBSMTNR.SMT_ODS_PGM_INSTNC a																
JOIN DBSMTNR.SMT_ODS_POLICY b ON (a.FULL_PLCY_NBR = b.FULL_PLCY_NBR)																
WHERE a.SR_PGM_TYPE IN('MO', 'MP') AND a.REVSN_TS = '3500-01-01 00:00:00' AND b.REVSN_TS = '3500-01-01 00:00:00'																
ORDER BY 																
	a.FULL_PLCY_NBR, 															
	a.VHCL_ID_NBR,															
	a.SR_ENRLMNT_DT ) j1															
																
Left JOIN																
																
(SELECT DISTINCT 																
	a.SR_PLCY_ID,															
	a.VHCL_ID_NBR, 															
	a.SR_DISCNT_CD,															
	Trim(a.MODEL_YR_NBR) MODEL_YR_NBR,															
	trim(a.MAKE_DS) MAKE_DS,															
	trim(a.MODEL_DS) MODEL_DS,															
	Trim(a.MODEL_YR_NBR)||' '||trim(a.MAKE_DS)||' '||trim(a.MODEL_DS) Car_Ds,															
	a.TRANS_TS TRANS_TS_VHCL,															
	b.VHCL_ID,															
	b.ACT_INSTL_DAY_CT,															
	b.INSTL_DT,															
	b.VNDR_ID_CD,															
	b.SR_ENRLMNT_DT,															
	b.TM_PGM_TYP_CD,															
	c.SCRNG_MDL_CD,															
	b.ACT_PGM_COMP_DT,															
	b.EST_PGM_COMP_DT,b.SR_VHCL_STAT_CD,b.DEV_STAT_IN															
FROM DBSMTNR.SMT_ODS_VEHICLE a																
JOIN DBSMTNR.SMT_ODS_VHCL_DTL b ON a.VHCL_ID = b.VHCL_ID																
JOIN DBSMTNR.SMT_ODS_SCRNG_MDL c ON a.SCRNG_MDL_ID = c.SCRNG_MDL_ID 																
WHERE b.TM_PGM_TYP_CD IN('MO', 'MP') AND a.REVSN_TS = '3500-01-01 00:00:00' AND b.REVSN_TS = '3500-01-01 00:00:00') j2 on  j1.SR_PLCY_ID = j2.SR_PLCY_ID AND j1.VHCL_ID_NBR = j2.VHCL_ID_NBR AND j1.SR_PGM_TYPE = j2.TM_PGM_TYP_CD AND j1.SR_ENRLMNT_DT = j2.SR_ENRLMNT_DT																
																
																
LEFT JOIN 																
																
(SELECT 																
COALESCE(j1.SR_PGM_ID, j2.SR_PGM_ID) SR_PGM_ID,																
j1.Most_Recent_Score_Date,																
j1.ACTL_SCOR_QT,																
j1.ANNL_MILEAGE_QT,																
j2.Most_Recent_Discount_Date,																
j2.DSCNTTYPE_DS,																
j2.DSCNT_PC																
																
FROM 																
																
	(SELECT 															
	a.SR_PGM_ID,															
	b.Most_Recent_Score_Date,															
	a.ACTL_SCOR_QT,															
	a.ANNL_MILEAGE_QT 															
	FROM DBSMTNR.SMT_ODS_SCORE a 															
	JOIN															
			(SELECT DISTINCT a.SR_PGM_ID,													
				MAX(a.SCOR_DT) AS Most_Recent_Score_Date												
			FROM DBSMTNR.SMT_ODS_SCORE a													
			GROUP BY a.SR_PGM_ID) b ON (a.SR_PGM_ID = b.SR_PGM_ID AND a.SCOR_DT = b.Most_Recent_Score_Date)) j1													
																
FULL OUTER JOIN 																
																
	(SELECT 															
	a.SR_PGM_ID,															
	b.Most_Recent_Discount_Date,															
	a.DSCNTTYPE_DS,															
	a.DSCNT_PC 															
	FROM DBSMTNR.SMT_ODS_DISCOUNT a 															
	JOIN															
			(SELECT DISTINCT a.SR_PGM_ID,													
				MAX(a.DSCNT_CALC_DT) AS Most_Recent_Discount_Date												
			FROM DBSMTNR.SMT_ODS_DISCOUNT a													
			GROUP BY a.SR_PGM_ID) b ON (a.SR_PGM_ID = b.SR_PGM_ID AND a.DSCNT_CALC_DT = b.Most_Recent_Discount_Date)													
	WHERE a.DSCNTTYPE_DS <> 'MAXIMUM') j2 ON (j1.SR_PGM_ID = j2.SR_PGM_ID)) j3 ON (j1.SR_PGM_ID = j3.SR_PGM_ID)															
	left join 	DBSMTNR.SMT_ODS_DEV_TRACK track on 	trim(leading '0' from track.DEV_ID_NBR) = trim(leading '0' from j1.DEV_ID_NBR) and
      track.INIT_SR_PGM_ID=j1.SR_PGM_ID		
    left join (select VHCL_ID_NBR,min(RM_DATE) first_heartbeat_dt,max(RM_DATE) last_heartbeat_dt from DBSMTNR.SMT_ODS_ACT_MLG WHERE trim(Heartbeat)='Y' group by VHCL_ID_NBR) hb on trim(CAST(COALESCE(j1.VHCL_ID_NBR,j2.VHCL_ID_NBR) AS VarChar(17))) =trim(hb.VHCL_ID_NBR)
ORDER BY 	
	Policy_NB, 
	VIN,
	Telematics_Program,
	SR_ENRLMNT_DT,
	j2.INSTL_DT DESC,
	j2.ACT_INSTL_DAY_CT
																
"""

# COMMAND ----------

srp_df = spark.read.format("jdbc")\
  .option("driver", driver)\
  .option("url", url)\
  .option("dbtable", '({sql}) as src'.format(sql=sql_srp))\
  .option("user", user)\
  .option("password", password)\
  .load()

srp_df.createOrReplaceTempView("srp_view")

# COMMAND ----------

from pyspark.sql.functions import *
srp_df.withColumn("LOAD_HR_TS",current_timestamp())     .withColumn("LOAD_DT",current_date()).write.format("delta").mode("append").partitionBy('LOAD_DT').saveAsTable("dhf_iot_analytics_raw.sip_srp")

# COMMAND ----------

# MAGIC %sql
# MAGIC select COUNT(*) from  srp_view --1638 , device 27k , sr_pg 8k
# MAGIC
# MAGIC
# MAGIC device nb , pid -- no duplicates 
# MAGIC device nb -- duplicates
# MAGIC
# MAGIC join with device nb & pid --> 8k
# MAGIC

# COMMAND ----------

sql_srp1="select * from DBSMTNR.SMT_ODS_VHCL_DTL"
# DBSMTNR.SMT_ODS_SCORE
srp_df1 = spark.read.format("jdbc")\
  .option("driver", driver)\
  .option("url", url)\
  .option("dbtable", '({sql}) as src'.format(sql=sql_srp1))\
  .option("user", user)\
  .option("password", password)\
  .load()
srp_df1.createOrReplaceTempView('test')


# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct SR_VHCL_STAT_CD,DEV_STAT_IN from test where FINAL_DISCNT_DT <current_date()
# MAGIC -- DEV_ID_NBR like '%4080544065%' 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM test
# MAGIC WHERE VHCL_ID_NBR = '5N1DR2MN0KC606596              '
# MAGIC  AND (RM_DATE IN (SELECT MIN(RM_DATE)
# MAGIC FROM test
# MAGIC WHERE VHCL_ID_NBR = '5N1DR2MN0KC606596              '
# MAGIC  GROUP BY HEARTBEAT)
# MAGIC  OR RM_DATE IN (SELECT MAX(RM_DATE)
# MAGIC FROM test
# MAGIC WHERE VHCL_ID_NBR = '5N1DR2MN0KC606596              '
# MAGIC  GROUP BY HEARTBEAT))
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select VHCL_ID_NBR,min(RM_DATE) first_heartbeat_dt,max(RM_DATE) last_heartbeat_dt from DBSMTNR.SMT_ODS_ACT_MLG WHERE trim(Heartbeat)='Y' group by VHCL_ID_NBR

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from test WHERE VHCL_ID_NBR = '5N1DR2MN0KC606596'

# COMMAND ----------

# MAGIC %sql
# MAGIC select INIT_SR_PGM_ID,count(*) from test
# MAGIC group by INIT_SR_PGM_ID having count(*) >1
# MAGIC -- select * from test