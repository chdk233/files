// Databricks notebook source
val browserHostName = dbutils.notebook.getContext.tags.getOrElse("browserHostName","none")
var environment =""

if(browserHostName.contains("-dev-"))
{
environment="dev"
  println("environment is set to : " + environment)
}else if(browserHostName.contains("-test-"))
{
environment="test"
  println("environment is set to : " + environment)
}else if(browserHostName.contains("-prod-"))
{
environment="prod"
  println("environment is set to : " + environment)
}

// COMMAND ----------


val account_id= environment match {
  case "dev"  => "786994105833"
  case "test"  => "168341759447"
  case "prod"  => "785562577411"
}

// COMMAND ----------

spark.sql(s""" CREATE DATABASE  IF NOT EXISTS pc_claims_dh LOCATION 's3://pcds-databricks-common-${account_id}/claim/harmonized/pc_claims_dh';""")



// COMMAND ----------

spark.sql(s"""CREATE OR REPLACE TABLE pc_claims_dh.CLAIM (
		CLAIM_KEY_ID STRING NOT NULL,
		CLAIM_ID INT NOT NULL,
		CLAIM_KEY STRING NOT NULL,
		POLICY_AT_LOSS_KEY_ID STRING,
		POLICY_KEY_ID STRING,
		LOSS_EVENT_KEY_ID STRING NOT NULL,
		LOSS_INCIDENT_KEY_ID STRING,
		ETL_ADD_TS TIMESTAMP NOT NULL,
		ETL_ROW_EXPRTN_TS TIMESTAMP,
		ETL_ROW_EFCTV_TS TIMESTAMP,
		ETL_CURR_ROW_IN STRING NOT NULL,
		ETL_PATCH_TS TIMESTAMP,
		ETL_ROW_CMPLTN_IN STRING NOT NULL,
		ROW_HASH STRING,
		SRC_SYS_CD STRING,
		SRC_SYS_TP INT,
		PRTN_VAL STRING,
		CLM_NB STRING,
		CLM_RPRTD_METHD_CD STRING,
		CLM_RPRTD_METHD_TP INT,
		FALT_RTG_TYPE_CD STRING,
		FALT_RTG_TYPE_TP INT,
		FALT_PCT DECIMAL(6 , 3),
		CLM_RPRTNG_PRTY_TYPE_CD STRING,
		CLM_RPRTNG_PRTY_TYPE_TP INT,
		CVRG_VRFCTN_CD STRING,
		CVRG_VRFCTN_TP INT,
		CLM_TIER_TYPE_CD STRING,
		CLM_TIER_TYPE_TP INT,
		CLM_SGMNT_CD STRING,
		CLM_SGMNT_TP INT,
		NAICS_CD STRING,
		NAICS_TP INT,
		LRG_LOSS_DERV_IN STRING,
		LRG_LOSS_SRC_IN STRING,
		LGCY_MGRTN_DERV_IN STRING,
		LGCY_MGRTN_SRC_IN STRING,
		RCRD_ONLY_DERV_IN STRING,
		RCRD_ONLY_SRC_IN STRING,
		NO_CVRG_DERV_IN STRING,
		NO_CVRG_SRC_IN STRING,
		PR_DSBLTY_DERV_IN STRING,
		PR_DSBLTY_SRC_IN STRING,
		SIU_ESCLT_DERV_IN STRING,
		SIU_ESCLT_SRC_IN STRING,
		UMBRL_PLCY_CLM_DERV_IN STRING,
		UMBRL_PLCY_CLM_SRC_IN STRING,
		WNDHL_EXCL_DERV_IN STRING,
		WNDHL_EXCL_SRC_IN STRING,
		EXTRNL_CLM_NB STRING,
		IN_SUIT_AT_CLM_DERV_IN STRING,
		IN_SUIT_AT_CLM_SRC_IN STRING)
	USING delta
    PARTITIONED BY (SRC_SYS_CD)
    LOCATION 's3://pcds-databricks-common-${account_id}/claim/harmonized/pc_claims_dh/CLAIM'
    TBLPROPERTIES (delta.enableChangeDataFeed=true);""")
		
		
spark.sql(s"""CREATE OR REPLACE TABLE pc_claims_dh.CLAIM_COVERAGE_SUBTYPE (
		CLAIM_COVERAGE_SUBTYPE_KEY_ID STRING NOT NULL,
		CLAIM_COVERAGE_SUBTYPE_ID INT NOT NULL,
		CLAIM_COVERAGE_SUBTYPE_KEY STRING NOT NULL,
		POLICY_AT_LOSS_COVERAGE_KEY_ID STRING,
		POLICY_COVERAGE_KEY_ID STRING,
		ETL_ADD_TS TIMESTAMP NOT NULL,
		ETL_ROW_EFCTV_TS TIMESTAMP,
		ETL_ROW_EXPRTN_TS TIMESTAMP,
		ETL_CURR_ROW_IN STRING,
		ETL_PATCH_TS TIMESTAMP,
		ETL_ROW_CMPLTN_IN STRING,
		ROW_HASH STRING,
		SRC_SYS_CD STRING,
		SRC_SYS_TP INT,
		PRTN_VAL STRING,
		CVRG_SUBTP_CD STRING,
		CVRG_SUBTP_TP INT)
	USING delta
    PARTITIONED BY (SRC_SYS_CD)
    LOCATION 's3://pcds-databricks-common-${account_id}/claim/harmonized/pc_claims_dh/CLAIM_COVERAGE_SUBTYPE'
    TBLPROPERTIES (delta.enableChangeDataFeed=true);""")
		
spark.sql(s"""CREATE OR REPLACE TABLE pc_claims_dh.CLAIM_EVALUATION (
		CLAIM_EVALUATION_KEY_ID STRING NOT NULL,
		CLAIM_EVALUATION_KEY STRING NOT NULL,
		CLAIM_EVALUATION_ID INT NOT NULL,
		CLAIM_KEY_ID STRING,
		CLAIM_EXPOSURE_KEY_ID STRING,
		LEGAL_MATTER_KEY_ID STRING,
		ETL_ADD_TS TIMESTAMP NOT NULL,
		ETL_ROW_EFCTV_TS TIMESTAMP,
		ETL_ROW_EXPRTN_TS TIMESTAMP,
		ETL_CURR_ROW_IN STRING,
		ETL_PATCH_TS TIMESTAMP,
		ETL_ROW_CMPLTN_IN STRING,
		ROW_HASH STRING,
		SRC_SYS_CD STRING,
		SRC_SYS_TP INT,
		PRTN_VAL STRING,
		EVLTN_CREAT_TS TIMESTAMP,
		CLM_EVLTN_TYPE_CD STRING,
		CLM_EVLTN_TYPE_TP INT)
	USING delta
    PARTITIONED BY (SRC_SYS_CD)
    LOCATION 's3://pcds-databricks-common-${account_id}/claim/harmonized/pc_claims_dh/CLAIM_EVALUATION'
    TBLPROPERTIES (delta.enableChangeDataFeed=true);""")
		
spark.sql(s"""CREATE OR REPLACE TABLE pc_claims_dh.CLAIM_EVALUATION_METRIC (
		CLAIM_EVALUATION_METRIC_KEY_ID STRING NOT NULL,
		CLAIM_EVALUATION_METRIC_KEY STRING NOT NULL,
		CLAIM_EVALUATION_METRIC_ID INT NOT NULL,
		CLAIM_EVALUATION_KEY_ID STRING NOT NULL,
		ETL_ADD_TS TIMESTAMP NOT NULL,
		ETL_ROW_EFCTV_TS TIMESTAMP,
		ETL_ROW_EXPRTN_TS TIMESTAMP,
		ETL_CURR_ROW_IN STRING,
		ETL_PATCH_TS TIMESTAMP,
		ETL_ROW_CMPLTN_IN STRING,
		ROW_HASH STRING,
		SRC_SYS_CD STRING,
		SRC_SYS_TP INT,
		PRTN_VAL STRING,
		CLM_EVLTN_METRIC_PCT DECIMAL(6 , 3),
		CLM_EVLTN_METRIC_AM DECIMAL(18 , 2),
		CLM_EVLTN_METRIC_TYPE_CD STRING,
		CLM_EVLTN_METRIC_TYPE_TP INT)
	USING delta
    PARTITIONED BY (SRC_SYS_CD)
    LOCATION 's3://pcds-databricks-common-${account_id}/claim/harmonized/pc_claims_dh/CLAIM_EVALUATION_METRIC'
    TBLPROPERTIES (delta.enableChangeDataFeed=true);""")
		
spark.sql(s"""CREATE OR REPLACE TABLE pc_claims_dh.CLAIM_EXPOSURE (
		CLAIM_EXPOSURE_KEY_ID STRING NOT NULL,
		CLAIM_EXPOSURE_ID INT NOT NULL,
		CLAIM_EXPOSURE_KEY STRING NOT NULL,
		CLAIM_KEY_ID STRING NOT NULL,
		LOSS_ITEM_KEY_ID STRING NOT NULL,
		CLAIM_COVERAGE_SUBTYPE_KEY_ID STRING NOT NULL,
		ETL_ADD_TS TIMESTAMP NOT NULL,
		ETL_ROW_EFCTV_TS TIMESTAMP,
		ETL_ROW_EXPRTN_TS TIMESTAMP,
		ETL_CURR_ROW_IN STRING,
		ETL_PATCH_TS TIMESTAMP,
		ETL_ROW_CMPLTN_IN STRING,
		ROW_HASH STRING,
		SRC_SYS_CD STRING,
		SRC_SYS_TP INT,
		PRTN_VAL STRING,
		ASGNMT_TS TIMESTAMP,
		JRSDCTN_ST_CD STRING,
		JRSDCTN_ST_TP INT,
		LGCY_TOT_INCUR_AM DECIMAL(18 , 2),
		CLM_EXPSR_TYPE_CD STRING,
		CLM_EXPSR_TYPE_TP INT,
		LOSS_ESTMT_AM DECIMAL(18 , 2),
		CLM_EXPSR_SEQ_NB DECIMAL(32 , 6),
		AVG_RSRV_DERV_IN STRING,
		AVG_RSRV_SRC_IN STRING,
		TOT_LOSS_DERV_IN STRING,
		TOT_LOSS_SRC_IN STRING,
		CLM_EXPSR_CREAT_TS TIMESTAMP,
		RTRCTV_DT DATE,
		TAIL_DT DATE)
	USING delta
    PARTITIONED BY (SRC_SYS_CD)
    LOCATION 's3://pcds-databricks-common-${account_id}/claim/harmonized/pc_claims_dh/CLAIM_EXPOSURE'
    TBLPROPERTIES (delta.enableChangeDataFeed=true);""")
		
spark.sql(s"""CREATE OR REPLACE TABLE pc_claims_dh.CLAIM_EXPOSURE_STATUS (
		CLAIM_EXPOSURE_STATUS_KEY_ID STRING NOT NULL,
		CLAIM_EXPOSURE_STATUS_ID INT NOT NULL,
		CLAIM_EXPOSURE_STATUS_KEY STRING NOT NULL,
		CLAIM_EXPOSURE_KEY_ID STRING NOT NULL,
		ETL_ADD_TS TIMESTAMP NOT NULL,
		ETL_ROW_EFCTV_TS TIMESTAMP,
		ETL_ROW_EXPRTN_TS TIMESTAMP,
		ETL_CURR_ROW_IN STRING,
		ETL_PATCH_TS TIMESTAMP,
		ETL_ROW_CMPLTN_IN STRING,
		ROW_HASH STRING,
		SRC_SYS_CD STRING,
		SRC_SYS_TP INT,
		PRTN_VAL STRING,
		CLM_EXPSR_STTS_TS TIMESTAMP,
		CLM_EXPSR_STTS_CD STRING,
		CLM_EXPSR_STTS_TP INT,
		CLM_EXPSR_STTS_RSN_CD STRING,
		CLM_EXPSR_STTS_RSN_TP INT)
	USING delta
    PARTITIONED BY (SRC_SYS_CD)
    LOCATION 's3://pcds-databricks-common-${account_id}/claim/harmonized/pc_claims_dh/CLAIM_EXPOSURE_STATUS'
    TBLPROPERTIES (delta.enableChangeDataFeed=true);""")
		
spark.sql(s"""CREATE OR REPLACE TABLE pc_claims_dh.CLAIM_INVESTIGATION (
		CLAIM_INVESTIGATION_KEY_ID STRING NOT NULL,
		CLAIM_INVESTIGATION_ID INT NOT NULL,
		CLAIM_INVESTIGATION_KEY STRING NOT NULL,
		SI_ACTIVATION_KEY_ID STRING NOT NULL,
		ETL_ADD_TS TIMESTAMP NOT NULL,
		ETL_ROW_EFCTV_TS TIMESTAMP,
		ETL_ROW_EXPRTN_TS TIMESTAMP,
		ETL_CURR_ROW_IN STRING,
		ETL_PATCH_TS TIMESTAMP,
		ETL_ROW_CMPLTN_IN STRING,
		ROW_HASH STRING,
		SRC_SYS_CD STRING,
		SRC_SYS_TP INT,
		PRTN_VAL STRING,
		SIU_IVTGN_SUBTP_CD STRING,
		SIU_IVTGN_SUBTP_TP INT,
		SIU_IVTGN_STTS_CD STRING,
		SIU_IVTGN_STTS_TP INT,
		SIU_RFRL_RSN_CD STRING,
		SIU_RFRL_RSN_TP INT,
		DSPSTN_TYPE_CD STRING,
		DSPSTN_TYPE_TP INT)
	USING delta
    PARTITIONED BY (SRC_SYS_CD)
    LOCATION 's3://pcds-databricks-common-${account_id}/claim/harmonized/pc_claims_dh/CLAIM_INVESTIGATION'
    TBLPROPERTIES (delta.enableChangeDataFeed=true);""")
		
spark.sql(s"""CREATE OR REPLACE TABLE pc_claims_dh.CLAIM_LEGACY_LINEAGE (
		CLAIM_LEGACY_LINEAGE_KEY_ID STRING NOT NULL,
		CLAIM_LEGACY_LINEAGE_ID INT NOT NULL,
		CLAIM_LEGACY_LINEAGE_KEY STRING NOT NULL,
		CLAIM_KEY_ID STRING NOT NULL,
		ETL_ADD_TS TIMESTAMP NOT NULL,
		ETL_ROW_EFCTV_TS TIMESTAMP,
		ETL_ROW_EXPRTN_TS TIMESTAMP,
		ETL_CURR_ROW_IN STRING,
		ETL_PATCH_TS TIMESTAMP,
		ETL_ROW_CMPLTN_IN STRING,
		ROW_HASH STRING,
		SRC_SYS_CD STRING,
		SRC_SYS_TP INT,
		PRTN_VAL STRING,
		LGCY_CLM_NB STRING,
		LGCY_SYS_CD STRING,
		LGCY_SYS_TP INT,
		PRMRY_LGCY_SYS_DERV_IN STRING,
		PRMRY_LGCY_SYS_SRC_IN STRING)
	USING delta
    PARTITIONED BY (SRC_SYS_CD)
    LOCATION 's3://pcds-databricks-common-${account_id}/claim/harmonized/pc_claims_dh/CLAIM_LEGACY_LINEAGE'
    TBLPROPERTIES (delta.enableChangeDataFeed=true);""")
		
spark.sql(s"""CREATE OR REPLACE TABLE pc_claims_dh.CLAIM_RESERVE (
		CLAIM_RESERVE_KEY_ID STRING NOT NULL,
		CLAIM_RESERVE_ID INT NOT NULL,
		CLAIM_RESERVE_KEY STRING NOT NULL,
		CLAIM_KEY_ID STRING,
		CLAIM_EXPOSURE_KEY_ID STRING,
		POSTING_LOSS_TRANSACTION_KEY_ID STRING NOT NULL,
		ETL_ADD_TS TIMESTAMP NOT NULL,
		ETL_ROW_EFCTV_TS TIMESTAMP,
		ETL_ROW_EXPRTN_TS TIMESTAMP,
		ETL_CURR_ROW_IN STRING,
		ETL_PATCH_TS TIMESTAMP,
		ETL_ROW_CMPLTN_IN STRING,
		ROW_HASH STRING,
		SRC_SYS_CD STRING,
		SRC_SYS_TP INT,
		PRTN_VAL STRING,
		BAL_POSTING_TS TIMESTAMP,
		COST_CTGRY_CD STRING,
		COST_CTGRY_TP INT,
		COST_TYPE_CD STRING,
		COST_TYPE_TP INT,
		RSRV_BAL_AM DECIMAL(18 , 2),
		RSRV_TRNSCTN_POST_AM DECIMAL(18 , 2))
	USING delta
    PARTITIONED BY (SRC_SYS_CD)
    LOCATION 's3://pcds-databricks-common-${account_id}/claim/harmonized/pc_claims_dh/CLAIM_RESERVE'
    TBLPROPERTIES (delta.enableChangeDataFeed=true);""")
		
spark.sql(s"""CREATE OR REPLACE TABLE pc_claims_dh.CLAIM_RESERVE_SETTLEMENT (
		CLAIM_RESERVE_SETTLEMENT_KEY_ID STRING NOT NULL,
		CLAIM_RESERVE_SETTLEMENT_ID INT NOT NULL,
		CLAIM_RESERVE_SETTLEMENT_KEY STRING NOT NULL,
		CLAIM_SETTLEMENT_KEY_ID STRING NOT NULL,
		CLAIM_RESERVE_KEY_ID STRING NOT NULL,
		ETL_ADD_TS TIMESTAMP NOT NULL,
		ETL_ROW_EFCTV_TS TIMESTAMP,
		ETL_ROW_EXPRTN_TS TIMESTAMP,
		ETL_CURR_ROW_IN STRING,
		ETL_PATCH_TS TIMESTAMP,
		ETL_ROW_CMPLTN_IN STRING,
		ROW_HASH STRING,
		SRC_SYS_CD STRING,
		SRC_SYS_TP INT,
		PRTN_VAL STRING)
	USING delta
    PARTITIONED BY (SRC_SYS_CD)
    LOCATION 's3://pcds-databricks-common-${account_id}/claim/harmonized/pc_claims_dh/CLAIM_RESERVE_SETTLEMENT'
    TBLPROPERTIES (delta.enableChangeDataFeed=true);""")