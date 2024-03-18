-- Databricks notebook source
select * from dhf_iot_curated_prod.trip_detail where ENRLD_VIN_NB in ('1FMSK7FHXNGD91011', '1FMSK7FHXNGB32587')
and LOAD_DT >='2022-07-01'

-- COMMAND ----------

select * from dhf_iot_harmonized_prod.program_enrollment where vin_nb = '19XFB2F85CE334730'