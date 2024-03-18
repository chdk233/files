// Databricks notebook source
val environment="prod"

// COMMAND ----------



val df=spark.sql(s""" with new_ie as (
    select
      distinct
        cast(
          row_number() over(
            order by
              NULL
          ) + coalesce(
            (
              select
                max(INTEGRATED_ENROLLMENT_ID)
              from
                dhf_iot_harmonized_${environment}.integrated_enrollment
            ),
            0
          ) as BIGINT
        ) as INTEGRATED_ENROLLMENT_ID,
      current_timestamp() as ETL_LAST_UPDT_DTS,
      coalesce(VIN_NB, ENRLD_VIN_NB, 'NOKEY') as VIN_NB,
      coalesce(DEVC_ID, DEVC_ID_NB) as DEVC_ID_OLD,
      coalesce(
        ENRLMNT_EFCTV_DT,
        ENRLMNT_DT,
        to_date('9999-12-31')
      ) as ENRLMNT_EFCTV_DT,
      PRGRM_INSTC_ID,
      DATA_CLCTN_ID,
      cast(
        coalesce(
          PRGRM_TERM_BEG_DT,
          ACTV_STRT_DT,
          to_date('1000-01-01')
        ) as DATE
      ) as PRGRM_TERM_BEG_DT,
      cast(
        coalesce(
          PRGRM_TERM_END_DT,
          ACTV_END_DT,
          to_date('9999-12-31')
        ) as DATE
      ) as PRGRM_TERM_END_DT,
      coalesce(PLCY_ST_CD, PLCY_RT_ST_CD, 'NOKEY') as PLCY_ST_CD,
      coalesce(pef.VNDR_CD, ods.SRC_SYS_CD) as SRC_SYS_CD,
      current_date() as LOAD_DT,
      to_timestamp(
        date_format(current_timestamp(), 'yyyy-MM-dd H:00:00+0000')
      ) as LOAD_HR_TS
    from
      (
        select
          distinct first(VNDR_CD) as VNDR_CD,
          DATA_CLCTN_ID,
          first(VIN_NB) as VIN_NB,
          first(DEVC_ID) as DEVC_ID,
          -- DEVC_ID as DEVC_ID,
          first(ENRLMNT_EFCTV_DT) as ENRLMNT_EFCTV_DT,
          PLCY_ST_CD,
          min(PRGRM_TERM_BEG_DT) as PRGRM_TERM_BEG_DT,
          max(PRGRM_TERM_END_DT) as PRGRM_TERM_END_DT,
          max(LOAD_HR_TS) as LOAD_HR_TS
        from
          (
            select
              distinct coalesce(a.VNDR_CD, 'PE') as VNDR_CD,
              a.DATA_CLCTN_ID as DATA_CLCTN_ID,
              a.VIN_NB,
              trim(a.DEVC_ID) as DEVC_ID,
              a.ENRLMNT_EFCTV_DT,
              a.PLCY_ST_CD,
              a.PRGRM_TERM_BEG_DT,
              a.PRGRM_TERM_END_DT,
              a.LOAD_HR_TS
            from
              dhf_iot_harmonized_${environment}.program_enrollment a
              inner join (
                select
                  distinct pe.DATA_CLCTN_ID,
                  pe.VIN_NB,
                  pe.PLCY_ST_CD
                from
                  dhf_iot_harmonized_${environment}.program_enrollment pe
                  inner join (
                    select
                      distinct VIN_NB,
                      max(EVNT_SYS_TS) as mrRec,
                      max(ENRLMNT_EFCTV_DT) as mrE
                    from
                      dhf_iot_harmonized_${environment}.program_enrollment
                    where
                      (
                        PRGRM_TERM_BEG_DT is not null
                        or PRGRM_TERM_END_DT is not null
                      )
                      and trim(VNDR_CD) not in ('CMT', 'LN')
                      and DATA_CLCTN_STTS = 'Active'
                      and PRGRM_STTS_CD in ('Active', 'Cancelled')
                    group by
                      VIN_NB
                  ) pe1 on pe.VIN_NB = pe1.VIN_NB
                  and pe.ENRLMNT_EFCTV_DT = mrE
                  and pe.EVNT_SYS_TS = mrRec
              ) b on a.DATA_CLCTN_ID = b.DATA_CLCTN_ID
              and a.VIN_NB = b.VIN_NB
              and a.PLCY_ST_CD = b.PLCY_ST_CD
          )
        group by
          DATA_CLCTN_ID,
          PLCY_ST_CD
      ) pef full
      outer join (
        select
          distinct 'ODS' as SRC_SYS_CD,
          PRGRM_INSTC_ID,
          trim(ods1.ENRLD_VIN_NB) as ENRLD_VIN_NB,
          DEVC_ID_NB,
          ENRLMNT_DT,
          PLCY_RT_ST_CD,
          ACTV_STRT_DT,
          ACTV_END_DT
        from
          dhf_iot_harmonized_${environment}.ods_table ods1
          inner join (
            select
              distinct trim(ENRLD_VIN_NB) as ENRLD_VIN_NB,
              max(PRGRM_INSTC_ID) as maxPIID
            from
              dhf_iot_harmonized_${environment}.ods_table
            where
              VHCL_STTS_CD = 'E'
              and ACTV_END_DT = '3500-01-01'
              and LOAD_DT = (select max(LOAD_DT) from dhf_iot_harmonized_${environment}.ods_table)
            group by
              ENRLD_VIN_NB
          ) ods2 on trim(ods1.ENRLD_VIN_NB) = trim(ods2.ENRLD_VIN_NB)
          and ods1.PRGRM_INSTC_ID = ods2.maxPIID
        where
          VHCL_STTS_CD = 'E'
          and ACTV_END_DT = '3500-01-01'
      ) ods on pef.VIN_NB = trim(ods.ENRLD_VIN_NB) )
      
    select v.*,case when trim(v.devc_id_old)in ('',0) then q.DEVC_ID_NEW else v.devc_id_old end devc_id from new_ie  v left join ( select  VIN_NB,first(DEVC_ID) AS DEVC_ID_NEW from (select distinct Z.VIN_NB,Z.DEVC_ID FROM (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD in ('Active', 'Cancelled') and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and vin_nb  in (select vin_nb from  new_ie) and trim(VNDR_CD) not in ('CMT', 'LN')) z inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD in ('Active', 'Cancelled') and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) y on z.VIN_NB=y.VIN_NB and z.EVNT_SYS_TS=max_e and z.ENRLMNT_EFCTV_DT=max_tbd)  group by VIN_NB ) q on v.VIN_NB=q.VIN_NB
      
      ;""").drop("devc_id_old")
df.write.format("delta").mode("overWrite").saveAsTable("dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT_test")

// COMMAND ----------

val df2=spark.sql("""
select
      distinct
        cast(
          row_number() over(
            order by
              NULL
          ) + coalesce(
            (
              select
                max(INTEGRATED_ENROLLMENT_ID)
              from
                dhf_iot_harmonized_prod.integrated_enrollment
            ),
            0
          ) as BIGINT
        ) as INTEGRATED_ENROLLMENT_ID,
      current_timestamp() as ETL_LAST_UPDT_DTS,
      coalesce(VIN_NB, ENRLD_VIN_NB, 'NOKEY') as VIN_NB,
      coalesce(DEVC_ID, DEVC_ID_NB) as DEVC_ID,
      cast(coalesce(
        ENRLMNT_EFCTV_DT,
        ENRLMNT_DT,
        to_date('9999-12-31')
      ) as date) as ENRLMNT_EFCTV_DT,
      PRGRM_INSTC_ID,
      DATA_CLCTN_ID,
      cast(
        coalesce(
          PRGRM_TERM_BEG_DT,
          ACTV_STRT_DT,
          to_date('1000-01-01')
        ) as DATE
      ) as PRGRM_TERM_BEG_DT,
      cast(
        coalesce(
          PRGRM_TERM_END_DT,
          ACTV_END_DT,
          to_date('9999-12-31')
        ) as DATE
      ) as PRGRM_TERM_END_DT,
      coalesce(PLCY_ST_CD, PLCY_RT_ST_CD, 'NOKEY') as PLCY_ST_CD,
      coalesce(pef.VNDR_CD, ods.SRC_SYS_CD) as SRC_SYS_CD,
      current_date() as LOAD_DT,
      to_timestamp(
        date_format(current_timestamp(), 'yyyy-MM-dd H:00:00+0000')
      ) as LOAD_HR_TS
    from
      (
        select
          distinct first(VNDR_CD) as VNDR_CD,
          DATA_CLCTN_ID,
          first(VIN_NB) as VIN_NB,
          first(DEVC_ID) as DEVC_ID,
          -- DEVC_ID as DEVC_ID,
          first(ENRLMNT_EFCTV_DT) as ENRLMNT_EFCTV_DT,
          PLCY_ST_CD,
          min(PRGRM_TERM_BEG_DT) as PRGRM_TERM_BEG_DT,
          max(PRGRM_TERM_END_DT) as PRGRM_TERM_END_DT,
          max(LOAD_HR_TS) as LOAD_HR_TS
        from
          (
            select
              distinct coalesce(a.VNDR_CD, 'PE') as VNDR_CD,
              a.DATA_CLCTN_ID as DATA_CLCTN_ID,
              a.VIN_NB,
              trim(a.DEVC_ID) as DEVC_ID,
              a.ENRLMNT_EFCTV_DT,
              a.PLCY_ST_CD,
              a.PRGRM_TERM_BEG_DT,
              a.PRGRM_TERM_END_DT,
              a.LOAD_HR_TS
            from
              dhf_iot_harmonized_prod.program_enrollment a
              inner join (
                select
                  distinct pe.DATA_CLCTN_ID,
                  pe.VIN_NB,
                  pe.PLCY_ST_CD
                from
                  dhf_iot_harmonized_prod.program_enrollment pe
                  inner join (
                    select
                      distinct VIN_NB,
                      max(EVNT_SYS_TS) as mrRec,
                      max(ENRLMNT_EFCTV_DT) as mrE
                    from
                      dhf_iot_harmonized_prod.program_enrollment
                    where
                      (
                        PRGRM_TERM_BEG_DT is not null
                        or PRGRM_TERM_END_DT is not null
                      )
                      and trim(VNDR_CD) not in ('CMT', 'LN')
                      and DATA_CLCTN_STTS = 'Active'
                      and PRGRM_STTS_CD in ('Active', 'Cancelled')
                    group by
                      VIN_NB
                  ) pe1 on pe.VIN_NB = pe1.VIN_NB
                  and pe.ENRLMNT_EFCTV_DT = mrE
                  and pe.EVNT_SYS_TS = mrRec
              ) b on a.DATA_CLCTN_ID = b.DATA_CLCTN_ID
              and a.VIN_NB = b.VIN_NB
              and a.PLCY_ST_CD = b.PLCY_ST_CD
          )
        group by
          DATA_CLCTN_ID,
          PLCY_ST_CD
      ) pef full
      outer join (
        select
          distinct 'ODS' as SRC_SYS_CD,
          PRGRM_INSTC_ID,
          trim(ods1.ENRLD_VIN_NB) as ENRLD_VIN_NB,
          DEVC_ID_NB,
          ENRLMNT_DT,
          PLCY_RT_ST_CD,
          ACTV_STRT_DT,
          ACTV_END_DT
        from
          dhf_iot_harmonized_prod.ods_table ods1
          inner join (
            select
              distinct trim(ENRLD_VIN_NB) as ENRLD_VIN_NB,
              max(PRGRM_INSTC_ID) as maxPIID
            from
              dhf_iot_harmonized_prod.ods_table
            where
              VHCL_STTS_CD = 'E'
              and ACTV_END_DT = '3500-01-01'
              and LOAD_DT = (select max(LOAD_DT) from dhf_iot_harmonized_prod.ods_table)
            group by
              ENRLD_VIN_NB
          ) ods2 on trim(ods1.ENRLD_VIN_NB) = trim(ods2.ENRLD_VIN_NB)
          and ods1.PRGRM_INSTC_ID = ods2.maxPIID
        where
          VHCL_STTS_CD = 'E'
          and ACTV_END_DT = '3500-01-01'
      ) ods on pef.VIN_NB = trim(ods.ENRLD_VIN_NB)""")
df2.createOrReplaceTempView("table")
// df.write.format("delta").mode("overWrite").saveAsTable("dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT_temp")

// COMMAND ----------

val df3=spark.sql("select v.*,case when trim(v.devc_id)in ('',0) then q.DEVC_ID_NEW else v.devc_id end devc_id_new from (select * from table order by load_hr_ts desc)  v left join ( select  VIN_NB,first(DEVC_ID) AS DEVC_ID_NEW from (select distinct Z.VIN_NB,Z.DEVC_ID FROM (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD in ('Active', 'Cancelled') and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active'  and trim(VNDR_CD) not in ('CMT', 'LN')) z inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD in ('Active', 'Cancelled') and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) y on z.VIN_NB=y.VIN_NB and z.EVNT_SYS_TS=max_e and z.ENRLMNT_EFCTV_DT=max_tbd)  group by VIN_NB ) q on v.VIN_NB=q.VIN_NB").drop("devc_id").withColumnRenamed("devc_id_new","devc_id")
// display(df3)

// COMMAND ----------

df2.write.format("delta").mode("overWrite").saveAsTable("dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT_temp2")
df3.write.format("delta").mode("overWrite").saveAsTable("dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT_temp1")

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from (select v.vin_nb,v.devc_id,
// MAGIC case when trim(v.devc_id)in ('',0) then q.DEVC_ID_NEW else v.devc_id end new_device,
// MAGIC q.DEVC_ID_NEW from table  v left join ( select  VIN_NB,first(DEVC_ID) AS DEVC_ID_NEW from (select distinct Z.VIN_NB,Z.DEVC_ID FROM (select * from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD in ('Active', 'Cancelled') and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN')) z inner join (select VIN_NB,max(EVNT_SYS_TS) as max_e,max(ENRLMNT_EFCTV_DT) as max_tbd from dhf_iot_harmonized_prod.program_enrollment where PRGRM_STTS_CD in ('Active', 'Cancelled') and  DEVC_ID not in ('',' ','0') and DATA_CLCTN_STTS='Active' and trim(VNDR_CD) not in ('CMT', 'LN') group by VIN_NB) y on z.VIN_NB=y.VIN_NB and z.EVNT_SYS_TS=max_e and z.ENRLMNT_EFCTV_DT=max_tbd)  group by VIN_NB ) q on v.VIN_NB=q.VIN_NB) where vin_nb not in (select vin_nb from  table)

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from  dhf_iot_harmonized_prod.INTEGRATED_ENROLLMENT
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_ims_raw_prod.tripsummary where enrolledvin='JTNK4RBE8K3068540'
// MAGIC -- select   ENRLD_VIN_NB,DEVC_KEY,count(*) from dhf_iot_curated_prod.device_status group by ENRLD_VIN_NB,DEVC_KEY

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from dhf_iot_harmonized_prod.ods_table where trim(ENRLD_VIN_NB)='1C6SRFFT4KN803751'  --'19UUB5F42MA001056'

// COMMAND ----------

// MAGIC %sql
// MAGIC select   (*) from dhf_iot_curated_prod.device_status where ENRLD_VIN_NB in (select enrld_vin_nb from dhf_iot_curated_prod.Device_status where STTS_EXPRTN_TS='9999-01-01'group by enrld_vin_nb,STTS_EXPRTN_TS having count(*)>1)