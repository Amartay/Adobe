from pyspark import SparkContext, SparkConf , StorageLevel
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from dateutil.rrule import rrule, MONTHLY
from datetime import datetime
import json
from pyspark.sql import functions
import sys
from pyspark.sql import Row
from pyspark.sql.functions import concat, col, lit
from pyspark.sql.functions import udf
from pyspark.sql.functions import when, sum, avg, col
from pyspark.sql import functions as F, types as T
from pyspark.sql import functions as F
from pyspark.sql.functions import input_file_name ,monotonically_increasing_id
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import col, to_date, date_format
from pyspark.sql.functions import split, explode
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pyspark import SparkContext, SparkConf , StorageLevel
import smtplib
from email.message import EmailMessage

class main() :
    def __init__(self):
        try :
             spark = SparkSession.builder \
                 .enableHiveSupport() \
                 .config('hive.exec.dynamic.partition', 'true') \
                 .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
                 .config('hive.exec.max.dynamic.partitions', '10000') \
                 .getOrCreate()
             log4j = spark._jvm.org.apache.log4j
             log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
             spark.sql('SET hive.warehouse.data.skiptrash=true;')
             spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
             spark.conf.set('spark.sql.cbo.enabled', True)
             spark.conf.set('spark.sql.cbo.join reorder.enabled', True)
             spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
             spark.sql('set spark.sql.parquet.enableVectorizedReader=false')
             spark.sql('set spark.sql.sources.partitionOverwriteMode=dynamic')
             spark.sql("set spark.databricks.sql.files.prorateMaxPartitionBytes.enabled=false")
             spark.sql("set spark.sql.adaptive.coalescePartitions.enabled=false")
             spark.sql("set spark.sql.adaptive.enabled=false")
             spark.sql("set spark.sql.shuffle.partitions=1000")

             dbutils.widgets.text("Custom_Settings", "")
             dbutils.widgets.text("TGT_TBL", "")
             dbutils.widgets.text("PIVOT_TBL", "")
             dbutils.widgets.text("FIRMOGRAPHICS_SMB_TBL", "")
             dbutils.widgets.text("LVT_PROFILE_TBL", "")
             dbutils.widgets.text("SCD_LICENSE_TBL", "")
             dbutils.widgets.text("DIM_SEAT_TBL", "")
             dbutils.widgets.text("DIM_CONTRACT", "")
             dbutils.widgets.text("SRC_TBL", "")
             dbutils.widgets.text("GENERIC_TBL", "")
             dbutils.widgets.text("FROM_DT", "")
             dbutils.widgets.text("TO_DT", "")
          
             Settings = dbutils.widgets.get("Custom_Settings")
             TGT_TBL = dbutils.widgets.get("TGT_TBL")
             PIVOT_TBL = dbutils.widgets.get("PIVOT_TBL")
             FIRMOGRAPHICS_SMB_TBL = dbutils.widgets.get("FIRMOGRAPHICS_SMB_TBL")
             LVT_PROFILE_TBL = dbutils.widgets.get("LVT_PROFILE_TBL")
             SCD_LICENSE_TBL = dbutils.widgets.get("SCD_LICENSE_TBL")
             DIM_SEAT_TBL = dbutils.widgets.get("DIM_SEAT_TBL") 
             DIM_CONTRACT = dbutils.widgets.get("DIM_CONTRACT")
             SRC_TBL = dbutils.widgets.get("SRC_TBL")
             GENERIC_TBL = dbutils.widgets.get("GENERIC_TBL")
             FROM_DT = dbutils.widgets.get("FROM_DT")
             TO_DT = dbutils.widgets.get("TO_DT")

             Set_list = Settings.split(',')
             if len(Set_list)>0:
                 for i in Set_list:
                     if i != "":
                         print("spark.sql(+i+)")
                         spark.sql("""{i}""".format(i=i))

### IN (Individual) Cancellation data from Pivot Table and Flag Created for Generic & Non_generic Domains:
             
             df_pivot_generic_chk=spark.sql("""
			 select distinct cncl.* 
             , case when lower(gen.type) = 'generic' then 'generic' else 'non_generic' end as IN_generic_type 
             from 
             (select 
			 a.*, 
			 case when trim(a.customer_email_domain) is null or trim(a.customer_email_domain) = '' or trim(upper(a.customer_email_domain)) = 'NULL' 
             then coalesce(SUBSTR(UPPER(d.pers_email), INSTR(UPPER(d.pers_email), '@') + 1), e.domain,a.customer_email_domain) 
			 when trim(upper(a.customer_email_domain)) = e.domain then e.domain
			 else coalesce(SUBSTR(UPPER(d.pers_email), INSTR(UPPER(d.pers_email), '@') + 1),e.domain,a.customer_email_domain) end as domain, 
			 case when trim(a.customer_email_domain) is null or trim(a.customer_email_domain) = '' or trim(upper(a.customer_email_domain)) = 'NULL' 
			 then coalesce(e.org_name) 
			 when trim(upper(a.customer_email_domain)) = e.domain then e.org_name
			 else coalesce(e.org_name) end as org_name,
			 coalesce(d.pers_email,e.email) as email
			 from
			 (( select 
			 subscription_account_guid 
			 , CRM_CUSTOMER_GUID
			 , cc_phone_vs_web
			 , vip_contract
			 , source_type
			 , event_source
			 , stype
			 , date_date
			 , sales_document
			 , sales_document_item
			 , fiscal_yr_and_qtr_desc
			 , fiscal_yr_and_wk_desc
			 , net_cancelled_arr_cfx
			 , net_cancelled_subs
			 , customer_email_domain
			 from {PIVOT_TBL}
			 where source_type IN ('IN','TM','OVERAGE') 
			 and event_source not in ('SNAPSHOT','F2P')  
			 and date_key between regexp_replace(date_add('{FROM_DT}',-63),'-','') and regexp_replace('{TO_DT}','-','')
			 and stype IN ('IN') 
			 and net_cancelled_arr_cfx > 0
			 )a  
			 left outer join 
			 {LVT_PROFILE_TBL} d on upper(a.CRM_CUSTOMER_GUID) = upper(d.user_guid)
			 left outer join 
			 (select * from 
             ( select *,row_number() over (partition by domain,email order by null)rn 
             from {FIRMOGRAPHICS_SMB_TBL} 
             ) where rn = 1) e 
             on UPPER(d.pers_email)=upper(e.email))) cncl
			 left outer join {GENERIC_TBL} gen
			 on lower(cncl.domain) = lower(gen.domain) """.format(PIVOT_TBL=PIVOT_TBL,FIRMOGRAPHICS_SMB_TBL=FIRMOGRAPHICS_SMB_TBL,LVT_PROFILE_TBL=LVT_PROFILE_TBL,GENERIC_TBL=GENERIC_TBL,FROM_DT=FROM_DT,TO_DT=TO_DT)) 
             df_pivot_generic_chk.createOrReplaceTempView("df_pivot_generic_chk")
             df_pivot_generic_chk.persist()
             
             df_pivot_non_generic_tmp=spark.sql(""" select * from df_pivot_generic_chk where IN_generic_type = 'non_generic' """)
             df_pivot_non_generic_tmp.createOrReplaceTempView("df_pivot_non_generic_tmp")
             
             df_pivot_generic=spark.sql(""" select * from df_pivot_generic_chk where IN_generic_type = 'generic' """)
             df_pivot_generic.createOrReplaceTempView("df_pivot_generic")
             
### IN (Individual) Cancellation data for Non_generic Domains :

             df_pivot_non_generic=spark.sql("""
             select distinct * from (select 
             a.*,  
			 case when trim(a.customer_email_domain) is null or trim(a.customer_email_domain) = '' or trim(upper(a.customer_email_domain)) = 'NULL' 
			 then coalesce(e.domain,b.domain,c.domain,a.customer_email_domain) 
    		 when trim(upper(a.customer_email_domain)) = e.domain then e.domain
			 when trim(upper(a.customer_email_domain)) = b.domain then b.domain
			 when trim(upper(a.customer_email_domain)) = c.domain then c.domain
			 else coalesce(e.domain,b.domain,c.domain,a.customer_email_domain) end as domain, 
			 case when trim(a.customer_email_domain) is null or trim(a.customer_email_domain) = '' or trim(upper(a.customer_email_domain)) = 'NULL' 
			 then coalesce(e.org_name,b.org_name,c.org_name) 
    		 when trim(upper(a.customer_email_domain)) = e.domain then e.org_name
			 when trim(upper(a.customer_email_domain)) = b.domain then b.org_name
			 when trim(upper(a.customer_email_domain)) = c.domain then c.org_name
			 else coalesce(e.org_name,b.org_name,c.org_name) end as org_name 
             from
             (( select 
             subscription_account_guid 
             , CRM_CUSTOMER_GUID
             , cc_phone_vs_web
             , vip_contract
             , source_type
             , event_source
             , stype
             , date_date
             , sales_document
             , sales_document_item
             , fiscal_yr_and_qtr_desc
             , fiscal_yr_and_wk_desc
             , net_cancelled_arr_cfx
             , net_cancelled_subs
             , customer_email_domain
             , IN_generic_type
             , email 
             from df_pivot_non_generic_tmp 
             )a  
             left outer join 
             ( select * from 
             ( select *,row_number() over (partition by subscription_account_guid order by null) rn from {FIRMOGRAPHICS_SMB_TBL}
             ) where rn = 1 ) b on upper(a.subscription_account_guid)=upper(b.subscription_account_guid)
             left outer join  
             ( select * from 
             ( select *,row_number() over (partition by contract_id order by null)rn from {FIRMOGRAPHICS_SMB_TBL}  
             ) where rn = 1 )c on upper(a.subscription_account_guid)=upper(c.contract_id)
             left outer join 
             {LVT_PROFILE_TBL} d on upper(a.CRM_CUSTOMER_GUID) = upper(d.user_guid)
             left outer join 
             ( select * from 
             ( select *,row_number() over (partition by domain order by null)rn from {FIRMOGRAPHICS_SMB_TBL}
             ) where rn = 1 )e on SUBSTR(UPPER(d.pers_email), INSTR(UPPER(d.pers_email), '@') + 1)=upper(e.domain))) 
             """.format(FIRMOGRAPHICS_SMB_TBL=FIRMOGRAPHICS_SMB_TBL,LVT_PROFILE_TBL=LVT_PROFILE_TBL,FROM_DT=FROM_DT,TO_DT=TO_DT)) 
             df_pivot_non_generic.createOrReplaceTempView("df_pivot_non_generic")
             #df_pivot_domain_org.write.format("parquet").mode("append").saveAsTable("b2b_tmp.b2b_I2T_df_pivot_domain_org_240105",overwrite=True)

### Union for IN (Individual) Cancellation data :

             df_pivot_domain_org=spark.sql("""
             select subscription_account_guid 
             , CRM_CUSTOMER_GUID
             , cc_phone_vs_web
             , vip_contract
             , source_type
             , event_source
             , stype
             , date_date
             , sales_document
             , sales_document_item
             , fiscal_yr_and_qtr_desc
             , fiscal_yr_and_wk_desc
             , net_cancelled_arr_cfx
             , net_cancelled_subs
             , customer_email_domain
             , IN_generic_type
             , email , domain , org_name from df_pivot_non_generic 
             union all
             select subscription_account_guid 
             , CRM_CUSTOMER_GUID
             , cc_phone_vs_web
             , vip_contract
             , source_type
             , event_source
             , stype
             , date_date
             , sales_document
             , sales_document_item
             , fiscal_yr_and_qtr_desc
             , fiscal_yr_and_wk_desc
             , net_cancelled_arr_cfx
             , net_cancelled_subs
             , customer_email_domain
             , IN_generic_type
             , email , domain , org_name from df_pivot_generic 
             """)
             df_pivot_domain_org.createOrReplaceTempView("df_pivot_domain_org")
             
# Getting Contract_Id for IN Cancelled : 

             df_pre_pivotbase=spark.sql("""           
             select Pvt.*, case when Pvt.cc_phone_vs_web = 'VIP-PHONE' then Pvt.vip_contract
             else coalesce(OcfContractID.contract_id,seatContractID.contract_id )
             end as contract_id
             from df_pivot_domain_org Pvt 
             left outer join
             (
             Select upper(subscription_account_guid) subs_guid, 
             upper(contract_id) as contract_id 
             from {SCD_LICENSE_TBL}
             where contract_type ='DIRECT_ORGANIZATION'
             --and contract_id is not null
             and contract_id != ''
             and subscription_account_guid is not null
             and subscription_account_guid != ''
             group by upper(subscription_account_guid), upper(contract_id)
             ) OcfContractID 
             on Pvt.subscription_account_guid  = OcfContractID.subs_guid
             left outer join
             (
             Select upper(subscription_account_guid) subs_guid, 
             upper(contract_id) contract_id 
             from {DIM_SEAT_TBL}
             where contract_id is not null
             and contract_id != ''
             and subscription_account_guid is not null
             and subscription_account_guid != ''
             group by upper(subscription_account_guid), upper(contract_id)
             ) seatContractID 
             on Pvt.subscription_account_guid  = seatContractID.subs_guid """.format(SCD_LICENSE_TBL=SCD_LICENSE_TBL,DIM_SEAT_TBL=DIM_SEAT_TBL)) 
             df_pre_pivotbase.createOrReplaceTempView("df_pre_pivotbase")
             #df_pre_pivotbase.write.format("parquet").mode("append").saveAsTable("b2b_tmp.b2b_I2T_df_pre_pivotbase_240105",overwrite=True)

             df_pivot_domain_base=spark.sql(""" select domain
             , org_name
             , event_source
             , stype
             , date_date
             , crm_customer_guid
             , sales_document
             , sales_document_item
             , fiscal_yr_and_qtr_desc
             , fiscal_yr_and_wk_desc
             , sum(net_cancelled_arr_cfx) OVER(partition by domain,org_name,date_date) as sum_net_cancelled_arr_cfx
             , sum(net_cancelled_subs) OVER(partition by domain,org_name,date_date) as sum_net_cancelled_subs
             , contract_id
             , IN_generic_type
             , email
             from df_pre_pivotbase """)
             df_pivot_domain_base.createOrReplaceTempView("df_pivot_domain_base")
             df_pivot_domain_base.persist()
             #df_pivot_domain_base.write.format("parquet").mode("append").saveAsTable("b2b_tmp.b2b_I2T_df_pivot_domain_base_240108",overwrite=True)

### TM (Team) Purchase Data from Abdashbase_rules Table for generic and Non_generic Domains:
             
             df_rules_generic_chk=spark.sql(""" select distinct pur.*,              
             case when lower(gen.type) = 'generic' then 'generic' else 'non_generic' end as TM_generic_type  
             from ( select
			 a.*,
             coalesce(c.email,e.email) as email,
			 case when trim(a.customer_email_domain) is null or trim(a.customer_email_domain) = '' or trim(upper(a.customer_email_domain)) = 'NULL'  
			 then coalesce(c.domain,e.domain,a.customer_email_domain)  
			 when trim(upper(a.customer_email_domain)) = c.domain then c.domain
			 when trim(upper(a.customer_email_domain)) = e.domain then e.domain
			 else coalesce(c.domain,e.domain,a.customer_email_domain) end as domain,
			 case when trim(a.customer_email_domain) is null or trim(a.customer_email_domain) = '' or trim(upper(a.customer_email_domain)) = 'NULL' 
			 then coalesce(c.org_name,e.org_name)
			 when trim(upper(a.customer_email_domain)) = c.domain then c.org_name
			 when trim(upper(a.customer_email_domain)) = e.domain then e.org_name
			 else coalesce(c.org_name,e.org_name) end as org_name
			 from
			 (( select * from
			 ( select *
			 from {SRC_TBL} 
			 where STYPE = 'TM' and txns in ('Initial Purchase','Rep-Add on') 
			 and TeamKey <> 'WEB NOT APPLICABLE' and event = 'RULES'
			 and date_date between '{FROM_DT}' and '{TO_DT}' --and init_purchase_arr_cfx <> 0
			 )) a 
			 left outer join 
			 {DIM_CONTRACT} b on upper(a.contract_id) = upper(b.contract_id) 
			 left outer join 
			 ( select * from 
			 ( select *,row_number() over (partition by enrollee_id order by null)rn from {FIRMOGRAPHICS_SMB_TBL}
			 ) where rn = 1 ) c on upper(b.enrollee_id) = upper(c.enrollee_id)
			 left outer join 
			 {LVT_PROFILE_TBL} d on upper(a.CRM_CUSTOMER_GUID) = upper(d.user_guid)
			 left outer join 
			 ( select * from 
             ( select *,row_number() over (partition by domain order by null)rn from {FIRMOGRAPHICS_SMB_TBL}
             ) where rn = 1 ) e 
			 on UPPER(d.pers_email)=upper(e.email))) pur
			 left outer join {GENERIC_TBL} gen
			 on lower(pur.domain) = lower(gen.domain) """.format(SRC_TBL=SRC_TBL,DIM_CONTRACT=DIM_CONTRACT,FIRMOGRAPHICS_SMB_TBL=FIRMOGRAPHICS_SMB_TBL,LVT_PROFILE_TBL=LVT_PROFILE_TBL,GENERIC_TBL=GENERIC_TBL,FROM_DT=FROM_DT,TO_DT=TO_DT)) 
             df_rules_generic_chk.createOrReplaceTempView("df_rules_generic_chk") 
             df_rules_generic_chk.persist()
             
             df_rules_non_generic_tmp=spark.sql(""" select * from df_rules_generic_chk where TM_generic_type = 'non_generic' """)
             df_rules_non_generic_tmp.createOrReplaceTempView("df_rules_non_generic_tmp")
             
             df_rules_generic=spark.sql(""" select sales_document_item,CREATED_BY,CRM_CUSTOMER_GUID,ENTITLEMENT_TYPE,date_date,cc_phone_vs_web, GEO,MARKET_AREA,MARKET_SEGMENT,offer_type_description,PRODUCT_CONFIG,product_config_description,
			 PRODUCT_NAME,product_name_description,PROMO_TYPE,PROMOTION,Region,SALES_DOCUMENT,DYLAN_ORDER_NUMBER,STYPE,SUBS_OFFER,
			 SUBSCRIPTION_ACCOUNT_GUID,VIP_CONTRACT,addl_purchase_diff,addl_purchase_same,
			 gross_cancel_arr_cfx,gross_new_arr_cfx,gross_new_subs,init_purchase_arr_cfx,migrated_from_arr_cfx,
			 migrated_to_arr_cfx,net_cancelled_arr_cfx,net_new_arr_cfx,net_new_subs,net_purchases_arr_cfx,reactivated_arr_cfx,
			 returns_arr_cfx,ecc_customer_id,SALES_DISTRICT,contract_start_date_veda,contract_end_date_veda,contract_id,
			 renewal_from_arr_cfx,renewal_to_arr_cfx,projected_dme_gtm_segment,gtm_acct_segment,route_to_market,cc_segment,
			 net_new_arr,Sales_Center,AgentGeo,RepName,RepLdap,con_RepLdap,RepTSM,
			 RepManger,RepTeam,cross_team_agent,cross_sell_team,Flag,AgentMapFlag,Txns,SFDC_opportunity_id,
			 SFDC_opportunity_created_date,SFDC_closed_date,SFDC_email,SFDC_ecc_salesordernumber,SFDC_campaignid,
			 SFDC_name,SFDC_cum_campaignid,SFDC_min_date,SFDC_min_fiscal_yr_and_qtr_desc,SFDC_min_fiscal_yr_and_wk_desc,
			 SFDC_Flag,Bob_Flag,TeamKey,gross_new_arr_cfx_unclaimed,net_purchases_arr_cfx_unclaimed,unclaimed_ldap,
			 unclaimed_team,unclaimed_TSM,unclaimed_Manager,agent_max_quarter,agent_curr_qtr,created_curr_qtr,
			 agent_TSM_Ldap,created_TSM_Ldap,agent_max_quarter_TSM,created_max_quarter_TSM,agent_TSM_team,
			 created_TSM_team,agent_TSM_team_key,created_TSM_team_key,agent_TSM_flag,created_TSM_flag,ABD_Flag,
			 agent_ABD_Flag,TSM_ABD_Flag,TSM_agent_ABD_Flag,gross_cancellations,net_cancelled_subs,migrated_from,
			 migrated_to,renewal_from,renewal_to,Custom_Flag,bob_ABD_flag,lat_ABD_flag,same_wk_cncl_flag,
			 cross_team_agent_name,cross_sell_team_key,net_value_usd,acct_name,customer_email_domain,payment_method,
			 net_purchase_agg,cancel_date_list,sum_cancel_arr,net_purchase_arr_incremental,event,FISCAL_YR_AND_QTR_DESC,
			 FISCAL_YR_AND_WK_DESC,email,TM_generic_type,domain,org_name 
    		 from df_rules_generic_chk where TM_generic_type = 'generic' """)
             df_rules_generic.createOrReplaceTempView("df_rules_generic")
             
### TM (Team) Purchase Data from Abdashbase_rules Table for Non_generic Domains:

             df_rules_non_generic=spark.sql(""" select distinct * from ( select
			 a.*,
			 case when trim(a.customer_email_domain) is null or trim(a.customer_email_domain) = '' or trim(upper(a.customer_email_domain)) = 'NULL' 
			 then coalesce(h.domain,e.domain,i.domain,j.domain,c.domain,a.customer_email_domain)  
			 when trim(upper(a.customer_email_domain)) = h.domain then h.domain
			 when trim(upper(a.customer_email_domain)) = e.domain then e.domain
			 when trim(upper(a.customer_email_domain)) = i.domain then i.domain
			 when trim(upper(a.customer_email_domain)) = j.domain then j.domain
    		 when trim(upper(a.customer_email_domain)) = c.domain then c.domain
			 else coalesce(h.domain,e.domain,i.domain,j.domain,c.domain,a.customer_email_domain) end as domain, 
			 case when trim(a.customer_email_domain) is null or trim(a.customer_email_domain) = '' or trim(upper(a.customer_email_domain)) = 'NULL' 
			 then coalesce(h.org_name,e.org_name,i.org_name,j.org_name,c.org_name)
			 when trim(upper(a.customer_email_domain)) = h.domain then h.org_name
			 when trim(upper(a.customer_email_domain)) = e.domain then e.org_name
			 when trim(upper(a.customer_email_domain)) = i.domain then i.org_name
			 when trim(upper(a.customer_email_domain)) = j.domain then j.org_name
			 when trim(upper(a.customer_email_domain)) = c.domain then c.org_name
			 else coalesce(h.org_name,e.org_name,i.org_name,j.org_name,c.org_name) end as org_name
			 from
			 (( select * from
			 ( select * except (domain, org_name)
			 from df_rules_non_generic_tmp
			 )) a 
			 left outer join 
			 {DIM_CONTRACT} b on upper(a.contract_id) = upper(b.contract_id) 
			 left outer join 
			 ( select * from 
			 ( select *,row_number() over (partition by enrollee_id order by null)rn from {FIRMOGRAPHICS_SMB_TBL}
			 ) where rn = 1 ) c on upper(b.enrollee_id) = upper(c.enrollee_id)
			 left outer join 
			 {LVT_PROFILE_TBL} d on upper(a.CRM_CUSTOMER_GUID) = upper(d.user_guid)
			 left outer join 
			 ( select * from 
			 ( select *,row_number() over (partition by domain order by null)rn from {FIRMOGRAPHICS_SMB_TBL}
			 ) where rn = 1 )e 
			 on SUBSTR(UPPER(d.pers_email), INSTR(UPPER(d.pers_email), '@') + 1)=upper(e.domain)
			 left outer join 
			 ( select * from 
			 ( select *,row_number() over (partition by contract_id order by null)rn from {DIM_SEAT_TBL}
			 ) where rn = 1 )f on upper(a.contract_id) = upper(f.contract_id) 
			 left outer join 
			 ( select * from 
			 ( select *,row_number() over (partition by user_guid order by row_update_dttm)rn from {LVT_PROFILE_TBL}
			 ) where rn = 1 )g on upper(f.member_guid) = upper(g.user_guid)
			 left outer join 
			 ( select * from 
			 ( select *,row_number() over (partition by domain order by null)rn from {FIRMOGRAPHICS_SMB_TBL}
			 ) where rn = 1 )h 
			 on SUBSTR(UPPER(g.pers_email), INSTR(UPPER(g.pers_email), '@') + 1)=upper(h.domain)
			 left outer join 
			 ( select * from 
			 ( select *,row_number() over (partition by subscription_account_guid order by null) rn from {FIRMOGRAPHICS_SMB_TBL} 
			 ) where rn = 1 ) i on upper(a.subscription_account_guid)=upper(i.subscription_account_guid)
			 left outer join  
			 ( select * from 
			 ( select *,row_number() over (partition by contract_id order by null)rn from {FIRMOGRAPHICS_SMB_TBL}
			 ) where rn = 1 )j on upper(a.subscription_account_guid)=upper(j.contract_id))) 
             where upper(domain) not in ('GMAIL.COM','YAHOO.COM','HOTMAIL.COM','OUTLOOK.COM','AOL.COM','UNKNOWN') """.format(SRC_TBL=SRC_TBL,DIM_CONTRACT=DIM_CONTRACT,FIRMOGRAPHICS_SMB_TBL=FIRMOGRAPHICS_SMB_TBL,LVT_PROFILE_TBL=LVT_PROFILE_TBL,DIM_SEAT_TBL=DIM_SEAT_TBL,FROM_DT=FROM_DT,TO_DT=TO_DT)) 
             df_rules_non_generic.createOrReplaceTempView("df_rules_non_generic") 
			 #df_rules_domain_org.write.format("parquet").mode("append").saveAsTable("b2b_tmp.b2b_I2T_df_rules_domain_org_240105",overwrite=True)
             
### Union for TM (Team) Purchase Data from Abdashbase_rules Table :
             
             df_rules_domain_org=spark.sql("""
             select * from df_rules_non_generic 
             union all
             select * from df_rules_generic """)
             df_rules_domain_org.createOrReplaceTempView("df_rules_domain_org")
             
             df_prev_rank=spark.sql(""" 
             select  *, 
             case when rk =1 then date_date else previous_purchase_dt_new end as new_previous_purchase_dt_new from
             (
             select domain, org_name, date_date , rk, 
             LAG(df1.date_date) OVER(partition by df1.domain, df1.org_name ORDER BY df1.date_date ) as previous_purchase_dt_new from 
             (select distinct date_date, domain, org_name, dense_rank() over (partition by df1.domain, df1.org_name ORDER BY df1.date_date ) as rk 
             from 
             df_rules_domain_org df1 
             order by date_date) df1) """)
             df_prev_rank.createOrReplaceTempView("df_prev_rank") 
             
             df_rules_domain_base=spark.sql(""" 
             select *,
             case when rn11 = 1 then null
             when previous_purchase_dt_new = date_date then null else previous_purchase_dt_new end as new_purchase_date 
             from 
             (
             select
             df1.*, row_number()  over (partition by df1.domain, df1.org_name ORDER BY df1.date_date ) as rn11,
             sum(net_purchases_arr_cfx) over (partition by df1.domain, df1.org_name, df1.date_date order by df1.date_date) as sum_net_purchase,
             df2.new_previous_purchase_dt_new as previous_purchase_dt_new
             from (select *, dense_rank() over (partition by df1.domain, df1.org_name ORDER BY df1.date_date ) as rk  
             from df_rules_domain_org df1 ) df1 left outer join df_prev_rank df2 
             on (lower(trim(df1.domain)) = lower(trim(df2.domain))
             and lower(trim(df1.org_name)) = lower(trim(df2.org_name)))
             and lower(trim(df1.date_date)) = lower(trim(df2.date_date))
             and lower(trim(df1.rk)) = lower(trim(df2.rk))
             order by date_date
             ) """) 
             df_rules_domain_base.createOrReplaceTempView("df_rules_domain_base") 
             #df_rules_domain_base.write.format("parquet").mode("append").saveAsTable("b2b_tmp.b2b_I2T_df_rules_domain_base_240108",overwrite=True)

# Join For IN Cancellation and TM Purchase for Non_generic Domains :

             df_base_join1_nongeneric=spark.sql(""" select tbl.* from  
			 (select
			 distinct team_df.*,
			 indivual_df.sum_net_cancelled_arr_cfx,
             indivual_df.sum_net_cancelled_subs,
             indivual_df.date_date as IN_cancel_Dt
			 from (select * from df_rules_domain_base where TM_generic_type = 'non_generic') team_df 
             left outer join 
             (select * from df_pivot_domain_base where IN_generic_type = 'non_generic') indivual_df
			 on (lower(trim(team_df.domain)) = lower(trim(indivual_df.domain))
			 and lower(trim(team_df.org_name)) = lower(trim(indivual_df.org_name)))
			 and team_df.date_date >= indivual_df.date_date
             order by team_df.domain, team_df.date_date) tbl """) 
             df_base_join1_nongeneric.createOrReplaceTempView("df_base_join1_nongeneric") 
             #df_base_join.write.format("parquet").mode("append").saveAsTable("b2b_tmp.b2b_I2T_df_base_join_240108",overwrite=True)
             
             df_base_join2_nongeneric=spark.sql(""" select tbl.* from  
			 (select
			 distinct team_df.*,
			 indivual_df.sum_net_cancelled_arr_cfx,
             indivual_df.sum_net_cancelled_subs,
             indivual_df.date_date as IN_cancel_Dt
			 from (select * except (sum_net_cancelled_arr_cfx , sum_net_cancelled_subs, IN_cancel_Dt) from df_base_join1_nongeneric a
             where a.IN_cancel_Dt is null and a.sum_net_cancelled_arr_cfx is null) team_df 
             left outer join (select * from df_pivot_domain_base where IN_generic_type = 'non_generic') indivual_df
			 on lower(trim(team_df.domain)) = lower(trim(indivual_df.domain))
			 and team_df.date_date >= indivual_df.date_date
             order by team_df.domain, team_df.date_date) tbl """) 
             df_base_join2_nongeneric.createOrReplaceTempView("df_base_join2_nongeneric") 
             
             df_base_join_nongeneric=spark.sql(""" 
             select tbl1.* from df_base_join1_nongeneric tbl1 
             where IN_cancel_Dt is not null and sum_net_cancelled_arr_cfx is not null 
             and datediff(date_date,IN_cancel_Dt) >= 0 and datediff(date_date, IN_cancel_Dt) <= 63 
             union all
             select tbl2.* from df_base_join2_nongeneric tbl2 
             where datediff(date_date,IN_cancel_Dt) >= 0 and datediff(date_date, IN_cancel_Dt) <= 63 
             """) 
             df_base_join_nongeneric.createOrReplaceTempView("df_base_join_nongeneric") 
             
# Join For IN Cancellation and TM Purchase for Generic Domains :
             
             df_base_join_generic=spark.sql(""" select tbl.* from  
			 (select
			 distinct team_df.*,
			 indivual_df.sum_net_cancelled_arr_cfx,
			 indivual_df.sum_net_cancelled_subs,
			 indivual_df.date_date as IN_cancel_Dt
			 from (select * from df_rules_domain_base where TM_generic_type = 'generic') team_df 
             inner join (select * from df_pivot_domain_base where IN_generic_type = 'generic') indivual_df
			 on 
			 (lower(trim(team_df.domain)) = lower(trim(indivual_df.domain))
			 and lower(trim(team_df.email)) = lower(trim(indivual_df.email)))
			 and team_df.date_date >= indivual_df.date_date
			 where datediff(team_df.date_date,indivual_df.date_date ) >= 0 
             and datediff(team_df.date_date, indivual_df.date_date ) <= 63 
			 ) tbl """) 
             df_base_join_generic.createOrReplaceTempView("df_base_join_generic") 
             
# Union for Join For IN Cancellation and TM Purchase :
             
             df_base_join=spark.sql(""" 
             select * from  df_base_join_nongeneric
             union all
             select * from  df_base_join_generic
             """) 
             df_base_join.createOrReplaceTempView("df_base_join") 
             
             
# Discarding Cancellation values which are tagged to prior Purchase Events :

             df_cancelled_distribution_base=spark.sql("""  
             select * from
             (
             select * ,
             case 
             when new_purchase_date is null then sum_net_cancelled_arr_cfx 
             when new_purchase_date is not null and datediff(new_purchase_date, in_cancel_dt) >= 0 and rn1<>1 and rk = 1 then 'discard'
             when new_purchase_date is not null and datediff(new_purchase_date, in_cancel_dt) >= 0 and rn1<>1 and rk <> 1 then 0
             when new_purchase_date is not null and datediff(new_purchase_date, in_cancel_dt) >= 0 and rn1=1 then 0
             else sum_net_cancelled_arr_cfx 
             end as new_net_cancel ,
             case 
             when new_purchase_date is null then sum_net_cancelled_subs 
             when new_purchase_date is not null and datediff(new_purchase_date, in_cancel_dt) >= 0 and rn1<>1 and rk = 1 then 'discard'
             when new_purchase_date is not null and datediff(new_purchase_date, in_cancel_dt) >= 0 and rn1<>1 and rk <> 1 then 0
             when new_purchase_date is not null and datediff(new_purchase_date, in_cancel_dt) >= 0 and rn1=1 then 0
             else sum_net_cancelled_subs 
             end as new_net_cancelled_subs 
             from
             (
             select * ,
             row_number() over (partition by domain,org_name,date_date order by in_cancel_dt desc) rn1
             from df_base_join
             order by date_date, IN_cancel_Dt
             )
             ) 
             where new_net_cancel <> 'discard' """) 
             df_cancelled_distribution_base.createOrReplaceTempView("df_cancelled_distribution_base") 
             df_cancelled_distribution_base.persist()
             #df_cancelled_distribution_base.write.format("parquet").mode("append").saveAsTable("b2b_tmp.b2b_I2T_df_cancelled_distribution_base_240108",overwrite=True)
             
# Finding the new net_cancelled_arr_cfx for domain and date_date:
             
             df_cancelled_distribution_net=spark.sql("""  
             select 
             domain
             , org_name
			 , sales_document_item
             , date_date
             , net_purchases_arr_cfx 
             , sum(new_net_cancel) as sum_new_net_cancel
             , sum(new_net_cancelled_subs) as sum_new_net_cancelled_subs
             from
             df_cancelled_distribution_base tbl 
             group by 
             domain
             , org_name
			 , sales_document_item
             , date_date
             , net_purchases_arr_cfx """) 
             df_cancelled_distribution_net.createOrReplaceTempView("df_cancelled_distribution_net")
             #df_cancelled_distribution_net.write.format("parquet").mode("append").saveAsTable("b2b_tmp.b2b_I2T_df_cancelled_distribution_net_240108",overwrite=True)

             df_cancelled_distribution_allCols=spark.sql("""  
             select 
			 tbl1.sales_document_item
			 , CREATED_BY
			 , CRM_CUSTOMER_GUID
			 , ENTITLEMENT_TYPE
			 , tbl1.date_date
			 , cc_phone_vs_web
			 , GEO
			 , MARKET_AREA
			 , MARKET_SEGMENT
			 , offer_type_description
			 , PRODUCT_CONFIG
			 , product_config_description
			 , PRODUCT_NAME
			 , product_name_description
			 , PROMO_TYPE
			 , PROMOTION
			 , Region
			 , SALES_DOCUMENT
			 , DYLAN_ORDER_NUMBER
			 , STYPE
			 , SUBS_OFFER
			 , SUBSCRIPTION_ACCOUNT_GUID
			 , VIP_CONTRACT
			 , addl_purchase_diff
			 , addl_purchase_same
			 , gross_cancel_arr_cfx
			 , gross_new_arr_cfx
			 , gross_new_subs
			 , init_purchase_arr_cfx
			 , migrated_from_arr_cfx
			 , migrated_to_arr_cfx
			 , net_cancelled_arr_cfx
			 , net_new_arr_cfx
			 , net_new_subs
			 , tbl1.net_purchases_arr_cfx
			 , reactivated_arr_cfx
			 , returns_arr_cfx
			 , ecc_customer_id
			 , SALES_DISTRICT
			 , contract_start_date_veda
			 , contract_end_date_veda
			 , contract_id
			 , renewal_from_arr_cfx
			 , renewal_to_arr_cfx
			 , projected_dme_gtm_segment
			 , gtm_acct_segment
			 , route_to_market
			 , cc_segment
			 , net_new_arr
			 , Sales_Center
			 , AgentGeo
			 , RepName
			 , RepLdap
			 , con_RepLdap
			 , RepTSM
			 , RepManger
			 , RepTeam
			 , cross_team_agent
			 , cross_sell_team
			 , Flag
			 , AgentMapFlag
			 , Txns
			 , SFDC_opportunity_id
			 , SFDC_opportunity_created_date
			 , SFDC_closed_date
			 , SFDC_email
			 , SFDC_ecc_salesordernumber
			 , SFDC_campaignid
			 , SFDC_name
			 , SFDC_cum_campaignid
			 , SFDC_min_date
			 , SFDC_min_fiscal_yr_and_qtr_desc
			 , SFDC_min_fiscal_yr_and_wk_desc
			 , SFDC_Flag
			 , Bob_Flag
			 , TeamKey
			 , gross_new_arr_cfx_unclaimed
			 , net_purchases_arr_cfx_unclaimed
			 , unclaimed_ldap
			 , unclaimed_team
			 , unclaimed_TSM
			 , unclaimed_Manager
			 , agent_max_quarter
			 , agent_curr_qtr
			 , created_curr_qtr
			 , agent_TSM_Ldap
			 , created_TSM_Ldap
			 , agent_max_quarter_TSM
			 , created_max_quarter_TSM
			 , agent_TSM_team
			 , created_TSM_team
			 , agent_TSM_team_key
			 , created_TSM_team_key
			 , agent_TSM_flag
			 , created_TSM_flag
			 , ABD_Flag
			 , agent_ABD_Flag
			 , TSM_ABD_Flag
			 , TSM_agent_ABD_Flag
			 , gross_cancellations
			 , net_cancelled_subs
			 , migrated_from
			 , migrated_to
			 , renewal_from
			 , renewal_to
			 , Custom_Flag
			 , bob_ABD_flag
			 , lat_ABD_flag
			 , same_wk_cncl_flag
			 , cross_team_agent_name
			 , cross_sell_team_key
			 , net_value_usd
			 , acct_name
			 , customer_email_domain
			 , payment_method
			 , net_purchase_agg
			 , cancel_date_list
			 , sum_cancel_arr
			 , net_purchase_arr_incremental
			 , event
			 , FISCAL_YR_AND_QTR_DESC
			 , FISCAL_YR_AND_WK_DESC
			 , tbl1.domain
			 , tbl1.org_name
             , new_purchase_date
			 , sum_net_purchase
			 , sum_new_net_cancel
             , sum_new_net_cancelled_subs
             , tbl1.rk
             , email
             , TM_generic_type
             from
             df_cancelled_distribution_base tbl1 inner join df_cancelled_distribution_net tbl2
             on lower(trim(tbl1.domain)) = lower(trim(tbl2.domain))
			 and lower(trim(tbl1.org_name)) = lower(trim(tbl2.org_name))
             and lower(trim(tbl1.date_date)) = lower(trim(tbl2.date_date))
              """) 
             df_cancelled_distribution_allCols.createOrReplaceTempView("df_cancelled_distribution_allCols")
             #df_cancelled_distribution_allCols.write.format("parquet").mode("append").saveAsTable("b2b_tmp.b2b_I2T_df_cancelled_distribution_allCols_240108",overwrite=True)

#Finding the Distribution Ratio of net_cancelled_arr_cfx based on the net_purchases_arr_cfx:

             df_cancelled_distribution_ratio=spark.sql(""" 
             select *,
             (sum_new_net_cancel)*(ratio) as final_cancel_amt,
             (sum_new_net_cancelled_subs)*(ratio) as final_cancelled_subs_amt
             from
             (
             select *,
             net_purchases_arr_cfx/sum_net_purchase as ratio
             from
             (
             select distinct * from df_cancelled_distribution_allCols 
             )) """)
             df_cancelled_distribution_ratio.createOrReplaceTempView("df_cancelled_distribution_ratio")
             #df_cancelled_distribution_ratio.write.format("parquet").mode("append").saveAsTable("b2b_tmp.b2b_I2T_df_cancelled_distribution_ratio_240108",overwrite=True)

# Adding Cancelled Contract_Id and Cancellation Date from Pivot Table for Non_generic Domains:

             df_cancelled_distribution_contract_dt1_nongeneric=spark.sql(""" 
             select team_df.*, 
             indivual_df.contract_id as IN_Contract_id, 
             indivual_df.date_date as IN_Cancel_Dt 
             from (select * from df_cancelled_distribution_ratio where TM_generic_type='non_generic') team_df 
             left join (select * from df_pivot_domain_base where IN_generic_type='non_generic' ) indivual_df
             on (lower(trim(team_df.domain)) = lower(trim(indivual_df.domain))
             and lower(trim(team_df.org_name)) = lower(trim(indivual_df.org_name)))
			 and team_df.date_date >= indivual_df.date_date 
             order by team_df.domain, team_df.date_date, indivual_df.date_date """)
             df_cancelled_distribution_contract_dt1_nongeneric.createOrReplaceTempView("df_cancelled_distribution_contract_dt1_nongeneric") 
             df_cancelled_distribution_contract_dt1_nongeneric.persist()
             #df_cancelled_distribution_contract_dt.write.format("parquet").mode("append").saveAsTable("b2b_tmp.b2b_I2T_df_cancelled_distribution_contract_dt_240108",overwrite=True)

             df_cancelled_distribution_contract_dt2_nongeneric=spark.sql(""" 
             select team_df.*, 
             indivual_df.contract_id as IN_Contract_id, 
             indivual_df.date_date as IN_Cancel_Dt 
             from (select * except (IN_Contract_id, IN_Cancel_Dt) from df_cancelled_distribution_contract_dt1_nongeneric a
             where a.IN_Contract_id is null and a.IN_Cancel_Dt is null) team_df 
             left join (select * from df_pivot_domain_base where IN_generic_type='non_generic' ) indivual_df
             on lower(trim(team_df.domain)) = lower(trim(indivual_df.domain))
			 and team_df.date_date >= indivual_df.date_date
             order by team_df.domain, team_df.date_date, indivual_df.date_date """)
             df_cancelled_distribution_contract_dt2_nongeneric.createOrReplaceTempView("df_cancelled_distribution_contract_dt2_nongeneric") 
             
             df_cancelled_distribution_contract_dt_nongeneric=spark.sql(""" 
             select tbl1.* from df_cancelled_distribution_contract_dt1_nongeneric tbl1              
             where IN_Contract_id is not null and IN_Cancel_Dt is not null
             and datediff(date_date, IN_Cancel_Dt) >= 0 and datediff(date_date, IN_Cancel_Dt) <= 63 
             union all 
             select tbl2.* from df_cancelled_distribution_contract_dt2_nongeneric tbl2
             where datediff(date_date, IN_Cancel_Dt) >= 0 and datediff(date_date, IN_Cancel_Dt) <= 63 """)
             df_cancelled_distribution_contract_dt_nongeneric.createOrReplaceTempView("df_cancelled_distribution_contract_dt_nongeneric") 
             df_cancelled_distribution_contract_dt_nongeneric.persist()
             
# Adding Cancelled Contract_Id and Cancellation Date from Pivot Table for Generic Domains:
             
             df_cancelled_distribution_contract_dt1_generic=spark.sql(""" 
             select team_df.*, 
             indivual_df.contract_id as IN_Contract_id, 
             indivual_df.date_date as IN_Cancel_Dt 
             from (select * from df_cancelled_distribution_ratio where TM_generic_type='generic') team_df 
             left join (select * from df_pivot_domain_base where IN_generic_type='generic' ) indivual_df
             on (lower(trim(team_df.domain)) = lower(trim(indivual_df.domain))
             and lower(trim(team_df.org_name)) = lower(trim(indivual_df.org_name)))
			 and team_df.date_date >= indivual_df.date_date 
             order by team_df.domain, team_df.date_date, indivual_df.date_date """)
             df_cancelled_distribution_contract_dt1_generic.createOrReplaceTempView("df_cancelled_distribution_contract_dt1_generic") 
             df_cancelled_distribution_contract_dt1_generic.persist()
             
# Union of Adding Cancelled Contract_Id and Cancellation Date from Pivot Table: 
             
             df_cancelled_distribution_contract_dt=spark.sql(""" 
             select * from df_cancelled_distribution_contract_dt_nongeneric
             union all
             select * from df_cancelled_distribution_contract_dt1_generic
             """)
             df_cancelled_distribution_contract_dt.createOrReplaceTempView("df_cancelled_distribution_contract_dt") 
             df_cancelled_distribution_contract_dt.persist()
             
# Adding Array of Cancelled Contract_Id and Cancellation Date :
             
             df_b2b_i2t_prefinal_union=spark.sql(""" 
             select *,'NPD' as unionevent, IN_Cancel_Dt as new_IN_Cancel_Dt, IN_Contract_id as new_IN_Contract_id  from df_cancelled_distribution_contract_dt where new_purchase_date is null
			 union all
			 select *,'RK_1' as unionevent, IN_Cancel_Dt as new_IN_Cancel_Dt,IN_Contract_id as new_IN_Contract_id from df_cancelled_distribution_contract_dt where new_purchase_date is not null and rk = 1 and (new_purchase_date < IN_Cancel_Dt)
             union all 
			 select *, 
			 case when unionevent = 'rk_not_1' and (date_date > IN_Cancel_Dt) and final_cancel_amt = 0 then ''
			 when unionevent = 'rk_not_1' and (new_purchase_date <= IN_Cancel_Dt) then IN_Cancel_Dt
			 when unionevent = 'rk_not_1' and (new_purchase_date > IN_Cancel_Dt) then ''
			 else IN_Cancel_Dt
			 end as new_IN_Cancel_Dt,
			 case when unionevent = 'rk_not_1' and (date_date > IN_Cancel_Dt) and final_cancel_amt = 0 then ''
			 when unionevent = 'rk_not_1' and (new_purchase_date <= IN_Cancel_Dt) then IN_Contract_id
			 when unionevent = 'rk_not_1' and (new_purchase_date > IN_Cancel_Dt) then ''
			 else IN_Contract_id
			 end as new_IN_Contract_id
			 from
			 (
             select *,'rk_not_1' as unionevent from df_cancelled_distribution_contract_dt where new_purchase_date is not null and rk <> 1 
             ) """) 
             df_b2b_i2t_prefinal_union.createOrReplaceTempView("df_b2b_i2t_prefinal_union") 
             df_b2b_i2t_prefinal_union.persist()

             df_b2b_i2t_concat=spark.sql(""" 
             select *, 
             concat('IN_Contract_id - ',new_IN_Contract_id,',','IN_Cancel_Dt - ',new_IN_Cancel_Dt) as concat_list 
             from df_b2b_i2t_prefinal_union """)
             df_b2b_i2t_concat.createOrReplaceTempView("df_b2b_i2t_concat") 
             
             df_b2b_i2t_final=spark.sql("""
             select distinct sales_document_item
			 , CREATED_BY
			 , CRM_CUSTOMER_GUID
			 , ENTITLEMENT_TYPE
			 , date_date
			 , cc_phone_vs_web
			 , GEO
			 , MARKET_AREA
			 , MARKET_SEGMENT
			 , offer_type_description
			 , PRODUCT_CONFIG
			 , product_config_description
			 , PRODUCT_NAME
			 , product_name_description
			 , PROMO_TYPE
			 , PROMOTION
			 , Region
			 , SALES_DOCUMENT
			 , DYLAN_ORDER_NUMBER
			 , STYPE
			 , SUBS_OFFER
			 , SUBSCRIPTION_ACCOUNT_GUID
			 , VIP_CONTRACT
			 , addl_purchase_diff
			 , addl_purchase_same
			 , gross_cancel_arr_cfx
			 , gross_new_arr_cfx
			 , gross_new_subs
			 , init_purchase_arr_cfx
			 , migrated_from_arr_cfx
			 , migrated_to_arr_cfx
			 , net_cancelled_arr_cfx
			 , net_new_arr_cfx
			 , net_new_subs
			 , net_purchases_arr_cfx
			 , reactivated_arr_cfx
			 , returns_arr_cfx
			 , ecc_customer_id
			 , SALES_DISTRICT
			 , contract_start_date_veda
			 , contract_end_date_veda
			 , contract_id
			 , renewal_from_arr_cfx
			 , renewal_to_arr_cfx
			 , projected_dme_gtm_segment
			 , gtm_acct_segment
			 , route_to_market
			 , cc_segment
			 , net_new_arr
			 , Sales_Center
			 , AgentGeo
			 , RepName
			 , RepLdap
			 , con_RepLdap
			 , RepTSM
			 , RepManger
			 , RepTeam
			 , cross_team_agent
			 , cross_sell_team
			 , Flag
			 , AgentMapFlag
			 , Txns
			 , SFDC_opportunity_id
			 , SFDC_opportunity_created_date
			 , SFDC_closed_date
			 , SFDC_email
			 , SFDC_ecc_salesordernumber
			 , SFDC_campaignid
			 , SFDC_name
			 , SFDC_cum_campaignid
			 , SFDC_min_date
			 , SFDC_min_fiscal_yr_and_qtr_desc
			 , SFDC_min_fiscal_yr_and_wk_desc
			 , SFDC_Flag
			 , Bob_Flag
			 , TeamKey
			 , gross_new_arr_cfx_unclaimed
			 , net_purchases_arr_cfx_unclaimed
			 , unclaimed_ldap
			 , unclaimed_team
			 , unclaimed_TSM
			 , unclaimed_Manager
			 , agent_max_quarter
			 , agent_curr_qtr
			 , created_curr_qtr
			 , agent_TSM_Ldap
			 , created_TSM_Ldap
			 , agent_max_quarter_TSM
			 , created_max_quarter_TSM
			 , agent_TSM_team
			 , created_TSM_team
			 , agent_TSM_team_key
			 , created_TSM_team_key
			 , agent_TSM_flag
			 , created_TSM_flag
			 , ABD_Flag
			 , agent_ABD_Flag
			 , TSM_ABD_Flag
			 , TSM_agent_ABD_Flag
			 , gross_cancellations
			 , net_cancelled_subs
			 , migrated_from
			 , migrated_to
			 , renewal_from
			 , renewal_to
			 , Custom_Flag
			 , bob_ABD_flag
			 , lat_ABD_flag
			 , same_wk_cncl_flag
			 , cross_team_agent_name
			 , cross_sell_team_key
			 , net_value_usd
			 , acct_name
			 , customer_email_domain
			 , payment_method
			 , net_purchase_agg
			 , cancel_date_list
			 , sum_cancel_arr
			 , net_purchase_arr_incremental
			 , domain
			 , org_name 
             , email
             , TM_generic_type as Generic_vs_buisness
			 , final_cancel_amt
             , final_cancelled_subs_amt
			 --, collect_set( named_struct("contract_id", new_IN_Contract_id,"IN_Cancel_Dt", new_IN_Cancel_Dt)) as IN_Cancelled_List 
             , collect_list(concat_list) as IN_Cancelled_List 
             , 'I2T_ADJUSTMENTS' as event
			 , FISCAL_YR_AND_QTR_DESC
			 , FISCAL_YR_AND_WK_DESC
			 from df_b2b_i2t_concat
             group by 
             sales_document_item
			 , CREATED_BY
			 , CRM_CUSTOMER_GUID
			 , ENTITLEMENT_TYPE
			 , date_date
			 , cc_phone_vs_web
			 , GEO
			 , MARKET_AREA
			 , MARKET_SEGMENT
			 , offer_type_description
			 , PRODUCT_CONFIG
			 , product_config_description
			 , PRODUCT_NAME
			 , product_name_description
			 , PROMO_TYPE
			 , PROMOTION
			 , Region
			 , SALES_DOCUMENT
			 , DYLAN_ORDER_NUMBER
			 , STYPE
			 , SUBS_OFFER
			 , SUBSCRIPTION_ACCOUNT_GUID
			 , VIP_CONTRACT
			 , addl_purchase_diff
			 , addl_purchase_same
			 , gross_cancel_arr_cfx
			 , gross_new_arr_cfx
			 , gross_new_subs
			 , init_purchase_arr_cfx
			 , migrated_from_arr_cfx
			 , migrated_to_arr_cfx
			 , net_cancelled_arr_cfx
			 , net_new_arr_cfx
			 , net_new_subs
			 , net_purchases_arr_cfx
			 , reactivated_arr_cfx
			 , returns_arr_cfx
			 , ecc_customer_id
			 , SALES_DISTRICT
			 , contract_start_date_veda
			 , contract_end_date_veda
			 , contract_id
			 , renewal_from_arr_cfx
			 , renewal_to_arr_cfx
			 , projected_dme_gtm_segment
			 , gtm_acct_segment
			 , route_to_market
			 , cc_segment
			 , net_new_arr
			 , Sales_Center
			 , AgentGeo
			 , RepName
			 , RepLdap
			 , con_RepLdap
			 , RepTSM
			 , RepManger
			 , RepTeam
			 , cross_team_agent
			 , cross_sell_team
			 , Flag
			 , AgentMapFlag
			 , Txns
			 , SFDC_opportunity_id
			 , SFDC_opportunity_created_date
			 , SFDC_closed_date
			 , SFDC_email
			 , SFDC_ecc_salesordernumber
			 , SFDC_campaignid
			 , SFDC_name
			 , SFDC_cum_campaignid
			 , SFDC_min_date
			 , SFDC_min_fiscal_yr_and_qtr_desc
			 , SFDC_min_fiscal_yr_and_wk_desc
			 , SFDC_Flag
			 , Bob_Flag
			 , TeamKey
			 , gross_new_arr_cfx_unclaimed
			 , net_purchases_arr_cfx_unclaimed
			 , unclaimed_ldap
			 , unclaimed_team
			 , unclaimed_TSM
			 , unclaimed_Manager
			 , agent_max_quarter
			 , agent_curr_qtr
			 , created_curr_qtr
			 , agent_TSM_Ldap
			 , created_TSM_Ldap
			 , agent_max_quarter_TSM
			 , created_max_quarter_TSM
			 , agent_TSM_team
			 , created_TSM_team
			 , agent_TSM_team_key
			 , created_TSM_team_key
			 , agent_TSM_flag
			 , created_TSM_flag
			 , ABD_Flag
			 , agent_ABD_Flag
			 , TSM_ABD_Flag
			 , TSM_agent_ABD_Flag
			 , gross_cancellations
			 , net_cancelled_subs
			 , migrated_from
			 , migrated_to
			 , renewal_from
			 , renewal_to
			 , Custom_Flag
			 , bob_ABD_flag
			 , lat_ABD_flag
			 , same_wk_cncl_flag
			 , cross_team_agent_name
			 , cross_sell_team_key
			 , net_value_usd
			 , acct_name
			 , customer_email_domain
			 , payment_method
			 , net_purchase_agg
			 , cancel_date_list
			 , sum_cancel_arr
			 , net_purchase_arr_incremental
			 , domain
			 , org_name 
             		 , email
             		 , TM_generic_type
			 , final_cancel_amt
             		 , final_cancelled_subs_amt
             		 , event
			 , FISCAL_YR_AND_QTR_DESC
			 , FISCAL_YR_AND_WK_DESC
             order by date_date """)
             df_b2b_i2t_final.createOrReplaceTempView("df_b2b_i2t_final") 
             
             df_b2b_i2t_final.repartition("event","FISCAL_YR_AND_QTR_DESC","FISCAL_YR_AND_WK_DESC").write.format("parquet").mode("append").insertInto("%s"%TGT_TBL,overwrite=True)

             try:
                 dbutils.notebook.exit("SUCCESS")   
             except Exception as e:                 
                 print("exception:",e)
        except Exception as e:
             dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()
