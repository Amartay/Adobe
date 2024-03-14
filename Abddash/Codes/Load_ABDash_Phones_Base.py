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
             spark.sql('set spark.sql.parquet.enableVectorizedReader=false')
             spark.sql('set spark.sql.sources.partitionOverwriteMode=dynamic')
             spark.sql("set spark.databricks.sql.files.prorateMaxPartitionBytes.enabled=false")
             spark.sql("set spark.sql.adaptive.coalescePartitions.enabled=false")
             spark.sql("set spark.sql.adaptive.enabled=false")
             spark.sql("set spark.sql.shuffle.partitions=1000")

             dbutils.widgets.text("Custom_Settings", "")
             dbutils.widgets.text("archive_path", "")
             dbutils.widgets.text("file_type", "")
             dbutils.widgets.text("TO_DT", "")
             dbutils.widgets.text("FROM_DT", "")
             dbutils.widgets.text("TGT_TBL", "")
             dbutils.widgets.text("TGT_TBL_RULES", "")
             dbutils.widgets.text("PIVOT_TBL", "")
             dbutils.widgets.text("BOB_TBL", "")
             dbutils.widgets.text("LATAM_TBL", "")
             dbutils.widgets.text("HENDRIX_TBL", "")
             dbutils.widgets.text("AGENT_TBL", "")
             dbutils.widgets.text("DATE_TBL", "")
             dbutils.widgets.text("SEAT_TBL", "")
             dbutils.widgets.text("LIC_TBL", "")
             dbutils.widgets.text("OPPORTUNITY_TBL", "")
             dbutils.widgets.text("USER_TBL", "")
             dbutils.widgets.text("LEAD_TBL", "")
             dbutils.widgets.text("CAMPAIGN_TBL", "")
             dbutils.widgets.text("TEAM_TBL", "")


             Settings = dbutils.widgets.get("Custom_Settings")
             archive_path = dbutils.widgets.get("archive_path")
             file_type = dbutils.widgets.get("file_type")
             TO_DT = dbutils.widgets.get("TO_DT")
             FROM_DT = dbutils.widgets.get("FROM_DT")
             TGT_TBL = dbutils.widgets.get("TGT_TBL")
             TGT_TBL_RULES = dbutils.widgets.get("TGT_TBL_RULES")
             PIVOT_TBL = dbutils.widgets.get("PIVOT_TBL")
             BOB_TBL = dbutils.widgets.get("BOB_TBL")
             LATAM_TBL = dbutils.widgets.get("LATAM_TBL")
             HENDRIX_TBL = dbutils.widgets.get("HENDRIX_TBL")
             AGENT_TBL = dbutils.widgets.get("AGENT_TBL")
             DATE_TBL = dbutils.widgets.get("DATE_TBL")
             SEAT_TBL = dbutils.widgets.get("SEAT_TBL")
             LIC_TBL = dbutils.widgets.get("LIC_TBL")
             OPPORTUNITY_TBL = dbutils.widgets.get("OPPORTUNITY_TBL")
             USER_TBL = dbutils.widgets.get("USER_TBL")
             LEAD_TBL = dbutils.widgets.get("LEAD_TBL")
             CAMPAIGN_TBL = dbutils.widgets.get("CAMPAIGN_TBL")
             TEAM_TBL = dbutils.widgets.get("TEAM_TBL")




             Set_list = Settings.split(',')
             if len(Set_list)>0:
                 for i in Set_list:
                     if i != "":
                         print("spark.sql(+i+)")
                         spark.sql("""{i}""".format(i=i))
             
             #FETCH CURRENT QTR

             df_curr_qtr = spark.sql("""select fiscal_yr_and_qtr_desc from {DATE_TBL} where date_date = {TO_DT}""".format(FROM_DT=FROM_DT,TO_DT=TO_DT,DATE_TBL=DATE_TBL))
             curr_qtr=df_curr_qtr.first()['fiscal_yr_and_qtr_desc']


             #AGENT

             df_agent = spark.sql(""" 
             select * from (select *, row_number() over (partition by trim(upper(Ldap)) order by Quarter desc) rn from {AGENT_TBL} where Quarter = '{curr_qtr}') where rn=1
             """.format(FROM_DT=FROM_DT,TO_DT=TO_DT,curr_qtr=curr_qtr,AGENT_TBL=AGENT_TBL))
             df_agent.createOrReplaceTempView("df_agent")

             #BOB
             df_bob = spark.sql(""" 
             select * from {BOB_TBL} where Fiscal_yr_and_qtr_desc = '{curr_qtr}'""".format(FROM_DT=FROM_DT,TO_DT=TO_DT,curr_qtr=curr_qtr,BOB_TBL=BOB_TBL))
             df_bob.createOrReplaceTempView("df_bob")

             #ABD code
             pre_pivotbase = spark.sql("""           
             select *,
             case when (
             gross_new_arr_cfx > 0
             or init_purchase_arr_cfx > 0
             or net_purchases_arr_cfx > 0
             ) 
             and 
             (gross_cancel_arr_cfx > 0
             or migrated_from_arr_cfx > 0
             or migrated_to_arr_cfx > 0
             or migrated_from > 0
             or migrated_to > 0
             or net_cancelled_arr_cfx > 0
             or reactivated_arr_cfx > 0
             or renewal_from_arr_cfx > 0
             or renewal_to_arr_cfx > 0
             or renewal_from > 0
             or renewal_to > 0
             or returns_arr_cfx > 0) then 'Y' else 'N' end as New_Row_Flag from (
             select Pvt.*,
             --OcfContractID.contract_id 
             case when Pvt.cc_phone_vs_web = 'VIP-PHONE' then Pvt.vip_contract
             else coalesce(OcfContractID.contract_id,seatContractID.contract_id )
             end as contract_id,
             case when net_value_usd <> 0 and ((upper(CREATED_BY) != 'WAS' and upper(CREATED_BY) != 'SGECCORD' and upper(CREATED_BY) != 'B2BCPRD' and upper(CREATED_BY) != 'B2BPRD') and upper(product_name_description) like '%STOCK%' and upper(cc_segment) = 'STOCK') then 'Y'
             when net_value_usd <> 0 then 'N'
             else 'NA' 
             end as stock_cred_pack
             from
             (
                (
                    select * from {PIVOT_TBL}
                            where source_type IN ('IN','TM','OVERAGE') and event_source not in ('SNAPSHOT','F2P')  and date_key between regexp_replace({FROM_DT},'-','') and regexp_replace({TO_DT},'-','')
                            and    (    cc_failure_cancel_arr_cfx <> 0
                                        OR explicit_cancel_arr_cfx <> 0
                        OR partial_cancel_cc_failure_arr_cfx <> 0
                        OR partial_cancel_explicit_arr_cfx <> 0
                        OR gross_cancel_arr_cfx <> 0
                        OR gross_new_arr_cfx <> 0
                        OR init_purchase_arr_cfx <> 0
                        OR migrated_from_arr_cfx <> 0
                        OR migrated_to_arr_cfx <> 0
                        OR net_cancelled_arr_cfx <> 0
                        OR net_purchases_arr_cfx <> 0
                        OR reactivated_arr_cfx <> 0
                        OR renewal_from_arr_cfx <> 0
                        OR renewal_to_arr_cfx <> 0
                        OR resubscription_arr_cfx <> 0
                        OR explicit_returns_arr_cfx <> 0
                        OR cc_failure_returns_arr_cfx <> 0
                        OR returns_arr_cfx <> 0
                        OR net_value_usd <> 0)
                ) Pvt 
                left outer join
                (
                    Select upper(subscription_account_guid) subs_guid, 
                    upper(contract_id) contract_id 
                    from {LIC_TBL}
                    where contract_type ='DIRECT_ORGANIZATION'
                    --and contract_id is not null
                    and contract_id != ''
                    and subscription_account_guid is not null
                    and subscription_account_guid != ''
                    group by upper(subscription_account_guid), upper(contract_id)
                )OcfContractID 
                on Pvt.subscription_account_guid  = OcfContractID.subs_guid
                left outer join
                (
                    Select upper(subscription_account_guid) subs_guid, 
                    upper(contract_id) contract_id 
                    from {SEAT_TBL}
                    where contract_id is not null
                    and contract_id != ''
                    and subscription_account_guid is not null
                    and subscription_account_guid != ''
                    group by upper(subscription_account_guid), upper(contract_id)
                )seatContractID 
                on Pvt.subscription_account_guid  = seatContractID.subs_guid
             ) 
             ) where stock_cred_pack != 'N'
             """.format(FROM_DT=FROM_DT,TO_DT=TO_DT,PIVOT_TBL=PIVOT_TBL,LIC_TBL=LIC_TBL,SEAT_TBL=SEAT_TBL)) 

             pre_pivotbase.createOrReplaceTempView("pre_pivotbase")
             pre_pivotbase.persist()


             df_cancel = spark.sql("""
select
source_type
,event_source
,stype
,date_key
,date_date
,crm_customer_guid
,sales_document
,sales_document_item
,fiscal_yr_desc
,fiscal_yr_and_qtr_desc
,fiscal_yr_and_per_desc
,fiscal_yr_and_wk_desc
,complementary_flag
,route_to_market
,entitlement_type
,entitlement_period_desc
,market_segment
,sales_district
,sales_district_description
,market_area_description
,market_area
,geo
,geo_description
,region
,region_description
,promotion
,promo_type
,cs_version
,product_config
,product_config_description
,product_name
,product_name_description
,prod_group_hier
,prod_group_hier_description
,prod_platform
,product_platform_desc
,manual_adjust_flag
,adjustment_reason
,posa_techghvendor_description
,cc_phone_vs_web
,paid_type
,arr_vs_units
,sub_product
,stock_flag
,material_metric
,bill_plan_type
,bill_plan_period
,offer_type
,offer_type_desc
,cancellation_date
,cc_overage_type
,winback_flag
,winback_counter
,material_number
,document_currency
,posa_type
,source_create_date
,subscription_account_guid
,contract_start_date_veda
,contract_end_date_veda
,vip_contract
,dylan_order_number
,payment_method
,ecc_customer_id
,store_ui
,store_ui_source
,exclusion_flag
,created_by
,sales_document_type
,subscription_category
,offer_id
,ref_crm_customer_guid
,org_id
,market_place
,delegate_purchase_flag
,trial_contract_id
,created_date_item
,p_entitlement_period_desc
,p_geo
,p_geo_description
,p_market_segment
,p_material_number
,p_product_name
,p_product_name_description
,p_promotion
,unit_price_lc
,unit_price_usd
,ba_unit_price_lc
,ea_unit_price_lc
,active_count
,gross_new_subs_f2p_d2p
,offer_subs_qty
,target_quantity
,local_curr_net_value
,'0' as net_value_usd
,'0' as addl_purchase_diff
,'0' as addl_purchase_same
,compl_cancel_after_1mo_cc_failure
,compl_cancel_after_1mo_explicit
,compl_cancel_cc_failure
,compl_cancel_explicit
,compl_cancel_in_1mo_cc_failure
,compl_cancel_in_1mo_explicit
,first_purchase
,gross_cancellations
,'0' as gross_new_subs
,init_purchase
,migr_from_after_1mo
,migr_from_within_1mo
,migr_to_after_1mo
,migr_to_within_1mo
,migrated_from
,migrated_subs
,migrated_to
,net_cancelled_subs
,'0' as net_new_subs
,net_purchases
,partial_cancel_cc_failure
,partial_cancel_explicit
,partial_returns_diff_cc_failure
,partial_returns_diff_explicit
,partial_returns_same_cc_failure
,partial_returns_same_explicit
,reactivated_subs
,reactivations_diff
,reactivations_same
,renewal_from
,renewal_to
,resubscription
,returns_explicit
,returns_never_paid
,returns
,fwk_begin_active
,fwk_end_active
,fmnth_begin_active
,fmnth_end_active
,fqtr_begin_active
,fqtr_end_active
,fyr_begin_active
,fyr_end_active
,exchange_rate
,factor
,current_exchange_rate
,current_factor
,cloud_detail
,subs_offer
,cc_segment
,cc_segment_offer
,target_config
,product_category
,stock_type
,etl_change_ts
,fwk_begin_stock_bob
,fwk_end_stock_bob
,net_new_stock_bob
,explicit_cancellations
,explicit_returns
,cc_failure_cancellations
,cc_failure_returns
,dim_sub_exist_flag
,first_purchase_arr_lc
,gross_cancel_arr_lc
,gross_new_arr_lc
,init_purchase_arr_lc
,migrated_from_arr_lc
,migrated_to_arr_lc
,net_cancelled_arr_lc
,net_purchases_arr_lc
,reactivated_arr_lc
,renewal_from_arr_lc
,renewal_to_arr_lc
,resubscription_arr_lc
,returns_arr_lc
,explicit_cancel_arr_lc
,explicit_returns_arr_lc
,cc_failure_cancel_arr_lc
,cc_failure_returns_arr_lc
,fwk_begin_arr_lc
,fwk_end_arr_lc
,fmnth_begin_arr_lc
,fmnth_end_arr_lc
,fqtr_begin_arr_lc
,fqtr_end_arr_lc
,fyr_begin_arr_lc
,fyr_end_arr_lc
,partial_cancel_explicit_arr_lc
,partial_cancel_cc_failure_arr_lc
,compl_cancel_in_1mo_explicit_arr_lc
,compl_cancel_in_1mo_cc_failure_arr_lc
,compl_cancel_after_1mo_explicit_arr_lc
,compl_cancel_after_1mo_cc_failure_arr_lc
,reactivations_diff_arr_lc
,reactivations_same_arr_lc
,customer_email_domain
,acct_name
,gtm_acct_segment_lvl1
,business_segment_lvl1
,gtm_acct_segment
,P_IDS_REPORTING
,purchase_add_rtm
,Agent_ID
,Migration_Path
,migration_type
,billing_frequency
,end_user_name
,ech_sub_id
,ech_parent_id
,ech_sub_name
,ech_parent_name
,ech_child_key
,projected_dme_gtm_segment
,trial_extension
,IDS_REPORTING
,LIVE_DME_GTM_SEGMENT
,first_purchase_arr
,gross_cancel_arr
,gross_new_arr
,init_purchase_arr
,migrated_from_arr
,migrated_to_arr
,net_cancelled_arr
,net_purchases_arr
,reactivated_arr
,renewal_from_arr
,renewal_to_arr
,resubscription_arr
,returns_arr
,explicit_cancel_arr
,explicit_returns_arr
,cc_failure_cancel_arr
,cc_failure_returns_arr
,partial_cancel_explicit_arr
,partial_cancel_cc_failure_arr
,compl_cancel_in_1mo_explicit_arr
,compl_cancel_in_1mo_cc_failure_arr
,compl_cancel_after_1mo_explicit_arr
,compl_cancel_after_1mo_cc_failure_arr
,reactivations_diff_arr
,reactivations_same_arr
,fwk_begin_arr
,fwk_end_arr
,fmnth_begin_arr
,fmnth_end_arr
,fqtr_begin_arr
,fqtr_end_arr
,fyr_begin_arr
,fyr_end_arr
,first_purchase_arr_cfx
,gross_cancel_arr_cfx
,'0' as gross_new_arr_cfx
,'0' as init_purchase_arr_cfx
,migrated_from_arr_cfx
,migrated_to_arr_cfx
,net_cancelled_arr_cfx
,'0' as net_purchases_arr_cfx
,reactivated_arr_cfx
,renewal_from_arr_cfx
,renewal_to_arr_cfx
,resubscription_arr_cfx
,returns_arr_cfx
,explicit_cancel_arr_cfx
,explicit_returns_arr_cfx
,cc_failure_cancel_arr_cfx
,cc_failure_returns_arr_cfx
,partial_cancel_explicit_arr_cfx
,partial_cancel_cc_failure_arr_cfx
,compl_cancel_in_1mo_explicit_arr_cfx
,compl_cancel_in_1mo_cc_failure_arr_cfx
,compl_cancel_after_1mo_explicit_arr_cfx
,compl_cancel_after_1mo_cc_failure_arr_cfx
,reactivations_diff_arr_cfx
,reactivations_same_arr_cfx
,fwk_begin_arr_cfx
,fwk_end_arr_cfx
,fmnth_begin_arr_cfx
,fmnth_end_arr_cfx
,fqtr_begin_arr_cfx
,fqtr_end_arr_cfx
,fyr_begin_arr_cfx
,fyr_end_arr_cfx
,net_new_arr_lc
,net_new_arr
,'0' as net_new_arr_cfx
,fwk_begin_avg_rpu_lc
,fwk_end_avg_rpu_lc
,fmnth_begin_avg_rpu_lc
,fmnth_end_avg_rpu_lc
,fqtr_begin_avg_rpu_lc
,fqtr_end_avg_rpu_lc
,fyr_begin_avg_rpu_lc
,fyr_end_avg_rpu_lc
,fwk_begin_avg_rpu
,fwk_end_avg_rpu
,fmnth_begin_avg_rpu
,fmnth_end_avg_rpu
,fqtr_begin_avg_rpu
,fqtr_end_avg_rpu
,fyr_begin_avg_rpu
,fyr_end_avg_rpu
,fwk_begin_avg_rpu_cfx
,fwk_end_avg_rpu_cfx
,fmnth_begin_avg_rpu_cfx
,fmnth_end_avg_rpu_cfx
,fqtr_begin_avg_rpu_cfx
,fqtr_end_avg_rpu_cfx
,fyr_begin_avg_rpu_cfx
,fyr_end_avg_rpu_cfx
,total_stock_bob
,total_net_new_bob
,cloud_type_filter
,business_segment
,contract_id
,stock_cred_pack
,New_Row_Flag
from pre_pivotbase 
where New_row_flag='Y' 
""")

             df_cancel.createOrReplaceTempView("df_cancel")

             df_purchase = spark.sql("""
select
source_type
,event_source
,stype
,date_key
,date_date
,crm_customer_guid
,sales_document
,sales_document_item
,fiscal_yr_desc
,fiscal_yr_and_qtr_desc
,fiscal_yr_and_per_desc
,fiscal_yr_and_wk_desc
,complementary_flag
,route_to_market
,entitlement_type
,entitlement_period_desc
,market_segment
,sales_district
,sales_district_description
,market_area_description
,market_area
,geo
,geo_description
,region
,region_description
,promotion
,promo_type
,cs_version
,product_config
,product_config_description
,product_name
,product_name_description
,prod_group_hier
,prod_group_hier_description
,prod_platform
,product_platform_desc
,manual_adjust_flag
,adjustment_reason
,posa_techghvendor_description
,cc_phone_vs_web
,paid_type
,arr_vs_units
,sub_product
,stock_flag
,material_metric
,bill_plan_type
,bill_plan_period
,offer_type
,offer_type_desc
,cancellation_date
,cc_overage_type
,winback_flag
,winback_counter
,material_number
,document_currency
,posa_type
,source_create_date
,subscription_account_guid
,contract_start_date_veda
,contract_end_date_veda
,vip_contract
,dylan_order_number
,payment_method
,ecc_customer_id
,store_ui
,store_ui_source
,exclusion_flag
,created_by
,sales_document_type
,subscription_category
,offer_id
,ref_crm_customer_guid
,org_id
,market_place
,delegate_purchase_flag
,trial_contract_id
,created_date_item
,p_entitlement_period_desc
,p_geo
,p_geo_description
,p_market_segment
,p_material_number
,p_product_name
,p_product_name_description
,p_promotion
,unit_price_lc
,unit_price_usd
,ba_unit_price_lc
,ea_unit_price_lc
,active_count
,gross_new_subs_f2p_d2p
,offer_subs_qty
,target_quantity
,local_curr_net_value
,net_value_usd
,addl_purchase_diff
,addl_purchase_same
,compl_cancel_after_1mo_cc_failure
,compl_cancel_after_1mo_explicit
,compl_cancel_cc_failure
,compl_cancel_explicit
,compl_cancel_in_1mo_cc_failure
,compl_cancel_in_1mo_explicit
,first_purchase
,'0' as gross_cancellations
,gross_new_subs
,init_purchase
,migr_from_after_1mo
,migr_from_within_1mo
,migr_to_after_1mo
,migr_to_within_1mo
,'0' as migrated_from
,migrated_subs
,'0' as migrated_to
,'0' as net_cancelled_subs
,net_new_subs
,net_purchases
,partial_cancel_cc_failure
,partial_cancel_explicit
,partial_returns_diff_cc_failure
,partial_returns_diff_explicit
,partial_returns_same_cc_failure
,partial_returns_same_explicit
,reactivated_subs
,reactivations_diff
,reactivations_same
,'0' as renewal_from
,'0' as renewal_to
,resubscription
,returns_explicit
,returns_never_paid
,returns
,fwk_begin_active
,fwk_end_active
,fmnth_begin_active
,fmnth_end_active
,fqtr_begin_active
,fqtr_end_active
,fyr_begin_active
,fyr_end_active
,exchange_rate
,factor
,current_exchange_rate
,current_factor
,cloud_detail
,subs_offer
,cc_segment
,cc_segment_offer
,target_config
,product_category
,stock_type
,etl_change_ts
,fwk_begin_stock_bob
,fwk_end_stock_bob
,net_new_stock_bob
,explicit_cancellations
,explicit_returns
,cc_failure_cancellations
,cc_failure_returns
,dim_sub_exist_flag
,first_purchase_arr_lc
,gross_cancel_arr_lc
,gross_new_arr_lc
,init_purchase_arr_lc
,migrated_from_arr_lc
,migrated_to_arr_lc
,net_cancelled_arr_lc
,net_purchases_arr_lc
,reactivated_arr_lc
,renewal_from_arr_lc
,renewal_to_arr_lc
,resubscription_arr_lc
,returns_arr_lc
,explicit_cancel_arr_lc
,explicit_returns_arr_lc
,cc_failure_cancel_arr_lc
,cc_failure_returns_arr_lc
,fwk_begin_arr_lc
,fwk_end_arr_lc
,fmnth_begin_arr_lc
,fmnth_end_arr_lc
,fqtr_begin_arr_lc
,fqtr_end_arr_lc
,fyr_begin_arr_lc
,fyr_end_arr_lc
,partial_cancel_explicit_arr_lc
,partial_cancel_cc_failure_arr_lc
,compl_cancel_in_1mo_explicit_arr_lc
,compl_cancel_in_1mo_cc_failure_arr_lc
,compl_cancel_after_1mo_explicit_arr_lc
,compl_cancel_after_1mo_cc_failure_arr_lc
,reactivations_diff_arr_lc
,reactivations_same_arr_lc
,customer_email_domain
,acct_name
,gtm_acct_segment_lvl1
,business_segment_lvl1
,gtm_acct_segment
,P_IDS_REPORTING
,purchase_add_rtm
,Agent_ID
,Migration_Path
,migration_type
,billing_frequency
,end_user_name
,ech_sub_id
,ech_parent_id
,ech_sub_name
,ech_parent_name
,ech_child_key
,projected_dme_gtm_segment
,trial_extension
,IDS_REPORTING
,LIVE_DME_GTM_SEGMENT
,first_purchase_arr
,gross_cancel_arr
,gross_new_arr
,init_purchase_arr
,migrated_from_arr
,migrated_to_arr
,net_cancelled_arr
,net_purchases_arr
,reactivated_arr
,renewal_from_arr
,renewal_to_arr
,resubscription_arr
,returns_arr
,explicit_cancel_arr
,explicit_returns_arr
,cc_failure_cancel_arr
,cc_failure_returns_arr
,partial_cancel_explicit_arr
,partial_cancel_cc_failure_arr
,compl_cancel_in_1mo_explicit_arr
,compl_cancel_in_1mo_cc_failure_arr
,compl_cancel_after_1mo_explicit_arr
,compl_cancel_after_1mo_cc_failure_arr
,reactivations_diff_arr
,reactivations_same_arr
,fwk_begin_arr
,fwk_end_arr
,fmnth_begin_arr
,fmnth_end_arr
,fqtr_begin_arr
,fqtr_end_arr
,fyr_begin_arr
,fyr_end_arr
,first_purchase_arr_cfx
,'0' as gross_cancel_arr_cfx
,gross_new_arr_cfx
,init_purchase_arr_cfx
,'0' as migrated_from_arr_cfx
,'0' as migrated_to_arr_cfx
,'0' as net_cancelled_arr_cfx
,net_purchases_arr_cfx
,'0' as reactivated_arr_cfx
,'0' as renewal_from_arr_cfx
,'0' as renewal_to_arr_cfx
,resubscription_arr_cfx
,'0' as returns_arr_cfx
,explicit_cancel_arr_cfx
,explicit_returns_arr_cfx
,cc_failure_cancel_arr_cfx
,cc_failure_returns_arr_cfx
,partial_cancel_explicit_arr_cfx
,partial_cancel_cc_failure_arr_cfx
,compl_cancel_in_1mo_explicit_arr_cfx
,compl_cancel_in_1mo_cc_failure_arr_cfx
,compl_cancel_after_1mo_explicit_arr_cfx
,compl_cancel_after_1mo_cc_failure_arr_cfx
,reactivations_diff_arr_cfx
,reactivations_same_arr_cfx
,fwk_begin_arr_cfx
,fwk_end_arr_cfx
,fmnth_begin_arr_cfx
,fmnth_end_arr_cfx
,fqtr_begin_arr_cfx
,fqtr_end_arr_cfx
,fyr_begin_arr_cfx
,fyr_end_arr_cfx
,net_new_arr_lc
,net_new_arr
,net_new_arr_cfx
,fwk_begin_avg_rpu_lc
,fwk_end_avg_rpu_lc
,fmnth_begin_avg_rpu_lc
,fmnth_end_avg_rpu_lc
,fqtr_begin_avg_rpu_lc
,fqtr_end_avg_rpu_lc
,fyr_begin_avg_rpu_lc
,fyr_end_avg_rpu_lc
,fwk_begin_avg_rpu
,fwk_end_avg_rpu
,fmnth_begin_avg_rpu
,fmnth_end_avg_rpu
,fqtr_begin_avg_rpu
,fqtr_end_avg_rpu
,fyr_begin_avg_rpu
,fyr_end_avg_rpu
,fwk_begin_avg_rpu_cfx
,fwk_end_avg_rpu_cfx
,fmnth_begin_avg_rpu_cfx
,fmnth_end_avg_rpu_cfx
,fqtr_begin_avg_rpu_cfx
,fqtr_end_avg_rpu_cfx
,fyr_begin_avg_rpu_cfx
,fyr_end_avg_rpu_cfx
,total_stock_bob
,total_net_new_bob
,cloud_type_filter
,business_segment
,contract_id
,stock_cred_pack
,New_Row_Flag
from pre_pivotbase 
where New_row_flag='Y' 
""")

             df_purchase.createOrReplaceTempView("df_purchase")



             pivotbase_tmp = spark.sql(""" 
                        select * from pre_pivotbase where New_row_flag='N'
                        union all 
                        select * from df_cancel
                        union all
                        select * from df_purchase 
                        """)

             pivotbase_tmp.createOrReplaceTempView("pivotbase_tmp") 
             
             spark.sql(""" drop table if exists b2b_tmp.abdashbase_pivotbase """)
 
             spark.sql(""" create table b2b_tmp.abdashbase_pivotbase stored as parquet select * from pivotbase_tmp""")

             pivotbase = spark.sql(""" 
                        select * from b2b_tmp.abdashbase_pivotbase
                        """)

             pivotbase.createOrReplaceTempView("pivotbase") 
             pivotbase.persist()


             Hendrix_all_events = spark.sql("""
             select Hendrix.* ,
             DT.fiscal_yr_and_wk_desc,
             AgentRoaster.Sales_Center as Sale_Center,
             AgentRoaster.Geo,
             AgentRoaster.Rep_Name as Rep_Name,
             AgentRoaster.Ldap,
             AgentRoaster.TSM,
             AgentRoaster.Manager,
             AgentRoaster.Final_Team  from     
             (
                (
                    select distinct 
                    case
                    when event_type = 'CHANGE_PLAN'
                    then trim(split(note, ':')[1])
                    else upper(order_number)
                    end as order_number,
                    inserteddate,
                    upper(agent_ldapid) agent_ldapid,
                    event_type,
					coalesce(product_code,'NULL') as product_code
                    from {HENDRIX_TBL}
                    where
                    inserteddate between date_add({FROM_DT},-7 ) and {TO_DT}
                    and order_number is not null
                    and agent_ldapid is not null
                    and trim(order_number)!=''
                    and upper(agent_ldapid) not in ('','IN_PRODUCT_SUPPORT1')
                ) Hendrix
                inner join 
                (
                    select * from df_agent where Sales_Center <> 'NULL' and Quarter = '{curr_qtr}'
                ) AgentRoaster
                on upper(Hendrix.agent_ldapid) = upper(AgentRoaster.Ldap)
                left outer join
                (
                        select distinct fiscal_yr_and_wk_desc,date_date
                        from {DATE_TBL}
                ) DT
                on Hendrix.inserteddate = DT.date_date
             ) 
             """.format(FROM_DT=FROM_DT,TO_DT=TO_DT,curr_qtr=curr_qtr,HENDRIX_TBL=HENDRIX_TBL,DATE_TBL=DATE_TBL))

             Hendrix_all_events.createOrReplaceTempView("Hendrix_all_events")

             Change_plan_cancel = spark.sql("""
             select Hendrix.* ,
             DT.fiscal_yr_and_wk_desc,
             AgentRoaster.Sales_Center as Sale_Center,
             AgentRoaster.Geo,
             AgentRoaster.Rep_Name as Rep_Name,
             AgentRoaster.Ldap,
             AgentRoaster.TSM,
             AgentRoaster.Manager,
             AgentRoaster.Final_Team  from      
             (
                (   
                    select distinct 
                    order_number,
                    inserteddate,
                    upper(agent_ldapid) agent_ldapid,
                    'CHANGE_PLAN_CANCEL' as event_type,
                    coalesce(product_code,'NULL') as product_code
                    from {HENDRIX_TBL}
                    where
                    inserteddate between date_add({FROM_DT},-7 ) and {TO_DT}
                    and order_number is not null
                    and agent_ldapid is not null
                    and trim(order_number)!=''
                    and upper(agent_ldapid) not in ('','IN_PRODUCT_SUPPORT1')
                    and event_type = 'CHANGE_PLAN'
                ) Hendrix
                inner join 
                (
                    select * from df_agent where Quarter = '{curr_qtr}'
                ) AgentRoaster
                on upper(Hendrix.agent_ldapid) = upper(AgentRoaster.Ldap)
                left outer join
                (
                        select fiscal_yr_and_wk_desc,date_date
                        from {DATE_TBL}
                        where date_date between {FROM_DT} and {TO_DT}
                         
                ) DT
                on Hendrix.inserteddate = DT.date_date
                ) 
             """.format(FROM_DT=FROM_DT,TO_DT=TO_DT,curr_qtr=curr_qtr,HENDRIX_TBL=HENDRIX_TBL,DATE_TBL=DATE_TBL))
             Change_plan_cancel.createOrReplaceTempView("Change_plan_cancel")


             Hendrixbase_tmp =spark.sql("""
                            select * from Hendrix_all_events 
                            union
                            select * from Change_plan_cancel
                        """)
             Hendrixbase_tmp.createOrReplaceTempView("Hendrixbase_tmp")
             Hendrixbase = Hendrixbase_tmp.withColumn("product_code_split",explode(split(Hendrixbase_tmp.product_code,',')))
             Hendrixbase.createOrReplaceTempView("Hendrixbase")

             Hendrixbase.persist()

             #Hendrixbase.write.mode('overwrite').saveAsTable("wwphoneops.HendrixbaseFinal")   

             #final
             Hendrix_join=spark.sql("""
                       select coalesce(sales.sales_document,dylan.sales_document) as sales_document,
                        x.order_number as hendrix_salesdocument,
                              coalesce(sales.sales_document_item,dylan.sales_document_item) as sales_document_item,
                              coalesce(sales.fiscal_yr_and_wk_desc,dylan.fiscal_yr_and_wk_desc ) as fiscal_yr_and_wk_desc,
                              coalesce(sales.fiscal_yr_and_qtr_desc,dylan.fiscal_yr_and_qtr_desc ) as fiscal_yr_and_qtr_desc,
                --collect_list(x.`Ldap`)[0] as RepLdap,
                case when x.inserteddate = sales.date_date then x.Ldap
                when x.inserteddate = dylan.date_date then x.Ldap
                else collect_list(x.`Ldap`)[0] 
                end as RepLdap,
                x.inserteddate,
                --coalesce(sales.date_date,dylan.date_date,x.inserteddate) as pivot_date,
                x.fiscal_yr_and_wk_desc as Hendrix_fiscal_yr_and_wk_desc,
                x.product_code_split as product_code,
                'Hendrix' as Flag from
             (select case when event_type IN ('CANCEL_PLAN_NOW', 'CCT_CANCEL_CONTRACT', 'CCT_CANCEL_LICENSE', 'CCT_CANCEL_SEAT', 'CCT_DR_CANCEL_CONTRACT', 'CCT_DR_CANCEL_SEAT', 'STOCKS_RETURN','CHANGE_PLAN_CANCEL') then 'CANCEL' else 'OTHERS' end as join_flag , * from Hendrixbase event   ) x
                left outer join
                (select * , case when nvl(gross_cancellations,0)>0 OR nvl(migrated_from,0)>0 then 'CANCEL' else 'OTHERS' end as join_flag from      pivotbase ) sales
                on upper(lpad(x.order_number,10,'0')) = sales.sales_document
                and x.join_flag=sales.join_flag
                and (sales.fiscal_yr_and_wk_desc = x.fiscal_yr_and_wk_desc or (x.inserteddate BETWEEN date_sub(sales.date_date,7) AND sales.date_date))
                

                left outer join (select * , case when nvl(gross_cancellations,0)>0 OR nvl(migrated_from,0)>0 then 'CANCEL' else 'OTHERS' end as join_flag from pivotbase  ) dylan
                on x.order_number = dylan.dylan_order_number
                and x.join_flag=dylan.join_flag
                and (dylan.fiscal_yr_and_wk_desc = x.fiscal_yr_and_wk_desc or (x.inserteddate BETWEEN date_sub(dylan.date_date,7) AND dylan.date_date))
                group by
                x.inserteddate,
                coalesce(sales.sales_document,dylan.sales_document) ,
                              coalesce(sales.sales_document_item,dylan.sales_document_item) ,
                              coalesce(sales.fiscal_yr_and_wk_desc,dylan.fiscal_yr_and_wk_desc ) ,
                              coalesce(sales.fiscal_yr_and_qtr_desc,dylan.fiscal_yr_and_qtr_desc ),
                              x.order_number,
                              sales.date_date,
                              dylan.date_date,
                              x.Ldap, 
							  --coalesce(sales.date_date,dylan.date_date,x.inserteddate),
         x.fiscal_yr_and_wk_desc,
            x.product_code_split""")


             Hendrix_join.createOrReplaceTempView("Hendrix_join")

             Hendrix_join_prd=spark.sql("""
            select coalesce(sales.sales_document,dylan.sales_document) as sales_document,
            x.order_number as hendrix_salesdocument,
            coalesce(sales.sales_document_item,dylan.sales_document_item) as sales_document_item,
            coalesce(sales.fiscal_yr_and_wk_desc,dylan.fiscal_yr_and_wk_desc ) as fiscal_yr_and_wk_desc,
            coalesce(sales.fiscal_yr_and_qtr_desc,dylan.fiscal_yr_and_qtr_desc ) as fiscal_yr_and_qtr_desc,
            --collect_list(x.`Ldap`)[0] as RepLdap,
            case when x.inserteddate = sales.date_date then x.Ldap
            when x.inserteddate = dylan.date_date then x.Ldap
            else collect_list(x.`Ldap`)[0] 
            end as RepLdap,
            x.inserteddate,
            --coalesce(sales.date_date,dylan.date_date,x.inserteddate) as pivot_date,
            x.fiscal_yr_and_wk_desc as Hendrix_fiscal_yr_and_wk_desc,
            x.product_code_split as product_code,
            'Hendrix' as Flag from
            (select case when event_type IN ('CANCEL_PLAN_NOW', 'CCT_CANCEL_CONTRACT', 'CCT_CANCEL_LICENSE', 'CCT_CANCEL_SEAT', 'CCT_DR_CANCEL_CONTRACT', 'CCT_DR_CANCEL_SEAT', 'STOCKS_RETURN','CHANGE_PLAN_CANCEL') then 'CANCEL' else 'OTHERS' end as join_flag , * from Hendrixbase event   ) x
            left outer join
            (select * , case when nvl(gross_cancellations,0)>0 OR nvl(migrated_from,0)>0 then 'CANCEL' else 'OTHERS' end as join_flag from      pivotbase ) sales
            on upper(lpad(x.order_number,10,'0')) = sales.sales_document
            and x.join_flag=sales.join_flag
            and (sales.fiscal_yr_and_wk_desc = x.fiscal_yr_and_wk_desc or (x.inserteddate BETWEEN date_sub(sales.date_date,7) AND sales.date_date))
            and x.product_code_split = sales.product_name

            left outer join (select * , case when nvl(gross_cancellations,0)>0 OR nvl(migrated_from,0)>0 then 'CANCEL' else 'OTHERS' end as join_flag from pivotbase  ) dylan
            on x.order_number = dylan.dylan_order_number
            and x.join_flag=dylan.join_flag
            and (dylan.fiscal_yr_and_wk_desc = x.fiscal_yr_and_wk_desc or (x.inserteddate BETWEEN date_sub(dylan.date_date,7) AND dylan.date_date))
            and x.product_code_split = dylan.product_name

            group by
            x.inserteddate,
            coalesce(sales.sales_document,dylan.sales_document) ,
            coalesce(sales.sales_document_item,dylan.sales_document_item) ,
            coalesce(sales.fiscal_yr_and_wk_desc,dylan.fiscal_yr_and_wk_desc ) ,
            coalesce(sales.fiscal_yr_and_qtr_desc,dylan.fiscal_yr_and_qtr_desc ),
            x.order_number,
            sales.date_date,
            dylan.date_date,
            x.Ldap, 
            --coalesce(sales.date_date,dylan.date_date,x.inserteddate),
            x.fiscal_yr_and_wk_desc,
            x.product_code_split  """)
             
             Hendrix_join_prd.createOrReplaceTempView("Hendrix_join_prd")
            
             Hendrix_join_prd_14=spark.sql("""
            select coalesce(sales.sales_document,dylan.sales_document) as sales_document,
            x.order_number as hendrix_salesdocument,
            coalesce(sales.sales_document_item,dylan.sales_document_item) as sales_document_item,
            coalesce(sales.fiscal_yr_and_wk_desc,dylan.fiscal_yr_and_wk_desc ) as fiscal_yr_and_wk_desc,
            coalesce(sales.fiscal_yr_and_qtr_desc,dylan.fiscal_yr_and_qtr_desc ) as fiscal_yr_and_qtr_desc,
            --collect_list(x.`Ldap`)[0] as RepLdap,
            case when x.inserteddate = sales.date_date then x.Ldap
            when x.inserteddate = dylan.date_date then x.Ldap
            else collect_list(x.`Ldap`)[0] 
            end as RepLdap,
            x.inserteddate,
            --coalesce(sales.date_date,dylan.date_date,x.inserteddate) as pivot_date,
            x.fiscal_yr_and_wk_desc as Hendrix_fiscal_yr_and_wk_desc,
            x.product_code_split as product_code,
            'Hendrix' as Flag from
            (select case when event_type IN ('CANCEL_PLAN_NOW', 'CCT_CANCEL_CONTRACT', 'CCT_CANCEL_LICENSE', 'CCT_CANCEL_SEAT', 'CCT_DR_CANCEL_CONTRACT', 'CCT_DR_CANCEL_SEAT', 'STOCKS_RETURN','CHANGE_PLAN_CANCEL') then 'CANCEL' else 'OTHERS' end as join_flag , * from Hendrixbase event   ) x
            left outer join
            (select * , case when nvl(gross_cancellations,0)>0 OR nvl(migrated_from,0)>0 then 'CANCEL' else 'OTHERS' end as join_flag from      pivotbase ) sales
            on upper(lpad(x.order_number,10,'0')) = sales.sales_document
            and x.join_flag=sales.join_flag
            and (sales.fiscal_yr_and_wk_desc = x.fiscal_yr_and_wk_desc or (x.inserteddate BETWEEN date_sub(sales.date_date,14) AND sales.date_date))
            and x.product_code_split = sales.product_name

            left outer join (select * , case when nvl(gross_cancellations,0)>0 OR nvl(migrated_from,0)>0 then 'CANCEL' else 'OTHERS' end as join_flag from pivotbase  ) dylan
            on x.order_number = dylan.dylan_order_number
            and x.join_flag=dylan.join_flag
            and (dylan.fiscal_yr_and_wk_desc = x.fiscal_yr_and_wk_desc or (x.inserteddate BETWEEN date_sub(dylan.date_date,14) AND dylan.date_date))
            and x.product_code_split = dylan.product_name

            group by
            x.inserteddate,
            coalesce(sales.sales_document,dylan.sales_document) ,
            coalesce(sales.sales_document_item,dylan.sales_document_item) ,
            coalesce(sales.fiscal_yr_and_wk_desc,dylan.fiscal_yr_and_wk_desc ) ,
            coalesce(sales.fiscal_yr_and_qtr_desc,dylan.fiscal_yr_and_qtr_desc ),
            x.order_number,
            sales.date_date,
            dylan.date_date,
            x.Ldap, 
            --coalesce(sales.date_date,dylan.date_date,x.inserteddate),
            x.fiscal_yr_and_wk_desc,
            x.product_code_split  """)


             Hendrix_join_prd_14.createOrReplaceTempView("Hendrix_join_prd_14")


             #Hendrix_join.write.mode('overwrite').saveAsTable("wwphoneops.HendrixJoinTest9")


             AOP = spark.sql(""" select 
                sales_document_item as sales_document_item,
                created_by as CREATED_BY,
                crm_customer_guid as CRM_CUSTOMER_GUID,
                entitlement_type as ENTITLEMENT_TYPE,
                date_date as date_date,
                fiscal_yr_and_qtr_desc as FISCAL_YR_AND_QTR_DESC,
                cc_phone_vs_web as cc_phone_vs_web,
                geo as GEO,
                market_area as MARKET_AREA,
                market_segment as MARKET_SEGMENT,
                offer_type_desc as offer_type_description,
                product_config as PRODUCT_CONFIG,
                product_config_description as product_config_description,
                product_name as PRODUCT_NAME,
                product_name_description as product_name_description,
                promo_type as PROMO_TYPE,
                promotion as PROMOTION,
                region as Region,
                sales_document as SALES_DOCUMENT,
                dylan_order_number as DYLAN_ORDER_NUMBER,
                stype as STYPE,
                subs_offer as SUBS_OFFER,
                subscription_account_guid as SUBSCRIPTION_ACCOUNT_GUID,
                vip_contract as VIP_CONTRACT,
                addl_purchase_diff as addl_purchase_diff,
                addl_purchase_same as addl_purchase_same,
                gross_cancel_arr_cfx as gross_cancel_arr_cfx,
                gross_new_arr_cfx as gross_new_arr_cfx,
                gross_new_subs as gross_new_subs,
                init_purchase_arr_cfx as init_purchase_arr_cfx,
                migrated_from_arr_cfx as migrated_from_arr_cfx,
                migrated_to_arr_cfx as migrated_to_arr_cfx,
                net_cancelled_arr_cfx as net_cancelled_arr_cfx,
                net_new_arr_cfx as net_new_arr_cfx,
                net_new_subs as net_new_subs,
                net_purchases_arr_cfx as net_purchases_arr_cfx,
                reactivated_arr_cfx as reactivated_arr_cfx,
                returns_arr_cfx as returns_arr_cfx,
                ecc_customer_id as ecc_customer_id,
                sales_district as SALES_DISTRICT,
                contract_start_date_veda as contract_start_date_veda,
                contract_end_date_veda as contract_end_date_veda,
                contract_id as contract_id,
                renewal_from_arr_cfx as renewal_from_arr_cfx,
                renewal_to_arr_cfx as renewal_to_arr_cfx,
                PROJECTED_DME_GTM_SEGMENT as projected_dme_gtm_segment, 
                case when upper(gtm_acct_segment) in ('CORP T1','CORP T2') then 'MID-MARKET'
                else gtm_acct_segment
                end as gtm_acct_segment,
                route_to_market as route_to_market,
                cc_segment as cc_segment,
                net_new_arr as net_new_arr,
                'None' as Sales_Center,
                'None' as AgentGeo,
                'None' as RepName,
                created_by as RepLdap,
                'None' as RepTSM,
                'None' as RepManger,
                'None' as RepTeam,
                'AOP' as Flag,
                gross_cancellations as gross_cancellations,
                net_cancelled_subs as net_cancelled_subs,
                migrated_from as migrated_from,
                migrated_to as migrated_to,
                renewal_from as renewal_from,
                renewal_to as renewal_to,
                net_value_usd as net_value_usd,
                stock_cred_pack as stock_cred_pack,
                acct_name as acct_name,
                customer_email_domain as customer_email_domain,
                payment_method as payment_method,
                'VIP' as Hendrix_Join_Flag,
                fiscal_yr_and_wk_desc as FISCAL_YR_AND_WK_DESC
                from pivotbase  
                where cc_phone_vs_web = 'VIP-PHONE' """)

             AOP.createOrReplaceTempView("AOP")

             BOB = spark.sql("""
                select 
                Pvt.sales_document_item as sales_document_item,
                Pvt.created_by as CREATED_BY,
                Pvt.crm_customer_guid as CRM_CUSTOMER_GUID,
                Pvt.entitlement_type as ENTITLEMENT_TYPE,
                Pvt.date_date as date_date,
                Pvt.fiscal_yr_and_qtr_desc as FISCAL_YR_AND_QTR_DESC,
                Pvt.fiscal_yr_and_wk_desc as FISCAL_YR_AND_WK_DESC,
                cc_phone_vs_web as cc_phone_vs_web,
                Pvt.geo as GEO,
                Pvt.market_area as MARKET_AREA,
                Pvt.market_segment as MARKET_SEGMENT,
                Pvt.offer_type_desc as offer_type_description,
                Pvt.product_config as PRODUCT_CONFIG,
                Pvt.product_config_description as product_config_description,
                Pvt.product_name as PRODUCT_NAME,
                Pvt.product_name_description as product_name_description,
                Pvt.promo_type as PROMO_TYPE,
                Pvt.promotion as PROMOTION,
                Pvt.region as Region,
                Pvt.sales_document as SALES_DOCUMENT,
                Pvt.dylan_order_number as DYLAN_ORDER_NUMBER,
                Pvt.stype as STYPE,
                Pvt.subs_offer as SUBS_OFFER,
                Pvt.subscription_account_guid as SUBSCRIPTION_ACCOUNT_GUID,
                Pvt.vip_contract as VIP_CONTRACT,
                Pvt.addl_purchase_diff as addl_purchase_diff,
                Pvt.addl_purchase_same as addl_purchase_same,
                Pvt.gross_cancel_arr_cfx as gross_cancel_arr_cfx,
                Pvt.gross_new_arr_cfx as gross_new_arr_cfx,
                Pvt.gross_new_subs as gross_new_subs,
                Pvt.init_purchase_arr_cfx as init_purchase_arr_cfx,
                Pvt.migrated_from_arr_cfx as migrated_from_arr_cfx,
                Pvt.migrated_to_arr_cfx as migrated_to_arr_cfx,
                Pvt.net_cancelled_arr_cfx as net_cancelled_arr_cfx,
                Pvt.net_new_arr_cfx as net_new_arr_cfx,
                Pvt.net_new_subs as net_new_subs,
                Pvt.net_purchases_arr_cfx as net_purchases_arr_cfx,
                Pvt.reactivated_arr_cfx as reactivated_arr_cfx,
                Pvt.returns_arr_cfx as returns_arr_cfx,
                Pvt.ecc_customer_id as ecc_customer_id,
                Pvt.sales_district as SALES_DISTRICT,
                Pvt.contract_start_date_veda as contract_start_date_veda,
                Pvt.contract_end_date_veda as contract_end_date_veda,
                Pvt.contract_id as contract_id,
                Pvt.renewal_from_arr_cfx as renewal_from_arr_cfx,
                Pvt.renewal_to_arr_cfx as renewal_to_arr_cfx,	
                Pvt.PROJECTED_DME_GTM_SEGMENT as projected_dme_gtm_segment, 
                Pvt.gtm_acct_segment as gtm_acct_segment,
                Pvt.route_to_market as route_to_market,
                Pvt.cc_segment as cc_segment,
                Pvt.net_new_arr as net_new_arr,
                'None' as Sales_Center,
                'None' as AgentGeo,
                'None' as RepName,
                'None' as RepLdap,
                'None' as RepTSM,
                'None' as RepManger,
                'None' as RepTeam,
                'BoB-Web' as Flag,
                Pvt.gross_cancellations as gross_cancellations,
                Pvt.net_cancelled_subs as net_cancelled_subs,
                Pvt.migrated_from as migrated_from,
                Pvt.migrated_to as migrated_to,
                Pvt.renewal_from as renewal_from,
                Pvt.renewal_to as renewal_to,
                Pvt.net_value_usd as net_value_usd,
                Pvt.stock_cred_pack as stock_cred_pack,
                Pvt.acct_name as acct_name,
                Pvt.customer_email_domain as customer_email_domain,
                Pvt.payment_method as payment_method
                from
                (
                                    
                     select * from pivotbase where cc_phone_vs_web = 'WEB'
     
                )Pvt
                """)    

             BOB.createOrReplaceTempView("BOB")


             Phones = spark.sql(""" select 
                sales_document_item as sales_document_item,
                created_by as CREATED_BY,
                crm_customer_guid as CRM_CUSTOMER_GUID,
                entitlement_type as ENTITLEMENT_TYPE,
                date_date as date_date,
                fiscal_yr_and_qtr_desc as FISCAL_YR_AND_QTR_DESC,
                fiscal_yr_and_wk_desc as FISCAL_YR_AND_WK_DESC,
                cc_phone_vs_web as cc_phone_vs_web,
                geo as GEO,
                market_area as MARKET_AREA,
                market_segment as MARKET_SEGMENT,
                offer_type_desc as offer_type_description,
                product_config as PRODUCT_CONFIG,
                product_config_description as product_config_description,
                product_name as PRODUCT_NAME,
                product_name_description as product_name_description,
                promo_type as PROMO_TYPE,
                promotion as PROMOTION,
                region as Region,
                sales_document as SALES_DOCUMENT,
                dylan_order_number as DYLAN_ORDER_NUMBER,
                stype as STYPE,
                subs_offer as SUBS_OFFER,
                subscription_account_guid as SUBSCRIPTION_ACCOUNT_GUID,
                vip_contract as VIP_CONTRACT,
                addl_purchase_diff as addl_purchase_diff,
                addl_purchase_same as addl_purchase_same,
                gross_cancel_arr_cfx as gross_cancel_arr_cfx,
                gross_new_arr_cfx as gross_new_arr_cfx,
                gross_new_subs as gross_new_subs,
                init_purchase_arr_cfx as init_purchase_arr_cfx,
                migrated_from_arr_cfx as migrated_from_arr_cfx,
                migrated_to_arr_cfx as migrated_to_arr_cfx,
                net_cancelled_arr_cfx as net_cancelled_arr_cfx,
                net_new_arr_cfx as net_new_arr_cfx,
                net_new_subs as net_new_subs,
                net_purchases_arr_cfx as net_purchases_arr_cfx,
                reactivated_arr_cfx as reactivated_arr_cfx,
                returns_arr_cfx as returns_arr_cfx,
                ecc_customer_id as ecc_customer_id,
                sales_district as SALES_DISTRICT,
                contract_start_date_veda as contract_start_date_veda,
                contract_end_date_veda as contract_end_date_veda,
                contract_id as contract_id,
                renewal_from_arr_cfx as renewal_from_arr_cfx,
                renewal_to_arr_cfx as renewal_to_arr_cfx,
                PROJECTED_DME_GTM_SEGMENT as projected_dme_gtm_segment, 
                gtm_acct_segment as gtm_acct_segment,
                route_to_market as route_to_market,
                cc_segment as cc_segment,
                net_new_arr as net_new_arr,
                'None' as Sales_Center,
                'None' as AgentGeo,
                'None' as RepName,
                'None' as RepLdap,
                'None' as RepTSM,
                'None' as RepManger,
                'None' as RepTeam,
                'Phones' as Flag,
                gross_cancellations as gross_cancellations,
                net_cancelled_subs as net_cancelled_subs,
                migrated_from as migrated_from,
                migrated_to as migrated_to,
                renewal_from as renewal_from,
                renewal_to as renewal_to,
                net_value_usd as net_value_usd,
                stock_cred_pack as stock_cred_pack,
                acct_name as acct_name,
                customer_email_domain as customer_email_domain,
                payment_method as payment_method
                from pivotbase  
                where cc_phone_vs_web = 'PHONE' """)

             Phones.createOrReplaceTempView("Phones")

             WebPhone =  spark.sql("""
                      select * from BOB
                        union
                      select * from Phones """)

             WebPhone.createOrReplaceTempView("WebPhone")

             FinalWebPhone = spark.sql("""
                select  
                WebPhone.sales_document_item as sales_document_item,
                WebPhone.CREATED_BY as CREATED_BY,
                WebPhone.CRM_CUSTOMER_GUID as CRM_CUSTOMER_GUID,
                WebPhone.ENTITLEMENT_TYPE as ENTITLEMENT_TYPE,
                WebPhone.date_date as date_date,
                WebPhone.FISCAL_YR_AND_QTR_DESC as FISCAL_YR_AND_QTR_DESC,
                WebPhone.FISCAL_YR_AND_WK_DESC as FISCAL_YR_AND_WK_DESC,
                WebPhone.cc_phone_vs_web as cc_phone_vs_web,
                WebPhone.GEO as GEO,
                WebPhone.MARKET_AREA as MARKET_AREA,
                WebPhone.MARKET_SEGMENT as MARKET_SEGMENT,
                WebPhone.offer_type_description as offer_type_description,
                WebPhone.PRODUCT_CONFIG as PRODUCT_CONFIG,
                WebPhone.product_config_description as product_config_description,
                WebPhone.PRODUCT_NAME as PRODUCT_NAME,
                WebPhone.product_name_description as product_name_description,
                WebPhone.PROMO_TYPE as PROMO_TYPE,
                WebPhone.PROMOTION as PROMOTION,
                WebPhone.Region as Region,
                WebPhone.SALES_DOCUMENT as SALES_DOCUMENT,
                WebPhone.DYLAN_ORDER_NUMBER as DYLAN_ORDER_NUMBER,
                WebPhone.STYPE as STYPE,
                WebPhone.SUBS_OFFER as SUBS_OFFER,
                WebPhone.SUBSCRIPTION_ACCOUNT_GUID as SUBSCRIPTION_ACCOUNT_GUID,
                WebPhone.VIP_CONTRACT as VIP_CONTRACT,
                addl_purchase_diff as addl_purchase_diff,
                addl_purchase_same as addl_purchase_same,
                gross_cancel_arr_cfx as gross_cancel_arr_cfx,
                gross_new_arr_cfx as gross_new_arr_cfx,
                gross_new_subs as gross_new_subs,
                init_purchase_arr_cfx as init_purchase_arr_cfx,
                migrated_from_arr_cfx as migrated_from_arr_cfx,
                migrated_to_arr_cfx as migrated_to_arr_cfx,
                net_cancelled_arr_cfx as net_cancelled_arr_cfx,
                net_new_arr_cfx as net_new_arr_cfx,
                net_new_subs as net_new_subs,
                net_purchases_arr_cfx as net_purchases_arr_cfx,
                reactivated_arr_cfx as reactivated_arr_cfx,
                returns_arr_cfx as returns_arr_cfx,
                WebPhone.ecc_customer_id as ecc_customer_id,
                WebPhone.SALES_DISTRICT as SALES_DISTRICT,
                WebPhone.contract_start_date_veda as contract_start_date_veda,
                WebPhone.contract_end_date_veda as contract_end_date_veda,
                WebPhone.contract_id as contract_id,
                WebPhone.renewal_from_arr_cfx as renewal_from_arr_cfx,
                WebPhone.renewal_to_arr_cfx as renewal_to_arr_cfx,
                WebPhone.projected_dme_gtm_segment as projected_dme_gtm_segment, 
                WebPhone.gtm_acct_segment as gtm_acct_segment,
                WebPhone.route_to_market as route_to_market,
                WebPhone.cc_segment as cc_segment,
                WebPhone.net_new_arr as net_new_arr,
                'None' as Sales_Center,
                'None' as AgentGeo,
                'None' as RepName,
                --coalesce(latam.LDAP,latam_missing.LDAP,Hendrix_join.repLdap) as RepLdap,
                coalesce(latam.LDAP,latam_missing.LDAP,Hendrix_join_pc_dt.repLdap,Hendrix_join_pc.repLdap,Hendrix_join_dt.repLdap,Hendrix_join.repLdap,Hendrix_join_pc_14.repLdap) as RepLdap,
                case when (latam.LDAP is not null or latam_missing.LDAP is not null) then 'Y'
                else 'N'
                end as latam_flag,
                'None' as RepTSM,
                'None' as RepManger,
                'None' as RepTeam,
                WebPhone.Flag as Flag,
                gross_cancellations as gross_cancellations,
                net_cancelled_subs as net_cancelled_subs,
                migrated_from as migrated_from,
                migrated_to as migrated_to,
                renewal_from as renewal_from,
                renewal_to as renewal_to,
                net_value_usd as net_value_usd,
                stock_cred_pack as stock_cred_pack,
                acct_name as acct_name,
                customer_email_domain as customer_email_domain,
                payment_method as payment_method,
                case when latam.LDAP is not null then 'LATAM'
                when latam_missing.LDAP is not null then 'LATAM'
                when Hendrix_join_pc_dt.repLdap is not null then 'Product - Same Date'
                when Hendrix_join_pc.repLdap is not null then 'Product - Same Week'
                when Hendrix_join_dt.repLdap is not null then 'Same Date'
                when Hendrix_join.repLdap is not null then 'Same Week'
                when Hendrix_join_pc_14.repLdap is not null then 'Product_14 - Same Week'
                else 'NA' end as Hendrix_Join_Flag
                from WebPhone 

                left outer join (select sales_document , sales_document_item ,fiscal_yr_and_wk_desc , 
                collect_list(RepLdap)[0] as repLdap,product_code
                from Hendrix_join_prd
                GROUP BY sales_document,sales_document_item,fiscal_yr_and_wk_desc,product_code) Hendrix_join_pc
                on WebPhone.SALES_DOCUMENT = Hendrix_join_pc.sales_document
                and WebPhone.sales_document_item=Hendrix_join_pc.sales_document_item  
                and WebPhone.FISCAL_YR_AND_WK_DESC=Hendrix_join_pc.fiscal_yr_and_wk_desc
                and WebPhone.product_name=Hendrix_join_pc.product_code
                left outer join (select sales_document , sales_document_item ,fiscal_yr_and_wk_desc , inserteddate,
                collect_list(RepLdap)[0] as repLdap,product_code
                from Hendrix_join_prd
                GROUP BY sales_document,sales_document_item,fiscal_yr_and_wk_desc,inserteddate,product_code) Hendrix_join_pc_dt
                on WebPhone.SALES_DOCUMENT = Hendrix_join_pc_dt.sales_document
                and WebPhone.sales_document_item=Hendrix_join_pc_dt.sales_document_item  
                and WebPhone.FISCAL_YR_AND_WK_DESC=Hendrix_join_pc_dt.fiscal_yr_and_wk_desc
                and WebPhone.date_date=Hendrix_join_pc_dt.inserteddate
                and WebPhone.product_name=Hendrix_join_pc_dt.product_code
                
                left outer join (select sales_document , sales_document_item ,fiscal_yr_and_wk_desc , 
                collect_list(RepLdap)[0] as repLdap,product_code
                from Hendrix_join_prd_14
                GROUP BY sales_document,sales_document_item,fiscal_yr_and_wk_desc,product_code) Hendrix_join_pc_14
                on WebPhone.SALES_DOCUMENT = Hendrix_join_pc_14.sales_document
                and WebPhone.sales_document_item=Hendrix_join_pc_14.sales_document_item  
                and WebPhone.FISCAL_YR_AND_WK_DESC=Hendrix_join_pc_14.fiscal_yr_and_wk_desc
                and WebPhone.product_name=Hendrix_join_pc_14.product_code


             left outer join (select sales_document , sales_document_item ,fiscal_yr_and_wk_desc , 
             collect_list(RepLdap)[0] as repLdap
             from Hendrix_join
             GROUP BY sales_document,sales_document_item,fiscal_yr_and_wk_desc) Hendrix_join
             on WebPhone.SALES_DOCUMENT = Hendrix_join.sales_document
             and WebPhone.sales_document_item=Hendrix_join.sales_document_item  
             and WebPhone.FISCAL_YR_AND_WK_DESC=Hendrix_join.fiscal_yr_and_wk_desc
             
             left outer join (select sales_document , sales_document_item ,fiscal_yr_and_wk_desc , inserteddate,
             collect_list(RepLdap)[0] as repLdap
             from Hendrix_join
             GROUP BY sales_document,sales_document_item,fiscal_yr_and_wk_desc,inserteddate) Hendrix_join_dt
             on WebPhone.SALES_DOCUMENT = Hendrix_join_dt.sales_document
             and WebPhone.sales_document_item=Hendrix_join_dt.sales_document_item  
             and WebPhone.FISCAL_YR_AND_WK_DESC=Hendrix_join_dt.fiscal_yr_and_wk_desc
             and WebPhone.date_date=Hendrix_join_dt.inserteddate
             
             left outer join (select  x.*,fiscal_yr_and_wk_desc
             from (select * from {LATAM_TBL} where trim(upper(Sales_Center)) = 'LATAM' and fiscal_yr_and_qtr_desc = (select fiscal_yr_and_qtr_desc from {DATE_TBL} where date_date = {TO_DT}) ) x
             inner join {DATE_TBL} y
             on x.date = y.date_date) latam
             on WebPhone.SALES_DOCUMENT = latam.sales_document
             and WebPhone.FISCAL_YR_AND_WK_DESC=latam.fiscal_yr_and_wk_desc
             left outer join (select * from (select *, row_number() over (partition by sales_document order by Date) rn from (select * from {LATAM_TBL} where trim(upper(Sales_Center)) = 'LATAM' and fiscal_yr_and_qtr_desc = (select fiscal_yr_and_qtr_desc from {DATE_TBL} where date_date = {TO_DT}) ) ) where rn=1) latam_missing
             on WebPhone.SALES_DOCUMENT = latam_missing.sales_document
             """.format(TO_DT=TO_DT,LATAM_TBL=LATAM_TBL,DATE_TBL=DATE_TBL))

             FinalWebPhone.createOrReplaceTempView("FinalWebPhone")

             final_web_phone_bob =spark.sql("""            
                select  
                sales_document_item as sales_document_item,
                CREATED_BY as CREATED_BY,
                CRM_CUSTOMER_GUID as CRM_CUSTOMER_GUID,
                ENTITLEMENT_TYPE as ENTITLEMENT_TYPE,
                date_date as date_date,
                FISCAL_YR_AND_QTR_DESC as FISCAL_YR_AND_QTR_DESC,
                cc_phone_vs_web as cc_phone_vs_web,
                GEO as GEO,
                MARKET_AREA as MARKET_AREA,
                MARKET_SEGMENT as MARKET_SEGMENT,
                offer_type_description as offer_type_description,
                PRODUCT_CONFIG as PRODUCT_CONFIG,
                product_config_description as product_config_description,
                PRODUCT_NAME as PRODUCT_NAME,
                product_name_description as product_name_description,
                PROMO_TYPE as PROMO_TYPE,
                PROMOTION as PROMOTION,
                Region as Region,
                SALES_DOCUMENT as SALES_DOCUMENT,
                DYLAN_ORDER_NUMBER as DYLAN_ORDER_NUMBER,
                STYPE as STYPE,
                SUBS_OFFER as SUBS_OFFER,
                SUBSCRIPTION_ACCOUNT_GUID as SUBSCRIPTION_ACCOUNT_GUID,
                VIP_CONTRACT as VIP_CONTRACT,
                addl_purchase_diff as addl_purchase_diff,
                addl_purchase_same as addl_purchase_same,
                gross_cancel_arr_cfx as gross_cancel_arr_cfx,
                gross_new_arr_cfx as gross_new_arr_cfx,
                gross_new_subs as gross_new_subs,
                init_purchase_arr_cfx as init_purchase_arr_cfx,
                migrated_from_arr_cfx as migrated_from_arr_cfx,
                migrated_to_arr_cfx as migrated_to_arr_cfx,
                net_cancelled_arr_cfx as net_cancelled_arr_cfx,
                net_new_arr_cfx as net_new_arr_cfx,
                net_new_subs as net_new_subs,
                net_purchases_arr_cfx as net_purchases_arr_cfx,
                reactivated_arr_cfx as reactivated_arr_cfx,
                returns_arr_cfx as returns_arr_cfx,
                ecc_customer_id as ecc_customer_id,
                SALES_DISTRICT as SALES_DISTRICT,
                contract_start_date_veda as contract_start_date_veda,
                contract_end_date_veda as contract_end_date_veda,
                contract_id as contract_id,
                renewal_from_arr_cfx as renewal_from_arr_cfx,
                renewal_to_arr_cfx as renewal_to_arr_cfx,
                projected_dme_gtm_segment as projected_dme_gtm_segment, 
                case when upper(gtm_acct_segment) in ('CORP T1','CORP T2') then 'MID-MARKET'
                else gtm_acct_segment
                end as gtm_acct_segment,
                route_to_market as route_to_market,
                cc_segment as cc_segment,
                net_new_arr as net_new_arr,
                'None' as Sales_Center,
                'None' as AgentGeo,
                'None' as RepName,
                 RepLdap as RepLdap,
                'None' as RepTSM,
                'None' as RepManger,
                'None' as RepTeam,
                Flag as Flag,
                gross_cancellations as gross_cancellations,
                net_cancelled_subs as net_cancelled_subs,
                migrated_from as migrated_from,
                migrated_to as migrated_to,
                renewal_from as renewal_from,
                renewal_to as renewal_to,
                net_value_usd as net_value_usd,
                stock_cred_pack as stock_cred_pack,
                acct_name as acct_name,
                customer_email_domain as customer_email_domain,
                payment_method as payment_method,
				Hendrix_Join_Flag as Hendrix_Join_Flag,
                FISCAL_YR_AND_WK_DESC as FISCAL_YR_AND_WK_DESC
                from 
                (select FinalWebPhone.* , case when Flag='Phones' then 'N' 
                                                when Flag='BoB-Web' and latam_flag = 'Y' then 'N'
                                                when Flag='BoB-Web' and (BobContract.BoBContractId is not null or RepLdap is not null) then 'N' 
                                                else 'Y' end as filter_flag 
                from FinalWebPhone left outer join 
                    (
                        select
                        bob.Contract_ID as BoBContractId
                        from df_bob bob
                    )BobContract 
                    on FinalWebPhone.contract_id = BobContract.BoBContractId)  WebPhone
                    where filter_flag='N'""".format(TO_DT=TO_DT,curr_qtr=curr_qtr))

             final_web_phone_bob.createOrReplaceTempView("final_web_phone_bob")
             
             Hendrix_Extra =  spark.sql("""select distinct x.* from 
                (select * from Hendrix_join where sales_document is null) x
                left outer join (select * from Hendrix_join_prd_14 where sales_document is not null) y on x.hendrix_salesdocument = y. hendrix_salesdocument where y.hendrix_salesdocument is null
                """)

             Hendrix_Extra.createOrReplaceTempView("Hendrix_Extra")

             hen_missing_data= spark.sql("""
                select 
                'None' as sales_document_item,
                'None' as CREATED_BY,
                'None' as CRM_CUSTOMER_GUID,
                'None' as ENTITLEMENT_TYPE,
                inserteddate as date_date,
                DT.fiscal_yr_and_qtr_desc as FISCAL_YR_AND_QTR_DESC,
                'None' as cc_phone_vs_web,
                'None' as GEO,
                'None' as MARKET_AREA,
                'None' as MARKET_SEGMENT,
                'None' as offer_type_description,
                'None' as PRODUCT_CONFIG,
                'None' as product_config_description,
                'None' as PRODUCT_NAME,
                'None' as product_name_description,
                'None' as PROMO_TYPE,
                'None' as PROMOTION,
                'None' as Region,
                hendrix_salesdocument as SALES_DOCUMENT,
                'None' as DYLAN_ORDER_NUMBER,
                'None' as STYPE,
                'None' as SUBS_OFFER,
                'None' as SUBSCRIPTION_ACCOUNT_GUID,
                'None' as VIP_CONTRACT,
                'None' as addl_purchase_diff,
                'None' as addl_purchase_same,
                'None' as gross_cancel_arr_cfx,
                'None' as gross_new_arr_cfx,
                'None' as gross_new_subs,
                'None' as init_purchase_arr_cfx,
                'None' as migrated_from_arr_cfx,
                'None' as migrated_to_arr_cfx,
                'None' as net_cancelled_arr_cfx,
                'None' as net_new_arr_cfx,
                'None' as net_new_subs,
                'None' as net_purchases_arr_cfx,
                'None' as reactivated_arr_cfx,
                'None' as returns_arr_cfx,
                'None' as ecc_customer_id,
                'None' as SALES_DISTRICT,
                'None' as contract_start_date_veda,
                'None' as contract_end_date_veda,
                'None' as contract_id,
                'None' as renewal_from_arr_cfx,
                'None' as renewal_to_arr_cfx,
                'None' as projected_dme_gtm_segment, 
                'None' as gtm_acct_segment,
                'None' as route_to_market,
                'None' as cc_segment,
                'None' as net_new_arr,
                'None' as Sales_Center,
                'None' as AgentGeo,
                'None' as RepName,
                RepLdap as RepLdap,
                'None' as RepTSM,
                'None' as RepManger,
                'None' as RepTeam,
                'Hendrix_Extra' as flag,
                'None' as gross_cancellations,
                'None' as net_cancelled_subs,
                'None' as migrated_from,
                'None' as migrated_to,
                'None' as renewal_from,
                'None' as renewal_to,
                'None' as net_value_usd,
                'NA' as stock_cred_pack,
                'NA' as acct_name,
                'NA' as customer_email_domain,
                'NA' as payment_method,
                'EXTRA' as Hendrix_Join_Flag,
                DT.FISCAL_YR_AND_WK_DESC as FISCAL_YR_AND_WK_DESC
                from Hendrix_Extra 
                left outer join
                (
                        select distinct fiscal_yr_and_qtr_desc,fiscal_yr_and_wk_desc,date_date
                        from {DATE_TBL}
                ) DT
                on Hendrix_Extra.inserteddate = DT.date_date
                where sales_document is null and inserteddate between {FROM_DT} and {TO_DT}
                """.format(FROM_DT=FROM_DT,TO_DT=TO_DT,DATE_TBL=DATE_TBL))

             hen_missing_data.createOrReplaceTempView("hen_missing_data")

             Base = spark.sql(""" 
                select * from final_web_phone_bob
                union 
                select * from AOP
                union
                select * from hen_missing_data""")
             
             Base.createOrReplaceTempView("Base")           

             Base.repartition("FISCAL_YR_AND_WK_DESC").write.format("parquet").mode("overwrite").insertInto("%s"%TGT_TBL,overwrite=True)
             
             
             spark.sql(f"msck repair table {TGT_TBL}".format(TGT_TBL=TGT_TBL))

             try:
                 dbutils.notebook.exit("SUCCESS")   
             except Exception as e:                 
                 print("exception:",e)
        except Exception as e:
             dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()
