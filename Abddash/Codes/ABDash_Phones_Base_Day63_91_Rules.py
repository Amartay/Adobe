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

             dbutils.widgets.text("Custom_Settings", "")
             dbutils.widgets.text("RULES_TBL", "")
             dbutils.widgets.text("TGT_TBL", "")             
            #  dbutils.widgets.text("UAT_TBL", "")
             dbutils.widgets.text("fiscal_yr_and_qtr_desc", "")



             Settings = dbutils.widgets.get("Custom_Settings")
             RULES_TBL = dbutils.widgets.get("RULES_TBL")
             TGT_TBL = dbutils.widgets.get("TGT_TBL")             
            #  UAT_TBL = dbutils.widgets.get("UAT_TBL")
             fiscal_yr_and_qtr_desc = dbutils.widgets.get("fiscal_yr_and_qtr_desc")




             Set_list = Settings.split(',')
             if len(Set_list)>0:
                 for i in Set_list:
                     if i != "":
                         print("spark.sql(+i+)")
                         spark.sql("""{i}""".format(i=i))

             # INITIAL PURCHASE, ADD ON, REACTIVATION CALCULATION             

             initial = spark.sql(""" select *,
                                 net_purchases_arr_cfx as final_net_purchase_arr_cfx 
                                 from {RULES_TBL} a 
                                 where event = 'RULES' and FISCAL_YR_AND_QTR_DESC >={fiscal_yr_and_qtr_desc} 
                                 and Txns in ('Initial Purchase','Rep-Add on') 
                                 and net_purchases_arr_cfx > 0 """.format(fiscal_yr_and_qtr_desc=fiscal_yr_and_qtr_desc,RULES_TBL=RULES_TBL))
             initial.createOrReplaceTempView("initial")  


             reactivation = spark.sql(""" select *,
                                      reactivated_arr_cfx as final_net_purchase_arr_cfx
                                      from {RULES_TBL} a 
                                      where event = 'RULES' and FISCAL_YR_AND_QTR_DESC >={fiscal_yr_and_qtr_desc}
                                      and reactivated_arr_cfx > 0 """.format(fiscal_yr_and_qtr_desc=fiscal_yr_and_qtr_desc,RULES_TBL=RULES_TBL))
             
             reactivation.createOrReplaceTempView("reactivation")          

             initial_reactivate = spark.sql(""" 
                                            select * from initial
                                            union all 
                                            select * from reactivation
                                             """)
             initial_reactivate.createOrReplaceTempView("initial_reactivate")

             initial_reactivate_penultimate = spark.sql("""select SALES_DOCUMENT,
                                                        SALES_DOCUMENT_ITEM,
                                                        date_date,
                                                        cc_phone_vs_web,
                                                        product_name,
                                                        GEO,
                                                        sum(final_net_purchase_arr_cfx) as net_purchase_agg,
                                                        sum(net_cancelled_arr_cfx) as net_cancel_agg,Sales_Center,
                                                        AgentGeo,
                                                        RepName,
                                                        RepLdap,
                                                        con_RepLdap,
                                                        RepTSM,
                                                        RepManger,
                                                        RepTeam,
                                                        TeamKey,
                                                        FISCAL_YR_AND_QTR_DESC,
                                                        FISCAL_YR_AND_WK_DESC 
                                                        from initial_reactivate 
                                                        group by SALES_DOCUMENT,
                                                        SALES_DOCUMENT_ITEM,
                                                        date_date,
                                                        cc_phone_vs_web,
                                                        product_name,
                                                        GEO,
                                                        Sales_Center,
                                                        AgentGeo,
                                                        RepName,
                                                        RepLdap,
                                                        con_RepLdap,
                                                        RepTSM,
                                                        RepManger,
                                                        RepTeam,
                                                        TeamKey,
                                                        FISCAL_YR_AND_QTR_DESC,
                                                        FISCAL_YR_AND_WK_DESC 
                                                        order by date_date desc""")
             
             initial_reactivate_penultimate.createOrReplaceTempView("initial_reactivate_penultimate")
             

             # CANCEL , RETURN CALCULATION

             cancel = spark.sql(""" select * from {RULES_TBL} b 
                                where event = 'RULES' and FISCAL_YR_AND_QTR_DESC >={fiscal_yr_and_qtr_desc}
                                and net_cancelled_arr_cfx > 0 
                                and Txns in ('UNKNOWN')""".format(fiscal_yr_and_qtr_desc=fiscal_yr_and_qtr_desc,RULES_TBL=RULES_TBL))
             
             cancel.createOrReplaceTempView("cancel")



             cancel_final = spark.sql(""" select SALES_DOCUMENT,
                                      SALES_DOCUMENT_ITEM,
                                      date_date,
                                      cc_phone_vs_web,
                                      product_name,
                                      GEO,
                                      sum(net_purchases_arr_cfx) as net_purchase_agg,
                                      sum(net_cancelled_arr_cfx) as net_cancel_agg,
                                      Sales_Center,
                                      AgentGeo,
                                      RepName,
                                      RepLdap,
                                      con_RepLdap,
                                      RepTSM,
                                      RepManger,
                                      RepTeam,
                                      TeamKey,
                                      FISCAL_YR_AND_QTR_DESC,
                                      FISCAL_YR_AND_WK_DESC 
                                      from cancel 
                                      group by SALES_DOCUMENT,
                                      SALES_DOCUMENT_ITEM,
                                      date_date,
                                      cc_phone_vs_web,
                                      product_name,
                                      GEO,
                                      Sales_Center,
                                      AgentGeo,
                                      RepName,
                                      RepLdap,
                                      con_RepLdap,
                                      RepTSM,
                                      RepManger,
                                      RepTeam,
                                      TeamKey,
                                      FISCAL_YR_AND_QTR_DESC,
                                      FISCAL_YR_AND_WK_DESC 
                                      order by date_date desc""")
             
             cancel_final.createOrReplaceTempView("cancel_final")

             # FILTER CANCEL ARR EXHAUSTED RECORDS

             filter_record = spark.sql("""           
                                    select  
                                    SALES_DOCUMENT,
                                    SALES_DOCUMENT_ITEM,
                                    add_date_date,
                                    net_purchase_agg, 
                                    previous_net_purchase_agg,                                   
                                    cancel_date_date,
                                    orig_net_cancel_agg,  
                                    previous_net_cancel_agg,                                  
                                    cc_phone_vs_web,
                                    rownum,
                                    Cancel_Flag,
                                    previous_cancel_flag,
                                    case when rownum > 1 and previous_cancel_flag='Pending' then
                                        previous_net_cancel_agg - prev_rolling_net_pur_agg 
                                         else orig_net_cancel_agg
                                    end as net_cancel_agg,     
                                    case when Cancel_Flag='Exhaust' and previous_cancel_flag='Exhaust' then 'Filter'
                                    else 'Keep'
                                    end as Final_Cancel_Flag,
                                    product_name,
                                    GEO,
                                    Sales_Center,
                                    AgentGeo,
                                    RepName,
                                    RepLdap,
                                    con_RepLdap,
                                    RepTSM,
                                    RepManger,
                                    RepTeam,
                                    TeamKey,
                                    FISCAL_YR_AND_QTR_DESC,
                                    FISCAL_YR_AND_WK_DESC
                                    from
                                        (
										select  SALES_DOCUMENT,
										SALES_DOCUMENT_ITEM,
										add_date_date,
										net_purchase_agg,
                                        LAG(net_purchase_agg) OVER(partition by SALES_DOCUMENT,SALES_DOCUMENT_ITEM,cancel_date_date ORDER BY cancel_date_date desc) as previous_net_purchase_agg,
                                        LAG(rolling_net_pur_agg) OVER(partition by SALES_DOCUMENT,SALES_DOCUMENT_ITEM,cancel_date_date ORDER BY cancel_date_date desc) as prev_rolling_net_pur_agg,
										cancel_date_date,
										net_cancel_agg as orig_net_cancel_agg,
                                        LAG(net_cancel_agg) OVER(partition by SALES_DOCUMENT,SALES_DOCUMENT_ITEM,cancel_date_date ORDER BY cancel_date_date desc) as previous_net_cancel_agg,
										cc_phone_vs_web,
										rownum,
										Cancel_Flag,
										LAG(Cancel_Flag) OVER(partition by SALES_DOCUMENT,SALES_DOCUMENT_ITEM,cancel_date_date ORDER BY cancel_date_date desc) as previous_cancel_flag,
										product_name,
										GEO,
										Sales_Center,
										AgentGeo,
										RepName,
										RepLdap,
										con_RepLdap,
										RepTSM,
										RepManger,
										RepTeam,
										TeamKey,
										FISCAL_YR_AND_QTR_DESC,
										FISCAL_YR_AND_WK_DESC                          
										from 
										    (
                                            select * from
                                                (
                                                    select  A.SALES_DOCUMENT,
                                                    A.SALES_DOCUMENT_ITEM,
                                                    A.date_date as add_date_date,
                                                    A.net_purchase_agg,
                                                    SUM(A.net_purchase_agg) OVER (partition by A.SALES_DOCUMENT,A.SALES_DOCUMENT_ITEM,B.date_date ORDER BY B.date_date rows between unbounded preceding and current row) AS rolling_net_pur_agg,
                                                    B.date_date as cancel_date_date,
                                                    B.net_cancel_agg,
                                                    A.cc_phone_vs_web,
                                                    ROW_NUMBER() OVER(PARTITION BY A.SALES_DOCUMENT,A.SALES_DOCUMENT_ITEM,B.date_date order BY TO_DATE(B.date_date) desc,TO_DATE(A.date_date)desc) as rownum,
                                                    case when A.net_purchase_agg >= B.net_cancel_agg then 'Exhaust' 
                                                        else 'Pending'
                                                    end as Cancel_Flag,
                                                    case when B.SALES_DOCUMENT is not null and ((A.cc_phone_vs_web in ('PHONE','WEB') and DATEDIFF(B.date_date, A.date_date) <= 63) or (A.cc_phone_vs_web in ('VIP-PHONE') and DATEDIFF(B.date_date, A.date_date) <= 91)) then 'KEEP' else 'FILTER' end as FILTER_FLAG,
                                                    A.product_name,
                                                    A.GEO,
                                                    A.Sales_Center,
                                                    A.AgentGeo,
                                                    A.RepName,
                                                    A.RepLdap,
                                                    A.con_RepLdap,
                                                    A.RepTSM,
                                                    A.RepManger,
                                                    A.RepTeam,
                                                    A.TeamKey,
                                                    A.FISCAL_YR_AND_QTR_DESC,
                                                    A.FISCAL_YR_AND_WK_DESC                          
                                                    from 
                                                    initial_reactivate_penultimate A
                                                    left outer join cancel_final B
                                                    on A.SALES_DOCUMENT= B.SALES_DOCUMENT and A.SALES_DOCUMENT_ITEM=B.SALES_DOCUMENT_ITEM and B.date_date >=A.date_date 
                                                ) x1 where FILTER_FLAG = 'KEEP'
											)x
                                        )y

                                    """)   

             filter_record.createOrReplaceTempView("filter_record") 
             
            #  spark.sql(""" drop table if exists b2b_tmp.filter_record """)

            #  spark.sql(""" create table b2b_tmp.filter_record as select * from filter_record """)

             # CANCEL ARR DERIVATION

             final_scenario = spark.sql("""            select  
                                    SALES_DOCUMENT,
                                    SALES_DOCUMENT_ITEM,
                                    add_date_date,
                                    previous_add_date_date,
                                    SameDay_Flag,
                                    net_purchase_agg,
                                    previous_net_purchase_agg,
                                    cancel_date_date,
                                    previous_cancel_date_date,
                                    net_cancel_agg,
                                    previous_net_cancel_agg,
                                    rolling_net_cancel_agg,
                                    LAG(rolling_net_cancel_agg) OVER(partition by SALES_DOCUMENT,SALES_DOCUMENT_ITEM ORDER BY add_date_date) as previous_rolling_net_cancel_agg,    
                                    case when ((cc_phone_vs_web in ('PHONE','WEB') and DATEDIFF(cancel_date_date, add_date_date) <= 63) or (cc_phone_vs_web in ('VIP-PHONE') and DATEDIFF(cancel_date_date, add_date_date) <= 91)) then 
                                        case when rownum = 1 then
                                            case when net_purchase_agg > net_cancel_agg then net_cancel_agg
                                                 else net_purchase_agg
                                            end         
                                             when rownum > 1 then
                                                case when SameDay_Flag = 'Y' then  
                                                    case when previous_net_purchase_agg > previous_net_cancel_agg then
                                                        case when  previous_purchase_agg > previous_rolling_net_cancel_agg then
                                                            case when current_purchase_agg > rolling_net_cancel_agg then
                                                                case when net_purchase_agg > net_cancel_agg then net_cancel_agg
                                                                     else net_purchase_agg
                                                                end
                                                                 else net_purchase_agg - previous_rolling_net_cancel_agg
                                                            end
                                                             else 0
                                                        end
                                                         else 0
                                                    end     
                                                     when SameDay_Flag = 'N' then 
                                                        case when net_purchase_agg > net_cancel_agg then net_cancel_agg
                                                             else net_purchase_agg    
                                                        end
                                                end
                                            end                                                        
                                    end as cancel_arr,
                                    case when rownum>=1 and cc_phone_vs_web in ('PHONE','WEB') and DATEDIFF(cancel_date_date, add_date_date) <= 63 
                                        then 'Y' else 'NA' end as Day_63_Flag,
                                    case when rownum>=1 and cc_phone_vs_web in ('VIP-PHONE') and DATEDIFF(cancel_date_date, add_date_date) <= 91 
                                        then 'Y' else 'NA' end as Day_91_Flag,
                                    DATEDIFF(cancel_date_date, add_date_date) as Date_Difference, 
                                    cc_phone_vs_web,
                                    rownum,
                                    product_name,
                                    GEO,
                                    Sales_Center,
                                    AgentGeo,
                                    RepName,
                                    RepLdap,
                                    con_RepLdap,
                                    RepTSM,
                                    RepManger,
                                    RepTeam,
                                    TeamKey,
                                    FISCAL_YR_AND_QTR_DESC,
                                    FISCAL_YR_AND_WK_DESC     
                                    from
                                        (
                                        select  
										SALES_DOCUMENT,
										SALES_DOCUMENT_ITEM,
										add_date_date,
										previous_add_date_date,
										SameDay_Flag,
										net_purchase_agg,
										net_purchase_agg as current_purchase_agg,
										previous_net_purchase_agg,
										previous_net_purchase_agg as previous_purchase_agg,
										cancel_date_date,
										previous_cancel_date_date,
										net_cancel_agg,
										previous_net_cancel_agg,
										rolling_net_cancel_agg,
										LAG(rolling_net_cancel_agg) OVER(partition by SALES_DOCUMENT,SALES_DOCUMENT_ITEM ORDER BY add_date_date) as previous_rolling_net_cancel_agg,                                    
										cc_phone_vs_web,
										rownum,
										product_name,
										GEO,
										Sales_Center,
										AgentGeo,
										RepName,
										RepLdap,
										con_RepLdap,
										RepTSM,
										RepManger,
										RepTeam,
										TeamKey,
										FISCAL_YR_AND_QTR_DESC,
										FISCAL_YR_AND_WK_DESC
										from 
											(
                                            select  
											SALES_DOCUMENT,
											SALES_DOCUMENT_ITEM,
											add_date_date,
											previous_add_date_date,
											case when add_date_date = previous_add_date_date then 'Y' else 'N' end as SameDay_Flag,
											net_purchase_agg,
											previous_net_purchase_agg,
											cancel_date_date,
											previous_cancel_date_date,
											net_cancel_agg,
											previous_net_cancel_agg,
											SUM(net_cancel_agg) OVER (partition by SALES_DOCUMENT,SALES_DOCUMENT_ITEM,add_date_date ORDER BY add_date_date rows between unbounded preceding and current row) AS rolling_net_cancel_agg,
											cc_phone_vs_web,
											rownum,
											product_name,
											GEO,
											Sales_Center,
											AgentGeo,
											RepName,
											RepLdap,
											con_RepLdap,
											RepTSM,
											RepManger,
											RepTeam,
											TeamKey,
											FISCAL_YR_AND_QTR_DESC,
											FISCAL_YR_AND_WK_DESC
											from  
												(
                								select  SALES_DOCUMENT,
												SALES_DOCUMENT_ITEM,
												add_date_date,
												LAG(add_date_date) OVER(partition by SALES_DOCUMENT,SALES_DOCUMENT_ITEM ORDER BY add_date_date) as previous_add_date_date,
												net_purchase_agg,
												LAG(net_purchase_agg) OVER(partition by SALES_DOCUMENT,SALES_DOCUMENT_ITEM ORDER BY add_date_date) as previous_net_purchase_agg,
												cancel_date_date,
												LAG(cancel_date_date) OVER(partition by SALES_DOCUMENT,SALES_DOCUMENT_ITEM ORDER BY cancel_date_date) as previous_cancel_date_date,
												net_cancel_agg,
												LAG(net_cancel_agg) OVER(partition by SALES_DOCUMENT,SALES_DOCUMENT_ITEM ORDER BY add_date_date) as previous_net_cancel_agg,
												cc_phone_vs_web,
												ROW_NUMBER() OVER(PARTITION BY SALES_DOCUMENT,SALES_DOCUMENT_ITEM,add_date_date order BY TO_DATE(add_date_date),TO_DATE(cancel_date_date)) as rownum,
												product_name,
												GEO,
												Sales_Center,
												AgentGeo,
												RepName,
												RepLdap,
												con_RepLdap,
												RepTSM,
												RepManger,
												RepTeam,
												TeamKey,
												FISCAL_YR_AND_QTR_DESC,
												FISCAL_YR_AND_WK_DESC
												from filter_record  
												where Final_Cancel_Flag = 'Keep'
												)e
											)f 
										)g 
          """)
             
             final_scenario.createOrReplaceTempView("final_scenario")

            #  spark.sql(""" drop table if exists b2b_tmp.final_scenario """)

            #  spark.sql(""" create table b2b_tmp.final_scenario as select * from final_scenario """)

             summary = spark.sql("""
                                    select y.* 
                                    from
                                    ( 
                                    select 
                                    SALES_DOCUMENT,
                                    SALES_DOCUMENT_ITEM,
                                    date_date,
                                    net_purchase_agg,
                                    cancel_date_list,
                                    sum_cancel_arr,
                                    -- case when sum_cancel_arr is null
                                    --     then net_purchase_agg
                                    --      else
                                    --      net_purchase_agg - sum_cancel_arr
                                    -- end as remaining_arr     ,
                                    (net_purchase_agg - sum_cancel_arr) as  remaining_arr,
                                    cc_phone_vs_web,
                                    product_name,
                                    GEO,
                                    Sales_Center,
                                    AgentGeo,
                                    RepName,
                                    RepLdap,
                                    con_RepLdap,
                                    RepTSM,
                                    RepManger,
                                    RepTeam,
                                    TeamKey,
                                    FISCAL_YR_AND_QTR_DESC,
                                    FISCAL_YR_AND_WK_DESC
                                    from
                                    ( 
                                        select  
                                        SALES_DOCUMENT,
                                        SALES_DOCUMENT_ITEM,
                                        add_date_date as date_date,
                                        net_purchase_agg,
                                        CONCAT_WS(',', COLLECT_LIST(cancel_date_date)) as cancel_date_list,
                                        sum(cancel_arr) as sum_cancel_arr,
                                        cc_phone_vs_web,
                                        product_name,
                                        GEO,
                                        Sales_Center,
                                        AgentGeo,
                                        RepName,
                                        RepLdap,
                                        con_RepLdap,
                                        RepTSM,
                                        RepManger,
                                        RepTeam,
                                        TeamKey,
                                        FISCAL_YR_AND_QTR_DESC,
                                        FISCAL_YR_AND_WK_DESC
                                        from 
                                        final_scenario
                                        group by sales_document, 
                                        sales_document_item,
                                        add_date_date,
                                        net_purchase_agg,
                                        cc_phone_vs_web,
                                        product_name,
                                        GEO,
                                        Sales_Center,
                                        AgentGeo,
                                        RepName,
                                        RepLdap,
                                        con_RepLdap,
                                        RepTSM,
                                        RepManger,
                                        RepTeam,
                                        TeamKey,
                                        FISCAL_YR_AND_QTR_DESC,
                                        FISCAL_YR_AND_WK_DESC
                                        )x
                                    )y where y.remaining_arr is not null
                                """)
 

             summary.createOrReplaceTempView("summary")

            #  spark.sql(""" drop table if exists b2b_tmp.final_scenario_summary """)

            #  spark.sql(""" create table b2b_tmp.final_scenario_summary  AS  select * from summary  """)             

             df_purchase_cancel = spark.sql("""
                                        select 
                                        y.sales_document_item,
                                        y.CREATED_BY,
                                        y.CRM_CUSTOMER_GUID,
                                        y.ENTITLEMENT_TYPE,
                                        y.date_date,
                                        y.cc_phone_vs_web,
                                        y.GEO,
                                        y.MARKET_AREA,
                                        y.MARKET_SEGMENT,
                                        y.offer_type_description,
                                        y.PRODUCT_CONFIG,
                                        y.product_config_description,
                                        y.PRODUCT_NAME,
                                        y.product_name_description,
                                        y.PROMO_TYPE,
                                        y.PROMOTION,
                                        y.Region,
                                        y.SALES_DOCUMENT,
                                        y.DYLAN_ORDER_NUMBER,
                                        y.STYPE,
                                        y.SUBS_OFFER,
                                        y.SUBSCRIPTION_ACCOUNT_GUID,
                                        y.VIP_CONTRACT,addl_purchase_diff,'0' as addl_purchase_same,
                                        '0' as gross_cancel_arr_cfx,
                                        '0' as gross_new_arr_cfx,
                                        '0' as gross_new_subs,
                                        '0' as init_purchase_arr_cfx,
                                        '0' as migrated_from_arr_cfx,
                                        '0' as migrated_to_arr_cfx,
                                        '0' as net_cancelled_arr_cfx,
                                        '0' as net_new_arr_cfx,
                                        '0' as net_new_subs,
                                        '0' as net_purchases_arr_cfx,
                                        '0' as reactivated_arr_cfx,
                                        '0' as returns_arr_cfx,
                                        y.ecc_customer_id,
                                        y.SALES_DISTRICT,
                                        y.contract_start_date_veda,
                                        y.contract_end_date_veda,
                                        y.contract_id,
                                        '0' as renewal_from_arr_cfx,
                                        '0' as renewal_to_arr_cfx,
                                        y.projected_dme_gtm_segment,
                                        y.gtm_acct_segment,
                                        y.route_to_market,
                                        y.cc_segment,
                                        '0' as net_new_arr,
                                        y.Sales_Center,
                                        y.AgentGeo,
                                        y.RepName,
                                        y.RepLdap,
                                        y.con_RepLdap,
                                        y.RepTSM,
                                        y.RepManger,
                                        y.RepTeam,
                                        y.cross_team_agent,
                                        y.cross_sell_team,
                                        y.Flag,
                                        y.AgentMapFlag,
                                        y.Txns,
                                        y.SFDC_opportunity_id,
                                        y.SFDC_opportunity_created_date,
                                        y.SFDC_closed_date,
                                        y.SFDC_email,
                                        y.SFDC_ecc_salesordernumber,
                                        y.SFDC_campaignid,
                                        y.SFDC_name,
                                        y.SFDC_cum_campaignid,
                                        y.SFDC_min_date,
                                        y.SFDC_min_fiscal_yr_and_qtr_desc,
                                        y.SFDC_min_fiscal_yr_and_wk_desc,
                                        y.SFDC_Flag,y.Bob_Flag,
                                        y.TeamKey,
                                        '0' as gross_new_arr_cfx_unclaimed,
                                        '0' as net_purchases_arr_cfx_unclaimed,
                                        y.unclaimed_ldap,
                                        y.unclaimed_team,
                                        y.unclaimed_TSM,
                                        y.unclaimed_Manager,
                                        y.agent_max_quarter,
                                        y.agent_curr_qtr,
                                        y.created_curr_qtr,
                                        y.agent_TSM_Ldap,
                                        y.created_TSM_Ldap,
                                        y.agent_max_quarter_TSM,
                                        y.created_max_quarter_TSM,
                                        y.agent_TSM_team,
                                        y.created_TSM_team,
                                        y.agent_TSM_team_key,
                                        y.created_TSM_team_key,
                                        y.agent_TSM_flag,
                                        y.created_TSM_flag,
                                        y.ABD_Flag,
                                        y.agent_ABD_Flag,
                                        y.TSM_ABD_Flag,
                                        y.TSM_agent_ABD_Flag,
                                        y.gross_cancellations,
                                        y.net_cancelled_subs,
                                        y.migrated_from,
                                        y.migrated_to,
                                        y.renewal_from,
                                        y.renewal_to,
                                        y.Custom_Flag,
                                        y.bob_ABD_flag,
                                        y.lat_ABD_flag,
                                        y.same_wk_cncl_flag,
                                        y.cross_team_agent_name,
                                        y.cross_sell_team_key,
                                        y.net_value_usd,
                                        y.acct_name,
                                        y.customer_email_domain,
                                        y.payment_method,
                                        summ.net_purchase_agg,
                                        summ.cancel_date_list,
                                        summ.sum_cancel_arr,
                                        summ.remaining_arr as net_purchase_arr_incremental,
                                        'PURCHASE_CANCEL_MAP' as event,
                                        y.FISCAL_YR_AND_QTR_DESC,
                                        y.FISCAL_YR_AND_WK_DESC
                                            from
                                                (
                                                select x.* from
                                                    (
                                                        select *,ROW_NUMBER() OVER(PARTITION BY SALES_DOCUMENT,SALES_DOCUMENT_ITEM,date_date order BY TO_DATE(date_date)) as rownum from initial_reactivate
                                                    )x where rownum=1
                                                )y
                                        right join summary summ
                                        on y.sales_document=summ.SALES_DOCUMENT 
                                        and y.sales_document_item=summ.SALES_DOCUMENT_ITEM 
                                        and y.date_date = summ.date_date
                                        """)
             
             df_purchase_cancel.createOrReplaceTempView("df_purchase_cancel")
    
             df_purchase_cancel.repartition("event","FISCAL_YR_AND_QTR_DESC","FISCAL_YR_AND_WK_DESC").write.format("parquet").mode("append").insertInto("%s"%TGT_TBL,overwrite=True)
             spark.sql(f"msck repair table {TGT_TBL}".format(TGT_TBL=TGT_TBL))

            #  df_purchase_cancel.repartition("event","FISCAL_YR_AND_QTR_DESC","FISCAL_YR_AND_WK_DESC").write.format("parquet").mode("append").insertInto("%s"%UAT_TBL,overwrite=True)
            #  spark.sql(f"msck repair table {UAT_TBL}".format(UAT_TBL=UAT_TBL))

             
             try:
                 dbutils.notebook.exit("SUCCESS")   
             except Exception as e:                 
                 print("exception:",e)
        except Exception as e:
             dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()                         
