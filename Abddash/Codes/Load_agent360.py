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
             dbutils.widgets.text("TGT1_TBL", "")
             dbutils.widgets.text("TO_DT", "")
             dbutils.widgets.text("FROM_DT", "")
             dbutils.widgets.text("BKP_TBL", "")
             dbutils.widgets.text("SRC_TBL", "")
             dbutils.widgets.text("ABDASH_RULES", "")
             dbutils.widgets.text("DIM_TEAM", "")
             dbutils.widgets.text("AGENT_DETAILS", "")
             dbutils.widgets.text("AGENT_DETAILS_ACTIVE", "")
             dbutils.widgets.text("DATE_TBL", "")
             dbutils.widgets.text("TMP_TBL", "")
             dbutils.widgets.text("DIM_SUBSCRIPTION", "")
             dbutils.widgets.text("DIM_COUNTRY", "")
             dbutils.widgets.text("FILE_PATH", "")
             dbutils.widgets.text("TO_ADDR", "")
             dbutils.widgets.text("FROM_ADDR", "")

             Settings = dbutils.widgets.get("Custom_Settings")
             TGT1_TBL = dbutils.widgets.get("TGT1_TBL")
             TO_DT = dbutils.widgets.get("TO_DT")
             FROM_DT = dbutils.widgets.get("FROM_DT")
             BKP_TBL = dbutils.widgets.get("BKP_TBL")
             SRC_TBL = dbutils.widgets.get("SRC_TBL")
             ABDASH_RULES = dbutils.widgets.get("ABDASH_RULES")
             DIM_TEAM = dbutils.widgets.get("DIM_TEAM")
             AGENT_DETAILS = dbutils.widgets.get("AGENT_DETAILS")
             AGENT_DETAILS_ACTIVE = dbutils.widgets.get("AGENT_DETAILS_ACTIVE")
             DATE_TBL = dbutils.widgets.get("DATE_TBL")
             TMP_TBL = dbutils.widgets.get("TMP_TBL")
             DIM_SUBSCRIPTION = dbutils.widgets.get("DIM_SUBSCRIPTION")
             DIM_COUNTRY = dbutils.widgets.get("DIM_COUNTRY")
             FILE_PATH = dbutils.widgets.get("FILE_PATH")
             TO_ADDR = dbutils.widgets.get("TO_ADDR")
             FROM_ADDR = dbutils.widgets.get("FROM_ADDR")

             df_curr_qtr = spark.sql("""select fiscal_yr_and_qtr_desc from {DATE_TBL} where date_date = {TO_DT}""".format(FROM_DT=FROM_DT,TO_DT=TO_DT,DATE_TBL=DATE_TBL))
             curr_qtr=df_curr_qtr.first()['fiscal_yr_and_qtr_desc']

             abddash_base = spark.sql("""
             SELECT
             date_date,
             FISCAL_YR_AND_WK_DESC,
             FISCAL_YR_AND_QTR_DESC,
             cc_phone_vs_web,
             Geo,
             MARKET_AREA,
             Sales_Center, 
             sales_document,
             DYLAN_ORDER_NUMBER,
             sales_document_item,
             contract_id,
             Txns,
             route_to_market,
             PROMO_TYPE,
             cc_segment,
             product_name_description,
             case when cc_segment = 'SIGN' then 'SIGN'
             when cc_segment = 'ACROBAT DC' then 'ACROBAT'
             when cc_segment = 'ACROBAT CC' then 'ACROBAT'
             when cc_segment = 'STUDENT' then 'K-12'
             when cc_segment = 'TEAM' and upper(PRODUCT_NAME) = 'SBST' then 'SUBSTANCE'
             when cc_segment = 'TEAM' and upper(PRODUCT_NAME) = 'FFLY' then 'FIREFLY'
             when cc_segment = 'TEAM' then 'CCT'
             when cc_segment = 'K12+EEA' then 'K-12'
             when cc_segment = 'STOCK' then 'STOCK'
             when cc_segment = 'INDIVIDUAL' and upper(PRODUCT_NAME) = 'FFLY' then 'FIREFLY'
             when cc_segment = 'INDIVIDUAL' then 'CONSUMER'
             else 'OTHERS'
             end as product_group,
             case 
             --when cc_segment = 'SIGN' and cc_phone_vs_web = 'VIP-PHONE' and PROMO_TYPE = 'AWS INTRO 20%' then 'SIGN Starter Pack'
             --when cc_segment = 'SIGN' and cc_phone_vs_web = 'VIP-PHONE' and PROMO_TYPE = 'AZURE INTRO 20%' then 'SIGN Starter Pack'
             --when cc_segment = 'SIGN' and cc_phone_vs_web = 'VIP-PHONE' and PROMO_TYPE = 'AWS 3K INTRO' then 'SIGN Starter Pack'
             when cc_segment = 'SIGN' and cc_phone_vs_web = 'VIP-PHONE' and PROMO_TYPE in ('AWS INTRO 20%','AZURE INTRO 20%','AWS 3K INTRO') then 'SIGN Starter Pack'
             when cc_segment = 'SIGN' then 'SIGN'
             when cc_segment = 'ACROBAT DC' and PROMO_TYPE = 'TEAM 50PK PROMO' then 'Acrobat 50 PK'
             when cc_segment = 'ACROBAT DC' and PROMO_TYPE = 'TEAM 25PK PROMO' then 'Acrobat 25 PK'
             when cc_segment = 'ACROBAT CC'  and PROMO_TYPE = 'TEAM 50PK PROMO' then 'Acrobat 50 PK'
             when cc_segment = 'ACROBAT CC' and PROMO_TYPE = 'TEAM 25PK PROMO' then 'Acrobat 25 PK'
             when cc_segment = 'ACROBAT DC' then 'ACROBAT' 
             when cc_segment = 'ACROBAT CC' then 'ACROBAT' 
             when cc_segment = 'TEAM' and PRODUCT_NAME = 'SBST' then 'SUBSTANCE'
             when cc_segment = 'TEAM' and PRODUCT_NAME = 'FFLY' then 'FIREFLY'
             when cc_segment = 'TEAM' and PRODUCT_NAME = 'CCEX' then 'Express'
             when cc_segment = 'TEAM' then 'CCT'
             when cc_segment = 'STOCK' and PRODUCT_NAME = 'STEL' then 'Stock Credit Packs'
             when cc_segment = 'STOCK' then 'STOCK'
             when cc_segment = 'INDIVIDUAL' and PRODUCT_NAME = 'CCEX' then 'Express'
             when cc_segment = 'INDIVIDUAL' and PRODUCT_NAME = 'FFLY' then 'FIREFLY'
             when cc_segment = 'INDIVIDUAL' then 'CONSUMER'
             when cc_segment = 'STUDENT' and PRODUCT_NAME = 'CCEX' then 'Express'
             when cc_segment = 'STUDENT' then 'K-12'
             when cc_segment = 'K12+EEA' then 'K-12'
             when PRODUCT_NAME = 'CCEX' then 'Express'
             else 'OTHERS'
             end as sub_product_group,
             STYPE,
             SALES_DISTRICT,
             AgentMapFlag,
             cross_sell_team,
             cross_team_agent,
             TeamKey,
             con_RepLdap,
             RepName,
             RepManger,
             RepTSM,
             case when cc_phone_vs_web in ('PHONE','WEB') then coalesce(RepLdap,created_by)
             when cc_phone_vs_web = 'VIP-PHONE' then created_by
             else 'UNKNOWN' 
             end created_ldap,
             customer_email_domain,
             acct_name,
             net_purchases_arr_cfx,
             net_cancelled_arr_cfx,
             reactivated_arr_cfx,
             renewal_to_arr_cfx,
             renewal_from_arr_cfx,
             migrated_to_arr_cfx,
             migrated_from_arr_cfx,
             ((migrated_to_arr_cfx - migrated_from_arr_cfx) + (renewal_to_arr_cfx -renewal_from_arr_cfx )) as net_migration_arr_cfx,
             returns_arr_cfx,
             net_new_subs,
             gross_new_subs
             from
             {ABDASH_RULES} where event = 'RULES' and  TeamKey <> 'WEB NOT APPLICABLE' and sales_document is not null and FISCAL_YR_AND_QTR_DESC >= '{curr_qtr}' """.format(ABDASH_RULES = ABDASH_RULES,curr_qtr=curr_qtr))
             abddash_base.createOrReplaceTempView("abddash_base")

             attainment_df = spark.sql("""
             select 
             financial.date_date as date,
             financial.FISCAL_YR_AND_WK_DESC as FISCAL_YR_AND_WK_DESC, 
             financial.FISCAL_YR_AND_QTR_DESC as FISCAL_YR_AND_QTR_DESC,
             financial.cc_phone_vs_web,
             financial.Geo as Geo,
             financial.Sales_Center as Sales_Center,
             financial.Sales_document,
             coalesce(dim_subscription_sales_document.dylan_order_number,'NA') as dylan_order_number,
             financial.sales_document_item,
             financial.contract_id,
             financial.Txns,
             financial.route_to_market,
             financial.PROMO_TYPE,
             financial.cc_segment,
             financial.product_name_description,
             financial.product_group,
             financial.sub_product_group,
             financial.stype,
             dim_country.market_area_description as market_area_description,
             financial.AgentMapFlag,
             upper(trim(financial.cross_sell_team)) as cross_sell_team,
             upper(trim(financial.cross_team_agent)) as cross_team_agent,
             upper(trim(financial.con_RepLdap)) as RepLdap,
             upper(trim(financial.created_ldap)) created_ldap,
             financial.RepName as RepName,
             financial.RepName as attained_RepName,
             coalesce(created_agent.Rep_name,created_tsm.TSM,'UNKNOWN') as created_RepName,
             financial.RepTSM as RepTSM,
             financial.RepTSM  as attained_TSM,
             coalesce(created_agent.TSM,created_tsm.Manager,'UNKNOWN') as created_TSM,
             case when financial.RepManger = 'UNKNOWN' then coalesce(team_unknown_manager.Manager,'UNKNOWN') else financial.RepManger end RepManger,
             case when financial.RepManger = 'UNKNOWN' then coalesce(team_unknown_manager.Manager,'UNKNOWN') else financial.RepManger end attained_manager,
             coalesce(created_agent.Manager,created_tsm.Manager,'UNKNOWN') as created_manager,
             dim_team.Standard_team_name as repteam,
             dim_team.Standard_team_name as attained_team,
             coalesce(created_agent.Standard_team_name,created_tsm.Standard_team_name,'UNKNOWN') as created_team,
             dim_team.Team_category,
             financial.customer_email_domain,
             financial.acct_name,
             financial.net_purchases_arr_cfx as net_purchases_arr_cfx_attained,
             financial.net_cancelled_arr_cfx as net_cancelled_arr_cfx_attained,
             financial.reactivated_arr_cfx as reactivated_arr_cfx_attained,
             financial.net_migration_arr_cfx as net_migration_arr_cfx_attained,
             financial.returns_arr_cfx as returns_arr_cfx_attained,
             financial.net_new_subs as net_new_subs_attained,
             financial.gross_new_subs as gross_new_subs_attained,
             0 as net_purchases_arr_cfx_created,
             0 as net_cancelled_arr_cfx_created,
             0 as reactivated_arr_cfx_created,
             0 as net_migration_arr_cfx_created,
             0 as returns_arr_cfx_created,
             0 as net_new_subs_created,
             0 as gross_new_subs_created,
             'Attainment' as flag
             from abddash_base financial
             left outer join
             (
             select distinct sales_document, dylan_order_number from {DIM_SUBSCRIPTION} where sales_document <> ''
             ) dim_subscription_sales_document
             on upper(trim(financial.SALES_DOCUMENT)) = upper(trim(dim_subscription_sales_document.sales_document))
             left outer  join 
             (
             select Team_Key, Manager,Quarter,rn from (
             select row_number() over (partition by Team_Key,Quarter order by Team_Key) as rn , Team_Key, Manager,Quarter from 
             (select distinct Team_Key, Manager,Quarter from {AGENT_DETAILS_ACTIVE} )
             ) where rn = 1 
             ) team_unknown_manager
             on upper(trim(financial.TeamKey)) = upper(trim(team_unknown_manager.Team_Key)) and financial.FISCAL_YR_AND_QTR_DESC = team_unknown_manager.Quarter
             left outer join 
             (select distinct Team_key,Standard_team_name, Team_category,Display_Team_Category from {DIM_TEAM}) dim_team
             on financial.TeamKey = dim_team.Team_key
             left outer join
             (
             select distinct ldap,Rep_name,TSM,TSM_ldap,Manager,Quarter,Team_key,Team_Category,Standard_team_name,start_date,end_date from {AGENT_DETAILS}
             ) created_agent
             on upper(trim(financial.created_ldap)) = upper(trim(created_agent.ldap)) and financial.fiscal_yr_and_qtr_desc = created_agent.Quarter and financial.date_date between created_agent.start_date and created_agent.end_date
             left outer join 
             (
             select distinct ldap,Rep_name,TSM,TSM_ldap,Manager,Quarter,Team_key,Team_Category,Standard_team_name,start_date,end_date from
             (Select  row_number() OVER(PARTITION BY TSM_ldap,Quarter ORDER BY Quarter) as rn,ldap,Rep_name,TSM,TSM_ldap,Manager,Quarter,Team_key,Team_Category,Standard_team_name,start_date,end_date  from {AGENT_DETAILS_ACTIVE}) where rn=1
             ) created_tsm
             on upper(trim(financial.created_ldap)) = upper(trim(created_tsm.TSM_ldap)) and financial.fiscal_yr_and_qtr_desc = created_tsm.Quarter
             left outer join 
             (select distinct country_code_iso2,market_area_description from {DIM_COUNTRY}) dim_country
             on financial.SALES_DISTRICT = dim_country.country_code_iso2 """.format(AGENT_DETAILS = AGENT_DETAILS,DIM_TEAM = DIM_TEAM,DIM_COUNTRY = DIM_COUNTRY,DIM_SUBSCRIPTION = DIM_SUBSCRIPTION,AGENT_DETAILS_ACTIVE = AGENT_DETAILS_ACTIVE))
             attainment_df.createOrReplaceTempView("attainment_df")

             created_df = spark.sql("""
             select 
             financial.date_date as date,
             financial.FISCAL_YR_AND_WK_DESC as FISCAL_YR_AND_WK_DESC, 
             financial.FISCAL_YR_AND_QTR_DESC as FISCAL_YR_AND_QTR_DESC,
             financial.cc_phone_vs_web,
             financial.Geo as Geo,
             financial.Sales_Center as Sales_Center,
             financial.Sales_document,
             coalesce(dim_subscription_sales_document.dylan_order_number,'NA') as dylan_order_number,
             financial.sales_document_item,
             financial.contract_id,
             financial.Txns,
             financial.route_to_market,
             financial.PROMO_TYPE,
             financial.cc_segment,
             financial.product_name_description,
             financial.product_group,
             financial.sub_product_group,
             financial.stype,
             dim_country.market_area_description as market_area_description,
             financial.AgentMapFlag,
             upper(trim(financial.cross_sell_team)) as cross_sell_team,
             upper(trim(financial.cross_team_agent)) as cross_team_agent,
             upper(trim(financial.con_RepLdap)) as RepLdap,
             upper(trim(financial.created_ldap)) as created_ldap,
             coalesce(created_agent.Rep_name,created_tsm.TSM,'UNKNOWN') as RepName,
             financial.RepName as attained_RepName,
             coalesce(created_agent.Rep_name,created_tsm.TSM,'UNKNOWN') as created_RepName,
             coalesce(created_agent.TSM,created_tsm.Manager,'UNKNOWN') as RepTSM,
             financial.RepTSM  as attained_TSM,
             coalesce(created_agent.TSM,created_tsm.Manager,'UNKNOWN') as created_TSM,
             coalesce(created_agent.Manager,created_tsm.Manager,'UNKNOWN') as RepManger,
             case when financial.RepManger = 'UNKNOWN' then coalesce(team_unknown_manager.Manager,'UNKNOWN') else financial.RepManger end attained_manager,
             coalesce(created_agent.Manager,created_tsm.Manager,'UNKNOWN') as created_manager,
             coalesce(created_agent.Standard_team_name,created_tsm.Standard_team_name,'UNKNOWN') as repteam,
             dim_team.Standard_team_name as attained_team,
             coalesce(created_agent.Standard_team_name,created_tsm.Standard_team_name,'UNKNOWN') as created_team,
             coalesce(created_agent.Team_Category,created_tsm.Team_Category,'UNKNOWN') as Team_Category,
             financial.customer_email_domain,
             financial.acct_name,
             0 as net_purchases_arr_cfx_attained,
             0 as net_cancelled_arr_cfx_attained,
             0 as reactivated_arr_cfx_attained,
             0 as net_migration_arr_cfx_attained,
             0 as returns_arr_cfx_attained,
             0 as net_new_subs_attained,
             0 as gross_new_subs_attained,
             financial.net_purchases_arr_cfx as net_purchases_arr_cfx_created,
             financial.net_cancelled_arr_cfx as net_cancelled_arr_cfx_created,
             financial.reactivated_arr_cfx as reactivated_arr_cfx_created,
             financial.net_migration_arr_cfx as net_migration_arr_cfx_created,
             financial.returns_arr_cfx as returns_arr_cfx_created,
             financial.net_new_subs as net_new_subs_created,
             financial.gross_new_subs as gross_new_subs_created,
             'Created' as flag
             from abddash_base financial
             left outer join
             (
             select distinct sales_document, dylan_order_number from {DIM_SUBSCRIPTION} where sales_document <> ''
             ) dim_subscription_sales_document
             on upper(trim(financial.SALES_DOCUMENT)) = upper(trim(dim_subscription_sales_document.sales_document))
             left outer join
             (select distinct Team_key,Standard_team_name, Team_category,Display_Team_Category from {DIM_TEAM}) dim_team
             on financial.TeamKey = dim_team.Team_key
             left outer  join 
             (
             select Team_Key, Manager,Quarter,rn from (
             select row_number() over (partition by Team_Key,Quarter order by Team_Key) as rn , Team_Key, Manager,Quarter from 
             (select distinct Team_Key, Manager,Quarter from {AGENT_DETAILS_ACTIVE} )
             ) where rn = 1 
             ) team_unknown_manager
             on upper(trim(financial.TeamKey)) = upper(trim(team_unknown_manager.Team_Key)) and financial.FISCAL_YR_AND_QTR_DESC = team_unknown_manager.Quarter
             left outer join
             (
             select distinct ldap,Rep_name,TSM,TSM_ldap,Manager,Quarter,Team_key,Team_Category,Standard_team_name,start_date,end_date from {AGENT_DETAILS}
             ) created_agent
             on upper(trim(financial.created_ldap)) = upper(trim(created_agent.ldap)) and financial.fiscal_yr_and_qtr_desc = created_agent.Quarter and financial.date_date between created_agent.start_date and created_agent.end_date
             left outer join 
             (
             select distinct ldap,Rep_name,TSM,TSM_ldap,Manager,Quarter,Team_key,Team_Category,Standard_team_name,start_date,end_date from
             (Select  row_number() OVER(PARTITION BY TSM_ldap,Quarter ORDER BY Quarter) as rn,ldap,Rep_name,TSM,TSM_ldap,Manager,Quarter,Team_key,Team_Category,Standard_team_name,start_date,end_date from {AGENT_DETAILS_ACTIVE}) where rn=1
             ) created_tsm
             on upper(trim(financial.created_ldap)) = upper(trim(created_tsm.TSM_ldap)) and financial.fiscal_yr_and_qtr_desc = created_tsm.Quarter and
             financial.date_date between created_tsm.start_date and created_tsm.end_date
             left outer join 
             (select distinct country_code_iso2,market_area_description from {DIM_COUNTRY}) dim_country
             on financial.SALES_DISTRICT = dim_country.country_code_iso2
             """.format(AGENT_DETAILS = AGENT_DETAILS,DIM_TEAM = DIM_TEAM,DIM_COUNTRY = DIM_COUNTRY,DIM_SUBSCRIPTION = DIM_SUBSCRIPTION,AGENT_DETAILS_ACTIVE = AGENT_DETAILS_ACTIVE ))
             created_df.createOrReplaceTempView("created_df")

             missing_orders_df = spark.sql("""
             select
             Missing_Orders.date_date as date,
             Missing_Orders.FISCAL_YR_AND_WK_DESC as FISCAL_YR_AND_WK_DESC, 
             Missing_Orders.FISCAL_YR_AND_QTR_DESC as FISCAL_YR_AND_QTR_DESC,
             'UNKNOWN' as cc_phone_vs_web,
             coalesce(created_agent.geo,created_tsm.geo,'UNKNOWN') as Geo,
             coalesce(created_agent.Sales_Center,created_tsm.Sales_Center,'UNKNOWN') as Sales_Center,
             --Missing_Orders.Sales_document as sales_document_real,
             case when length(Missing_Orders.Sales_document) <= 10 then coalesce(dim_subscription_sales_document.sales_document,Missing_Orders.Sales_document,'NA')
             else coalesce(dim_subscription_dylan.sales_document,'NA') end Sales_document,
             case when length(Missing_Orders.Sales_document) > 10 then coalesce(dim_subscription_dylan.dylan_order_number,Missing_Orders.Sales_document,'NA')
             else coalesce(dim_subscription_sales_document.dylan_order_number,'NA') end dylan_order_number,
             'UNKNOWN' as sales_document_item,
             'UNKNOWN' as contract_id,
             'UNKNOWN' as Txns,
             'UNKNOWN' as route_to_market,
             'UNKNOWN' as PROMO_TYPE,
             'UNKNOWN' as cc_segment,
             'UNKNOWN' as product_name_description,
             'UNKNOWN' as product_group,
             'UNKNOWN' as sub_product_group,
             'UNKNONW' as stype,
             'UNKNOWN' as market_area_description,
             'UNKNOWN' as AgentMapFlag,
             'UNKNOWN' as cross_sell_team,
             'UNKNOWN' as cross_team_agent,
             'UNKNOWN' as RepLdap,
             upper(trim(Missing_Orders.RepLdap)) as created_ldap,
             coalesce(created_agent.Rep_name,created_tsm.TSM,'UNKNOWN') as RepName,
             'UNKNOWN' as attained_RepName,
             coalesce(created_agent.Rep_name,created_tsm.TSM,'UNKNOWN') as created_RepName,
             coalesce(created_agent.TSM,created_tsm.Manager,'UNKNOWN') as RepTSM,
             'UNKNOWN' as attained_TSM,
             coalesce(created_agent.TSM,created_tsm.Manager,'UNKNOWN') as created_TSM,
             coalesce(created_agent.Manager,created_tsm.Manager,'UNKNOWN') as RepManger,
             'UNKNOWN' as attained_manager,
             coalesce(created_agent.Manager,created_tsm.Manager,'UNKNOWN') as created_manager,
             coalesce(created_agent.Standard_team_name,created_tsm.Standard_team_name,'UNKNOWN') as repteam,
             'UNKNOWN' as attained_team,
             coalesce(created_agent.Standard_team_name,created_tsm.Standard_team_name,'UNKNOWN') as created_team,
             coalesce(created_agent.Team_Category,created_tsm.Team_Category,'UNKNOWN') as Team_Category,
             'UNKNOWN' as customer_email_domain,
             'UNKNOWN' as acct_name,
             0 as net_purchases_arr_cfx_attained,
             0 as net_cancelled_arr_cfx_attained,
             0 as reactivated_arr_cfx_attained,
             0 as net_migration_arr_cfx_attained,
             0 as returns_arr_cfx_attained,
             0 as net_new_subs_attained,
             0 as gross_new_subs_attained,
             0 as net_purchases_arr_cfx_created,
             0 as net_cancelled_arr_cfx_created,
             0 as reactivated_arr_cfx_created,
             0 as net_migration_arr_cfx_created,
             0 as returns_arr_cfx_created,
             0 as net_new_subs_created,
             0 as gross_new_subs_created,
             'Missing Orders' as flag
             from(
             (select 
             date_date,
             RepLdap,
             SALES_DOCUMENT,
             FISCAL_YR_AND_QTR_DESC,
             FISCAL_YR_AND_WK_DESC
             from(
             select * from {TMP_TBL}
             where flag = 'Hendrix_Extra' and FISCAL_YR_AND_QTR_DESC >= '{curr_qtr}' ) where fiscal_yr_and_qtr_desc <> 'None') Missing_Orders
             left outer join
             (
             select distinct sales_document, dylan_order_number from
             (select distinct sales_document, dylan_order_number,row_number() OVER(PARTITION BY dylan_order_number ORDER BY dylan_order_number) as rn 
             from {DIM_SUBSCRIPTION} where sales_document <> '')
             where rn = 1
             ) dim_subscription_dylan
             on upper(trim(Missing_Orders.SALES_DOCUMENT)) = upper(trim(dim_subscription_dylan.dylan_order_number))
             left outer join
             (
             select distinct sales_document, dylan_order_number from
             (select distinct sales_document, dylan_order_number,row_number() OVER(PARTITION BY dylan_order_number ORDER BY dylan_order_number) as rn 
             from {DIM_SUBSCRIPTION} where sales_document <> '')
             where rn = 1
             ) dim_subscription_sales_document
             on upper(trim(Missing_Orders.SALES_DOCUMENT)) = upper(trim(dim_subscription_sales_document.sales_document))
             left outer join
             (
             select distinct ldap,Rep_name,TSM,TSM_ldap,Manager,Quarter,Team_key,Team_Category,Standard_team_name,geo,Sales_Center,start_date,end_date from {AGENT_DETAILS}
             ) created_agent
             on upper(trim(Missing_Orders.RepLdap)) = upper(trim(created_agent.ldap)) and Missing_Orders.fiscal_yr_and_qtr_desc = created_agent.Quarter and Missing_Orders.date_date between created_agent.start_date and created_agent.end_date
             left outer join 
             (
             select distinct ldap,Rep_name,TSM,TSM_ldap,Manager,Quarter,Team_key,Team_Category,Standard_team_name,geo,Sales_Center,start_date,end_date from
             (Select  row_number() OVER(PARTITION BY TSM_ldap,Quarter ORDER BY Quarter) as rn,ldap,Rep_name,TSM,TSM_ldap,Manager,Quarter,Team_key,Team_Category,Standard_team_name,geo,Sales_Center,start_date,end_date from {AGENT_DETAILS_ACTIVE}) where rn=1
             ) created_tsm
             on upper(trim(Missing_Orders.RepLdap)) = upper(trim(created_tsm.TSM_ldap)) and Missing_Orders.fiscal_yr_and_qtr_desc = created_tsm.Quarter and Missing_Orders.date_date between created_tsm.start_date and created_tsm.end_date)""".format(AGENT_DETAILS = AGENT_DETAILS,TMP_TBL = TMP_TBL,DIM_SUBSCRIPTION = DIM_SUBSCRIPTION,curr_qtr = curr_qtr ,AGENT_DETAILS_ACTIVE = AGENT_DETAILS_ACTIVE))
             missing_orders_df.createOrReplaceTempView("missing_orders_df")

             union_df = spark.sql("""
             select * from attainment_df
             union all
             select * from created_df
             union all
             select * from missing_orders_df
             """)
             union_df.createOrReplaceTempView("union_df")

             final_df = spark.sql("""select
             date,
             FISCAL_YR_AND_WK_DESC, 
             FISCAL_YR_AND_QTR_DESC,
             cc_phone_vs_web,
             Geo,
             Sales_Center,
             Sales_document,
             dylan_order_number,
             sales_document_item,
             contract_id,
             Txns,
             route_to_market,
             PROMO_TYPE,
             cc_segment,
             product_name_description,
             product_group,
             sub_product_group,
             stype,
             market_area_description,
             AgentMapFlag,
             cross_sell_team,
             cross_team_agent,
             RepLdap,
             created_ldap,
             RepName,
             attained_RepName,
             created_RepName,
             RepTSM,
             attained_TSM,
             created_TSM,
             RepManger,
             attained_manager,
             created_manager,
             repteam,
             attained_team,
             created_team,
             Team_Category,
             flag,
             customer_email_domain,
             acct_name,

             sum(net_purchases_arr_cfx_attained) as net_purchases_arr_cfx_attained,
             sum(net_cancelled_arr_cfx_attained) as net_cancelled_arr_cfx_attained,
             sum(reactivated_arr_cfx_attained) as reactivated_arr_cfx_attained,
             sum(net_migration_arr_cfx_attained) as net_migration_arr_cfx_attained,
             sum(returns_arr_cfx_attained) as returns_arr_cfx_attained,
             sum(net_new_subs_attained) as net_new_subs_attained,
             sum(gross_new_subs_attained) as gross_new_subs_attained,
             sum(net_purchases_arr_cfx_created) as net_purchases_arr_cfx_created,
             sum(net_cancelled_arr_cfx_created) as net_cancelled_arr_cfx_created,
             sum(reactivated_arr_cfx_created) as reactivated_arr_cfx_created,
             sum(net_migration_arr_cfx_created) as net_migration_arr_cfx_created,
             sum(returns_arr_cfx_created) as returns_arr_cfx_created,
             sum(net_new_subs_created) as net_new_subs_created,
             sum(gross_new_subs_created) as gross_new_subs_created
             from union_df 
             group by
             date,
             FISCAL_YR_AND_WK_DESC, 
             FISCAL_YR_AND_QTR_DESC,
             cc_phone_vs_web,
             Geo,
             Sales_Center,
             Sales_document,
             dylan_order_number,
             sales_document_item,
             contract_id,
             Txns,
             route_to_market,
             PROMO_TYPE,
             cc_segment,
             product_name_description,
             product_group,
             sub_product_group,
             stype,
             market_area_description,
             AgentMapFlag,
             cross_sell_team,
             cross_team_agent,
             RepLdap,
             created_ldap,
             RepName,
             attained_RepName,
             created_RepName,
             RepTSM,
             attained_TSM,
             created_TSM,
             RepManger,
             attained_manager,
             created_manager,
             repteam,
             attained_team,
             created_team,
             Team_Category,
             flag,
             customer_email_domain,
             acct_name
             """)
             final_df.createOrReplaceTempView("final_df")

             final_df_insert = spark.sql("""
             select 
             final_df.date,
             final_df.cc_phone_vs_web,
             final_df.Geo,
             final_df.Sales_Center,
             final_df.Sales_document,
             final_df.dylan_order_number,
             final_df.sales_document_item,
             final_df.contract_id,
             final_df.Txns,
             final_df.route_to_market,
             final_df.PROMO_TYPE,
             final_df.cc_segment,
             final_df.product_name_description,
             final_df.product_group,
             final_df.sub_product_group,
             final_df.stype,
             final_df.market_area_description,
             final_df.AgentMapFlag,
             final_df.cross_sell_team,
             final_df.cross_team_agent,
             final_df.RepLdap,
             final_df.created_ldap,
             final_df.RepName,
             final_df.created_RepName,
             final_df.attained_RepName,
             final_df.RepTSM,
             final_df.attained_TSM,
             final_df.created_TSM,
             final_df.RepManger,
             final_df.attained_manager,
             final_df.created_manager,
             coalesce(Managerldap_repmanager.Manager_ldap,'UNKNOWN') as rep_manager_ldap,
             coalesce(Managerldap_attained.Manager_ldap,'UNKNOWN') as attained_manager_ldap,
             coalesce(Managerldap_created.Manager_ldap,'UNKNOWN') as created_manager_ldap,
             final_df.repteam,
             final_df.attained_team,
             final_df.created_team,
             coalesce(dim_team_display_team_category_attained.Display_Team_Category,'UNKNOWN') as display_team_category_attained,
             coalesce(dim_team_display_team_category_created.Display_Team_Category,'UNKNOWN') as display_team_category_created,
             final_df.Team_Category,
             final_df.flag,
             final_df.customer_email_domain,
             final_df.acct_name,
             final_df.net_purchases_arr_cfx_attained,
             final_df.net_cancelled_arr_cfx_attained,
             final_df.reactivated_arr_cfx_attained,
             final_df.net_migration_arr_cfx_attained,
             final_df.returns_arr_cfx_attained,
             final_df.net_new_subs_attained,
             final_df.gross_new_subs_attained,
             final_df.net_purchases_arr_cfx_created,
             final_df.net_cancelled_arr_cfx_created,
             final_df.reactivated_arr_cfx_created,
             final_df.net_migration_arr_cfx_created,
             final_df.returns_arr_cfx_created,
             final_df.net_new_subs_created,
             final_df.gross_new_subs_created,
             SUM(CASE WHEN final_df.txns in ('Rep-Add on','Initial Purchase','UNKNOWN') then final_df.net_purchases_arr_cfx_attained else 0 END) OVER (partition by sales_document,product_group,FISCAL_YR_AND_WK_DESC ORDER BY sales_document) as net_purchases_deal_band_arr_item,
             SUM(CASE WHEN final_df.txns in ('Rep-Add on','Initial Purchase','UNKNOWN') then final_df.net_cancelled_arr_cfx_attained else 0 END) OVER (partition by sales_document,product_group,FISCAL_YR_AND_WK_DESC ORDER BY sales_document) as net_cancelled_band_arr_item,
             SUM(CASE WHEN final_df.txns in ('Rep-Add on','Initial Purchase','UNKNOWN') then final_df.net_purchases_arr_cfx_attained else 0 END) OVER (partition by sales_document,FISCAL_YR_AND_WK_DESC ORDER BY sales_document) as net_purchases_deal_band_arr_overall,
             SUM(CASE WHEN final_df.txns in ('Rep-Add on','Initial Purchase','UNKNOWN') then final_df.net_cancelled_arr_cfx_attained else 0 END) OVER (partition by sales_document,FISCAL_YR_AND_WK_DESC ORDER BY sales_document) as net_cancelled_band_arr_overall,
             row_number() OVER (partition by sales_document,product_group,FISCAL_YR_AND_WK_DESC ORDER BY sales_document) as band_row_num,
             from_utc_timestamp(current_timestamp(), 'America/Los_Angeles') as refresh_date,
             final_df.FISCAL_YR_AND_QTR_DESC,
             final_df.FISCAL_YR_AND_WK_DESC
             from final_df 
             left outer join 
             (select distinct Standard_team_name,Display_Team_Category from {DIM_TEAM}) dim_team_display_team_category_attained
             on upper(trim(final_df.repteam)) = upper(trim(dim_team_display_team_category_attained.Standard_team_name))
             left outer join 
             (select distinct Standard_team_name,Display_Team_Category from {DIM_TEAM}) dim_team_display_team_category_created
             on upper(trim(final_df.created_team)) = upper(trim(dim_team_display_team_category_created.Standard_team_name))
             left outer join
             (
             select distinct Manager,Manager_ldap,quarter from {AGENT_DETAILS_ACTIVE}
             ) Managerldap_attained
             on upper(trim(final_df.attained_manager)) = upper(trim(Managerldap_attained.Manager)) and final_df.fiscal_yr_and_qtr_desc = Managerldap_attained.Quarter
             left outer join
             (
             select distinct Manager,Manager_ldap,quarter from {AGENT_DETAILS_ACTIVE}
             ) Managerldap_created
             on upper(trim(final_df.created_manager)) = upper(trim(Managerldap_created.Manager)) and final_df.fiscal_yr_and_qtr_desc = Managerldap_created.Quarter
             left outer join
             (
             select distinct Manager,Manager_ldap,quarter from {AGENT_DETAILS_ACTIVE}
             ) Managerldap_repmanager
             on upper(trim(final_df.RepManger)) = upper(trim(Managerldap_repmanager.Manager)) and final_df.fiscal_yr_and_qtr_desc = Managerldap_repmanager.Quarter 
             """.format(DIM_TEAM = DIM_TEAM,AGENT_DETAILS=AGENT_DETAILS,AGENT_DETAILS_ACTIVE = AGENT_DETAILS_ACTIVE))
             final_df_insert.createOrReplaceTempView("final_df_insert")

             final_df_with_deal_band = spark.sql("""
             select 
             date,
             cc_phone_vs_web,
             Geo,
             Sales_Center,
             Sales_document,
             dylan_order_number,
             sales_document_item,
             contract_id,
             Txns,
             route_to_market,
             PROMO_TYPE,
             cc_segment,
             product_name_description,
             product_group,
             sub_product_group,
             stype,
             market_area_description,
             AgentMapFlag,
             cross_sell_team,
             cross_team_agent,
             RepLdap,
             created_ldap,
             RepName,
             created_RepName,
             attained_RepName,
             RepTSM,
             attained_TSM,
             created_TSM,
             RepManger,
             attained_manager,
             created_manager,
             rep_manager_ldap,
             attained_manager_ldap,
             created_manager_ldap,
             repteam,
             attained_team,
             created_team,
             display_team_category_attained,
             display_team_category_created,
             Team_Category,
             flag,
             customer_email_domain,
             acct_name,
             net_purchases_arr_cfx_attained,
             net_cancelled_arr_cfx_attained,
             reactivated_arr_cfx_attained,
             net_migration_arr_cfx_attained,
             returns_arr_cfx_attained,
             net_new_subs_attained,
             gross_new_subs_attained,
             net_purchases_arr_cfx_created,
             net_cancelled_arr_cfx_created,
             reactivated_arr_cfx_created,
             net_migration_arr_cfx_created,
             returns_arr_cfx_created,
             net_new_subs_created,
             gross_new_subs_created,
             case when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_purchases_deal_band_arr_item <=2500 then '<=2500'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_purchases_deal_band_arr_item >2500 and net_purchases_deal_band_arr_item <=5000 then '2500-5000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_purchases_deal_band_arr_item >5000 and net_purchases_deal_band_arr_item <=10000 then '5000-10000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_purchases_deal_band_arr_item >10000 and net_purchases_deal_band_arr_item <=20000 then '10000-20000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_purchases_deal_band_arr_item >20000 and net_purchases_deal_band_arr_item <=50000 then '20000-50000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_purchases_deal_band_arr_item >50000  then '>50000'
             else 'UNKNOWN'
             end net_purchases_deal_band_item,
             case when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_cancelled_band_arr_item <=2500 then '<=2500'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_cancelled_band_arr_item >2500 and net_cancelled_band_arr_item <=5000 then '2500-5000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_cancelled_band_arr_item >5000 and net_cancelled_band_arr_item <=10000 then '5000-10000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_cancelled_band_arr_item >10000 and net_cancelled_band_arr_item <=20000 then '10000-20000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_cancelled_band_arr_item >20000 and net_cancelled_band_arr_item <=50000 then '20000-50000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_cancelled_band_arr_item >50000  then '>50000'
             else 'UNKNOWN'
             end net_cancelled_deal_band_item,
             net_purchases_deal_band_arr_overall,
             net_cancelled_band_arr_overall,
             band_row_num,
             case when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_purchases_deal_band_arr_overall <=2500 then '<=2500'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_purchases_deal_band_arr_overall >2500 and net_purchases_deal_band_arr_overall <=5000 then '2500-5000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_purchases_deal_band_arr_overall >5000 and net_purchases_deal_band_arr_overall <=10000 then '5000-10000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_purchases_deal_band_arr_overall >10000 and net_purchases_deal_band_arr_overall <=20000 then '10000-20000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_purchases_deal_band_arr_overall >20000 and net_purchases_deal_band_arr_overall <=50000 then '20000-50000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_purchases_deal_band_arr_overall >50000  then '>50000'
             else 'UNKNOWN'
             end net_purchases_deal_band_overall,
             case when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_cancelled_band_arr_overall <=2500 then '<=2500'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_cancelled_band_arr_overall >2500 and net_cancelled_band_arr_overall <=5000 then '2500-5000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_cancelled_band_arr_overall >5000 and net_cancelled_band_arr_overall <=10000 then '5000-10000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_cancelled_band_arr_overall >10000 and net_cancelled_band_arr_overall <=20000 then '10000-20000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_cancelled_band_arr_overall >20000 and net_cancelled_band_arr_overall <=50000 then '20000-50000'
             when txns in ('Rep-Add on','Initial Purchase','UNKNOWN') and net_cancelled_band_arr_overall >50000  then '>50000'
             else 'UNKNOWN'
             end net_cancelled_deal_band_overall,
             refresh_date,
             FISCAL_YR_AND_QTR_DESC,
             FISCAL_YR_AND_WK_DESC
             from final_df_insert
             """)
             final_df_with_deal_band.createOrReplaceTempView("final_df_with_deal_band")

             
             final_df_with_deal_band.repartition("FISCAL_YR_AND_QTR_DESC","FISCAL_YR_AND_WK_DESC").write.format("parquet").mode("overwrite").insertInto("%s"%TGT1_TBL,overwrite=True)
             spark.sql(f"msck repair table {TGT1_TBL}")

  
             try:
                 dbutils.notebook.exit("SUCCESS")   
             except Exception as e:                 
                 print("exception:",e)
        except Exception as e:
             dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()
