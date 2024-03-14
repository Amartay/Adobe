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
             dbutils.widgets.text("OPPORTUNITY_TBL", "")
             dbutils.widgets.text("USER_TBL", "")
             dbutils.widgets.text("LEAD_TBL", "")
             dbutils.widgets.text("CAMPAIGN_TBL", "")
             dbutils.widgets.text("TEAM_TBL", "")
             dbutils.widgets.text("SALES_ITEM", "")
             dbutils.widgets.text("AGENT_TBL_SCD", "")

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
             OPPORTUNITY_TBL = dbutils.widgets.get("OPPORTUNITY_TBL")
             USER_TBL = dbutils.widgets.get("USER_TBL")
             LEAD_TBL = dbutils.widgets.get("LEAD_TBL")
             CAMPAIGN_TBL = dbutils.widgets.get("CAMPAIGN_TBL")
             TEAM_TBL = dbutils.widgets.get("TEAM_TBL")
             SALES_ITEM = dbutils.widgets.get("SALES_ITEM")
             AGENT_TBL_SCD = dbutils.widgets.get("AGENT_TBL_SCD")


             Set_list = Settings.split(',')
             if len(Set_list)>0:
                 for i in Set_list:
                     if i != "":
                         print("spark.sql(+i+)")
                         spark.sql("""{i}""".format(i=i))
             
             #FETCH CURRENT QTR

             df_curr_qtr = spark.sql("""select fiscal_yr_and_qtr_desc from {DATE_TBL} where date_date = {TO_DT}""".format(FROM_DT=FROM_DT,TO_DT=TO_DT,DATE_TBL=DATE_TBL))
             curr_qtr=df_curr_qtr.first()['fiscal_yr_and_qtr_desc']
             
             df_NPARR = spark.sql ("""
                        SELECT SALES_DOCUMENT,sales_document_item,FISCAL_YR_AND_WK_DESC,date_date,sum(net_purchases_arr_cfx) as NPARR from {TGT_TBL} where flag != 'Hendrix_Extra' and FISCAL_YR_AND_QTR_DESC = '{curr_qtr}' and init_purchase_arr_cfx=0
                        GROUP BY SALES_DOCUMENT,sales_document_item,FISCAL_YR_AND_WK_DESC,date_date
                        HAVING NPARR>0 
                        """.format(TGT_TBL=TGT_TBL,curr_qtr=curr_qtr))
             df_NPARR.createOrReplaceTempView("NPARR")

             df_NCARR = spark.sql ("""
                        SELECT SALES_DOCUMENT,sales_document_item,FISCAL_YR_AND_WK_DESC,date_date,sum(net_cancelled_arr_cfx) as NCARR from {TGT_TBL} where flag != 'Hendrix_Extra' and FISCAL_YR_AND_QTR_DESC = '{curr_qtr}'
                        GROUP BY SALES_DOCUMENT,sales_document_item,FISCAL_YR_AND_WK_DESC,date_date
                        HAVING NCARR>0
                        """.format(TGT_TBL=TGT_TBL,curr_qtr=curr_qtr))

             df_NCARR.createOrReplaceTempView("NCARR")

             df_same_wk_cncl_list = spark.sql ("""
                select * from
                (
                SELECT NP.* from NPARR NP
                INNER JOIN NCARR NC
                ON NP.SALES_DOCUMENT = NC.SALES_DOCUMENT and
                   NP.sales_document_item = NC.sales_document_item
                   where
                   NP.FISCAL_YR_AND_WK_DESC = NC.FISCAL_YR_AND_WK_DESC
                UNION
                SELECT NP.* from NPARR NP
                INNER JOIN NCARR NC
                ON NP.SALES_DOCUMENT = NC.SALES_DOCUMENT and
                   NP.sales_document_item = NC.sales_document_item
                   where
                    NC.date_date BETWEEN NP.date_date and date_add(NP.date_date,7 )
                    and NP.FISCAL_YR_AND_WK_DESC <> NC.FISCAL_YR_AND_WK_DESC
                )
                order by sales_document,sales_document_item""")
             df_same_wk_cncl_list.createOrReplaceTempView("df_same_wk_cncl_list")


             df_tmp = spark.sql ("""
					SELECT tmp.*, case when cncl.sales_document is not null then 'Y' 
					else 'N'
					end as same_wk_cncl_flag
					from 
					(select * from {TGT_TBL} where flag != 'Hendrix_Extra' and FISCAL_YR_AND_QTR_DESC = '{curr_qtr}') tmp
					left outer join
					df_same_wk_cncl_list cncl
					on tmp.sales_document = cncl.sales_document
					and tmp.sales_document_item = cncl.sales_document_item
                     """.format(TGT_TBL=TGT_TBL,curr_qtr=curr_qtr))
             df_tmp.createOrReplaceTempView("df_tmp")
             
             df_tmp_final = spark.sql ("""
				SELECT 
                tmp.sales_document_item,
                case when upper(CREATED_BY) in ('B2BCPRD','B2BPRD','WAS') and (sales_item.RESELLER_PO_NUMBER is not null and trim(sales_item.RESELLER_PO_NUMBER) != '') then sales_item.RESELLER_PO_NUMBER
                else tmp.CREATED_BY
                end as CREATED_BY,
                tmp.CRM_CUSTOMER_GUID,
                tmp.ENTITLEMENT_TYPE,
                tmp.date_date,
                tmp.FISCAL_YR_AND_QTR_DESC,
                tmp.cc_phone_vs_web,
                tmp.GEO,
                tmp.MARKET_AREA,
                tmp.MARKET_SEGMENT,
                tmp.offer_type_description,
                tmp.PRODUCT_CONFIG,
                tmp.product_config_description,
                tmp.PRODUCT_NAME,
                tmp.product_name_description,
                tmp.PROMO_TYPE,
                tmp.PROMOTION,
                tmp.Region,
                tmp.SALES_DOCUMENT,
                tmp.DYLAN_ORDER_NUMBER,
                tmp.STYPE,
                tmp.SUBS_OFFER,
                tmp.SUBSCRIPTION_ACCOUNT_GUID,
                tmp.VIP_CONTRACT,
                tmp.addl_purchase_diff,
                tmp.addl_purchase_same,
                tmp.gross_cancel_arr_cfx,
                tmp.gross_new_arr_cfx,
                tmp.gross_new_subs,
                tmp.init_purchase_arr_cfx,
                tmp.migrated_from_arr_cfx,
                tmp.migrated_to_arr_cfx,
                tmp.net_cancelled_arr_cfx,
                tmp.net_new_arr_cfx,
                tmp.net_new_subs,
                tmp.net_purchases_arr_cfx,
                tmp.reactivated_arr_cfx,
                tmp.returns_arr_cfx,
                tmp.ecc_customer_id,
                tmp.SALES_DISTRICT,
                tmp.contract_start_date_veda,
                tmp.contract_end_date_veda,
                tmp.contract_id,
                tmp.renewal_from_arr_cfx,
                tmp.renewal_to_arr_cfx,
                tmp.projected_dme_gtm_segment, 
                tmp.gtm_acct_segment,
                tmp.route_to_market,
                tmp.cc_segment,
                tmp.net_new_arr,
                tmp.Sales_Center,
                tmp.AgentGeo,
                tmp.RepName,
                tmp.RepLdap,
                tmp.RepTSM,
                tmp.RepManger,
                tmp.RepTeam,
                tmp.flag,
                tmp.gross_cancellations,
                tmp.net_cancelled_subs,
                tmp.migrated_from,
                tmp.migrated_to,
                tmp.renewal_from,
                tmp.renewal_to,
                tmp.net_value_usd,
                tmp.stock_cred_pack,
                tmp.acct_name,
                tmp.customer_email_domain,
                tmp.payment_method,
                tmp.FISCAL_YR_AND_WK_DESC,
                tmp.same_wk_cncl_flag                
                from df_tmp tmp
                left outer join (select distinct SALES_DOCUMENT,SALES_DOCUMENT_ITEM,RESELLER_PO_NUMBER from {SALES_ITEM}) sales_item
                on tmp.SALES_DOCUMENT = sales_item.SALES_DOCUMENT
                and tmp.SALES_DOCUMENT_ITEM = sales_item.SALES_DOCUMENT_ITEM
             """.format(TGT_TBL=TGT_TBL,curr_qtr=curr_qtr,SALES_ITEM=SALES_ITEM))
             df_tmp_final.createOrReplaceTempView("df_tmp_final")
             

             df_sfdc = spark.sql(""" 
             select A.* ,
             B.name
             from
             (select distinct id as opportunity_id ,
             createddate as opportunity_created_date,
             createdbyid as sfdc_created_by_id,
             closedate as sfdc_closed_date,
             user.username as email,
             ecc_salesordernumber,
             coalesce(td_lead.campaignid,dim_oppo.campaignid) as campaignid
             from
             (select id , createddate , createdbyid ,closedate,
             case when length(ecc_salesordernumber) < 10 then lpad(ecc_salesordernumber,10,'0')
             else ecc_salesordernumber
             end as ecc_salesordernumber 
             from {OPPORTUNITY_TBL} 
             where createddate <= {TO_DT} and createddate > date_sub({TO_DT}, 180)
             and upper(stage)='CLOSED - BOOKED' 
             and ecc_salesordernumber is not null
             and as_of_date <= date_add({TO_DT},2 )
             --as_of_date <= '2023-07-09'
             ) oppo
             left outer join (select distinct username ,userid from {USER_TBL})user
             on oppo.createdbyid=user.userid
             left outer join (select distinct campaignid , opportunityid from
             b2b.uda_uda_sales_dbo_vw_td_opportunity )dim_oppo
             on oppo.id=dim_oppo.opportunityid
             left outer join (select distinct campaignid , convertedopportunityid from {LEAD_TBL} where convertedopportunityid is not null 
             and  as_of_date <= date_add({TO_DT},2 )
             --as_of_date <= '2023-07-09'
             )td_lead
             on oppo.id=td_lead.convertedopportunityid
             ) A
             left outer join (select distinct campaignid , name from {CAMPAIGN_TBL} where as_of_date <= date_add({TO_DT},2 )
             --as_of_date <= '2023-07-09'
             ) B
             on A.campaignid=B.campaignid
                --where ecc_salesordernumber = '7101602737'
             """.format(FROM_DT=FROM_DT,TO_DT=TO_DT,OPPORTUNITY_TBL=OPPORTUNITY_TBL,USER_TBL=USER_TBL,LEAD_TBL=LEAD_TBL,CAMPAIGN_TBL=CAMPAIGN_TBL))
             df_sfdc.createOrReplaceTempView("df_sfdc")

             df_sfdc_campaign = spark.sql(""" 
             select sfdc.*,coalesce(sfdc.campaignid,sfdc_fc.campaignid) as cum_campaignid
             from
             (                        
             select *, row_number() over (partition by ecc_salesordernumber order by opportunity_created_date) rn
             from df_sfdc 
             ) sfdc
             left outer join
             (                        
             select * from (select *, row_number() over (partition by ecc_salesordernumber order by opportunity_created_date) rn
             from df_sfdc where campaignid is not null) where rn = 1
             ) sfdc_fc
             on sfdc.ecc_salesordernumber = sfdc_fc.ecc_salesordernumber
             --where sfdc.rn >= sfdc_fc.rn
             """.format(FROM_DT=FROM_DT,TO_DT=TO_DT))
             df_sfdc_campaign.createOrReplaceTempView("df_sfdc_campaign")

             df_sfdc_final = spark.sql(""" 
             select 
             opportunity_id,
             opportunity_created_date,
             sfdc_created_by_id,
             sfdc_closed_date,
             email,
             ecc_salesordernumber,
             campaignid,
             name,
             cum_campaignid,
             min_date,
             dt1.fiscal_yr_and_qtr_desc as min_fiscal_yr_and_qtr_desc,
             dt1.fiscal_yr_and_wk_desc as min_fiscal_yr_and_wk_desc
             from
             (
                select * from 
                (
                select *,row_number() over (partition by ecc_salesordernumber order by opportunity_created_date) as rno, min(opportunity_created_date) over (partition by ecc_salesordernumber) as min_date from
                    (                        
                        select sfdc.*,dt.fiscal_yr_and_qtr_desc,dt.fiscal_yr_and_wk_desc 
                        from df_sfdc_campaign sfdc
                        left outer join {DATE_TBL} dt 
                        on sfdc.sfdc_closed_date = dt.date_date
                    ) where fiscal_yr_and_qtr_desc = '{curr_qtr}' 
                ) where  rno = 1
             )sfdc_final
             left outer join {DATE_TBL} dt1
             on sfdc_final.min_date = dt1.date_date 
             """.format(FROM_DT=FROM_DT,TO_DT=TO_DT,curr_qtr=curr_qtr,DATE_TBL=DATE_TBL))
             df_sfdc_final.createOrReplaceTempView("df_sfdc_final")

             df_agent_scd = spark.sql(""" 
             select *, max(Quarter) over (partition by trim(upper(Ldap))) as max_quarter 
             from {AGENT_TBL_SCD}
             where Quarter <= '{curr_qtr}'
             """.format(FROM_DT=FROM_DT,TO_DT=TO_DT,curr_qtr=curr_qtr,AGENT_TBL_SCD=AGENT_TBL_SCD))
             df_agent_scd.createOrReplaceTempView("df_agent_scd")
             
             df_agent = spark.sql(""" 
             select * from (select *, row_number() over (partition by trim(upper(Ldap)) order by Quarter desc) rn, max(Quarter) over (partition by trim(upper(Ldap))) as max_quarter 
             from {AGENT_TBL}
             where Quarter <= '{curr_qtr}'
             ) where rn=1
             """.format(FROM_DT=FROM_DT,TO_DT=TO_DT,curr_qtr=curr_qtr,AGENT_TBL=AGENT_TBL))
             df_agent.createOrReplaceTempView("df_agent")
             
             df_sc = spark.sql("""              
             select * from (select *, row_number() over (partition by trim(upper(Team_key)) order by Quarter desc) rn
             from (select distinct quarter,Team_key,sales_center from {AGENT_TBL})
             where Quarter <= '{curr_qtr}'
             ) where rn=1
             """.format(FROM_DT=FROM_DT,TO_DT=TO_DT,curr_qtr=curr_qtr,AGENT_TBL=AGENT_TBL))
             df_sc.createOrReplaceTempView("df_sc")

             df_tsm = spark.sql(""" 
             select * from (select *, row_number() over (partition by trim(upper(TSM_Ldap)) order by Quarter desc) rn, max(Quarter) over (partition by trim(upper(TSM_Ldap))) as max_quarter 
             from (select distinct TSM,TSM_Ldap,Manager,Final_team,Team_key,Sales_Center,Geo,Quarter from {AGENT_TBL})
             where Quarter <= '{curr_qtr}'
             ) where rn=1
             """.format(FROM_DT=FROM_DT,TO_DT=TO_DT,curr_qtr=curr_qtr,AGENT_TBL=AGENT_TBL))
             df_tsm.createOrReplaceTempView("df_tsm")

             #BOB
             df_bob_tmp = spark.sql(""" 
             select * from (select *,row_number() over (partition by Contract_ID order by Fiscal_yr_and_qtr_desc) rn from {BOB_TBL} where Fiscal_yr_and_qtr_desc = '{curr_qtr}') where rn=1 """.format(FROM_DT=FROM_DT,TO_DT=TO_DT,curr_qtr=curr_qtr,BOB_TBL=BOB_TBL))
             df_bob_tmp.createOrReplaceTempView("df_bob_tmp")
             
             df_l2_mgr_ldap= spark.sql(""" 
             select * from (select *,row_number() over (partition by team_key order by manager_ldap) rn from (select distinct quarter,team_key,manager_ldap,manager,TSM,TSM_Ldap,sales_center,geo from {AGENT_TBL} where quarter = '{curr_qtr}')) where rn=1 """.format(AGENT_TBL=AGENT_TBL,curr_qtr=curr_qtr))
             df_l2_mgr_ldap.createOrReplaceTempView("df_l2_mgr_ldap")
             
             df_bob = spark.sql(""" 
             select df_bob_tmp.*,mgr_ldap.manager_ldap,mgr_ldap.manager,mgr_ldap.sales_center as bob_sales_center,mgr_ldap.geo as bob_geo,mgr_ldap.TSM as bob_TSM,mgr_ldap.TSM_ldap as bob_TSM_ldap from df_bob_tmp
             left outer join df_l2_mgr_ldap mgr_ldap
             on df_bob_tmp.Team_key = mgr_ldap.team_key""".format(FROM_DT=FROM_DT,TO_DT=TO_DT,curr_qtr=curr_qtr,BOB_TBL=BOB_TBL))
             df_bob.createOrReplaceTempView("df_bob")
             
             
             df_negotiate = spark.sql(""" 
             select * from (select *,row_number() over (partition by Contract_ID order by Fiscal_yr_and_qtr_desc) rn from {LATAM_TBL} where Fiscal_yr_and_qtr_desc = '{curr_qtr}') where rn=1 """.format(FROM_DT=FROM_DT,TO_DT=TO_DT,curr_qtr=curr_qtr,LATAM_TBL=LATAM_TBL))
             df_negotiate.createOrReplaceTempView("df_negotiate")

             Base_with_Catagory = spark.sql(""" 
             select Base.*,
             sfdc.*,
             coalesce(trim(agentdetails_scd.Team_Category),trim(agentdetails.Team_Category)) as Agent_Category,
             coalesce(trim(agentdetails_scd.Final_Team),trim(agentdetails.Final_Team)) as Agent_Team,
             coalesce(trim(agentdetails_scd.Team_key),trim(agentdetails.Team_key)) as Agent_Team_Key,
             coalesce(trim(agentdetails_scd.Sales_Center),trim(agentdetails.Sales_Center)) as Agent_Sales_Center,
             coalesce(trim(agentdetails_scd.Rep_name),trim(agentdetails.Rep_name)) as Agent_RepName,
             coalesce(trim(agentdetails_scd.SFDC_Flag),trim(agentdetails.SFDC_Flag)) as Agent_SFDC_Flag,
             coalesce(trim(agentdetails_scd.CSAM_INIT_FLAG),trim(agentdetails.CSAM_INIT_FLAG)) as Agent_CSAM_INIT_FLAG,
             coalesce(trim(agentdetails_scd.max_quarter),trim(agentdetails.max_quarter)) as agent_max_quarter,
             case when coalesce(trim(agentdetails_scd.max_quarter),trim(agentdetails.max_quarter)) = '{curr_qtr}' then 'Y'
             else 'N'
             end as agent_curr_qtr,
             coalesce(trim(agentdetails_created_scd.Team_Category),trim(agentdetails_created.Team_Category)) as Created_By_Category,
             coalesce(trim(agentdetails_created_scd.Final_Team),trim(agentdetails_created.Final_Team)) as Created_By_Team,
             coalesce(trim(agentdetails_created_scd.Team_key),trim(agentdetails_created.Team_key)) as Created_By_Team_Key,
             coalesce(trim(agentdetails_created_scd.Sales_Center),trim(agentdetails_created.Sales_Center)) as Created_By_Sales_Center,
             coalesce(trim(agentdetails_created_scd.Rep_name),trim(agentdetails_created.Rep_name)) as Created_By_RepName,
             coalesce(trim(agentdetails_created_scd.SFDC_Flag),agentdetails_created.SFDC_Flag) as Created_By_SFDC_Flag,
             coalesce(agentdetails_created_scd.max_quarter,agentdetails_created.max_quarter) as created_max_quarter,
             coalesce(agentdetails_created_scd.CSAM_INIT_FLAG,agentdetails_created.CSAM_INIT_FLAG) as Created_By_CSAM_INIT_FLAG,
             trim(agentdetails_bob.Team_Category) as BOB_Category,
             agentdetails_bob.SFDC_Flag as BOB_SFDC_Flag,
             case when coalesce(agentdetails_created_scd.max_quarter,agentdetails_created.max_quarter) = '{curr_qtr}' then 'Y'
             else 'N'
             end as created_curr_qtr,
             trim(agentdetails_tsm.TSM_Ldap) as agent_TSM_Ldap,
             trim(agentdetails_created_tsm.TSM_Ldap) as created_TSM_Ldap,
             agentdetails_tsm.max_quarter as agent_max_quarter_TSM,
             agentdetails_created_tsm.max_quarter as created_max_quarter_TSM,
             trim(agentdetails_tsm.Final_team) as agent_TSM_team,
             trim(agentdetails_created_tsm.Final_team) as created_TSM_team,
             trim(agentdetails_tsm.Team_key) as agent_TSM_team_key,
             trim(agentdetails_created_tsm.Team_key) as created_TSM_team_key,
             trim(agentdetails_tsm.Sales_Center) as agent_TSM_Sales_Center,
             trim(agentdetails_created_tsm.Sales_Center) as created_TSM_Sales_Center,
             'NA' as agent_TSM_roster_key,
             'NA' as created_TSM_roster_key,
             case when agentdetails_tsm.TSM_Ldap is not null then 'Y'
             else 'N'
             end as agent_TSM_flag,
             case when agentdetails_created_tsm.TSM_Ldap is not null then 'Y'
             else 'N'
             end as created_TSM_flag,
             coalesce(tm_scd.ABD_Flag,tm.ABD_Flag) as ABD_Flag,
             coalesce(tm1_scd.ABD_Flag,tm1.ABD_Flag) as agent_ABD_Flag,
             tsm_tm.ABD_Flag as TSM_ABD_Flag,
             tsm_tm1.ABD_Flag as TSM_agent_ABD_Flag,
             BobContractID,
             trim(bob_ldap) as bob_ldap,
             trim(bob_team) as bob_team,
             case when trim(bob_name) = 'NOT_PRESENT' then agentdetails_bob.Rep_name else trim(bob_name) end as bob_name,
             trim(bob_team_key) as bob_team_key,
             exclusion_flag,
             bob_manager_ldap,
             bob_manager,
             bob_TSM,
             bob_sales_center,
             bob_geo,
             case when BobContract.BobContractID is null then 'N' else 'Y' end as Bob_Flag,
             case when sfdc.ecc_salesordernumber is null then 'NA'
             when sfdc.min_fiscal_yr_and_wk_desc <= Base.FISCAL_YR_AND_WK_DESC then 'Y'
             when datediff(min_date,date_date) <= 7 then 'Y'
             else 'N'
             end as SFDC_Flag,
             
             case when latam.Contract_ID is not null then 'Y'
             else 'N'
             end as Custom_Flag,
             trim(latam.Assignee_Name) as Custom_Rep_Name,
             trim(latam.LDAP) as Custom_LDAP,
             trim(latam.Base) as Custom_Team_Name,
             trim(latam.Team_Key) as Custom_Team_Key,
             bob_tm.ABD_Flag as bob_ABD_flag,
             lat_tm.ABD_Flag as lat_ABD_flag,
             coalesce(tm_sfdc.ABD_Flag,tm_sfdc_tsm.ABD_Flag,'NA') as sfdc_abd_flag,
             tm_sfdc.ABD_Flag as tm_sfdc_ABD_Flag,
             tm_sfdc_tsm.ABD_Flag as tm_sfdc_tsm_ABD_Flag
             --agentdetails_latam.agent_roster_key as custom_roster_key
             from df_tmp_final Base
             
             left outer join df_agent_scd agentdetails_scd 
             on 
             upper(agentdetails_scd.Ldap) = upper(Base.repLdap)
             and Base.date_date between agentdetails_scd.start_date and agentdetails_scd.end_date
             left outer join df_agent agentdetails
             on 
             upper(agentdetails.Ldap) = upper(Base.repLdap)
             
             left outer join df_agent_scd agentdetails_created_scd
             on 
             upper(agentdetails_created_scd.Ldap) = upper(Base.CREATED_BY)
             and Base.date_date between agentdetails_created_scd.start_date and agentdetails_created_scd.end_date
             left outer join df_agent agentdetails_created
             on 
             upper(agentdetails_created.Ldap) = upper(Base.CREATED_BY)
             left outer Join
             (
                select
                bob.Contract_ID as BobContractID,
                case when (upper(substr(bob.CSAM_Email, 0, instr(bob.CSAM_Email,'@')-1)) = 'NULL' or substr(bob.CSAM_Email, 0, instr(bob.CSAM_Email,'@')-1) is null or trim(substr(bob.CSAM_Email, 0, instr(bob.CSAM_Email,'@')-1)) = '') then bob.manager_ldap
                else substr(bob.CSAM_Email, 0, instr(bob.CSAM_Email,'@')-1)
                end as bob_ldap,
                bob.CSAM_Team as bob_team,
                case when upper(substr(bob.CSAM_Email, 0, instr(bob.CSAM_Email,'@')-1)) is not null and (upper(bob.CSAM_Name) = 'NULL' or bob.CSAM_Name is null or trim(bob.CSAM_Name) = '' ) then 'NOT_PRESENT'
                when (upper(bob.CSAM_Name) = 'NULL' or bob.CSAM_Name is null or trim(bob.CSAM_Name) = '' ) then bob.manager
                else CSAM_Name
                end as bob_name,
                bob.Team_key as bob_team_key,
                coalesce(exclusion_flag,'N') as exclusion_flag,
                bob.manager_ldap as bob_manager_ldap,
                bob.manager as bob_manager,
                bob.bob_TSM as bob_TSM,
                bob.bob_sales_center as bob_sales_center,
                bob.bob_geo as bob_geo
                from df_bob bob
             )BobContract 
             on Base.contract_id = BobContract.BoBContractId
             left outer join df_agent_scd agentdetails_bob
             on 
             upper(agentdetails_bob.Ldap) = upper(BobContract.bob_ldap)
             and Base.date_date between agentdetails_bob.start_date and agentdetails_bob.end_date
             left outer join
             df_sfdc_final sfdc
             on Base.SALES_DOCUMENT = sfdc.ecc_salesordernumber
             left outer join (select * from df_agent_scd where max_quarter = '{curr_qtr}') sfdc_agent
             on upper(trim(substr(sfdc.email,1,(instr(sfdc.email,'@')-1)))) = upper(trim(sfdc_agent.Ldap))
             and Base.date_date between sfdc_agent.start_date and sfdc_agent.end_date
             left outer join (select * from df_tsm where max_quarter = '{curr_qtr}') sfdc_tsm
             on upper(trim(substr(sfdc.email,1,(instr(sfdc.email,'@')-1)))) = upper(trim(sfdc_tsm.TSM_Ldap))
             left outer join (select distinct ABD_Flag,Team_key from b2b_phones.dim_team) tm_sfdc
             on sfdc_agent.Team_key = tm_sfdc.Team_key
             left outer join (select distinct ABD_Flag,Team_key from b2b_phones.dim_team) tm_sfdc_tsm
             on sfdc_tsm.Team_key = tm_sfdc_tsm.Team_key
             
             
             left outer join (select distinct ABD_Flag,Team_key from b2b_phones.dim_team) tm_scd
             on agentdetails_created_scd.Team_key = tm_scd.Team_key
                          and Base.date_date between agentdetails_created_scd.start_date and agentdetails_created_scd.end_date
             left outer join (select distinct ABD_Flag,Team_key from b2b_phones.dim_team) tm
             on agentdetails_created.Team_key = tm.Team_key
             left outer join (select distinct ABD_Flag,Team_key from b2b_phones.dim_team) tm1_scd
             on agentdetails_scd.Team_key = tm1_scd.Team_key
                          and Base.date_date between agentdetails_scd.start_date and agentdetails_scd.end_date
             left outer join (select distinct ABD_Flag,Team_key from b2b_phones.dim_team) tm1
             on agentdetails.Team_key = tm1.Team_key
             left outer join df_tsm agentdetails_tsm
             on upper(agentdetails_tsm.TSM_Ldap) = upper(Base.repLdap)
             left outer join df_tsm agentdetails_created_tsm 
             on upper(agentdetails_created_tsm.TSM_Ldap) = upper(Base.CREATED_BY)
             left outer join (select distinct ABD_Flag,Team_key from b2b_phones.dim_team) tsm_tm
             on agentdetails_created_tsm.Team_key = tsm_tm.Team_key
            left outer join (select distinct ABD_Flag,Team_key from b2b_phones.dim_team) tsm_tm1
             on agentdetails_tsm.Team_key = tsm_tm1.Team_key
             left outer join (select distinct ABD_Flag,Team_key from b2b_phones.dim_team) bob_tm
             on BobContract.bob_team_key = bob_tm.Team_key
            left outer join (select * from df_negotiate where upper(cc_segment) = 'SIGN' and fiscal_yr_and_qtr_desc = (select fiscal_yr_and_qtr_desc from {DATE_TBL} where date_date = {TO_DT}) ) latam
            on Base.contract_id = latam.Contract_ID
             left outer join (select distinct ABD_Flag,Team_key from b2b_phones.dim_team) lat_tm
             on latam.Team_key = lat_tm.Team_key
             """.format(FROM_DT=FROM_DT,TO_DT=TO_DT,curr_qtr=curr_qtr,TGT_TBL=TGT_TBL,DATE_TBL=DATE_TBL,LATAM_TBL=LATAM_TBL))
             Base_with_Catagory.createOrReplaceTempView("Base_with_Catagory")
 
             Base_with_init_bob_agent_map = spark.sql(""" 
                select 
                Base.sales_document_item as sales_document_item,
                Base.CREATED_BY as CREATED_BY,
                Base.CRM_CUSTOMER_GUID as CRM_CUSTOMER_GUID,
                Base.ENTITLEMENT_TYPE as ENTITLEMENT_TYPE,
                Base.date_date as date_date,
                Base.FISCAL_YR_AND_QTR_DESC as FISCAL_YR_AND_QTR_DESC,
                Base.cc_phone_vs_web as cc_phone_vs_web,
                Base.GEO as GEO,
                Base.MARKET_AREA as MARKET_AREA,
                Base.MARKET_SEGMENT as MARKET_SEGMENT,
                Base.offer_type_description as offer_type_description,
                Base.PRODUCT_CONFIG as PRODUCT_CONFIG,
                Base.product_config_description as product_config_description,
                Base.PRODUCT_NAME as PRODUCT_NAME,
                Base.product_name_description as product_name_description,
                Base.PROMO_TYPE as PROMO_TYPE,
                Base.PROMOTION as PROMOTION,
                Base.Region as Region,
                Base.SALES_DOCUMENT as SALES_DOCUMENT,
                Base.DYLAN_ORDER_NUMBER as DYLAN_ORDER_NUMBER,
                Base.STYPE as STYPE,
                Base.SUBS_OFFER as SUBS_OFFER,
                Base.SUBSCRIPTION_ACCOUNT_GUID as SUBSCRIPTION_ACCOUNT_GUID,
                Base.VIP_CONTRACT as VIP_CONTRACT,
                Base.addl_purchase_diff as addl_purchase_diff,
                Base.addl_purchase_same as addl_purchase_same,
                Base.gross_cancel_arr_cfx as gross_cancel_arr_cfx,
                Base.gross_new_arr_cfx as gross_new_arr_cfx,
                Base.gross_new_subs as gross_new_subs,
                Base.init_purchase_arr_cfx as init_purchase_arr_cfx,
                Base.migrated_from_arr_cfx as migrated_from_arr_cfx,
                Base.migrated_to_arr_cfx as migrated_to_arr_cfx,
                Base.net_cancelled_arr_cfx as net_cancelled_arr_cfx,
                Base.net_new_arr_cfx as net_new_arr_cfx,
                Base.net_new_subs as net_new_subs,
                case when stock_cred_pack = 'Y' then Base.net_value_usd else Base.net_purchases_arr_cfx end as net_purchases_arr_cfx,
                Base.reactivated_arr_cfx as reactivated_arr_cfx,
                Base.returns_arr_cfx as returns_arr_cfx,
                Base.ecc_customer_id as ecc_customer_id,
                Base.SALES_DISTRICT as SALES_DISTRICT,
                Base.contract_start_date_veda as contract_start_date_veda,
                Base.contract_end_date_veda as contract_end_date_veda,
                Base.contract_id as contract_id,
                Base.renewal_from_arr_cfx as renewal_from_arr_cfx,
                Base.renewal_to_arr_cfx as renewal_to_arr_cfx,
                Base.projected_dme_gtm_segment,
                Base.gtm_acct_segment,
                Base.route_to_market,
                Base.cc_segment,
                Base.net_new_arr,
                'None' as Sales_Center,
                'None' as AgentGeo,
                
                case when Custom_Flag = 'Y' then Custom_Rep_Name
                when cc_phone_vs_web = 'VIP-PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and BOB_Category = 'CSAM' and upper(bob_team) like '%PO%'  and Bob_Flag = 'Y' and BOB_SFDC_Flag = 'Y' then 'UNKNOWN'

                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and SFDC_Flag = 'Y' and Created_By_SFDC_Flag = 'Y' then Created_By_RepName
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and Created_By_SFDC_Flag = 'N' then Created_By_RepName
                
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' then bob_name
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' and Bob_Flag = 'Y' then 'UNKNOWN'
                
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' and sfdc_abd_flag = 'ABD' then bob_name
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and Bob_Flag = 'Y' and (SFDC_Flag != 'Y' or sfdc_abd_flag != 'ABD') then 'UNKNOWN'
                
                when cc_phone_vs_web = 'VIP-PHONE' and Bob_Flag = 'Y' then bob_name
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('INBOUND') and created_curr_qtr = 'Y'  then Created_By_RepName
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category = 'CSAM' and init_purchase_arr_cfx > 0  and created_curr_qtr = 'Y' then Created_By_RepName
                when cc_phone_vs_web = 'VIP-PHONE' and ABD_Flag = 'ABD' and upper(Created_By_Team) like '%PO%' and created_curr_qtr = 'Y' then Created_By_RepName
                when cc_phone_vs_web = 'VIP-PHONE' and upper(gtm_acct_segment) = 'MID-MARKET' and (ABD_Flag != 'ABD' or (ABD_Flag = 'ABD' and upper(Created_By_Team) like '%NOT IN BOB%')) then Created_By_RepName
                when cc_phone_vs_web = 'VIP-PHONE' and upper(gtm_acct_segment) = 'MID-MARKET' then Created_By_RepName
                when cc_phone_vs_web = 'VIP-PHONE' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and BOB_Category = 'CSAM' and upper(bob_team) like '%PO%'  and Bob_Flag = 'Y' and BOB_SFDC_Flag = 'Y' then 'UNKNOWN'
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Bob_Flag = 'Y' then bob_name
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Created_By_Category = 'CSAM' and created_curr_qtr = 'Y' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Created_By_Category in ('INBOUND','MQL') and created_curr_qtr = 'Y' and Bob_Flag = 'N' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_max_quarter < '{curr_qtr}'  and ABD_Flag = 'ABD' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_max_quarter < '{curr_qtr}'  and ABD_Flag != 'ABD' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}' and TSM_ABD_Flag = 'ABD' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}'  and TSM_ABD_Flag != 'ABD' then Created_By_RepName
                when cc_phone_vs_web = 'VIP-PHONE' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' then Agent_RepName
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' then Agent_RepName
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_name
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'UNKNOWN'
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM'  and Bob_Flag = 'Y' and  bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' then Agent_RepName
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key then bob_name
                when cc_phone_vs_web = 'PHONE' and Bob_Flag = 'Y' then bob_name
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is not null and agent_curr_qtr = 'Y' then Agent_RepName
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and Created_By_RepName is not null  and created_curr_qtr = 'Y' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and created_max_quarter < '{curr_qtr}' and (Created_By_Team is not null and upper(Created_By_Team) != 'NULL' and trim(Created_By_Team) != '') then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' then Created_By_RepName                
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is null  and created_curr_qtr = 'Y' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and coalesce(init_purchase_arr_cfx,0) = 0 and (Base.FISCAL_YR_AND_QTR_DESC = subs.Fiscal_yr_and_qtr_desc)  and created_curr_qtr = 'Y' then Created_By_RepName                			 
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' then Agent_RepName
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and Agent_Category in ('CSAM','MQL') and RepLdap is not null and agent_curr_qtr = 'Y' then Agent_RepName
                when cc_phone_vs_web = 'PHONE' and Created_By_Category = 'CSAM' and RepLdap is null  and created_curr_qtr = 'Y' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' then Created_By_RepName
                when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and BOB_Category = 'CSAM' and upper(bob_team) like '%PO%'  and Bob_Flag = 'Y' and BOB_SFDC_Flag = 'Y' then 'UNKNOWN'
                when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Bob_Flag = 'Y' and exclusion_flag = 'N' then bob_name
                when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Bob_Flag = 'N' then 'NON BOB A.COM AGENT'
                when cc_phone_vs_web = 'WEB' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' then Agent_RepName
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' then Agent_RepName
                when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_name
                when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'UNKNOWN'
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' then Agent_RepName
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key then bob_name
                when cc_phone_vs_web = 'WEB' and Bob_Flag = 'Y' and exclusion_flag = 'N' then bob_name
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and init_purchase_arr_cfx > 0 and Agent_CSAM_INIT_FLAG = 'Y' and agent_curr_qtr = 'Y' then Agent_RepName   
                when cc_phone_vs_web = 'WEB' and agent_TSM_Flag = 'Y' then Agent_RepName
                when cc_phone_vs_web = 'WEB' and created_TSM_flag = 'Y' then Created_By_RepName
                when cc_phone_vs_web = 'WEB' then Created_By_RepName              
                else 'UNKNOWN'
                end as RepName,	
                
                Base.RepLdap  as RepLdap,
                
                
                case when Custom_Flag = 'Y' then Custom_LDAP
                when cc_phone_vs_web = 'VIP-PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and BOB_Category = 'CSAM' and upper(bob_team) like '%PO%'  and Bob_Flag = 'Y' and BOB_SFDC_Flag = 'Y' then 'UNKNOWN'
                
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and SFDC_Flag = 'Y' and Created_By_SFDC_Flag = 'Y' then CREATED_BY
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and Created_By_SFDC_Flag = 'N' then CREATED_BY
                
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y'  then bob_ldap
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' and Bob_Flag = 'Y' then 'UNKNOWN'
                
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' and sfdc_abd_flag = 'ABD' then bob_ldap
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and Bob_Flag = 'Y' and (SFDC_Flag != 'Y' or sfdc_abd_flag != 'ABD') then 'UNKNOWN'
                
                when cc_phone_vs_web = 'VIP-PHONE' and Bob_Flag = 'Y' then bob_ldap
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('INBOUND') and created_curr_qtr = 'Y' then CREATED_BY
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category = 'CSAM' and init_purchase_arr_cfx > 0 and created_curr_qtr = 'Y' then CREATED_BY
                when cc_phone_vs_web = 'VIP-PHONE' and ABD_Flag = 'ABD' and upper(Created_By_Team) like '%PO%' and created_curr_qtr = 'Y' then CREATED_BY
                when cc_phone_vs_web = 'VIP-PHONE' and upper(gtm_acct_segment) = 'MID-MARKET' and (ABD_Flag != 'ABD' or (ABD_Flag = 'ABD' and upper(Created_By_Team) like '%NOT IN BOB%')) then CREATED_BY
                when cc_phone_vs_web = 'VIP-PHONE' and upper(gtm_acct_segment) = 'MID-MARKET' then CREATED_BY
				when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and BOB_Category = 'CSAM' and upper(bob_team) like '%PO%'  and Bob_Flag = 'Y' and BOB_SFDC_Flag = 'Y' then 'UNKNOWN'
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Bob_Flag = 'Y' then bob_ldap
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Created_By_Category = 'CSAM'  and created_curr_qtr = 'Y' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Created_By_Category in ('INBOUND','MQL') and created_curr_qtr = 'Y' and Bob_Flag = 'N' then CREATED_BY
                when cc_phone_vs_web = 'VIP-PHONE' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_max_quarter < '{curr_qtr}'  and ABD_Flag = 'ABD' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_max_quarter < '{curr_qtr}'  and ABD_Flag != 'ABD' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}' and TSM_ABD_Flag = 'ABD' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}'  and TSM_ABD_Flag != 'ABD' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' then RepLdap
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' then RepLdap
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_ldap
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'UNKNOWN'
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM'  and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' then RepLdap
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key then bob_ldap
                when cc_phone_vs_web = 'PHONE' and Bob_Flag = 'Y' then bob_ldap
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is not null  and agent_curr_qtr = 'Y' then RepLdap
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and Created_By_RepName is not null and created_curr_qtr = 'Y' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and created_max_quarter < '{curr_qtr}' and (Created_By_Team is not null and upper(Created_By_Team) != 'NULL' and trim(Created_By_Team) != '') then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' then CREATED_BY                                
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is null and created_curr_qtr = 'Y' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and coalesce(init_purchase_arr_cfx,0) = 0 and (Base.FISCAL_YR_AND_QTR_DESC = subs.Fiscal_yr_and_qtr_desc) and created_curr_qtr = 'Y' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' then RepLdap
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and Agent_Category in ('CSAM','MQL') and RepLdap is not null and agent_curr_qtr = 'Y' then RepLdap
                when cc_phone_vs_web = 'PHONE' and Created_By_Category = 'CSAM' and RepLdap is null and created_curr_qtr = 'Y' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' then CREATED_BY
                when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and BOB_Category = 'CSAM' and upper(bob_team) like '%PO%'  and Bob_Flag = 'Y' and BOB_SFDC_Flag = 'Y' then 'UNKNOWN'
                when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Bob_Flag = 'Y' and exclusion_flag = 'N' then bob_ldap
                when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Bob_Flag = 'N' then 'NON BOB A.COM AGENT'
                when cc_phone_vs_web = 'WEB' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N'))  and agent_curr_qtr = 'Y' then RepLdap
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' then RepLdap
				when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_ldap
                when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'UNKNOWN'
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' then RepLdap
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key then bob_ldap
                when cc_phone_vs_web = 'WEB' and Bob_Flag = 'Y' and exclusion_flag = 'N' then bob_ldap
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and init_purchase_arr_cfx > 0 and Agent_CSAM_INIT_FLAG = 'Y' and agent_curr_qtr = 'Y' then RepLdap                				
				when cc_phone_vs_web = 'WEB' and agent_TSM_Flag = 'Y' then RepLdap
                when cc_phone_vs_web = 'WEB' and created_TSM_flag = 'Y' then CREATED_BY
                when cc_phone_vs_web = 'WEB' then CREATED_BY
                else 'UNKNOWN'
                end as con_RepLdap,
                'None' as RepTSM,
                'None' as RepManger,
                case when Custom_Flag = 'Y' then Custom_Team_Name
                when cc_phone_vs_web = 'VIP-PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and BOB_Category = 'CSAM' and upper(bob_team) like '%PO%'  and Bob_Flag = 'Y' and BOB_SFDC_Flag = 'Y' then 'OB-CSAM-SMB_PO_PENDING_SEATS-GBD'
                
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and SFDC_Flag = 'Y' and Created_By_SFDC_Flag = 'Y' then Created_By_Team
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and Created_By_SFDC_Flag = 'N' then Created_By_Team
                
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' then bob_team
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' and Bob_Flag = 'Y' then 'OB-CSAM-SMB_PO_PENDING_SEATS-GBD'
                
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' and sfdc_abd_flag = 'ABD' then bob_team
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and Bob_Flag = 'Y' and (SFDC_Flag != 'Y' or sfdc_abd_flag != 'ABD') then 'OB-CSAM-SMB_PO_PENDING_SEATS-GBD'
                
                when cc_phone_vs_web = 'VIP-PHONE' and Bob_Flag = 'Y' then bob_team
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('INBOUND') and created_curr_qtr = 'Y' then Created_By_Team
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category = 'CSAM' and init_purchase_arr_cfx > 0 and created_curr_qtr = 'Y' then Created_By_Team
                when cc_phone_vs_web = 'VIP-PHONE' and ABD_Flag = 'ABD' and upper(Created_By_Team) like '%PO%' and created_curr_qtr = 'Y' then Created_By_Team
                when cc_phone_vs_web = 'VIP-PHONE' and upper(gtm_acct_segment) = 'MID-MARKET' and (ABD_Flag != 'ABD' or (ABD_Flag = 'ABD' and upper(Created_By_Team) like '%NOT IN BOB%')) then 'Mid-Market'
                when cc_phone_vs_web = 'VIP-PHONE' and upper(gtm_acct_segment) = 'MID-MARKET' then 'Mid-Market'
                when cc_phone_vs_web = 'VIP-PHONE' and created_max_quarter < '{curr_qtr}'  and ABD_Flag != 'ABD' then Created_By_Team
                when cc_phone_vs_web = 'VIP-PHONE' then concat(Created_By_Sales_Center,' - NOT IN BOB')
				when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and BOB_Category = 'CSAM' and upper(bob_team) like '%PO%'  and Bob_Flag = 'Y' and BOB_SFDC_Flag = 'Y' then 'OB-CSAM-SMB_PO_PENDING_SEATS-GBD'
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Bob_Flag = 'Y' then bob_team
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Created_By_Category = 'CSAM' and created_curr_qtr = 'Y' then concat(Created_By_Sales_Center,' - NOT IN BOB')
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Created_By_Category in ('INBOUND','MQL') and created_curr_qtr = 'Y' and Bob_Flag = 'N' then Created_By_Team
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_max_quarter < '{curr_qtr}'  and ABD_Flag = 'ABD' then concat(Created_By_Sales_Center,' - NOT IN BOB')
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_max_quarter < '{curr_qtr}'  and ABD_Flag != 'ABD' then Created_By_Team
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' then created_TSM_team
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}' and TSM_ABD_Flag = 'ABD' then concat(created_TSM_Sales_Center,' - NOT IN BOB')
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}'  and TSM_ABD_Flag != 'ABD' then created_TSM_team
                when cc_phone_vs_web = 'PHONE' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' then Agent_Team
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' then Agent_Team
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_team
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'OB-CSAM-SMB_PO_PENDING_SEATS-GBD'
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' then Agent_Team
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key then bob_team
                
                when cc_phone_vs_web = 'PHONE' and Bob_Flag = 'Y' then bob_team
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is not null and agent_curr_qtr = 'Y' then Agent_Team                          
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and Created_By_RepName is not null and created_curr_qtr = 'Y' then Created_By_Team 
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and created_max_quarter < '{curr_qtr}' and (Created_By_Team is not null and upper(Created_By_Team) != 'NULL' and trim(Created_By_Team) != '') then Created_By_Team
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' then 'Customer Care'                               
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is null and created_curr_qtr = 'Y' then Created_By_Team
                when cc_phone_vs_web = 'PHONE' and coalesce(init_purchase_arr_cfx,0) = 0 and (Base.FISCAL_YR_AND_QTR_DESC = subs.Fiscal_yr_and_qtr_desc) and created_curr_qtr = 'Y' then Created_By_Team               
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM = '{curr_qtr}' then agent_TSM_team
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM < '{curr_qtr}' and TSM_agent_ABD_Flag = 'ABD' then concat(agent_TSM_Sales_Center,' - NOT IN BOB')
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM < '{curr_qtr}' and TSM_agent_ABD_Flag != 'ABD' then agent_TSM_team
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' and RepLdap is null then created_TSM_team
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}' and TSM_ABD_Flag = 'ABD' and RepLdap is null then concat(created_TSM_Sales_Center,' - NOT IN BOB')
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}'  and TSM_ABD_Flag != 'ABD' and RepLdap is null then created_TSM_team
                when cc_phone_vs_web = 'PHONE' and Agent_Category in ('CSAM','MQL') and RepLdap is not null and agent_curr_qtr = 'Y' then concat(Agent_Sales_Center,' - NOT IN BOB')
                when cc_phone_vs_web = 'PHONE' and Created_By_Category = 'CSAM' and RepLdap is null and created_curr_qtr = 'Y' then concat(Created_By_Sales_Center,' - NOT IN BOB')
                when cc_phone_vs_web = 'PHONE' and created_max_quarter < '{curr_qtr}'  and ABD_Flag = 'ABD' then concat(Created_By_Sales_Center,' - NOT IN BOB')
                when cc_phone_vs_web = 'PHONE' and created_max_quarter < '{curr_qtr}'  and ABD_Flag != 'ABD' then Created_By_Team
                --when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'CON%' and Created_By_Team is null then 'Customer Care' 
                when cc_phone_vs_web = 'PHONE' then Created_By_Team
				when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and BOB_Category = 'CSAM' and upper(bob_team) like '%PO%'  and Bob_Flag = 'Y' and BOB_SFDC_Flag = 'Y' then 'OB-CSAM-SMB_PO_PENDING_SEATS-GBD'
                when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Bob_Flag = 'Y' and exclusion_flag = 'N' then bob_team
                when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Bob_Flag = 'N' then 'NON BOB A.COM ACCOUNT'
                when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) then CREATED_BY
                when cc_phone_vs_web = 'WEB' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N'))  and agent_curr_qtr = 'Y' then Agent_Team
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y'  and agent_curr_qtr = 'Y' then Agent_Team
				when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_team
                when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'OB-CSAM-SMB_PO_PENDING_SEATS-GBD'
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y'  then Agent_Team
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key then bob_team
                when cc_phone_vs_web = 'WEB' and Bob_Flag = 'Y' and exclusion_flag = 'N' then bob_team
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and init_purchase_arr_cfx > 0 and Agent_CSAM_INIT_FLAG = 'Y'  and agent_curr_qtr = 'Y' then Agent_Team
                when cc_phone_vs_web = 'WEB' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM = '{curr_qtr}' then agent_TSM_team
                when cc_phone_vs_web = 'WEB' and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' and RepLdap is null then created_TSM_team                
                when cc_phone_vs_web = 'WEB' then 'WEB NOT APPLICABLE'
                else 'UNKNOWN'
                end as RepTeam,
                                            
                case when Custom_Flag = 'Y' then Custom_Team_Key
                when cc_phone_vs_web = 'VIP-PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and BOB_Category = 'CSAM' and upper(bob_team) like '%PO%'  and Bob_Flag = 'Y' and BOB_SFDC_Flag = 'Y' then md5('OB-CSAM-SMB_PO_PENDING_SEATS-GBD')
                
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and SFDC_Flag = 'Y' and Created_By_SFDC_Flag = 'Y' then Created_By_Team_Key
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and Created_By_SFDC_Flag = 'N' then Created_By_Team_Key
                
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' then bob_team_key
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' and Bob_Flag = 'Y' then md5('OB-CSAM-SMB_PO_PENDING_SEATS-GBD')
                
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' and sfdc_abd_flag = 'ABD' then bob_team_key
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and Bob_Flag = 'Y' and (SFDC_Flag != 'Y' or sfdc_abd_flag != 'ABD') then md5('OB-CSAM-SMB_PO_PENDING_SEATS-GBD')
                
                when cc_phone_vs_web = 'VIP-PHONE' and Bob_Flag = 'Y' then bob_team_key
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('INBOUND') and created_curr_qtr = 'Y' then Created_By_Team_Key
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category = 'CSAM' and init_purchase_arr_cfx > 0 and created_curr_qtr = 'Y' then Created_By_Team_Key
                when cc_phone_vs_web = 'VIP-PHONE' and ABD_Flag = 'ABD' and upper(Created_By_Team) like '%PO%' and created_curr_qtr = 'Y' then Created_By_Team_Key
                when cc_phone_vs_web = 'VIP-PHONE' and upper(gtm_acct_segment) = 'MID-MARKET' and (ABD_Flag != 'ABD' or (ABD_Flag = 'ABD' and upper(Created_By_Team) like '%NOT IN BOB%')) then md5('OB-MM- -MID_MARKET')
                when cc_phone_vs_web = 'VIP-PHONE' and upper(gtm_acct_segment) = 'MID-MARKET' then md5('OB-MM- -MID_MARKET')
				when cc_phone_vs_web = 'VIP-PHONE' and created_max_quarter < '{curr_qtr}'  and ABD_Flag != 'ABD' then Created_By_Team_Key
                when cc_phone_vs_web = 'VIP-PHONE' then md5(concat('NON_BOB- -',upper(Created_By_Sales_Center)))
				when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and BOB_Category = 'CSAM' and upper(bob_team) like '%PO%'  and Bob_Flag = 'Y' and BOB_SFDC_Flag = 'Y' then md5('OB-CSAM-SMB_PO_PENDING_SEATS-GBD')
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Bob_Flag = 'Y' then bob_team_key
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Created_By_Category = 'CSAM' and created_curr_qtr = 'Y' then md5(concat('NON_BOB- -',upper(Created_By_Sales_Center)))
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Created_By_Category in ('INBOUND','MQL') and created_curr_qtr = 'Y' and Bob_Flag = 'N' then Created_By_Team_Key
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_max_quarter < '{curr_qtr}'  and ABD_Flag = 'ABD' then md5(concat('NON_BOB- -',upper(Created_By_Sales_Center)))
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_max_quarter < '{curr_qtr}'  and ABD_Flag != 'ABD' then Created_By_Team_Key
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' then created_TSM_team_key
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}' and TSM_ABD_Flag = 'ABD' then md5(concat('NON_BOB- -',upper(created_TSM_Sales_Center)))
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}'  and TSM_ABD_Flag != 'ABD' then created_TSM_team_key
                when cc_phone_vs_web = 'PHONE' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' then Agent_Team_Key
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' then Agent_Team_Key
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_team_key
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then md5('OB-CSAM-SMB_PO_PENDING_SEATS-GBD')
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' then Agent_Team_Key
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key then bob_team_key
                when cc_phone_vs_web = 'PHONE' and Bob_Flag = 'Y' then bob_team_key
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is not null and agent_curr_qtr = 'Y' then Agent_Team_Key
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and Created_By_RepName is not null and created_curr_qtr = 'Y' then Created_By_Team_Key 
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and created_max_quarter < '{curr_qtr}' and (Created_By_Team is not null and upper(Created_By_Team) != 'NULL' and trim(Created_By_Team) != '') then Created_By_Team_Key
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' then md5('CC- -CUSTOMER CARE')
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is null and created_curr_qtr = 'Y' then Created_By_Team_Key
                when cc_phone_vs_web = 'PHONE' and coalesce(init_purchase_arr_cfx,0) = 0 and (Base.FISCAL_YR_AND_QTR_DESC = subs.Fiscal_yr_and_qtr_desc) and created_curr_qtr = 'Y' then Created_By_Team_Key
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM = '{curr_qtr}' then agent_TSM_team_key
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM < '{curr_qtr}' and TSM_agent_ABD_Flag = 'ABD' then md5(concat('NON_BOB- -',upper(agent_TSM_Sales_Center)))
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM < '{curr_qtr}' and TSM_agent_ABD_Flag != 'ABD' then agent_TSM_team_key
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' and RepLdap is null then created_TSM_team_key
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}' and TSM_ABD_Flag = 'ABD' and RepLdap is null then md5(concat('NON_BOB- -',upper(created_TSM_Sales_Center)))
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}' and TSM_ABD_Flag != 'ABD' and RepLdap is null then created_TSM_team_key
                when cc_phone_vs_web = 'PHONE' and Agent_Category in ('CSAM','MQL') and RepLdap is not null and agent_curr_qtr = 'Y' then md5(concat('NON_BOB- -',upper(Agent_Sales_Center)))
                when cc_phone_vs_web = 'PHONE' and Created_By_Category = 'CSAM' and RepLdap is null and created_curr_qtr = 'Y' then md5(concat('NON_BOB- -',upper(Created_By_Sales_Center)))
                when cc_phone_vs_web = 'PHONE' and created_max_quarter < '{curr_qtr}' and ABD_Flag = 'ABD' then md5(concat('NON_BOB- -',upper(Created_By_Sales_Center)))
                when cc_phone_vs_web = 'PHONE' and created_max_quarter < '{curr_qtr}'  and ABD_Flag != 'ABD' then Created_By_Team_Key
                --when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'CON%' and Created_By_Team is null then md5('CC- -CUSTOMER CARE')
                when cc_phone_vs_web = 'PHONE' then Created_By_Team_Key
                when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and BOB_Category = 'CSAM' and upper(bob_team) like '%PO%'  and Bob_Flag = 'Y' and BOB_SFDC_Flag = 'Y' then md5('OB-CSAM-SMB_PO_PENDING_SEATS-GBD')
                when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Bob_Flag = 'Y' and exclusion_flag = 'N' then bob_team_key
                when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Bob_Flag = 'N' then md5('NON BOB A.COM ACCOUNT')
                when cc_phone_vs_web = 'WEB' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' then Agent_Team_Key
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' then Agent_Team_Key
				when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_team_key
                when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then md5('OB-CSAM-SMB_PO_PENDING_SEATS-GBD')
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' then Agent_Team_Key
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key then bob_team_key
                when cc_phone_vs_web = 'WEB' and Bob_Flag = 'Y' and exclusion_flag = 'N' then bob_team_key
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and init_purchase_arr_cfx > 0 and Agent_CSAM_INIT_FLAG = 'Y' and agent_curr_qtr = 'Y' then Agent_Team_Key
                when cc_phone_vs_web = 'WEB' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM = '{curr_qtr}' then agent_TSM_team_key
                when cc_phone_vs_web = 'WEB' and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' and RepLdap is null then created_TSM_team_key
                when cc_phone_vs_web = 'WEB' then 'WEB NOT APPLICABLE'
                else 'UNKNOWN'
                end as TeamKey,
 
                --case when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key then RepLdap
                --else 'NA'
                --end as cross_team_agent,
                --case when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key then Agent_Team
                --else 'NA'
                --end as cross_sell_team,
                
                case when Custom_Flag = 'Y' and lat_ABD_flag = 'ABD' then Custom_Rep_Name
                
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and SFDC_Flag = 'Y' and ABD_Flag = 'ABD' and Created_By_SFDC_Flag = 'Y' then Created_By_RepName
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' and Created_By_SFDC_Flag = 'N' then Created_By_RepName
                
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' then bob_name
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' and Bob_Flag = 'Y' then 'UNKNOWN'
                
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' and sfdc_abd_flag = 'ABD' then bob_name
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and Bob_Flag = 'Y' and (SFDC_Flag != 'Y' or sfdc_abd_flag != 'ABD') then 'UNKNOWN'
                
                when cc_phone_vs_web = 'VIP-PHONE' and Bob_Flag = 'Y' and bob_ABD_flag = 'ABD' then bob_name
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('INBOUND') and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_RepName
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category = 'CSAM' and init_purchase_arr_cfx > 0  and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_RepName
                when cc_phone_vs_web = 'VIP-PHONE' and ABD_Flag = 'ABD' and upper(Created_By_Team) like '%PO%' then Created_By_RepName
                when cc_phone_vs_web = 'VIP-PHONE' and ABD_Flag = 'ABD' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_RepName
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_RepName
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM'  and Bob_Flag = 'Y' and  bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_RepName
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_name
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'UNKNOWN'
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key and bob_ABD_flag = 'ABD' then bob_name
				--when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key and agent_ABD_Flag = 'ABD' then Agent_RepName
                when cc_phone_vs_web = 'PHONE' and Bob_Flag = 'Y' and bob_ABD_flag = 'ABD' then bob_name
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is not null and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_RepName
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and Created_By_RepName is not null  and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and ABD_Flag = 'ABD' then Created_By_RepName                
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is null  and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and coalesce(init_purchase_arr_cfx,0) = 0 and (Base.FISCAL_YR_AND_QTR_DESC = subs.Fiscal_yr_and_qtr_desc)  and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_RepName                			 
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and TSM_agent_ABD_Flag = 'ABD' then Agent_RepName
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and TSM_ABD_Flag = 'ABD' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and Agent_Category in ('CSAM','MQL') and RepLdap is not null and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_RepName
                when cc_phone_vs_web = 'PHONE' and Created_By_Category = 'CSAM' and RepLdap is null  and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_RepName
                when cc_phone_vs_web = 'PHONE' and ABD_Flag = 'ABD' then Created_By_RepName
                when cc_phone_vs_web = 'WEB' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_RepName
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_RepName
				when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_name
                when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'UNKNOWN'
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_RepName
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key and bob_ABD_flag = 'ABD' then bob_name
                when cc_phone_vs_web = 'WEB' and Bob_Flag = 'Y' and exclusion_flag = 'N' and bob_ABD_flag = 'ABD' then bob_name
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and init_purchase_arr_cfx > 0 and Agent_CSAM_INIT_FLAG = 'Y' and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_RepName   
                when cc_phone_vs_web = 'WEB' and agent_TSM_Flag = 'Y' and TSM_agent_ABD_Flag = 'ABD' then Agent_RepName
                when cc_phone_vs_web = 'WEB' and created_TSM_flag = 'Y' and ABD_Flag = 'ABD' then Created_By_RepName
                when cc_phone_vs_web = 'WEB' and ABD_Flag = 'ABD' then Created_By_RepName         
                else 'NA'
                end as cross_team_agent_name,  

                case when Custom_Flag = 'Y' and lat_ABD_flag = 'ABD' then Custom_LDAP
                
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and SFDC_Flag = 'Y' and ABD_Flag = 'ABD' and Created_By_SFDC_Flag = 'Y' then CREATED_BY
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' and Created_By_SFDC_Flag = 'N' then CREATED_BY
                
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' then bob_ldap
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' and Bob_Flag = 'Y' then 'UNKNOWN'
                
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' and sfdc_abd_flag = 'ABD' then bob_ldap
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and Bob_Flag = 'Y' and (SFDC_Flag != 'Y' or sfdc_abd_flag != 'ABD') then 'UNKNOWN'
                
                when cc_phone_vs_web = 'VIP-PHONE' and Bob_Flag = 'Y' and bob_ABD_flag = 'ABD' then bob_ldap
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('INBOUND') and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then CREATED_BY
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category = 'CSAM' and init_purchase_arr_cfx > 0  and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then CREATED_BY
                when cc_phone_vs_web = 'VIP-PHONE' and ABD_Flag = 'ABD' and upper(Created_By_Team) like '%PO%' then CREATED_BY
                when cc_phone_vs_web = 'VIP-PHONE' and ABD_Flag = 'ABD' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then RepLdap
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then RepLdap
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_ldap
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'UNKNOWN'
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM'  and Bob_Flag = 'Y' and  bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then RepLdap
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key and bob_ABD_flag = 'ABD' then bob_ldap
				--when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key and agent_ABD_Flag = 'ABD' then RepLdap
                when cc_phone_vs_web = 'PHONE' and Bob_Flag = 'Y' and bob_ABD_flag = 'ABD' then bob_ldap
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is not null and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then RepLdap
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and Created_By_RepName is not null  and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and ABD_Flag = 'ABD' then CREATED_BY                
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is null  and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and coalesce(init_purchase_arr_cfx,0) = 0 and (Base.FISCAL_YR_AND_QTR_DESC = subs.Fiscal_yr_and_qtr_desc)  and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then CREATED_BY                			 
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and TSM_agent_ABD_Flag = 'ABD' then RepLdap
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and TSM_ABD_Flag = 'ABD' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and Agent_Category in ('CSAM','MQL') and RepLdap is not null and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then RepLdap
                when cc_phone_vs_web = 'PHONE' and Created_By_Category = 'CSAM' and RepLdap is null  and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then CREATED_BY
                when cc_phone_vs_web = 'PHONE' and ABD_Flag = 'ABD' then CREATED_BY
                when cc_phone_vs_web = 'WEB' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then RepLdap
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then RepLdap
				when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_ldap
                when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'UNKNOWN'
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then RepLdap
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key and bob_ABD_flag = 'ABD' then bob_ldap
                when cc_phone_vs_web = 'WEB' and Bob_Flag = 'Y' and exclusion_flag = 'N' and bob_ABD_flag = 'ABD' then bob_ldap
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and init_purchase_arr_cfx > 0 and Agent_CSAM_INIT_FLAG = 'Y' and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then RepLdap   
                when cc_phone_vs_web = 'WEB' and agent_TSM_Flag = 'Y' and TSM_agent_ABD_Flag = 'ABD' then RepLdap
                when cc_phone_vs_web = 'WEB' and created_TSM_flag = 'Y' and ABD_Flag = 'ABD' then CREATED_BY
                when cc_phone_vs_web = 'WEB' and ABD_Flag = 'ABD' then CREATED_BY         
                else 'NA'
                end as cross_team_agent,                    
				
				case when Custom_Flag = 'Y' and lat_ABD_flag = 'ABD' then Custom_Team_Name
                
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and SFDC_Flag = 'Y' and ABD_Flag = 'ABD' and Created_By_SFDC_Flag = 'Y' then Created_By_Team
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' and Created_By_SFDC_Flag = 'N' then Created_By_Team
                
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' then bob_team
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' and Bob_Flag = 'Y' then 'OB-CSAM-SMB_PO_PENDING_SEATS-GBD'
                
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' and sfdc_abd_flag = 'ABD' then bob_team
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and Bob_Flag = 'Y' and (SFDC_Flag != 'Y' or sfdc_abd_flag != 'ABD') then 'OB-CSAM-SMB_PO_PENDING_SEATS-GBD'
                
                when cc_phone_vs_web = 'VIP-PHONE' and Bob_Flag = 'Y' and bob_ABD_flag = 'ABD' then bob_team
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('INBOUND') and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_Team
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category = 'CSAM' and init_purchase_arr_cfx > 0 and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_Team
                when cc_phone_vs_web = 'VIP-PHONE' and ABD_Flag = 'ABD' and upper(Created_By_Team) like '%PO%' then Created_By_Team
                when cc_phone_vs_web = 'VIP-PHONE' and ABD_Flag = 'ABD' then concat(Created_By_Sales_Center,' - NOT IN BOB')
                when cc_phone_vs_web = 'PHONE' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_Team
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_Team
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_team
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'OB-CSAM-SMB_PO_PENDING_SEATS-GBD'
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_Team
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key and bob_ABD_flag = 'ABD' then bob_team
				--when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key and agent_ABD_Flag = 'ABD' then Agent_Team
                when cc_phone_vs_web = 'PHONE' and Bob_Flag = 'Y' and bob_ABD_flag = 'ABD' then bob_team
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is not null and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_Team                          
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and Created_By_RepName is not null and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_Team 
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and ABD_Flag = 'ABD' then 'Customer Care'                               
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is null and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_Team
                when cc_phone_vs_web = 'PHONE' and coalesce(init_purchase_arr_cfx,0) = 0 and (Base.FISCAL_YR_AND_QTR_DESC = subs.Fiscal_yr_and_qtr_desc) and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_Team               
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM = '{curr_qtr}' and TSM_agent_ABD_Flag = 'ABD' then agent_TSM_team
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM < '{curr_qtr}' and TSM_agent_ABD_Flag = 'ABD' then concat(agent_TSM_Sales_Center,' - NOT IN BOB')
                --when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM < '{curr_qtr}' and TSM_agent_ABD_Flag != 'ABD' then agent_TSM_team
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' and RepLdap is null and TSM_ABD_Flag = 'ABD' then created_TSM_team
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}' and TSM_ABD_Flag = 'ABD' and RepLdap is null then concat(created_TSM_Sales_Center,' - NOT IN BOB')
                --when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}'  and TSM_ABD_Flag != 'ABD' and RepLdap is null then created_TSM_team
                when cc_phone_vs_web = 'PHONE' and Agent_Category in ('CSAM','MQL') and RepLdap is not null and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then concat(Agent_Sales_Center,' - NOT IN BOB')
                when cc_phone_vs_web = 'PHONE' and Created_By_Category = 'CSAM' and RepLdap is null and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then concat(Created_By_Sales_Center,' - NOT IN BOB')
                when cc_phone_vs_web = 'PHONE' and created_max_quarter < '{curr_qtr}'  and ABD_Flag = 'ABD' then concat(Created_By_Sales_Center,' - NOT IN BOB')
                --when cc_phone_vs_web = 'PHONE' and created_max_quarter < '{curr_qtr}'  and ABD_Flag != 'ABD' then Created_By_Team
                --when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'CON%' and Created_By_Team is null and ABD_Flag = 'ABD' then 'Customer Care' 
                when cc_phone_vs_web = 'PHONE' and ABD_Flag = 'ABD' then Created_By_Team
                when cc_phone_vs_web = 'WEB' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N'))  and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_Team
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y'  and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_Team
				when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_team
                when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'OB-CSAM-SMB_PO_PENDING_SEATS-GBD'
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y'  and agent_ABD_Flag = 'ABD' then Agent_Team
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key and bob_ABD_flag = 'ABD' then bob_team
                when cc_phone_vs_web = 'WEB' and Bob_Flag = 'Y' and exclusion_flag = 'N' and bob_ABD_flag = 'ABD' then bob_team
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and init_purchase_arr_cfx > 0 and Agent_CSAM_INIT_FLAG = 'Y'  and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_Team
                when cc_phone_vs_web = 'WEB' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM = '{curr_qtr}' and TSM_agent_ABD_Flag = 'ABD' then agent_TSM_team
                when cc_phone_vs_web = 'WEB' and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' and RepLdap is null and TSM_ABD_Flag = 'ABD' then created_TSM_team
                else 'NA'
                end as cross_sell_team,
                
                case when Custom_Flag = 'Y' and lat_ABD_flag = 'ABD' then Custom_Team_Key
                
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and SFDC_Flag = 'Y' and ABD_Flag = 'ABD' and Created_By_SFDC_Flag = 'Y' then Created_By_Team_Key
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' and Created_By_SFDC_Flag = 'N' then Created_By_Team_Key
                
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' then bob_team_key
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' and Bob_Flag = 'Y' then md5('OB-CSAM-SMB_PO_PENDING_SEATS-GBD')
                
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' and sfdc_abd_flag = 'ABD' then bob_team_key
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and Bob_Flag = 'Y' and (SFDC_Flag != 'Y' or sfdc_abd_flag != 'ABD') then md5('OB-CSAM-SMB_PO_PENDING_SEATS-GBD')
                
                when cc_phone_vs_web = 'VIP-PHONE' and Bob_Flag = 'Y' and bob_ABD_flag = 'ABD' then bob_team_key
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('INBOUND') and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_Team_Key
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category = 'CSAM' and init_purchase_arr_cfx > 0 and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_Team_Key
                when cc_phone_vs_web = 'VIP-PHONE' and ABD_Flag = 'ABD' and upper(Created_By_Team) like '%PO%' then Created_By_Team_Key
                when cc_phone_vs_web = 'VIP-PHONE' and ABD_Flag = 'ABD' then md5(concat('NON_BOB- -',upper(Created_By_Sales_Center)))
                when cc_phone_vs_web = 'PHONE' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_Team_Key
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_Team_Key
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_team_key
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then md5('OB-CSAM-SMB_PO_PENDING_SEATS-GBD')
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_Team_Key
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key and bob_ABD_flag = 'ABD' then bob_team_key
				--when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key and agent_ABD_Flag = 'ABD' then Agent_Team_Key
                when cc_phone_vs_web = 'PHONE' and Bob_Flag = 'Y' and bob_ABD_flag = 'ABD' then bob_team_key
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is not null and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_Team_Key                          
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and Created_By_RepName is not null and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_Team_Key 
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and ABD_Flag = 'ABD' then md5('CC- -CUSTOMER CARE')                               
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is null and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_Team_Key
                when cc_phone_vs_web = 'PHONE' and coalesce(init_purchase_arr_cfx,0) = 0 and (Base.FISCAL_YR_AND_QTR_DESC = subs.Fiscal_yr_and_qtr_desc) and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then Created_By_Team_Key               
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM = '{curr_qtr}' and TSM_agent_ABD_Flag = 'ABD' then agent_TSM_team_key
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM < '{curr_qtr}' and TSM_agent_ABD_Flag = 'ABD' then md5(concat('NON_BOB- -',upper(agent_TSM_Sales_Center)))
                --when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM < '{curr_qtr}' and TSM_agent_ABD_Flag != 'ABD' then agent_TSM_team_key
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' and RepLdap is null and TSM_ABD_Flag = 'ABD' then created_TSM_team_key
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}' and TSM_ABD_Flag = 'ABD' and RepLdap is null then md5(concat('NON_BOB- -',upper(created_TSM_Sales_Center)))
                --when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}'  and TSM_ABD_Flag != 'ABD' and RepLdap is null then created_TSM_team_key
                when cc_phone_vs_web = 'PHONE' and Agent_Category in ('CSAM','MQL') and RepLdap is not null and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then md5(concat('NON_BOB- -',upper(Agent_Sales_Center)))
                when cc_phone_vs_web = 'PHONE' and Created_By_Category = 'CSAM' and RepLdap is null and created_curr_qtr = 'Y' and ABD_Flag = 'ABD' then md5(concat('NON_BOB- -',upper(Created_By_Sales_Center)))
                when cc_phone_vs_web = 'PHONE' and created_max_quarter < '{curr_qtr}'  and ABD_Flag = 'ABD' then md5(concat('NON_BOB- -',upper(Created_By_Sales_Center)))
                --when cc_phone_vs_web = 'PHONE' and created_max_quarter < '{curr_qtr}'  and ABD_Flag != 'ABD' then Created_By_Team_Key
                --when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'CON%' and Created_By_Team is null and ABD_Flag = 'ABD' then md5('CC- -CUSTOMER CARE')  
                when cc_phone_vs_web = 'PHONE' and ABD_Flag = 'ABD' then Created_By_Team_Key
                when cc_phone_vs_web = 'WEB' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N'))  and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_Team_Key
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y'  and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_Team_Key
				when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then bob_team_key
                when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then md5('OB-CSAM-SMB_PO_PENDING_SEATS-GBD')
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y'  and agent_ABD_Flag = 'ABD' then Agent_Team_Key
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key and bob_ABD_flag = 'ABD' then bob_team_key
                when cc_phone_vs_web = 'WEB' and Bob_Flag = 'Y' and exclusion_flag = 'N' and bob_ABD_flag = 'ABD' then bob_team_key
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and init_purchase_arr_cfx > 0 and Agent_CSAM_INIT_FLAG = 'Y'  and agent_curr_qtr = 'Y' and agent_ABD_Flag = 'ABD' then Agent_Team_Key
                when cc_phone_vs_web = 'WEB' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM = '{curr_qtr}' and TSM_agent_ABD_Flag = 'ABD' then agent_TSM_team_key
                when cc_phone_vs_web = 'WEB' and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' and RepLdap is null and TSM_ABD_Flag = 'ABD' then created_TSM_team_key
                else 'NA'
                end as cross_sell_team_key,

                case when Custom_Flag = 'Y' then 'CUSTOM MAP'
                when cc_phone_vs_web = 'VIP-PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and BOB_Category = 'CSAM' and upper(bob_team) like '%PO%'  and Bob_Flag = 'Y' and BOB_SFDC_Flag = 'Y' then 'VIP CANCEL BOB PO'
                
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and SFDC_Flag = 'Y'  and Created_By_SFDC_Flag = 'Y' then 'VIP-MQL-SFDC-Y'
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and Created_By_SFDC_Flag = 'N' then 'VIP-MQL-AGENT SFDC-N'
                
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' then 'VIP PO SFDC-Y BOB-Y'
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' and Bob_Flag = 'Y' then 'VIP PO SFDC-N BOB-Y'
                
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' and sfdc_abd_flag = 'ABD' then 'VIP PO SFDC-Y BOB-Y-create_from_bob'
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and Bob_Flag = 'Y' and (SFDC_Flag != 'Y' or sfdc_abd_flag != 'ABD') then 'VIP PO SFDC-N BOB-Y'
                
                when cc_phone_vs_web = 'VIP-PHONE' and Bob_Flag = 'Y' then 'VIP BOB-create_from_bob'
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('INBOUND') and created_curr_qtr = 'Y' then 'VIP NOT IN BOB, IN'
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category = 'CSAM' and init_purchase_arr_cfx > 0 and created_curr_qtr = 'Y' then 'VIP CSAM, INIT PURCHASE'
                when cc_phone_vs_web = 'VIP-PHONE' and ABD_Flag = 'ABD' and upper(Created_By_Team) like '%PO%' and created_curr_qtr = 'Y' then 'VIP PO CREATED BY'
                when cc_phone_vs_web = 'VIP-PHONE' and upper(gtm_acct_segment) = 'MID-MARKET' and (ABD_Flag != 'ABD' or (ABD_Flag = 'ABD' and upper(Created_By_Team) like '%NOT IN BOB%')) then 'VIP MID MARKET'
                when cc_phone_vs_web = 'VIP-PHONE' and upper(gtm_acct_segment) = 'MID-MARKET' then 'VIP MID MARKET DEFAULT'
                when cc_phone_vs_web = 'VIP-PHONE' then 'VIP MISC'
				when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and BOB_Category = 'CSAM' and upper(bob_team) like '%PO%'  and Bob_Flag = 'Y' and BOB_SFDC_Flag = 'Y' then 'PHN CANCEL BOB PO'
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Bob_Flag = 'Y' then 'PHN CANCEL BOB-create_from_bob'
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Created_By_Category = 'CSAM' and created_curr_qtr = 'Y' then 'PHN CANCEL CSAM'
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Created_By_Category in ('INBOUND','MQL') and created_curr_qtr = 'Y' and Bob_Flag = 'N' then 'PHN CANCEL BOB-N'
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_max_quarter < '{curr_qtr}'  and ABD_Flag = 'ABD' then 'PHN CANCEL HIST ABD'
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_max_quarter < '{curr_qtr}'  and ABD_Flag != 'ABD' then 'PHN CANCEL HIST NON ABD'
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' then 'PHN CANCEL HIST TSM'
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}' and TSM_ABD_Flag = 'ABD' then 'PHN CANCEL TSM HIST ABD'
                when cc_phone_vs_web = 'PHONE' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}'  and TSM_ABD_Flag != 'ABD' then 'PHN CANCEL TSM HIST NON ABD'
                when cc_phone_vs_web = 'PHONE' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' then 'PHN IN/MQL SFDC-N'
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' then 'PHN MQL SFDC-Y'
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then 'PHN SFDC-Y BOB-Y PO-create_from_bob'
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'PHN SFDC-N BOB-Y PO'
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' then 'PHN CSAM BOB-Y, TEAM & AGENT TEAM MATCHED'
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key then 'PHN CSAM BOB-Y, TEAM & AGENT TEAM NOT MATCHED-create_from_bob'
                when cc_phone_vs_web = 'PHONE' and Bob_Flag = 'Y' then 'PHN AGENT NULL, GET BOB-create_from_bob'
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is not null and agent_curr_qtr = 'Y' then 'PHN INIT PURCHASE & AGENT LDAP PRESENT'
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and Created_By_RepName is not null and created_curr_qtr = 'Y' then 'PHN REP' 
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and created_max_quarter < '{curr_qtr}' and (Created_By_Team is not null and upper(Created_By_Team) != 'NULL' and trim(Created_By_Team) != '') then 'PHN CC-TEL HIST'
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' then 'PHN CC-TEL'               
                when cc_phone_vs_web = 'PHONE' and init_purchase_arr_cfx > 0 and RepLdap is null and created_curr_qtr = 'Y' then 'PHN INIT PURCHASE & AGENT LDAP NOT PRESENT'
                when cc_phone_vs_web = 'PHONE' and coalesce(init_purchase_arr_cfx,0) = 0 and (Base.FISCAL_YR_AND_QTR_DESC = subs.Fiscal_yr_and_qtr_desc) and created_curr_qtr = 'Y' then 'PHN ADD PURCHASE SAME QTR'
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM = '{curr_qtr}' then 'PHN TSM, AGENT SAME QTR'
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM < '{curr_qtr}' and TSM_agent_ABD_Flag = 'ABD' then 'PHN TSM, AGENT DIFF QTR, ABD'
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM < '{curr_qtr}' and TSM_agent_ABD_Flag != 'ABD' then 'PHN TSM, AGENT DIFF QTR, NON-ABD'
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' and RepLdap is null then 'PHN TSM, CREATED SAME QTR'
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}'  and TSM_ABD_Flag = 'ABD' and RepLdap is null then 'PHN TSM, CREATED DIFF QTR, ABD'
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM < '{curr_qtr}'  and TSM_ABD_Flag != 'ABD' and RepLdap is null then 'PHN TSM, CREATED DIFF QTR, NON ABD'				
                when cc_phone_vs_web = 'PHONE' and Agent_Category in ('CSAM','MQL') and RepLdap is not null and agent_curr_qtr = 'Y' then 'PHN ADD PURCHASE, DIFF QTR, AGENT LDAP PRESENT, CSAM/MQL'
                when cc_phone_vs_web = 'PHONE' and Created_By_Category = 'CSAM' and RepLdap is null and created_curr_qtr = 'Y' then 'PHN ADD PURCHASE, DIFF QTR, AGENT LDAP NOT PRESENT'
                --when cc_phone_vs_web = 'PHONE' and created_max_quarter < '{curr_qtr}' then 'PHN HIST'
                when cc_phone_vs_web = 'PHONE' and created_max_quarter < '{curr_qtr}' and ABD_Flag = 'ABD' then 'PHN HIST ABD'
                when cc_phone_vs_web = 'PHONE' and created_max_quarter < '{curr_qtr}'  and ABD_Flag != 'ABD' then 'PHN HIST NON ABD'
                --when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'CON%' and Created_By_Team is null then 'PHN CC CON'
                when cc_phone_vs_web = 'PHONE' then 'PHN-MISC'
                when cc_phone_vs_web = 'WEB' and same_wk_cncl_flag = 'Y' then 'WEB NOT APPLICABLE'
				when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and BOB_Category = 'CSAM' and upper(bob_team) like '%PO%'  and Bob_Flag = 'Y' and BOB_SFDC_Flag = 'Y' then 'WEB CANCEL BOB PO'
                when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Bob_Flag = 'Y' and exclusion_flag = 'N' then 'WEB CANCEL BOB-create_from_bob'
                when cc_phone_vs_web = 'WEB' and (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) and Bob_Flag = 'N' then 'WEB CANCEL BOB-N'
                when cc_phone_vs_web = 'WEB' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' then 'WEB IN/MQL SFDC-N'
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and Agent_SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' then 'WEB MQL SFDC-Y'
                when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then 'WEB SFDC-Y BOB-Y PO-create_from_bob'
                when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'WEB SFDC-N BOB-Y PO'
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' then 'WEB CSAM, BOB TEAM & AGENT TEAM MATCHED'
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key then 'WEB CSAM BOB, TEAM & AGENT TEAM NOT MATCHED-create_from_bob'
                when cc_phone_vs_web = 'WEB' and Bob_Flag = 'Y' and exclusion_flag = 'N' then 'WEB AGENT NULL, GET BOB-create_from_bob'
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and init_purchase_arr_cfx > 0 and Agent_CSAM_INIT_FLAG = 'Y' and agent_curr_qtr = 'Y' then 'WEB CSAM INIT'
                when cc_phone_vs_web = 'WEB' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM = '{curr_qtr}' then 'WEB TSM AGENT'
                when cc_phone_vs_web = 'WEB' and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' and RepLdap is null then 'WEB TSM CREATED'
                when cc_phone_vs_web = 'WEB' then 'WEB NOT APPLICABLE'               
                else 'UNKNOWN'
                end as AgentMapFlag,

                Base.Flag as Flag,

                case when init_purchase_arr_cfx<>0 then 'Initial Purchase'
                when (gross_cancel_arr_cfx > 0 or gross_cancel_arr_cfx < 0 or net_cancelled_arr_cfx > 0 or net_cancelled_arr_cfx < 0 or migrated_to > 0 or migrated_from > 0 or renewal_to > 0 or renewal_from > 0 or migrated_from_arr_cfx <> 0 or migrated_to_arr_cfx <> 0 or renewal_from_arr_cfx > 0 or renewal_to_arr_cfx > 0) then 'UNKNOWN'
                when (cc_phone_vs_web = 'WEB' or cc_phone_vs_web = 'PHONE' ) and same_wk_cncl_flag = 'Y' then 'Customer-Add on'
                when Custom_Flag = 'Y' then 'Rep-Add on'
                
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and SFDC_Flag = 'Y'  and Created_By_SFDC_Flag = 'Y' then 'Rep-Add on'
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('MQL') and created_curr_qtr = 'Y' and Created_By_SFDC_Flag = 'N' then 'Rep-Add on'
                
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' then 'Rep-Add on'
                --when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' and Bob_Flag = 'Y' then 'Customer-Add on'
                
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' and Bob_Flag = 'Y' and sfdc_abd_flag = 'ABD' then 'Rep-Add on'
                when cc_phone_vs_web = 'VIP-PHONE' and BOB_Category = 'CSAM' and  BOB_SFDC_Flag = 'Y' and bob_ABD_flag = 'ABD' and upper(bob_team) like '%PO%' and Bob_Flag = 'Y' and (SFDC_Flag != 'Y' or sfdc_abd_flag != 'ABD') then 'Customer-Add on'
                
                when cc_phone_vs_web = 'VIP-PHONE' and Bob_Flag = 'Y' then 'Rep-Add on'
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category in ('INBOUND') and created_curr_qtr = 'Y' then 'Rep-Add on'
                when cc_phone_vs_web = 'VIP-PHONE' and Created_By_Category = 'CSAM' and init_purchase_arr_cfx > 0 and created_curr_qtr = 'Y' then 'Rep-Add on'
                when cc_phone_vs_web = 'VIP-PHONE' and ABD_Flag = 'ABD' and upper(Created_By_Team) like '%PO%' and created_curr_qtr = 'Y'  then 'Rep-Add on'
                when cc_phone_vs_web = 'VIP-PHONE' and upper(gtm_acct_segment) = 'MID-MARKET' and (ABD_Flag != 'ABD' or (ABD_Flag = 'ABD' and upper(Created_By_Team) like '%NOT IN BOB%')) then 'Rep-Add on'
                when cc_phone_vs_web = 'VIP-PHONE' and upper(gtm_acct_segment) = 'MID-MARKET' then 'Rep-Add on'
                when cc_phone_vs_web = 'VIP-PHONE' then 'Rep-Add on'
                when cc_phone_vs_web = 'PHONE' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' then 'Rep-Add on'
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' then 'Rep-Add on'
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then 'Rep-Add on'
                when cc_phone_vs_web = 'PHONE' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'Customer-Add on'
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM'  and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' then 'Rep-Add on'
                when cc_phone_vs_web = 'PHONE' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key then 'Customer-Add on'
                when cc_phone_vs_web = 'PHONE' and Bob_Flag = 'Y' then 'Customer-Add on'
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and Created_By_RepName is not null and created_curr_qtr = 'Y' then 'Customer-Add on'
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' and created_max_quarter < '{curr_qtr}' and (Created_By_Team is not null and upper(Created_By_Team) != 'NULL' and trim(Created_By_Team) != '') then 'Customer-Add on'                
                when cc_phone_vs_web = 'PHONE' and  upper(CREATED_BY) like 'TEL%' then 'Customer-Add on'
                when cc_phone_vs_web = 'PHONE' and coalesce(init_purchase_arr_cfx,0) = 0 and (Base.FISCAL_YR_AND_QTR_DESC = subs.Fiscal_yr_and_qtr_desc) and created_curr_qtr = 'Y' then 'Customer-Add on'
                when cc_phone_vs_web = 'PHONE' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM = '{curr_qtr}' then 'Rep-Add on'
                when cc_phone_vs_web = 'PHONE' and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' and RepLdap is null then 'Customer-Add on'				
                when cc_phone_vs_web = 'PHONE' and Agent_Category in ('CSAM','MQL') and RepLdap is not null then 'Rep-Add on'
                when cc_phone_vs_web = 'PHONE' and Created_By_Category = 'CSAM'  and RepLdap is null then 'Customer-Add on'
                when cc_phone_vs_web = 'PHONE' then 'Customer-Add on'
                when cc_phone_vs_web = 'WEB' and (Agent_Category = 'INBOUND' or (Agent_Category = 'MQL' and Agent_SFDC_Flag = 'N')) and agent_curr_qtr = 'Y' then 'Rep-Add on'
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'MQL' and SFDC_Flag = 'Y' and agent_curr_qtr = 'Y' then 'Rep-Add on'
                when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag = 'Y' then 'Rep-Add on'
                when cc_phone_vs_web = 'WEB' and BOB_Category = 'CSAM' and BOB_SFDC_Flag = 'Y' and Bob_Flag = 'Y' and upper(bob_team) like '%PO%' and SFDC_Flag != 'Y' then 'Customer-Add on'
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key = Agent_Team_Key and agent_curr_qtr = 'Y' then 'Rep-Add on'
                when cc_phone_vs_web = 'WEB' and Agent_Category = 'CSAM' and Bob_Flag = 'Y' and bob_team_key != Agent_Team_Key then 'Customer-Add on'
                when cc_phone_vs_web = 'WEB' and Bob_Flag = 'Y' and exclusion_flag = 'N' then 'Customer-Add on'  
                when cc_phone_vs_web = 'WEB' and agent_TSM_Flag = 'Y' and agent_max_quarter_TSM = '{curr_qtr}' then 'Rep-Add on'
                when cc_phone_vs_web = 'WEB' and created_TSM_flag = 'Y' and created_max_quarter_TSM = '{curr_qtr}' and RepLdap is null then 'Customer-Add on'
                when cc_phone_vs_web = 'WEB' then 'WEB NOT APPLICABLE'
                else 'UNKNOWN'
                end as Txns,

                Base.opportunity_id as SFDC_opportunity_id,
                Base.opportunity_created_date as SFDC_opportunity_created_date,
                Base.sfdc_closed_date as SFDC_closed_date,
                Base.email as SFDC_email,
                Base.ecc_salesordernumber as SFDC_ecc_salesordernumber,
                Base.campaignid as SFDC_campaignid,
                Base.name as SFDC_name,
                Base.cum_campaignid as SFDC_cum_campaignid,
                Base.min_date as SFDC_min_date,
                Base.min_fiscal_yr_and_qtr_desc as SFDC_min_fiscal_yr_and_qtr_desc,
                Base.min_fiscal_yr_and_wk_desc as SFDC_min_fiscal_yr_and_wk_desc,
                Base.SFDC_Flag as SFDC_Flag,
                Base.Bob_Flag as Bob_Flag,
                case when cc_phone_vs_web = 'WEB' and Agent_Category = 'MQL' and  Agent_SFDC_Flag != 'N' and SFDC_Flag != 'Y' then gross_new_arr_cfx
                else '0'
                end as gross_new_arr_cfx_unclaimed,
                case when cc_phone_vs_web = 'WEB' and Agent_Category = 'MQL' and  Agent_SFDC_Flag != 'N' and SFDC_Flag != 'Y' then net_purchases_arr_cfx
                else '0'
                end as net_purchases_arr_cfx_unclaimed,
                case when cc_phone_vs_web = 'WEB' and Agent_Category = 'MQL' and  Agent_SFDC_Flag != 'N' and SFDC_Flag != 'Y' then RepLdap
                else 'None'
                end as unclaimed_ldap,
                case when cc_phone_vs_web = 'WEB' and Agent_Category = 'MQL' and  Agent_SFDC_Flag != 'N' and SFDC_Flag != 'Y' then Agent_Team
                else 'None'
                end as unclaimed_team,
                Base.agent_max_quarter,
                Base.agent_curr_qtr,
                Base.created_curr_qtr,
                Base.agent_TSM_Ldap,
                Base.created_TSM_Ldap,
                Base.agent_max_quarter_TSM,
                Base.created_max_quarter_TSM,
                Base.agent_TSM_team,
                Base.created_TSM_team,
                Base.agent_TSM_team_key,
                Base.created_TSM_team_key,
                Base.agent_TSM_flag,
                Base.created_TSM_flag,
                Base.ABD_Flag,
                Base.agent_ABD_Flag,
                Base.TSM_ABD_Flag,
                Base.TSM_agent_ABD_Flag,
                Base.gross_cancellations,
                Base.net_cancelled_subs,
                Base.migrated_from,
                Base.migrated_to,
                Base.renewal_from,
                Base.renewal_to,
                Base.Custom_Flag,
                Base.bob_ABD_flag,
                Base.lat_ABD_flag,
                Base.same_wk_cncl_flag,
                Base.net_value_usd,
                Base.acct_name as acct_name,
                Base.customer_email_domain as customer_email_domain,
                Base.payment_method as payment_method,
				'0' as net_purchase_agg,
				'NA' as cancel_date_list,
				'0' as sum_cancel_arr,
				'0' net_purchase_arr_incremental,
                Base.bob_ldap,
                Base.bob_manager,
                Base.bob_TSM,
                Base.bob_sales_center,
                Base.bob_geo,
                Base.bob_team_key,
				'RULES' as event,
                Base.FISCAL_YR_AND_WK_DESC as FISCAL_YR_AND_WK_DESC
                from Base_with_Catagory Base
                left outer join 
                (select sales_document, sales_document_item,contract_start_date_veda,date.fiscal_yr_and_qtr_desc from csmb.vw_dim_subscription sub
                left outer join {DATE_TBL} date
                on  sub.contract_start_date_veda = date.date_date) subs
                on Base.SALES_DOCUMENT = subs.sales_document and Base.sales_document_item = subs.sales_document_item """.format(FROM_DT=FROM_DT,TO_DT=TO_DT,curr_qtr=curr_qtr,DATE_TBL=DATE_TBL))
 
             Base_with_init_bob_agent_map.createOrReplaceTempView("Base_with_init_bob_agent_map")

             BaseWithAgentDetails_Txn = spark.sql(""" 
                select * from (
                select  
                Base.sales_document_item as sales_document_item,
                Base.CREATED_BY as CREATED_BY,
                Base.CRM_CUSTOMER_GUID as CRM_CUSTOMER_GUID,
                Base.ENTITLEMENT_TYPE as ENTITLEMENT_TYPE,
                Base.date_date as date_date,
                Base.FISCAL_YR_AND_QTR_DESC as FISCAL_YR_AND_QTR_DESC,
                Base.cc_phone_vs_web as cc_phone_vs_web,
                Base.GEO as GEO,
                Base.MARKET_AREA as MARKET_AREA,
                Base.MARKET_SEGMENT as MARKET_SEGMENT,
                Base.offer_type_description as offer_type_description,
                Base.PRODUCT_CONFIG as PRODUCT_CONFIG,
                Base.product_config_description as product_config_description,
                Base.PRODUCT_NAME as PRODUCT_NAME,
                Base.product_name_description as product_name_description,
                Base.PROMO_TYPE as PROMO_TYPE,
                Base.PROMOTION as PROMOTION,
                Base.Region as Region,
                Base.SALES_DOCUMENT as SALES_DOCUMENT,
                Base.DYLAN_ORDER_NUMBER as DYLAN_ORDER_NUMBER,
                Base.STYPE as STYPE,
                Base.SUBS_OFFER as SUBS_OFFER,
                Base.SUBSCRIPTION_ACCOUNT_GUID as SUBSCRIPTION_ACCOUNT_GUID,
                Base.VIP_CONTRACT as VIP_CONTRACT,
                Base.addl_purchase_diff as addl_purchase_diff,
                Base.addl_purchase_same as addl_purchase_same,
                COALESCE(Base.gross_cancel_arr_cfx,0) as gross_cancel_arr_cfx,
                COALESCE(Base.gross_new_arr_cfx,0) as gross_new_arr_cfx,
                Base.gross_new_subs as gross_new_subs,
                COALESCE(Base.init_purchase_arr_cfx,0) as init_purchase_arr_cfx,
                COALESCE(Base.migrated_from_arr_cfx,0) as migrated_from_arr_cfx,
                COALESCE(Base.migrated_to_arr_cfx,0) as migrated_to_arr_cfx,
                COALESCE(Base.net_cancelled_arr_cfx,0) as net_cancelled_arr_cfx,
                COALESCE(Base.net_new_arr_cfx,0) as net_new_arr_cfx,
                COALESCE(Base.net_new_subs,0) as net_new_subs,
                COALESCE(Base.net_purchases_arr_cfx,0) as net_purchases_arr_cfx,
                COALESCE(Base.reactivated_arr_cfx,0) as reactivated_arr_cfx,
                COALESCE(Base.returns_arr_cfx,0) as returns_arr_cfx,
                Base.ecc_customer_id as ecc_customer_id,
                Base.SALES_DISTRICT as SALES_DISTRICT,
                Base.contract_start_date_veda as contract_start_date_veda,
                Base.contract_end_date_veda as contract_end_date_veda,
                Base.contract_id as contract_id,
                COALESCE(Base.renewal_from_arr_cfx,0) as renewal_from_arr_cfx,
                COALESCE(Base.renewal_to_arr_cfx,0) as renewal_to_arr_cfx,
                Base.projected_dme_gtm_segment,
                Base.gtm_acct_segment,
                Base.route_to_market,
                Base.cc_segment,
                COALESCE(Base.net_new_arr,0) as net_new_arr,
                case when (coalesce(agentdetails_scd.Sales_Center,agentdetails.Sales_Center) is null or upper(coalesce(agentdetails_scd.Sales_Center,agentdetails.Sales_Center)) = 'NULL' or trim(coalesce(agentdetails_scd.Sales_Center,agentdetails.Sales_Center))= '') and upper(RepTeam) = 'CUSTOMER CARE' then 'CUSTOMER CARE'
				when trim(upper(Base.RepTeam)) in ('MID MARKET','MID-MARKET','SIGN MM','TTEC SIGN','LEAD QUAL TEAM') then 'MID MARKET'
				when upper(con_RepLdap) = 'UNKNOWN'  then sc.Sales_Center
                when tsm.Sales_center is not null then tsm.Sales_center
                when agentmapflag like '%create_from_bob%' then Base.bob_sales_center
                when (coalesce(agentdetails_scd.Sales_Center,agentdetails.Sales_Center) is null or upper(coalesce(agentdetails_scd.Sales_Center,agentdetails.Sales_Center)) = 'NULL' or trim(coalesce(agentdetails_scd.Sales_Center,agentdetails.Sales_Center))= '') then df_bob.bob_sales_center
                else coalesce(agentdetails_scd.Sales_Center,agentdetails.Sales_Center) end as Sales_Center,
                case when tsm.geo is not null then tsm.geo
                when agentmapflag like '%create_from_bob%' then Base.bob_geo
                when (coalesce(agentdetails_scd.Geo,agentdetails.Geo) is null or upper(coalesce(agentdetails_scd.Geo,agentdetails.Geo)) = 'NULL' or trim(coalesce(agentdetails_scd.Geo,agentdetails.Geo))= '') then df_bob.bob_geo
                else coalesce(agentdetails_scd.Geo,agentdetails.Geo,tsm.Geo) end as AgentGeo,
                COALESCE(Base.RepName,'UNKNOWN') as RepName,
                Base.RepLdap  as RepLdap,
                Base.con_RepLdap as con_RepLdap,                              

                case when trim(upper(Base.RepTeam)) like '%SPECIALIST%' or upper(Base.RepTeam) like '%NOT IN BOB%' then 'UNKNOWN'
                when tsm.TSM_Ldap is not null then tsm.Manager
                when upper(coalesce(agentdetails_scd.TSM,agentdetails.TSM)) = 'NULL' or coalesce(agentdetails_scd.TSM,agentdetails.TSM) is null then 'UNKNOWN'
                when agentmapflag like '%create_from_bob%' then Base.bob_TSM
                else coalesce(agentdetails_scd.TSM,agentdetails.TSM)
                end as RepTSM,
                
                case when trim(upper(Base.RepTeam)) like '%SPECIALIST%' or upper(Base.RepTeam) like '%NOT IN BOB%' then 'UNKNOWN'
                when tsm.TSM_Ldap is not null then tsm.Manager
                when upper(coalesce(agentdetails_scd.Manager,agentdetails.Manager)) = 'NULL' or coalesce(agentdetails_scd.Manager,agentdetails.Manager) is null then 'UNKNOWN'
                when agentmapflag like '%create_from_bob%' then Base.bob_manager
                else coalesce(agentdetails_scd.Manager,agentdetails.Manager) 
                end as RepManger,                
                
                case when trim(upper(Base.RepTeam)) like '%SPECIALIST%' then concat(coalesce(agentdetails_scd.Sales_Center,agentdetails.Sales_Center),' - NOT IN BOB')
				when trim(upper(Base.RepTeam)) like '%NULL - NOT IN BOB%' then 'UNKNOWN - NOT IN BOB'
                else COALESCE(Base.RepTeam,'UNKNOWN') 
                end as RepTeam,


                Base.cross_team_agent as cross_team_agent,
                case when trim(upper(Base.cross_sell_team)) like '%NULL - NOT IN BOB%' then 'UNKNOWN - NOT IN BOB'
				else Base.cross_sell_team
				end as cross_sell_team,
                Base.Flag as Flag,
                Base.AgentMapFlag,
                Base.Txns as Txns,
                Base.SFDC_opportunity_id as SFDC_opportunity_id,
                Base.SFDC_opportunity_created_date as SFDC_opportunity_created_date,
                Base.SFDC_closed_date as SFDC_closed_date,
                Base.SFDC_email as SFDC_email,
                Base.SFDC_ecc_salesordernumber as SFDC_ecc_salesordernumber,
                Base.SFDC_campaignid as SFDC_campaignid,
                Base.SFDC_name as SFDC_name,
                Base.SFDC_cum_campaignid as SFDC_cum_campaignid,
                Base.SFDC_min_date as SFDC_min_date,
                Base.SFDC_min_fiscal_yr_and_qtr_desc as SFDC_min_fiscal_yr_and_qtr_desc,
                Base.SFDC_min_fiscal_yr_and_wk_desc as SFDC_min_fiscal_yr_and_wk_desc,
                Base.SFDC_Flag as SFDC_Flag,
                Base.Bob_Flag as Bob_Flag,

                CASE WHEN upper(Base.RepTeam) like '%CUSTOMER%' then md5('CC- -CUSTOMER CARE')
				when Base.RepTeam = 'CNX - NOT IN BOB' then '6505ea6b07af19a7b27a69ce16e34bf7'
                --when trim(upper(Base.RepTeam)) like '%SPECIALIST%' then md5(concat('NON_BOB- -',upper(coalesce(agentdetails_scd.Sales_Center,agentdetails.Sales_Center))))
                when trim(upper(Base.RepTeam)) like '%SPECIALIST%' and upper(coalesce(agentdetails_scd.Sales_Center,agentdetails.Sales_Center)) = 'CNX' then md5('NON_BOB- -EBD')
				when trim(upper(Base.RepTeam)) like '%SPECIALIST%' and upper(coalesce(agentdetails_scd.Sales_Center,agentdetails.Sales_Center)) != 'CNX' then md5(concat('NON_BOB- -',upper(coalesce(agentdetails_scd.Sales_Center,agentdetails.Sales_Center))))
                when TeamKey = 'UNKNOWN' then 'ded9fc06a6aac4c1c8a1825c51dac99b'
				when trim(upper(Base.RepTeam)) like '%NULL - NOT IN BOB%' then md5('UNKNOWN - NOT IN BOB')
                else COALESCE(Base.TeamKey,'ded9fc06a6aac4c1c8a1825c51dac99b') 
                end as TeamKey,
                
                Base.gross_new_arr_cfx_unclaimed,
                Base.net_purchases_arr_cfx_unclaimed,
                Base.unclaimed_team,
                Base.unclaimed_ldap,
                Base.agent_max_quarter,
                Base.agent_curr_qtr,
                Base.created_curr_qtr,
                Base.agent_TSM_Ldap,
                Base.created_TSM_Ldap,
                Base.agent_max_quarter_TSM,
                Base.created_max_quarter_TSM,
                Base.agent_TSM_team,
                Base.created_TSM_team,
                Base.agent_TSM_team_key,
                Base.created_TSM_team_key,
                Base.agent_TSM_flag,
                Base.created_TSM_flag,
                Base.ABD_Flag,
                Base.agent_ABD_Flag,
                Base.TSM_ABD_Flag,
                Base.TSM_agent_ABD_Flag,
                Base.gross_cancellations,
                Base.net_cancelled_subs,
                Base.migrated_from,
                Base.migrated_to,
                Base.renewal_from,
                Base.renewal_to,
                Base.Custom_Flag,
                Base.bob_ABD_flag,
                Base.lat_ABD_flag,
                Base.same_wk_cncl_flag,
                Base.cross_team_agent_name,
                CASE WHEN upper(Base.cross_sell_team) like '%CUSTOMER%' then md5('CC- -CUSTOMER CARE')
				when trim(upper(Base.cross_sell_team)) like '%NULL - NOT IN BOB%' then md5('UNKNOWN - NOT IN BOB')
				else Base.cross_sell_team_key
				end as cross_sell_team_key,
                Base.net_value_usd,
                Base.acct_name as acct_name,
                Base.customer_email_domain as customer_email_domain,
                Base.payment_method as payment_method,
				Base.net_purchase_agg,
				Base.cancel_date_list,
				Base.sum_cancel_arr,
				Base.net_purchase_arr_incremental,
                Base.bob_ldap,
                Base.bob_manager,
                Base.bob_TSM,
                Base.bob_sales_center,
                Base.bob_geo,
                Base.bob_team_key,
				Base.event,
                Base.FISCAL_YR_AND_WK_DESC as FISCAL_YR_AND_WK_DESC
                from  Base_with_init_bob_agent_map Base
                left outer Join
                df_agent_scd agentdetails_scd on upper(Base.con_RepLdap) = upper(agentdetails_scd.Ldap) and Base.date_date between agentdetails_scd.start_date and agentdetails_scd.end_date
                left outer Join
                df_agent agentdetails on upper(Base.con_RepLdap) = upper(agentdetails.Ldap)
                left outer Join
                df_tsm tsm on upper(Base.con_RepLdap) = upper(tsm.TSM_Ldap)
                left outer join (select distinct manager_ldap,manager,Team_key,bob_sales_center,bob_geo from df_bob) df_bob
                on upper(Base.con_RepLdap) = upper(df_bob.manager_ldap)
                and Base.TeamKey = df_bob.Team_key
                left outer Join
                df_sc sc on upper(Base.TeamKey) = upper(sc.Team_key))""")
 
             BaseWithAgentDetails_Txn.createOrReplaceTempView("BaseWithAgentDetails_Txn")
             

             df_base_rules_temp = spark.sql(""" 
                select
                Base.sales_document_item as sales_document_item,
                Base.CREATED_BY as CREATED_BY,
                Base.CRM_CUSTOMER_GUID as CRM_CUSTOMER_GUID,
                Base.ENTITLEMENT_TYPE as ENTITLEMENT_TYPE,
                Base.date_date as date_date,
                Base.cc_phone_vs_web as cc_phone_vs_web,
                Base.GEO as GEO,
                Base.MARKET_AREA as MARKET_AREA,
                Base.MARKET_SEGMENT as MARKET_SEGMENT,
                Base.offer_type_description as offer_type_description,
                Base.PRODUCT_CONFIG as PRODUCT_CONFIG,
                Base.product_config_description as product_config_description,
                Base.PRODUCT_NAME as PRODUCT_NAME,
                Base.product_name_description as product_name_description,
                Base.PROMO_TYPE as PROMO_TYPE,
                Base.PROMOTION as PROMOTION,
                Base.Region as Region,
                Base.SALES_DOCUMENT as SALES_DOCUMENT,
                Base.DYLAN_ORDER_NUMBER as DYLAN_ORDER_NUMBER,
                Base.STYPE as STYPE,
                Base.SUBS_OFFER as SUBS_OFFER,
                Base.SUBSCRIPTION_ACCOUNT_GUID as SUBSCRIPTION_ACCOUNT_GUID,
                Base.VIP_CONTRACT as VIP_CONTRACT,
                Base.addl_purchase_diff as addl_purchase_diff,
                Base.addl_purchase_same as addl_purchase_same,
                Base.gross_cancel_arr_cfx,
                Base.gross_new_arr_cfx,
                Base.gross_new_subs as gross_new_subs,
                Base.init_purchase_arr_cfx,
                Base.migrated_from_arr_cfx,
                Base.migrated_to_arr_cfx,
                Base.net_cancelled_arr_cfx,
                Base.net_new_arr_cfx,
                Base.net_new_subs,
                Base.net_purchases_arr_cfx,
                Base.reactivated_arr_cfx,
                Base.returns_arr_cfx,
                Base.ecc_customer_id as ecc_customer_id,
                Base.SALES_DISTRICT as SALES_DISTRICT,
                Base.contract_start_date_veda as contract_start_date_veda,
                Base.contract_end_date_veda as contract_end_date_veda,
                Base.contract_id as contract_id,
                Base.renewal_from_arr_cfx,
                Base.renewal_to_arr_cfx,
                Base.projected_dme_gtm_segment,
                Base.gtm_acct_segment,
                Base.route_to_market,
                Base.cc_segment,
                Base.net_new_arr,
                Base.Sales_Center as Sales_Center,
                Base.Geo as AgentGeo,
                Base.RepName,
                Base.RepLdap  as RepLdap,
                Base.con_RepLdap as con_RepLdap,
                Base.RepTSM,
                Base.RepManger,
                Base.RepTeam,
                Base.cross_team_agent as cross_team_agent,
                Base.cross_sell_team as cross_sell_team,
                Base.Flag as Flag,
                Base.AgentMapFlag,
                Base.Txns as Txns,
                Base.SFDC_opportunity_id as SFDC_opportunity_id,
                Base.SFDC_opportunity_created_date as SFDC_opportunity_created_date,
                Base.SFDC_closed_date as SFDC_closed_date,
                Base.SFDC_email as SFDC_email,
                Base.SFDC_ecc_salesordernumber as SFDC_ecc_salesordernumber,
                Base.SFDC_campaignid as SFDC_campaignid,
                Base.SFDC_name as SFDC_name,
                Base.SFDC_cum_campaignid as SFDC_cum_campaignid,
                Base.SFDC_min_date as SFDC_min_date,
                Base.SFDC_min_fiscal_yr_and_qtr_desc as SFDC_min_fiscal_yr_and_qtr_desc,
                Base.SFDC_min_fiscal_yr_and_wk_desc as SFDC_min_fiscal_yr_and_wk_desc,
                Base.SFDC_Flag as SFDC_Flag,
                Base.Bob_Flag as Bob_Flag,
                Base.TeamKey as TeamKey,
                '0' as gross_new_arr_cfx_unclaimed,
                '0' as net_purchases_arr_cfx_unclaimed,
                'None' as unclaimed_ldap,
                'None' as unclaimed_team,
                'None' as unclaimed_TSM,
                'None' as unclaimed_Manager,
                Base.agent_max_quarter,
                Base.agent_curr_qtr,
                Base.created_curr_qtr,
                Base.agent_TSM_Ldap,
                Base.created_TSM_Ldap,
                Base.agent_max_quarter_TSM,
                Base.created_max_quarter_TSM,
                Base.agent_TSM_team,
                Base.created_TSM_team,
                Base.agent_TSM_team_key,
                Base.created_TSM_team_key,
                Base.agent_TSM_flag,
                Base.created_TSM_flag,
                Base.ABD_Flag,
                Base.agent_ABD_Flag,
                Base.TSM_ABD_Flag,
                Base.TSM_agent_ABD_Flag,
                Base.gross_cancellations,
                Base.net_cancelled_subs,
                Base.migrated_from,
                Base.migrated_to,
                Base.renewal_from,
                Base.renewal_to,
                Base.Custom_Flag,
                Base.bob_ABD_flag,
                Base.lat_ABD_flag,
                Base.same_wk_cncl_flag,
                Base.cross_team_agent_name,
                Base.cross_sell_team_key,
                Base.net_value_usd,
                Base.acct_name as acct_name,
                Base.customer_email_domain as customer_email_domain,
                Base.payment_method as payment_method,
				Base.net_purchase_agg,
				Base.cancel_date_list,
				Base.sum_cancel_arr,
				Base.net_purchase_arr_incremental,
				Base.event,
                Base.FISCAL_YR_AND_QTR_DESC as FISCAL_YR_AND_QTR_DESC,
                Base.FISCAL_YR_AND_WK_DESC as FISCAL_YR_AND_WK_DESC
                from BaseWithAgentDetails_Txn Base
                """)
             df_base_rules_temp.createOrReplaceTempView("df_base_rules_temp")

             
             df_unclaimed = spark.sql(""" 
                select * from BaseWithAgentDetails_Txn where net_purchases_arr_cfx_unclaimed != 0 or gross_new_arr_cfx_unclaimed != 0
                """)
             df_unclaimed.createOrReplaceTempView("df_unclaimed")
             
             df_unclaimed_agent_details = spark.sql(""" 
                select
                Base.sales_document_item as sales_document_item,
                Base.CREATED_BY as CREATED_BY,
                Base.CRM_CUSTOMER_GUID as CRM_CUSTOMER_GUID,
                Base.ENTITLEMENT_TYPE as ENTITLEMENT_TYPE,
                Base.date_date as date_date,
                Base.cc_phone_vs_web as cc_phone_vs_web,
                Base.GEO as GEO,
                Base.MARKET_AREA as MARKET_AREA,
                Base.MARKET_SEGMENT as MARKET_SEGMENT,
                Base.offer_type_description as offer_type_description,
                Base.PRODUCT_CONFIG as PRODUCT_CONFIG,
                Base.product_config_description as product_config_description,
                Base.PRODUCT_NAME as PRODUCT_NAME,
                Base.product_name_description as product_name_description,
                Base.PROMO_TYPE as PROMO_TYPE,
                Base.PROMOTION as PROMOTION,
                Base.Region as Region,
                Base.SALES_DOCUMENT as SALES_DOCUMENT,
                Base.DYLAN_ORDER_NUMBER as DYLAN_ORDER_NUMBER,
                Base.STYPE as STYPE,
                Base.SUBS_OFFER as SUBS_OFFER,
                Base.SUBSCRIPTION_ACCOUNT_GUID as SUBSCRIPTION_ACCOUNT_GUID,
                Base.VIP_CONTRACT as VIP_CONTRACT,
                '0' as addl_purchase_diff,
                '0' as addl_purchase_same,
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
                Base.ecc_customer_id as ecc_customer_id,
                Base.SALES_DISTRICT as SALES_DISTRICT,
                Base.contract_start_date_veda as contract_start_date_veda,
                Base.contract_end_date_veda as contract_end_date_veda,
                Base.contract_id as contract_id,
                '0' as renewal_from_arr_cfx,
                '0' as renewal_to_arr_cfx,
                Base.projected_dme_gtm_segment,
                Base.gtm_acct_segment,
                Base.route_to_market,
                Base.cc_segment,
                '0' as net_new_arr,
                Base.Sales_Center as Sales_Center,
                Base.Geo as AgentGeo,
                Base.RepName,
                Base.RepLdap  as RepLdap,
                Base.con_RepLdap as con_RepLdap,
                Base.RepTSM,
                Base.RepManger,
                Base.RepTeam,
                Base.cross_team_agent as cross_team_agent,
                Base.cross_sell_team as cross_sell_team,
                Base.Flag as Flag,
                Base.AgentMapFlag,
                Base.Txns as Txns,
                Base.SFDC_opportunity_id as SFDC_opportunity_id,
                Base.SFDC_opportunity_created_date as SFDC_opportunity_created_date,
                Base.SFDC_closed_date as SFDC_closed_date,
                Base.SFDC_email as SFDC_email,
                Base.SFDC_ecc_salesordernumber as SFDC_ecc_salesordernumber,
                Base.SFDC_campaignid as SFDC_campaignid,
                Base.SFDC_name as SFDC_name,
                Base.SFDC_cum_campaignid as SFDC_cum_campaignid,
                Base.SFDC_min_date as SFDC_min_date,
                Base.SFDC_min_fiscal_yr_and_qtr_desc as SFDC_min_fiscal_yr_and_qtr_desc,
                Base.SFDC_min_fiscal_yr_and_wk_desc as SFDC_min_fiscal_yr_and_wk_desc,
                Base.SFDC_Flag as SFDC_Flag,
                Base.Bob_Flag as Bob_Flag,
                Base.TeamKey as TeamKey,
                Base.gross_new_arr_cfx_unclaimed as gross_new_arr_cfx_unclaimed,
                Base.net_purchases_arr_cfx_unclaimed as net_purchases_arr_cfx_unclaimed,
                Base.unclaimed_ldap as unclaimed_ldap,
                Base.unclaimed_team as unclaimed_team,
                coalesce(agentdetails_scd.TSM,agentdetails.TSM) as unclaimed_TSM,
                coalesce(agentdetails_scd.Manager,agentdetails.Manager) as unclaimed_Manager,
                Base.agent_max_quarter,
                Base.agent_curr_qtr,
                Base.created_curr_qtr,
                Base.agent_TSM_Ldap,
                Base.created_TSM_Ldap,
                Base.agent_max_quarter_TSM,
                Base.created_max_quarter_TSM,
                Base.agent_TSM_team,
                Base.created_TSM_team,
                Base.agent_TSM_team_key,
                Base.created_TSM_team_key,
                Base.agent_TSM_flag,
                Base.created_TSM_flag,
                Base.ABD_Flag,
                Base.agent_ABD_Flag,
                Base.TSM_ABD_Flag,
                Base.TSM_agent_ABD_Flag,
                Base.gross_cancellations,
                Base.net_cancelled_subs,
                Base.migrated_from,
                Base.migrated_to,
                Base.renewal_from,
                Base.renewal_to,
                Base.Custom_Flag,
                Base.bob_ABD_flag,
                Base.lat_ABD_flag,
                Base.same_wk_cncl_flag,
                Base.cross_team_agent_name,
                Base.cross_sell_team_key,
                Base.net_value_usd,
                Base.acct_name as acct_name,
                Base.customer_email_domain as customer_email_domain,
                Base.payment_method as payment_method,
				Base.net_purchase_agg,
				Base.cancel_date_list,
				Base.sum_cancel_arr,
				Base.net_purchase_arr_incremental,
				Base.event,
                Base.FISCAL_YR_AND_QTR_DESC as FISCAL_YR_AND_QTR_DESC,
                Base.FISCAL_YR_AND_WK_DESC as FISCAL_YR_AND_WK_DESC
                from df_unclaimed Base
                left outer Join
                df_agent_scd agentdetails_scd on upper(Base.unclaimed_ldap) = upper(agentdetails_scd.Ldap) and Base.date_date between agentdetails_scd.start_date and agentdetails_scd.end_date
                left outer Join
                df_agent agentdetails on upper(Base.unclaimed_ldap) = upper(agentdetails.Ldap)
                """)
             df_unclaimed_agent_details.createOrReplaceTempView("df_unclaimed_agent_details")
             
             df_base_rules = spark.sql(""" 
             select * from df_base_rules_temp
             union all
             select * from df_unclaimed_agent_details
             """)
             df_base_rules.createOrReplaceTempView("df_base_rules") 

             df_base_rules.repartition("event","FISCAL_YR_AND_QTR_DESC","FISCAL_YR_AND_WK_DESC").write.format("parquet").mode("overwrite").insertInto("%s"%TGT_TBL_RULES,overwrite=True)
             spark.sql(f"msck repair table b2b_phones.abdashbase_rules")

             try:
                 dbutils.notebook.exit("SUCCESS")   
             except Exception as e:                 
                 print("exception:",e)
        except Exception as e:
             dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()
