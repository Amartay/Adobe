%python
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
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import smtplib
from email.message import EmailMessage
import pandas as pd
from datetime import datetime

def mail(FROM_ADDR,TO_ADDR,MSGTXT):
    SMTPServer = 'adobe-com.mail.protection.outlook.com'
    port = 25
    msg = EmailMessage()
    msgtxt = MSGTXT
    msg.set_content(msgtxt)
    msg['Subject'] =  "ABDash Pre-check Status"
    msg['From'] = FROM_ADDR
    msg['To'] = TO_ADDR
    server = smtplib.SMTP(SMTPServer, port)
    server.send_message(msg)
    server.quit()

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
             dbutils.widgets.text("PIVOT_TBL", "")
             dbutils.widgets.text("HENDRIX_TBL", "")
             dbutils.widgets.text("OPPORTUNITY_TBL", "")
            #  dbutils.widgets.text("PIVOT_DATE", "")
            #  dbutils.widgets.text("HENDRIX_DATE", "")
            #  dbutils.widgets.text("OPPORTUNITY_DATE", "")  
             dbutils.widgets.text("LEAD_TBL", "")
             dbutils.widgets.text("USER_TBL", "")
             dbutils.widgets.text("CAMPAIGN_TBL", "")
             dbutils.widgets.text("SALES_ITEM", "")
            #  dbutils.widgets.text("LEAD_DATE", "")
            #  dbutils.widgets.text("USER_DATE", "")
            #  dbutils.widgets.text("CAMPAIGN_DATE", "")                         
             dbutils.widgets.text("FROM_ADDR", "")
             dbutils.widgets.text("TO_ADDR", "")
             dbutils.widgets.text("PRE_CHECK_EXECUTION", "")
             dbutils.widgets.text("TO_DT", "")
             dbutils.widgets.text("DATE_TBL", "")
             dbutils.widgets.text("FROM_DT", "")


             Settings = dbutils.widgets.get("Custom_Settings")
             PIVOT_TBL = dbutils.widgets.get("PIVOT_TBL")
             HENDRIX_TBL = dbutils.widgets.get("HENDRIX_TBL")
             OPPORTUNITY_TBL = dbutils.widgets.get("OPPORTUNITY_TBL")
            #  PIVOT_DATE = dbutils.widgets.get("PIVOT_DATE")
            #  HENDRIX_DATE = dbutils.widgets.get("HENDRIX_DATE")
            #  OPPORTUNITY_DATE = dbutils.widgets.get("OPPORTUNITY_DATE")   
             LEAD_TBL = dbutils.widgets.get("LEAD_TBL")
             USER_TBL = dbutils.widgets.get("USER_TBL")
             CAMPAIGN_TBL = dbutils.widgets.get("CAMPAIGN_TBL")
             SALES_ITEM = dbutils.widgets.get("SALES_ITEM")
            #  LEAD_DATE = dbutils.widgets.get("LEAD_DATE")
            #  USER_DATE = dbutils.widgets.get("USER_DATE")
            #  CAMPAIGN_DATE = dbutils.widgets.get("CAMPAIGN_DATE")                        
             FROM_ADDR = dbutils.widgets.get("FROM_ADDR")
             TO_ADDR = dbutils.widgets.get("TO_ADDR")
             PRE_CHECK_EXECUTION = dbutils.widgets.get("PRE_CHECK_EXECUTION")
             TO_DT = dbutils.widgets.get("TO_DT")
             DATE_TBL = dbutils.widgets.get("DATE_TBL")
             FROM_DT = dbutils.widgets.get("FROM_DT")


             Set_list = Settings.split(',')
             if len(Set_list)>0:
                 for i in Set_list:
                     if i != "":
                         print("spark.sql(+i+)")
                         spark.sql("""{i}""".format(i=i))

             #PRE_CHECK_EXECUTION FLAG CHECK
             if PRE_CHECK_EXECUTION == 'Y':
                 print("Initiating pre-check execution")


             #CURRENT DATE

                 df_current_date = spark.sql("""select current_date() as curr_date """)
                 values_list = [row.curr_date for row in df_current_date.collect()] 
                 current_date = values_list[0]

             #SFDC CURRENT DATE
                 df_sfdc_curr_date = spark.sql("""select date_add(current_date,-1) as sfdc_curr_date """)
                 values_list = [row.sfdc_curr_date for row in df_sfdc_curr_date.collect()]
                 sfdc_current_date=values_list[0]

             #HENDRIX CHECK

                 df_hendrix_date = spark.sql("""SELECT (cast(max(inserteddate) AS DATE)) as hendrix_date  from {HENDRIX_TBL} where inserteddate >= {FROM_DT} """.format(HENDRIX_TBL=HENDRIX_TBL,FROM_DT=FROM_DT)) 
                 values_list = [row.hendrix_date for row in df_hendrix_date.collect()] 
                 MSGTXT_1='Hi Team,\n\n{HENDRIX_TBL} did not load any new data, last load date is {values_list[0]}.\n\nRegards,\nB2B DE INDIA TEAM '.format(HENDRIX_TBL=HENDRIX_TBL,values_list=values_list)


             
                 if current_date == values_list[0]:
                     print("Current Date and Hendrix date is equal")
                 else:
                     print("Hendrix date and Current Date are not equal")
                     mail(FROM_ADDR,TO_ADDR,MSGTXT_1)
                #  dbutils.notebook.exit("No new Hendrix data loaded")

            #PIVOT CHECK     


                 df_pivot_date = spark.sql(""" select max(date_date) as pivot_date from {PIVOT_TBL} where  date_key between regexp_replace({FROM_DT},'-','') and regexp_replace({TO_DT},'-','') and source_type IN ('IN','TM') and event_source not in ('SNAPSHOT','F2P') 
                             and (    cc_failure_cancel_arr_cfx <> 0
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
                        OR returns_arr_cfx <> 0) """.format(PIVOT_TBL=PIVOT_TBL,FROM_DT=FROM_DT,TO_DT=TO_DT))
                 values_list = [row.pivot_date for row in df_pivot_date.collect()] 
                 MSGTXT_2='Hi Team,\n\n{PIVOT_TBL} did not load any new data, last load date is {values_list[0]}.\n\nRegards,\nB2B DE INDIA TEAM '.format(PIVOT_TBL=PIVOT_TBL,values_list=values_list)

                 if current_date == values_list[0]:
                     print("Current Date and Pivot date is equal")
                 else:
                     print("Pivot date and Current Date are not equal")
                     mail(FROM_ADDR,TO_ADDR,MSGTXT_2)
                #  dbutils.notebook.exit("No new pivot data loaded")  

            #OPP CHECK            

             
                 df_opp_date = spark.sql(""" select (cast(max(as_of_date) AS DATE)) as opp_date from {OPPORTUNITY_TBL} where as_of_date >= {FROM_DT}  """.format(OPPORTUNITY_TBL=OPPORTUNITY_TBL,FROM_DT=FROM_DT))
                 values_list = [row.opp_date for row in df_opp_date.collect()] 
                 MSGTXT_3='Hi Team,\n\n{OPPORTUNITY_TBL} did not load any new data,last load date is {values_list[0]}.\n\nRegards,\nB2B DE INDIA TEAM '.format(OPPORTUNITY_TBL=OPPORTUNITY_TBL,values_list=values_list)

                 if sfdc_current_date == values_list[0]:
                     print("Current Date and Opportunity date is equal")
                 else:
                     print("Opportunity date and Current Date are not equal")
                     mail(FROM_ADDR,TO_ADDR,MSGTXT_3)
                #  dbutils.notebook.exit("No new data opportunity loaded")  


            #LEAD CHECK            

             
                 df_lead_date = spark.sql(""" select (cast(max(as_of_date) AS DATE)) as lead_date from {LEAD_TBL} where as_of_date >= {FROM_DT} """.format(LEAD_TBL=LEAD_TBL,FROM_DT=FROM_DT))
                 values_list = [row.lead_date for row in df_lead_date.collect()] 
                 MSGTXT_4='Hi Team,\n\n{LEAD_TBL} did not load any new data,last load date is {values_list[0]}.\n\nRegards,\nB2B DE INDIA TEAM '.format(LEAD_TBL=LEAD_TBL,values_list=values_list)

                 if sfdc_current_date == values_list[0]:
                     print("Current Date and Lead date is equal")
                 else:
                     print("Lead date and Current Date are not equal")
                     mail(FROM_ADDR,TO_ADDR,MSGTXT_4)


            #USER CHECK            

             
                 df_user_date = spark.sql(""" select (cast(max(as_of_date) AS DATE)) as user_date from {USER_TBL} where as_of_date >= {FROM_DT} """.format(USER_TBL=USER_TBL,FROM_DT=FROM_DT))
                 values_list = [row.user_date for row in df_user_date.collect()] 
                 MSGTXT_5='Hi Team,\n\n{USER_TBL} did not load any new data,last load date is {values_list[0]}.\n\nRegards,\nB2B DE INDIA TEAM '.format(USER_TBL=USER_TBL,values_list=values_list)

                 if sfdc_current_date == values_list[0]:
                     print("Current Date and User date is equal")
                 else:
                     print("User date and Current Date are not equal")
                     mail(FROM_ADDR,TO_ADDR,MSGTXT_5)       


            #CAMPAIGN CHECK            

             
                 df_campaign_date = spark.sql(""" select (cast(max(as_of_date) AS DATE)) as campaign_date from {CAMPAIGN_TBL} where as_of_date >= {FROM_DT} """.format(CAMPAIGN_TBL=CAMPAIGN_TBL,FROM_DT=FROM_DT))
                 values_list = [row.campaign_date for row in df_campaign_date.collect()] 
                 MSGTXT_6='Hi Team,\n\n{CAMPAIGN_TBL} did not load any new data,last load date is {values_list[0]}.\n\nRegards,\nB2B DE INDIA TEAM '.format(CAMPAIGN_TBL=CAMPAIGN_TBL,values_list=values_list)

                 if sfdc_current_date == values_list[0]:
                     print("Current Date and Campaign date is equal")
                 else:
                     print("User date and Current Date are not equal")
                     mail(FROM_ADDR,TO_ADDR,MSGTXT_6)  

            #sales item check
                 
                 df_si = spark.sql(""" select (from_unixtime(UNIX_TIMESTAMP(max(MODIFIED_DATE_IN_DB),'yyyyMMdd'), 'yyyy-MM-dd')) as si_date from {SALES_ITEM} where  MODIFIED_DATE_IN_DB >= {FROM_DT} """.format(SALES_ITEM=SALES_ITEM,FROM_DT=FROM_DT))
                 values_list = [row.si_date for row in df_si.collect()] 
                 MSGTXT_7='Hi Team,\n\n{SALES_ITEM} did not load any new data,last load date is {values_list[0]}.\n\nRegards,\nB2B DE INDIA TEAM '.format(SALES_ITEM=SALES_ITEM,values_list=values_list)

                 if sfdc_current_date == values_list[0]:
                     print("Current Date and si date is equal")
                 else:
                     print("User date and si date are not equal")
                     mail(FROM_ADDR,TO_ADDR,MSGTXT_7)  
                 
                 
                 print("All pre-checks of ABDash completed successfully")

             try:
                 dbutils.notebook.exit("SUCCESS")   
             except Exception as e:                 
                 print("exception:",e)
        except Exception as e:
             dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()
