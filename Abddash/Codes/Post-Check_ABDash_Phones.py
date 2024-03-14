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

def mail(sender_email,recipient_email,subject,message1,message2):
    smtp_server = 'adobe-com.mail.protection.outlook.com'  # Use the SMTP server appropriate for your email provider
    smtp_port = 25  # Use the SMTP port appropriate for your email provider

    html_content = f"""
    <html>
    <body>
    <p>Hi All,</p>
    BASE
    <p>{message1}</p>
    RULES
    <p>{message2}</p>
    <p></p>
    <p>Thanks.</p>
    </body>
    </html>
    """

    # Create a MIME multipart message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject

    # Attach the HTML content without modification
    msg.attach(MIMEText(html_content, 'html'))

    server = smtplib.SMTP(smtp_server, smtp_port)
    server.sendmail(sender_email, recipient_email, msg.as_string())
    server.quit()
    print("Email sent successfully")

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
             dbutils.widgets.text("FROM_ADDR", "")
             dbutils.widgets.text("TO_ADDR", "")


             Settings = dbutils.widgets.get("Custom_Settings")
             PIVOT_TBL = dbutils.widgets.get("PIVOT_TBL")
             HENDRIX_TBL = dbutils.widgets.get("HENDRIX_TBL")
             OPPORTUNITY_TBL = dbutils.widgets.get("OPPORTUNITY_TBL")
             FROM_ADDR = dbutils.widgets.get("FROM_ADDR")
             TO_ADDR = dbutils.widgets.get("TO_ADDR")


             Set_list = Settings.split(',')
             if len(Set_list)>0:
                 for i in Set_list:
                     if i != "":
                         print("spark.sql(+i+)")
                         spark.sql("""{i}""".format(i=i))
             
             #FETCH CURRENT QTR

            #  df_pivot = spark.sql(""" select fiscal_yr_and_qtr_desc,FISCAL_YR_AND_WK_DESC,cc_phone_vs_web,round(sum(net_purchases_arr_cfx)) from csmb.vw_ccm_pivot4_all
            #             where source_type IN ('IN','TM') and event_source not in ('SNAPSHOT','F2P')  
            #             and fiscal_yr_and_qtr_desc = '2023-Q4' and cc_phone_vs_web in ('PHONE','VIP-PHONE')
            #             and    ( cc_failure_cancel_arr_cfx <> 0
            #             OR explicit_cancel_arr_cfx <> 0
            #             OR partial_cancel_cc_failure_arr_cfx <> 0
            #             OR partial_cancel_explicit_arr_cfx <> 0
            #             OR gross_cancel_arr_cfx <> 0
            #             OR gross_new_arr_cfx <> 0
            #             OR init_purchase_arr_cfx <> 0
            #             OR migrated_from_arr_cfx <> 0
            #             OR migrated_to_arr_cfx <> 0
            #             OR net_cancelled_arr_cfx <> 0
            #             OR net_purchases_arr_cfx <> 0
            #             OR reactivated_arr_cfx <> 0
            #             OR renewal_from_arr_cfx <> 0
            #             OR renewal_to_arr_cfx <> 0
            #             OR resubscription_arr_cfx <> 0
            #             OR explicit_returns_arr_cfx <> 0
            #             OR cc_failure_returns_arr_cfx <> 0
            #             OR returns_arr_cfx <> 0)
            #             group by fiscal_yr_and_qtr_desc,FISCAL_YR_AND_WK_DESC,cc_phone_vs_web 
            #             order by fiscal_yr_and_qtr_desc,FISCAL_YR_AND_WK_DESC,cc_phone_vs_web """)

             

             df_base = spark.sql(""" select fiscal_yr_and_qtr_desc,FISCAL_YR_AND_WK_DESC,cc_phone_vs_web,round(sum(net_purchases_arr_cfx)) from b2b_phones.abdashbase_tmp where fiscal_yr_and_qtr_desc = '2023-Q4' and cc_phone_vs_web in ('PHONE','VIP-PHONE') group by fiscal_yr_and_qtr_desc,FISCAL_YR_AND_WK_DESC,cc_phone_vs_web order by fiscal_yr_and_qtr_desc,FISCAL_YR_AND_WK_DESC,cc_phone_vs_web """)

             base = df_base.toPandas()

             df_rules = spark.sql(""" select fiscal_yr_and_qtr_desc,FISCAL_YR_AND_WK_DESC,cc_phone_vs_web,round(sum(net_purchases_arr_cfx)) from b2b_phones.abdashbase_rules  where fiscal_yr_and_qtr_desc = '2023-Q4' and cc_phone_vs_web in ('PHONE','VIP-PHONE') group by fiscal_yr_and_qtr_desc,FISCAL_YR_AND_WK_DESC,cc_phone_vs_web order by fiscal_yr_and_qtr_desc,FISCAL_YR_AND_WK_DESC,cc_phone_vs_web """)

             rules = df_rules.toPandas()
             
             sender_email = 'ksantra@adobe.com'
             recipient_email = 'ksantra@adobe.com'

             if base.equals(rules) == True:
                 print("Base and Rules Tables match")
                 base = base.to_html()
                 rules = rules.to_html()
                 mail(sender_email,recipient_email,'ABDash Final Status (Base & Rules Table Match)',base,rules)
             else:
                 print("Discrepancy between Base and Rules Table")             
             
             print("All post-checks of ABDash completed successfully")

             try:
                 dbutils.notebook.exit("SUCCESS")   
             except Exception as e:                 
                 print("exception:",e)
        except Exception as e:
             dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()
