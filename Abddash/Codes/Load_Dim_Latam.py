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

def mail(sender_email,recipient_email,subject,message1,message2):
    smtp_server = 'adobe-com.mail.protection.outlook.com'  # Use the SMTP server appropriate for your email provider
    smtp_port = 25  # Use the SMTP port appropriate for your email provider

    html_content = f"""
    <html>
    <body>
    <p>Hi All,</p>
    <p>{message1}</p>
    <p>{message2}</p>
    <p>Please re-uplaoad after correction.</p>
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
             dbutils.widgets.text("TGT1_TBL", "")
             dbutils.widgets.text("BKP_TBL", "")
             dbutils.widgets.text("SRC_TBL", "")
             dbutils.widgets.text("DIM_TEAM", "")
             dbutils.widgets.text("FILE_PATH", "")
             dbutils.widgets.text("TO_ADDR", "")
             dbutils.widgets.text("FROM_ADDR", "")

             Settings = dbutils.widgets.get("Custom_Settings")
             TGT1_TBL = dbutils.widgets.get("TGT1_TBL")
             BKP_TBL = dbutils.widgets.get("BKP_TBL")
             SRC_TBL = dbutils.widgets.get("SRC_TBL")
             DIM_TEAM = dbutils.widgets.get("DIM_TEAM")
             FILE_PATH = dbutils.widgets.get("FILE_PATH")
             TO_ADDR = dbutils.widgets.get("TO_ADDR")
             FROM_ADDR = dbutils.widgets.get("FROM_ADDR")

             to_addr=TO_ADDR
             from_addr=FROM_ADDR

             Set_list = Settings.split(',')
             if len(Set_list)>0:
                 for i in Set_list:
                     if i != "":
                         print("spark.sql(+i+)")
                         spark.sql("""{i}""".format(i=i))
             
             #taking backup
             backup_df = spark.sql("""select * from  b2b_phones.dim_orders_negotiated""")
             backup_df.repartition("Sales_Center","fiscal_yr_and_qtr_desc").write.format("parquet").mode("overwrite").insertInto("%s"%BKP_TBL,overwrite=True)

             latam_orders_df = spark.sql("""
             select                     
             dim_date.fiscal_yr_and_wk_desc as Week,
             Date,
             case when CRMGUID = '' then 'UNKNOWN' else CRMGUID end as CRMGUID,
             Sales_Document,
             Assignee_Name,
             LDAP,
             Subs_offer,
             Units,
             Direction,
             Country as Country,
             Base as Base,
             Phone_Vs_Web,
             'UNKNOWN' as Contract_ID,
             'UNKNOWN' as cc_segment,
             'LATAM' as sales_Center,
             dim_date.fiscal_yr_and_qtr_desc as fiscal_yr_and_qtr_desc
             from b2b_phones.dim_latam_stg
             left outer join ids_coredata.dim_date
             on upper(trim(dim_latam_stg.Date)) = upper(trim(dim_date.date_date)) """)
             latam_orders_df.createOrReplaceTempView("latam_orders_df")
             

             negoitated_orders_df = spark.sql("""
             select
             'UNKNOWN' as Week,
             'UNKNOWN' as Date,
             'UNKNOWN' as CRMGUID,
             'UNKNOWN' as Sales_Document,
             Assignee_Name,
             LDAP,
             'UNKNOWN' as Subs_offer,
             'UNKNOWN' as Units,
             'UNKNOWN' as Direction,
             'UNKNOWN' as Country,
             Base,
             'UNKNOWN' as Phone_Vs_Web,
             Contract_ID,
             cc_segment,
             sales_Center,
             fiscal_yr_and_qtr_desc
             from b2b_phones.dim_negotiaded_stg """)
             negoitated_orders_df.createOrReplaceTempView("negoitated_orders_df")

                        
             df_dim_negotiated = spark.sql("""
             select * from latam_orders_df
             union all
             select * from negoitated_orders_df """)
             df_dim_negotiated.createOrReplaceTempView("df_dim_negotiated")

             cnt_df  = spark.sql("""select count(*) as cnt from df_dim_negotiated""")
             cnt_test = cnt_df.first()['cnt']

             if cnt_test == 0:
                mail(from_addr,to_addr,'Dim Files Alert:Dim Negotiaded Alert - No Entry in Staging table','Staging Table b2b_phones.dim_negotiaded_stg is Empty.','Re-run the process')
                raise Exception("No Entry Found in b2b_phones.dim_negotiaded_stg ")


             stage_df = spark.sql("""
             select 
             a.Week,
             a.Date,
             a.CRMGUID,
             a.Sales_Document,
             a.Assignee_Name,
             a.LDAP,
             a.Subs_offer,
             a.Units,
             a.Direction,
             a.Country,
             a.base,
             a.Phone_vs_web,
             a.Contract_ID,
             a.cc_segment,
             b.Legacy_team_name,
             b.Standard_team_name,
             b.Team_key,
             a.Sales_Center,
             a.fiscal_yr_and_qtr_desc
             from df_dim_negotiated a
             left outer join {DIM_TEAM} b
             on upper(a.base) = upper(b.Legacy_team_name) """.format(DIM_TEAM = DIM_TEAM))
             stage_df.createOrReplaceTempView("stage_df")
             
             #checking entry in dim team
             check_df = spark.sql("""select count(*) as cnt from (select distinct base from stage_df where sales_Center = 'GBD' and team_key is null)""")
             final_count=check_df.first()['cnt']
             if final_count > 0:
                team_df = spark.sql("""select distinct base from stage_df where sales_Center = 'GBD' and team_key is null""")
                str = team_df.toPandas()
                str = str.to_html()
                mail(from_addr,to_addr,'Dim Files Alert:Dim Negotiated Alert - Team Not Found in dim Team','Team Key for below teams is missing in Dim Team',str)

             #Checking Null values in primary key columns
             null_value = spark.sql("""select 
             count(*) as cnt
             from stage_df 
             where (upper(fiscal_yr_and_qtr_desc) = 'NULL') or (upper(Sales_Center) = 'NULL') """)
             null_value_count = null_value.first()['cnt']

             if null_value_count > 0:
                mail(from_addr,to_addr,'Dim Files Alert:Dim Negotiated Alert - Blank Value Found','Null Found for one of the key columns',"fiscal_yr_and_qtr_desc, Sales_Center")
            
             stage_df.repartition("Sales_Center","fiscal_yr_and_qtr_desc").write.format("parquet").mode("overwrite").insertInto("%s"%TGT1_TBL,overwrite=True)
                        
             try:
                 dbutils.notebook.exit("SUCCESS")   
             except Exception as e:                 
                 print("exception:",e)
        except Exception as e:
             dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()
