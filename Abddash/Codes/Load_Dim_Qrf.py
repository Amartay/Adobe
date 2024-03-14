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
             dbutils.widgets.text("DIM_AGENT", "")
             dbutils.widgets.text("FILE_PATH", "")
             dbutils.widgets.text("TO_ADDR", "")
             dbutils.widgets.text("FROM_ADDR", "")

             Settings = dbutils.widgets.get("Custom_Settings")
             TGT1_TBL = dbutils.widgets.get("TGT1_TBL")
             BKP_TBL = dbutils.widgets.get("BKP_TBL")
             SRC_TBL = dbutils.widgets.get("SRC_TBL")
             DIM_TEAM = dbutils.widgets.get("DIM_TEAM")
             DIM_AGENT = dbutils.widgets.get("DIM_AGENT")
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

             backup_df = spark.sql("""select  * from  b2b_phones.dim_qrf""")
             backup_df.repartition("Quarter").write.format("parquet").mode("overwrite").insertInto("%s"%BKP_TBL,overwrite=True)

             sales_center_list = ['JAPAN','LATAM','APAC','GBD','EBD','CUSTOMER CARE','MID MARKET']
             geo_list = ['AMER', 'ASIA', 'EMEA', 'JPN']
             
             #to Fetch current Qtr
             df_curr_qtr = spark.sql("""select distinct fiscal_yr_and_qtr_desc from ids_coredata.dim_date where date_date = current_date()""")
             curr_qtr=df_curr_qtr.first()['fiscal_yr_and_qtr_desc']

             df_qrf_temp = spark.sql(""" select * from {SRC_TBL} """.format(SRC_TBL = SRC_TBL))
             df_qrf_temp.createOrReplaceTempView("df_qrf_temp")

             cnt_df  = spark.sql("""select count(*) as cnt from df_qrf_temp""")
             cnt_test = cnt_df.first()['cnt']

             if cnt_test == 0:
                mail(from_addr,to_addr,'Dim Files Alert:Dim QRF Alert - No Entry in Staging table.','Staging Table b2b_phones.dim_qrf_stg is Empty.','Re-run the process')
                raise Exception("No Entry Found in b2b_phones.dim_qrf_stg ")


             stage_df = spark.sql("""
             select 
             a.KPI,
             a.Fiscal_WK,
             upper(trim(a.Geo)) as Geo,
             a.Final_Team,
             a.Process,
             upper(trim(a.Sales_centre)) as Sales_centre,
             a.PHONE_VS_WEB,
             a.Product_Mapping,
             a.Team_grouping,
             a.TSM,
             a.Rep,
             a.Txn,
             a.flag,
             a.GROSS,
             a.MIGRATION,
             a.CANCEL,
             a.Buffer,
             a.Incremental_arr,
             b.Standard_team_name,
             b.Team_key,
             b.Team_category,
             a.Quarter
             from df_qrf_temp a
             left outer join
             (select distinct Standard_team_name,Team_key,Team_category,Legacy_team_name from b2b_phones.dim_team )b
             on upper(a.Final_Team) = upper(b.Legacy_team_name)
             """.format(DIM_TEAM = DIM_TEAM))
             stage_df.createOrReplaceTempView("stage_df")
 

             stage_df_intial_purchase = spark.sql("""
             select 
             a.KPI,
             a.Fiscal_WK,
             a.Geo,
             a.Final_Team,
             a.Process,
             a.Sales_centre,
             a.PHONE_VS_WEB,
             a.Product_Mapping,
             a.Team_grouping,
             a.TSM,
             a.Rep,
             a.Txn,
             a.flag,
             a.GROSS,
             a.MIGRATION,
             a.CANCEL,
             a.Buffer,
             a.Incremental_arr as Incremental_arr,
             0 as Incremental_arr_customer_addon,
             a.Standard_team_name,
             a.Team_key,
             a.Team_category,
             b.Manager,
             a.Quarter
             from stage_df a
             left outer join
             (select Team_key, Manager from (
             select  Team_key,Manager,row_number() OVER(PARTITION BY Team_key ORDER BY Team_key) as rn
             from (select distinct Team_key,Manager from {DIM_AGENT} where Quarter = '{curr_qtr}'))
             where rn=1) b 
             on a.Team_key = b.Team_key """.format(curr_qtr=curr_qtr,DIM_AGENT=DIM_AGENT))
             stage_df_intial_purchase.createOrReplaceTempView("stage_df_intial_purchase")

             stage_df_customer_addon = spark.sql("""
             select 
             a.KPI,
             a.Fiscal_WK,
             a.Geo,
             a.Final_Team,
             a.Process,
             a.Sales_centre,
             a.PHONE_VS_WEB,
             a.Product_Mapping,
             a.Team_grouping,
             a.TSM,
             a.Rep,
             'CUSTOMER-ADD ON' as Txn,
             'Customer-add on' as flag,
             0 as GROSS,
             0 as MIGRATION,
             0 as CANCEL,
             0 as Buffer,
             0 as Incremental_arr,
             a.Incremental_arr as Incremental_arr_customer_addon,
             a.Standard_team_name,
             a.Team_key,
             a.Team_category,
             b.Manager,
             a.Quarter
             from (select * from stage_df where flag = 'Incremental') a
             left outer join
             (select Team_key, Manager from (
             select  Team_key,Manager,row_number() OVER(PARTITION BY Team_key ORDER BY Team_key) as rn
             from (select distinct Team_key,Manager from {DIM_AGENT} where Quarter = '{curr_qtr}'))
             where rn=1) b 
             on a.Team_key = b.Team_key """.format(curr_qtr=curr_qtr,DIM_AGENT=DIM_AGENT))
             stage_df_customer_addon.createOrReplaceTempView("stage_df_customer_addon")

             stage_df_final = spark.sql(
               """ 
               select * from stage_df_intial_purchase
               union all
               select * from stage_df_customer_addon
               """)
             stage_df_final.createOrReplaceTempView("stage_df_final")


             #checking sales center with standard values
             sales_center_flag = 0
             sales_center = spark.sql("""select distinct Sales_centre from stage_df_final""")
             for i in sales_center.collect():
                if i[0] not in sales_center_list:
                    sales_center_flag = 1
             
             if sales_center_flag > 0:
                mail(from_addr,to_addr,'Dim Files Alert:Dim QRF Alert - Sales Center Not Matched','Sales Center should be one of the standard Value ',"'JAPAN','LATAM','APAC','GBD','EBD','NULL'")
                  
             #checking geo with standard values
             geo_flag = 0
             geo = spark.sql("""select distinct geo from stage_df_final""")
             for i in geo.collect():
                if i[0] not in geo_list:
                    geo_flag = 1

             if geo_flag > 0:
                mail(from_addr,to_addr,'Dim Files Alert:Dim Agent Alert - Geo Not Matched','Geo should be one of the standard Value ',"'AMER', 'ASIA', 'EMEA', 'JPN'")

             #checking team entry in dim Team
             check_df = spark.sql("""select count(*) as cnt from stage_df where Team_key is null""")
             final_count=check_df.first()['cnt']
             if final_count > 0:
                team_df = spark.sql("""select distinct Final_Team from stage_df where Team_key is null""")
                str = team_df.toPandas()
                str = str.to_html()
                mail(from_addr,to_addr,'Dim Files Alert:Dim QRF Alert - Team Not Found in dim Team','Team Key for below teams is missing in Dim Team',str)
                
             stage_df_final.repartition("Quarter").write.format("parquet").mode("overwrite").insertInto("%s"%TGT1_TBL,overwrite=True)

             try:
                 dbutils.notebook.exit("SUCCESS")   
             except Exception as e:                 
                 print("exception:",e)
        except Exception as e:
             dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()
