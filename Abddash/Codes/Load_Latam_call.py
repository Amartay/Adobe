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
             dbutils.widgets.text("TGT_TBL", "")
             dbutils.widgets.text("BKP_TBL", "")
             dbutils.widgets.text("SRC_TBL", "")
             dbutils.widgets.text("DIM_TEAM", "")
             dbutils.widgets.text("FILE_PATH", "")
             dbutils.widgets.text("TO_ADDR", "")
             dbutils.widgets.text("FROM_ADDR", "")

             Settings = dbutils.widgets.get("Custom_Settings")
             TGT_TBL = dbutils.widgets.get("TGT_TBL")
             BKP_TBL = dbutils.widgets.get("BKP_TBL")
             SRC_TBL = dbutils.widgets.get("SRC_TBL")
             DIM_TEAM = dbutils.widgets.get("DIM_TEAM")
             FILE_PATH = dbutils.widgets.get("FILE_PATH")
             TO_ADDR = dbutils.widgets.get("TO_ADDR")
             FROM_ADDR = dbutils.widgets.get("FROM_ADDR")

             to_addr=TO_ADDR
             from_addr=FROM_ADDR

             #print(FILE_PATH)

             Set_list = Settings.split(',')
             if len(Set_list)>0:
                 for i in Set_list:
                     if i != "":
                         print("spark.sql(+i+)")
                         spark.sql("""{i}""".format(i=i))

             latam_data_df =spark.sql("""select * from {SRC_TBL}""".format(SRC_TBL = SRC_TBL))
             latam_data_df.createOrReplaceTempView("latam_data_df")

             stage_df = spark.sql("""
             select 
             View,
             geo_code,
             Sales_Center,
             Market_area,
             Market_Area_Code,
             Market_Area_Desc,
             Language,
             rep_team,
             Ldap,
             Name,
             Channel,
             Initiation_method,
             Surface_app_ID,
             interaction_type,
             queue_type,
             queue,
             Entered,
             accepeted,
             Non_Interactive,
             abandoned,
             Talk_time_Sec,
             Wrap_time_Sec,
             fiscal_yr_and_qtr_desc,
             fiscal_yr_and_wk_desc
             from latam_data_df """)
             stage_df.createOrReplaceTempView("stage_df")

             stage_df.write.format("parquet").mode("overwrite").insertInto("%s"%TGT_TBL,overwrite=True)

   
             try:
                 dbutils.notebook.exit("SUCCESS")   
             except Exception as e:                 
                 print("exception:",e)
        except Exception as e:
             dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()
