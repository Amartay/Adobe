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
             dbutils.widgets.text("FILE_PATH", "")
             dbutils.widgets.text("TO_ADDR", "")
             dbutils.widgets.text("FROM_ADDR", "")

             Settings = dbutils.widgets.get("Custom_Settings")
             TGT1_TBL = dbutils.widgets.get("TGT1_TBL")
             BKP_TBL = dbutils.widgets.get("BKP_TBL")
             SRC_TBL = dbutils.widgets.get("SRC_TBL")
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
             backup_df = spark.sql("""select * from  b2b_phones.dim_team""")
             backup_df.write.format("parquet").mode("overwrite").insertInto("%s"%BKP_TBL,overwrite=True)

             df_dim_team_temp = spark.sql(""" select distinct * from {SRC_TBL} """.format(SRC_TBL = SRC_TBL))
             df_dim_team_temp.createOrReplaceTempView("df_dim_team_temp")

             cnt_df  = spark.sql("""select count(*) as cnt from df_dim_team_temp""")
             cnt_test = cnt_df.first()['cnt']

             if cnt_test == 0:
                mail(from_addr,to_addr,'Dim Files Alert:Dim Team Alert - No Entry in Staging table','Staging Table b2b_phones.dim_team_stg is Empty.','Re-run the process')
                raise Exception("No Entry Found in b2b_phones.dim_team_stg ")

             stage_df = spark.sql("""select Legacy_team_name,Standard_team_name,md5(Standard_team_name) as Team_key,ABD_Flag,Team_category,Display_Team_Category,Display_team_name,Display_team_manager,Display_team_manager_ldap from df_dim_team_temp""")
             stage_df.createOrReplaceTempView("stage_df")
             
             #flag check
             flag_check = spark.sql("""select count(*) as cnt from (
             select Team_key,count(distinct ABD_Flag,Team_category,Display_Team_Category) from stage_df 
             group by Team_key
             having count(distinct ABD_Flag,Team_category,Display_Team_Category)  > 1) """)
             team_count = flag_check.first()['cnt']

             if team_count > 0:
                flag_check_df = spark.sql("""
                select Team_key,count(distinct ABD_Flag,Team_category,Display_Team_Category) from stage_df 
                group by Team_key
                having count(distinct ABD_Flag,Team_category,Display_Team_Category)  > 1 """)
                str = flag_check_df.toPandas()
                str = str.to_html()
                mail(from_addr,to_addr,'Dim Files Alert:Dim Team Alert - Team With Multiple Flags.','Multiple Flags for below Team.',str)   

             #Checking Null values in primary key columns
             null_value = spark.sql("""select 
             count(*) as cnt
             from stage_df 
             where (upper(Legacy_team_name) = 'NULL') or (upper(Standard_team_name) = 'NULL') or (upper(ABD_Flag) = 'NULL') or (upper(Team_category) = 'NULL') or (upper(Display_Team_Category) = 'NULL') """)
             null_value_count = null_value.first()['cnt']

             if null_value_count > 0:
                mail(from_addr,to_addr,'Dim Files Alert:Dim Team Alert - Blank Value Found','Null Found for one of the key columns',"Legacy_team_name, Standard_team_name, ABD_Flag, Team_category, Display_Team_Category")         

             stage_df.write.format("parquet").mode("overwrite").insertInto("%s"%TGT1_TBL,overwrite=True)

             try:
                 dbutils.notebook.exit("SUCCESS")   
             except Exception as e:                 
                 print("exception:",e)
        except Exception as e:
             dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()
