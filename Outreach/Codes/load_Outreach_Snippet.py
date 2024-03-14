from pyspark import SparkContext, SparkConf , StorageLevel
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from dateutil.rrule import rrule, MONTHLY
from datetime import datetime,date
import json
from pyspark.sql import functions
import sys
import requests
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
    <p>Thanks.</p>
    </body>
    </html>
    """
    # Create a MIME multipart message
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    to_list = recipient_email.split(",")
    # Attach the HTML content without modification
    msg.attach(MIMEText(html_content, 'html'))
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.sendmail(sender_email, to_list, msg.as_string())
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
             dbutils.widgets.text("OUTREACH_BASE", "")
             dbutils.widgets.text("DIM_DATE", "")
             dbutils.widgets.text("EVENTS", "")
             dbutils.widgets.text("FROM_DT", "")
             dbutils.widgets.text("TO_DT", "")
             dbutils.widgets.text("PAGE_SIZE", "")
             dbutils.widgets.text("TO_ADDR", "")
             dbutils.widgets.text("FROM_ADDR", "")
             dbutils.widgets.text("INCREMENTAL_FLAG", "")

             Settings = dbutils.widgets.get("Custom_Settings")
             TGT_TBL = dbutils.widgets.get("TGT_TBL")
             OUTREACH_BASE = dbutils.widgets.get("OUTREACH_BASE")
             DIM_DATE = dbutils.widgets.get("DIM_DATE")
             EVENTS = dbutils.widgets.get("EVENTS")
             FROM_DT = dbutils.widgets.get("FROM_DT")
             TO_DT = dbutils.widgets.get("TO_DT")
             PAGE_SIZE = dbutils.widgets.get("PAGE_SIZE")
             TO_ADDR = dbutils.widgets.get("TO_ADDR")
             FROM_ADDR = dbutils.widgets.get("FROM_ADDR")
             INCREMENTAL_FLAG = dbutils.widgets.get("INCREMENTAL_FLAG")

             Set_list = Settings.split(',')
             if len(Set_list)>0:
                 for i in Set_list:
                     if i != "":
                         print("spark.sql(+i+)")
                         spark.sql("""{i}""".format(i=i))

             from_dt = FROM_DT
             to_dt = TO_DT

             schema = StructType([
             StructField('id', StringType(), True),
             StructField('bodyText', StringType(), True), 
             StructField('relationship_creator_id', StringType(), True), 
             StructField('relationship_owner_id', StringType(), True), 
             StructField('relationship_updater_id', StringType(), True), 
             StructField('createdAt', StringType(), True)])

             union_df = spark.createDataFrame([],schema)

             itr_df = spark.sql("""
                                select json_string,row_number() over (order by from_dt) as itr 
                                from {OUTREACH_BASE} 
                                where event = 'snippets' and string(to_date(from_dt))  between '{from_dt}' and '{to_dt}'
                                """.format(OUTREACH_BASE = OUTREACH_BASE,from_dt =from_dt,to_dt = to_dt))
             itr_df.createOrReplaceTempView('itr_df')

             max_itr_df = spark.sql("""select max(itr) as itr from itr_df""" )
             max_itr = max_itr_df.collect()[0][0]

             #max_itr = 2
             for i in range(1,max_itr+1):
                 print(i)
                 json_string_df = spark.sql("""select json_string from itr_df where itr = {i} """.format(i=i))
                 json_string = json_string_df.collect()[0][0]
                 json_object = json.loads(json_string)
                 if len(json_object['data']) == 0:
                     continue
                 df = spark.read.json(sc.parallelize([json_string]))
                 df_data_explode = df.withColumn('data_explode',explode(df['data']))

                 df_final = df_data_explode.select(
                 df_data_explode['data_explode']['id'].cast(StringType()).alias('id'),
                 df_data_explode['data_explode']['attributes']['bodyText'].cast(StringType()).alias('bodyText'),
                 get_json_object(to_json(struct(df_data_explode['data_explode']['relationships']['creator'])),'$.creator.data.id').cast(StringType()).alias('relationship_creator_id'),
                 get_json_object(to_json(struct(df_data_explode['data_explode']['relationships']['owner'])),'$.owner.data.id').cast(StringType()).alias('relationship_owner_id'),
                 get_json_object(to_json(struct(df_data_explode['data_explode']['relationships']['updater'])),'$.updater.data.id').cast(StringType()).alias('relationship_updater_id'),
                 to_date(df_data_explode['data_explode']['attributes']['createdAt']).cast(StringType()).alias('createdAt')
                 )
                
                 union_df = union_df.unionAll(df_final)

             union_df.write.format("parquet").mode("overwrite").insertInto("%s"%TGT_TBL,overwrite=True)

             try:
                 dbutils.notebook.exit("SUCCESS")   
             except Exception as e:                 
                 print("exception:",e)
        except Exception as e:
             dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()
