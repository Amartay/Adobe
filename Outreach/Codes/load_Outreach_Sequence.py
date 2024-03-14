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

             schema = StructType(
             [StructField('id', StringType(), True), 
             StructField('bounceCount', StringType(), True),
             StructField('clickCount', StringType(), True), 
             StructField('createdAt_tbl', StringType(), True),
             StructField('deliverCount', StringType(), True),
             StructField('description', StringType(), True),
             StructField('durationInDays', StringType(), True),
             StructField('enabled', StringType(), True),
             StructField('enabledAt', StringType(), True),
             StructField('failureCount', StringType(), True),
             StructField('finishOnReply', StringType(), True),
             StructField('name', StringType(), True),
             StructField('negativeReplyCount', StringType(), True),
             StructField('neutralReplyCount', StringType(), True),
             StructField('numContactedProspects', StringType(), True),
             StructField('numRepliedProspects', StringType(), True),
             StructField('openCount', StringType(), True),
             StructField('optOutCount', StringType(), True),
             StructField('positiveReplyCount', StringType(), True),
             StructField('primaryReplyAction', StringType(), True),
             StructField('replyCount', StringType(), True),
             StructField('scheduleCount', StringType(), True),
             StructField('secondaryReplyAction', StringType(), True),
             StructField('sequenceStepCount', StringType(), True),
             StructField('sequenceType', StringType(), True),
             StructField('throttleMaxAddsPerDay', StringType(), True),
             StructField('updatedAt', StringType(), True),
             StructField('relationship_creator_id', StringType(), True),
             StructField('relationship_owner_id', StringType(), True),
             StructField('relationship_ruleset_id', StringType(), True),
             StructField('relationship_schedule_id', StringType(), True),
             StructField('relationship_sequenceSteps_id', StringType(), True),
             StructField('relationship_sequenceSteps_count', StringType(), True),
             StructField('relationship_updater_id', StringType(), True),
             StructField('createdAt', StringType(), True)]
             )

             union_df = spark.createDataFrame([],schema)

             itr_df = spark.sql("""
                            select json_string,row_number() over (order by from_dt) as itr 
                            from {OUTREACH_BASE} 
                            where event = 'sequences' and string(to_date(from_dt))  between '{from_dt}' and '{to_dt}'
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
                 df_data_explode = df.withColumn('data_explode',explode_outer(df['data']))
                
                 df_data_explode_level1 = df_data_explode.withColumn('sequenceSteps',explode_outer(df_data_explode['data_explode']['relationships']['sequenceSteps']['data']))


                 df_final = df_data_explode_level1.select(
                 df_data_explode_level1['data_explode']['id'].cast(StringType()).alias('id'),
                 df_data_explode_level1['data_explode']['attributes']['bounceCount'].cast(StringType()).alias('bounceCount'),
                 df_data_explode_level1['data_explode']['attributes']['clickCount'].cast(StringType()).alias('clickCount'),
                 df_data_explode_level1['data_explode']['attributes']['createdAt'].cast(StringType()).alias('createdAt_tbl'),
                 df_data_explode_level1['data_explode']['attributes']['deliverCount'].cast(StringType()).alias('deliverCount'),
                 df_data_explode_level1['data_explode']['attributes']['description'].cast(StringType()).alias('description'),
                 df_data_explode_level1['data_explode']['attributes']['durationInDays'].cast(StringType()).alias('durationInDays'),
                 df_data_explode_level1['data_explode']['attributes']['enabled'].cast(StringType()).alias('enabled'),
                 df_data_explode_level1['data_explode']['attributes']['enabledAt'].cast(StringType()).alias('enabledAt'),
                 df_data_explode_level1['data_explode']['attributes']['failureCount'].cast(StringType()).alias('failureCount'),
                 df_data_explode_level1['data_explode']['attributes']['finishOnReply'].cast(StringType()).alias('finishOnReply'),
                 df_data_explode_level1['data_explode']['attributes']['name'].cast(StringType()).alias('name'),
                 df_data_explode_level1['data_explode']['attributes']['negativeReplyCount'].cast(StringType()).alias('negativeReplyCount'),
                 df_data_explode_level1['data_explode']['attributes']['neutralReplyCount'].cast(StringType()).alias('neutralReplyCount'),
                 df_data_explode_level1['data_explode']['attributes']['numContactedProspects'].cast(StringType()).alias('numContactedProspects'),
                 df_data_explode_level1['data_explode']['attributes']['numRepliedProspects'].cast(StringType()).alias('numRepliedProspects'),
                 df_data_explode_level1['data_explode']['attributes']['openCount'].cast(StringType()).alias('openCount'),
                 df_data_explode_level1['data_explode']['attributes']['optOutCount'].cast(StringType()).alias('optOutCount'),
                 df_data_explode_level1['data_explode']['attributes']['positiveReplyCount'].cast(StringType()).alias('positiveReplyCount'),
                 df_data_explode_level1['data_explode']['attributes']['primaryReplyAction'].cast(StringType()).alias('primaryReplyAction'),
                 df_data_explode_level1['data_explode']['attributes']['replyCount'].cast(StringType()).alias('replyCount'),
                 df_data_explode_level1['data_explode']['attributes']['scheduleCount'].cast(StringType()).alias('scheduleCount'),
                 df_data_explode_level1['data_explode']['attributes']['secondaryReplyAction'].cast(StringType()).alias('secondaryReplyAction'),
                 df_data_explode_level1['data_explode']['attributes']['sequenceStepCount'].cast(StringType()).alias('sequenceStepCount'),
                 df_data_explode_level1['data_explode']['attributes']['sequenceType'].cast(StringType()).alias('sequenceType'),
                 df_data_explode_level1['data_explode']['attributes']['throttleMaxAddsPerDay'].cast(StringType()).alias('throttleMaxAddsPerDay'),
                 df_data_explode_level1['data_explode']['attributes']['updatedAt'].cast(StringType()).alias('updatedAt'),
                 get_json_object(to_json(struct(df_data_explode_level1['data_explode']['relationships']['creator'])),'$.creator.data.id').cast(StringType()).alias('relationship_creator_id'),
                 get_json_object(to_json(struct(df_data_explode_level1['data_explode']['relationships']['owner'])),'$.owner.data.id').cast(StringType()).alias('relationship_owner_id'),
                 get_json_object(to_json(struct(df_data_explode_level1['data_explode']['relationships']['ruleset'])),'$.ruleset.data.id').cast(StringType()).alias('relationship_ruleset_id'),
                 get_json_object(to_json(struct(df_data_explode_level1['data_explode']['relationships']['schedule'])),'$.schedule.data.id').cast(StringType()).alias('relationship_schedule_id'),
                
                 get_json_object(to_json(struct(df_data_explode_level1['sequenceSteps'])),'$.sequenceSteps.id').cast(StringType()).alias('relationship_sequenceSteps_id'),
                 get_json_object(to_json(struct(df_data_explode_level1['data_explode']['relationships']['sequenceSteps']['meta'])),'$.meta.count').cast(StringType()).alias('relationship_sequenceSteps_count'),
                 get_json_object(to_json(struct(df_data_explode_level1['data_explode']['relationships']['updater'])),'$.updater.data.id').cast(StringType()).alias('relationship_updater_id'),
                 to_date(df_data_explode_level1['data_explode']['attributes']['createdAt']).cast(StringType()).alias('createdAt')
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
