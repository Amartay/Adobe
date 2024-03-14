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
             StructField('accountName', StringType(), True), 
             StructField('addedAt', StringType(), True), 
             StructField('addressCity', StringType(), True), 
             StructField('addressCountry', StringType(), True), 
             StructField('addressState', StringType(), True), 
             StructField('callOptedOut', StringType(), True), 
             StructField('callsOptStatus', StringType(), True), 
             StructField('callsOptedAt', StringType(), True), 
             StructField('campaignName', StringType(), True), 
             StructField('clickCount', StringType(), True), 
             StructField('company', StringType(), True), 
             StructField('companyIndustry', StringType(), True), 
             StructField('custom3', StringType(), True), 
             StructField('custom12', StringType(), True),
             StructField('custom14', StringType(), True),
             StructField('custom16', StringType(), True), 
             StructField('custom19', StringType(), True), 
             StructField('custom2', StringType(), True), 
             StructField('custom25', StringType(), True), 
             StructField('custom31', StringType(), True), 
             StructField('custom32', StringType(), True), 
             StructField('custom33', StringType(), True), 
             StructField('custom34', StringType(), True), 
             StructField('custom35', StringType(), True), 
             StructField('custom4', StringType(), True), 
             StructField('custom5', StringType(), True), 
             StructField('custom7', StringType(), True), 
             StructField('emailContacts_id', StringType(), True), 
             StructField('emailContacts_prospect_id', StringType(), True), 
             StructField('emailContacts_email', StringType(), True), 
             StructField('emailContacts_email_hash', StringType(), True), 
             StructField('emailContacts_domain_hash', StringType(), True), 
             StructField('emailContacts_created_at', StringType(), True), 
             StructField('emailContacts_updated_at', StringType(), True), 
             StructField('emailOptedOut', StringType(), True), 
             StructField('emails', StringType(), True), 
             StructField('firstName', StringType(), True), 
             StructField('lastName', StringType(), True), 
             StructField('middleName', StringType(), True), 
             StructField('name', StringType(), True), 
             StructField('openCount', StringType(), True), 
             StructField('optedOut', StringType(), True), 
             StructField('replyCount', StringType(), True), 
             StructField('smsOptedOut', StringType(), True), 
             StructField('stageName', StringType(), True), 
             StructField('touchedAt', StringType(), True), 
             StructField('updatedAt', StringType(), True), 
             StructField('workPhones', StringType(), True),
             StructField('relationship_account_id', StringType(), True), 
             StructField('relationship_assignedUsers_id', StringType(), True), 
             StructField('relationship_defaultPluginMapping_id', StringType(), True), 
             StructField('relationship_emailAddresses_id', StringType(), True), 
             StructField('relationship_owner_id', StringType(), True),
             StructField('relationship_phoneNumbers_id', StringType(), True), 
             StructField('relationship_stage_id', StringType(), True),
             StructField('createdAt', StringType(), True)]
             )

             union_df = spark.createDataFrame([],schema)

             itr_df = spark.sql("""
                                select json_string,row_number() over (order by from_dt) as itr 
                                from {OUTREACH_BASE} 
                                where event = 'prospects' and string(to_date(from_dt))  between '{from_dt}' and '{to_dt}'
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

                 df_data_explode_level1 = df_data_explode.withColumn('emailContacts',explode_outer(df_data_explode['data_explode']['attributes']['emailContacts']))

                 df_data_explode_level2 = df_data_explode_level1.withColumn('emails',explode_outer(df_data_explode_level1['data_explode']['attributes']['emails']))

                 df_data_explode_level3 = df_data_explode_level2.withColumn('workPhones',explode_outer(df_data_explode_level2['data_explode']['attributes']['workPhones']))

                 df_data_explode_level4 = df_data_explode_level3.withColumn('assignedUsers',explode_outer(df_data_explode_level3['data_explode']['relationships']['assignedUsers']['data']))

                 df_data_explode_level5 = df_data_explode_level4.withColumn('emailAddresses',explode_outer(df_data_explode_level4['data_explode']['relationships']['emailAddresses']['data']))

                 df_data_explode_level7 = df_data_explode_level5.withColumn('phoneNumbers',explode_outer(df_data_explode_level5['data_explode']['relationships']['phoneNumbers']['data']))

                 df_final = df_data_explode_level7.select(
                 df_data_explode_level7['data_explode']['id'].cast(StringType()).alias('id'),
                 df_data_explode_level7['data_explode']['attributes']['accountName'].cast(StringType()).alias('accountName'),
                 df_data_explode_level7['data_explode']['attributes']['addedAt'].cast(StringType()).alias('addedAt'),
                 df_data_explode_level7['data_explode']['attributes']['addressCity'].cast(StringType()).alias('addressCity'),
                 df_data_explode_level7['data_explode']['attributes']['addressCountry'].cast(StringType()).alias('addressCountry'),
                 df_data_explode_level7['data_explode']['attributes']['addressState'].cast(StringType()).alias('addressState'),
                 df_data_explode_level7['data_explode']['attributes']['callOptedOut'].cast(StringType()).alias('callOptedOut'),
                 df_data_explode_level7['data_explode']['attributes']['callsOptStatus'].cast(StringType()).alias('callsOptStatus'),
                 df_data_explode_level7['data_explode']['attributes']['callsOptedAt'].cast(StringType()).alias('callsOptedAt'),
                 df_data_explode_level7['data_explode']['attributes']['campaignName'].cast(StringType()).alias('campaignName'),
                 df_data_explode_level7['data_explode']['attributes']['clickCount'].cast(StringType()).alias('clickCount'),
                 df_data_explode_level7['data_explode']['attributes']['company'].cast(StringType()).alias('company'),
                 df_data_explode_level7['data_explode']['attributes']['companyIndustry'].cast(StringType()).alias('companyIndustry'),
                 df_data_explode_level7['data_explode']['attributes']['custom3'].cast(StringType()).alias('custom3'),
                 df_data_explode_level7['data_explode']['attributes']['custom12'].cast(StringType()).alias('custom12'),
                 df_data_explode_level7['data_explode']['attributes']['custom14'].cast(StringType()).alias('custom14'),
                 df_data_explode_level7['data_explode']['attributes']['custom16'].cast(StringType()).alias('custom16'),
                 df_data_explode_level7['data_explode']['attributes']['custom19'].cast(StringType()).alias('custom19'),
                 df_data_explode_level7['data_explode']['attributes']['custom2'].cast(StringType()).alias('custom2'),
                 df_data_explode_level7['data_explode']['attributes']['custom25'].cast(StringType()).alias('custom25'),
                 df_data_explode_level7['data_explode']['attributes']['custom31'].cast(StringType()).alias('custom31'),
                 df_data_explode_level7['data_explode']['attributes']['custom32'].cast(StringType()).alias('custom32'),
                 df_data_explode_level7['data_explode']['attributes']['custom33'].cast(StringType()).alias('custom33'),
                 df_data_explode_level7['data_explode']['attributes']['custom34'].cast(StringType()).alias('custom34'),
                 df_data_explode_level7['data_explode']['attributes']['custom35'].cast(StringType()).alias('custom35'),
                 df_data_explode_level7['data_explode']['attributes']['custom4'].cast(StringType()).alias('custom4'),
                 df_data_explode_level7['data_explode']['attributes']['custom5'].cast(StringType()).alias('custom5'),
                 df_data_explode_level7['data_explode']['attributes']['custom7'].cast(StringType()).alias('custom7'),
                 get_json_object(to_json(struct(df_data_explode_level7['emailContacts'])),'$.emailContacts.id').cast(StringType()).alias('emailContacts_id'),
                
                 get_json_object(to_json(struct(df_data_explode_level7['emailContacts'])),'$.emailContacts.prospect_id').cast(StringType()).alias('emailContacts_prospect_id'),
                 get_json_object(to_json(struct(df_data_explode_level7['emailContacts'])),'$.emailContacts.email').cast(StringType()).alias('emailContacts_email'),
                 get_json_object(to_json(struct(df_data_explode_level7['emailContacts'])),'$.emailContacts.email_hash').cast(StringType()).alias('emailContacts_email_hash'),
                 get_json_object(to_json(struct(df_data_explode_level7['emailContacts'])),'$.emailContacts.domain_hash').cast(StringType()).alias('emailContacts_domain_hash'),
                
                 get_json_object(to_json(struct(df_data_explode_level7['emailContacts'])),'$.emailContacts.created_at').cast(StringType()).alias('emailContacts_created_at'),
                 get_json_object(to_json(struct(df_data_explode_level7['emailContacts'])),'$.emailContacts.updated_at').cast(StringType()).alias('emailContacts_updated_at'),
                 df_data_explode_level7['data_explode']['attributes']['emailOptedOut'].cast(StringType()).alias('emailOptedOut'),
                 get_json_object(to_json(struct(df_data_explode_level7['emails'])),'$.emails').cast(StringType()).alias('emails'),
                 df_data_explode_level7['data_explode']['attributes']['firstName'].cast(StringType()).alias('firstName'),
                 df_data_explode_level7['data_explode']['attributes']['lastName'].cast(StringType()).alias('lastName'),
                 df_data_explode_level7['data_explode']['attributes']['middleName'].cast(StringType()).alias('middleName'),
                 df_data_explode_level7['data_explode']['attributes']['name'].cast(StringType()).alias('name'),
                 df_data_explode_level7['data_explode']['attributes']['openCount'].cast(StringType()).alias('openCount'),
                 df_data_explode_level7['data_explode']['attributes']['optedOut'].cast(StringType()).alias('optedOut'),
                 df_data_explode_level7['data_explode']['attributes']['replyCount'].cast(StringType()).alias('replyCount'),
                 df_data_explode_level7['data_explode']['attributes']['smsOptedOut'].cast(StringType()).alias('smsOptedOut'),
                 df_data_explode_level7['data_explode']['attributes']['stageName'].cast(StringType()).alias('stageName'),
                 df_data_explode_level7['data_explode']['attributes']['touchedAt'].cast(StringType()).alias('touchedAt'),
                 df_data_explode_level7['data_explode']['attributes']['updatedAt'].cast(StringType()).alias('updatedAt'),
                 get_json_object(to_json(struct(df_data_explode_level7['workPhones'])),'$.workPhones').cast(StringType()).alias('workPhones'),
                 get_json_object(to_json(struct(df_data_explode_level7['data_explode']['relationships']['account'])),'$.account.data.id').cast(StringType()).alias('relationship_account_id'),
                
                 get_json_object(to_json(struct(df_data_explode_level7['assignedUsers'])),'$.assignedUsers.id').cast(StringType()).alias('relationship_assignedUsers_id'),
                 get_json_object(to_json(struct(df_data_explode_level7['data_explode']['relationships']['defaultPluginMapping'])),'$.defaultPluginMapping.data.id').cast(StringType()).alias('relationship_defaultPluginMapping_id'),
                 get_json_object(to_json(struct(df_data_explode_level7['emailAddresses'])),'$.emailAddresses.id').cast(StringType()).alias('relationship_emailAddresses_id'),
                 get_json_object(to_json(struct(df_data_explode_level7['data_explode']['relationships']['owner'])),'$.owner.data.id').cast(StringType()).alias('relationship_owner_id'),
                 get_json_object(to_json(struct(df_data_explode_level7['phoneNumbers'])),'$.phoneNumbers.id').cast(StringType()).alias('relationship_phoneNumbers_id'),
                 get_json_object(to_json(struct(df_data_explode_level7['data_explode']['relationships']['stage'])),'$.stage.data.id').cast(StringType()).alias('relationship_stage_id'),
                 to_date(df_data_explode_level7['data_explode']['attributes']['createdAt']).cast(StringType()).alias('createdAt')
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
