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

def generate_access_token():
    refresh_token_df = spark.sql(""" select refresh_token from b2b_tmp.token_outreach""")
    refresh_token = refresh_token_df.collect()[0][0]
    token_response=requests.post('https://api.outreach.io/oauth/token',
                                         headers={'content-type':'application/x-www-form-urlencoded'},
                                         data={'grant_type': "authorization_code",
                                               'client_id':"2b6pU411EnKtdaMHkkM_.5Ti7THI_NcV6JmrQFmrAYBb",
                                               'client_secret':"fm10_k<rAx^!4JlewvRPy$)6zvq02ioZ$:_gAe)~lxI",
                                               'grant_type':"refresh_token",
                                               'refresh_token': f"{refresh_token}"})
    token_response_json=token_response.json()
    print(token_response_json)
    access_token=token_response_json['access_token']
    refresh_token= token_response_json['refresh_token']
    df_token = spark.createDataFrame([(access_token,refresh_token)],['access_token','refresh_token'])
    df_token.write.format("parquet").mode("overwrite").insertInto("b2b_tmp.token_outreach")
    return access_token

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
             dbutils.widgets.text("DIM_DATE", "")
             dbutils.widgets.text("FILE_PATH", "")
             dbutils.widgets.text("EVENTS", "")
             dbutils.widgets.text("FROM_DT", "")
             dbutils.widgets.text("TO_DT", "")
             dbutils.widgets.text("PAGE_SIZE", "")
             dbutils.widgets.text("TO_ADDR", "")
             dbutils.widgets.text("FROM_ADDR", "")
             dbutils.widgets.text("INCREMENTAL_FLAG", "")

             Settings = dbutils.widgets.get("Custom_Settings")
             TGT1_TBL = dbutils.widgets.get("TGT1_TBL")
             DIM_DATE = dbutils.widgets.get("DIM_DATE")
             FILE_PATH = dbutils.widgets.get("FILE_PATH")
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

             
             to_addr=TO_ADDR
             from_addr=FROM_ADDR
             to_dt = TO_DT+'T24:00:00.000Z'
             events = EVENTS.split(",")
             default_page_size = PAGE_SIZE

             spark.sql(""" refresh table b2b_tmp.token_outreach""")
             spark.sql(""" refresh table {DIM_DATE}""".format(DIM_DATE = DIM_DATE))
             spark.sql(""" refresh table {TGT1_TBL} """.format(TGT1_TBL = TGT1_TBL))

             event_failed_flag = 'N'
             event_failed_schema = StructType(
             [
                StructField('event',StringType()),
                StructField('from_dt',StringType()),
                StructField('to_dt',StringType()),
                StructField('InsertDate',StringType())
             ])
             events_failed_df = spark.createDataFrame([],event_failed_schema)

             if INCREMENTAL_FLAG == 'Y':
                    
                to_date_df = spark.sql("""select date_date from {DIM_DATE} where date_date = current_date() - 1""".format(DIM_DATE = DIM_DATE))
                dt = to_date_df.collect()[0][0]
                string_dt = dt.strftime('%Y-%m-%d')
                to_dt = string_dt + 'T24:00:00.000Z'

                from_date_df = spark.sql("""select event,max(to_date(to_dt))+1 as Max_createdAt from {TGT1_TBL} group by event""".format(TGT1_TBL = TGT1_TBL))
                from_date_list = from_date_df.collect()

                # Create a dictionary
                from_date_dic = {}
                for row in from_date_list:
                    from_date_dic[row[0]] = row[1].strftime('%Y-%m-%d')

                for event in events:
                    if event in from_date_dic.keys():
                        from_date_dic[event] = from_date_dic[event]+'T00:00:00.000Z'
                    else:
                        from_date_dic[event] = to_dt + 'T00:00:00.000Z'
      
             else:
                # Create a dictionary
                from_date_dic = {}
                for event in events:
                    if event in from_date_dic.keys():
                        from_date_dic[event] = from_date_dic[event]+'T00:00:00.000Z'
                    else:
                        from_date_dic[event] = FROM_DT+'T00:00:00.000Z'
                 
             #Getting Access token from table
             spark.sql(""" refresh table b2b_tmp.token_outreach""")
             access_token_df = spark.sql(""" select access_token from b2b_tmp.token_outreach""")
             access_token = access_token_df.collect()[0][0]
             print(access_token)

             for event in from_date_dic.keys():
                if event == 'prospects':
                    page_size = 50
                else:
                    page_size = default_page_size
                
                #initalizing Variables
                from_dt = from_date_dic[event]
                
                print('Calling API for',event,'Event')
                #creating url string
                url =  f"https://api.outreach.io/api/v2/{event}?page[size]={page_size}&count=false&filter[createdAt]={from_dt}..{to_dt}"
                print(url)

                #defining Headers
                headers = {
                'Authorization': f'Bearer {access_token}',
                }

                #Calling API
                print('Calling 1' ,event,' API')
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=30
                )

                if response.status_code == 401:
                    print('Access token Expired')
                    print('Generating New token')
                    access_token = generate_access_token()
                    print('New token Generated')
                    print('Retrying 1',event,' API')
                    headers = {
                    'Authorization': f'Bearer {access_token}',
                    }
                    response = requests.get(
                    url,
                    headers=headers,
                    timeout=30
                    )

                elif response.status_code == 200:
                    print("Succesful")

                else:
                    event_failed_flag = 'Y'
                    event_failed_df = spark.createDataFrame([(event,from_dt,to_dt,etl_date)],event_failed_schema)
                    events_failed_df = events_failed_df.unionAll(event_failed_df)
                    #mail(from_addr,to_addr,'Outreach Job Alert','Below event Failed',event)
                    continue

                result = response.json()
                jsn = json.dumps(result)
                etl_date = date.today().strftime('%Y-%m-%d')
                schema = StructType(
                [
                    StructField('json_string',StringType()),
                    StructField('event',StringType()),
                    StructField('itr',IntegerType()),
                    StructField('from_dt',StringType()),
                    StructField('to_dt',StringType()),
                    StructField('InsertDate',StringType()),
                ])
                union_df = spark.createDataFrame([(jsn,event,1,from_dt,to_dt,etl_date)],schema)

                #iterating for next links
                if 'links' in result.keys():
                    count = 2
                    while 'next' in result['links'].keys():
                        url = result['links']['next']
                        response = requests.get(
                        url,
                        headers=headers,
                        timeout=30)
                        if response.status_code == 401:
                            print('Access token Expired')
                            print('Generating New token')
                            access_token = generate_access_token()
                            print('New token Generated')
                            headers = {
                            'Authorization': f'Bearer {access_token}',
                            }
                            response = requests.get(
                            url,
                            headers=headers,
                            timeout=30
                            )
                        print('Calling',count," ",event,'API')
                        result = response.json()
                        jsn = json.dumps(result)
                        df = spark.createDataFrame([(jsn,event,count,from_dt,to_dt,etl_date)],schema)
                        union_df = union_df.unionAll(df)
                        count = count+1
                    print('Writing Data at LO table')
                    union_df.write.format("parquet").mode("append").insertInto("%s"%TGT1_TBL)
             
             if event_failed_flag == 'Y':
                str = events_failed_df.toPandas()
                str = str.to_html()
                mail(from_addr,to_addr,'Outreach Job Alert','Below event Failed',str)
                raise Exception("Some Events Failed")

             try:
                 dbutils.notebook.exit("SUCCESS")   
             except Exception as e:                 
                 print("exception:",e)
        except Exception as e:
             dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()
