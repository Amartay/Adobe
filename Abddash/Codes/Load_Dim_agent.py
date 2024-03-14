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

             #print(FILE_PATH)

             Set_list = Settings.split(',')
             if len(Set_list)>0:
                 for i in Set_list:
                     if i != "":
                         print("spark.sql(+i+)")
                         spark.sql("""{i}""".format(i=i))
            
             #taking backup
             backup_df = spark.sql("""select * from b2b_phones.dim_agent_details""")
             backup_df.repartition("Quarter").write.format("parquet").mode("overwrite").insertInto("%s"%BKP_TBL,overwrite=True)

             sales_center_list = ['JAPAN','LATAM','APAC','GBD','EBD']
             geo_list = ['AMER', 'ASIA', 'EMEA', 'JPN']

             df_agent_details_temp = spark.sql(""" select distinct * from {SRC_TBL} """.format(SRC_TBL = SRC_TBL))
             df_agent_details_temp.createOrReplaceTempView("df_agent_details_temp")    
             
             #Checking entry in staging table
             cnt_df  = spark.sql("""select count(*) as cnt from df_agent_details_temp""")
             cnt_test = cnt_df.first()['cnt']

             if cnt_test == 0:
                mail(from_addr,to_addr,'Dim Files Alert:Dim Agent Alert - No Entry in Staging table','Staging Table b2b_phones.dim_agent_stg is Empty','Re-run the process')
                raise Exception("No Entry Found in dim_agent_stg ")
  

             stage_df = spark.sql("""
             select upper(trim(a.Sales_Center)) as Sales_Center,
             upper(trim(a.Geo)) as Geo,
             a.Rep_name,
             a.Ldap,
             a.TSM,
             a.TSM_Ldap,
             a.Manager,
             a.Manager_ldap,
             a.Final_Team,
             b.Team_Category,
             a.ActiveFlag,
             a.SFDC_Flag,
             a.CSAM_INIT_FLAG,
             a.Shift_Time,
             a.Time_Zone,
             b.Legacy_team_name,
             b.Standard_team_name,
             b.Team_key,
             a.Quarter
             from df_agent_details_temp a
             left outer join {DIM_TEAM} b
             on upper(trim(a.Final_team)) = upper(trim(b.Legacy_team_name)) """.format(DIM_TEAM = DIM_TEAM))
             stage_df.createOrReplaceTempView("stage_df")
             
             #checking entry in dim team
             check_df = spark.sql("""select count(*) as cnt from stage_df where Team_key is null""")
             final_count=check_df.first()['cnt']
             if final_count > 0:
                team_df = spark.sql("""select distinct Final_Team from stage_df where Team_key is null""")
                str = team_df.toPandas()
                str = str.to_html()
                mail(from_addr,to_addr,'Dim Files Alert:Dim Agent Alert - Team Not Found in dim Team','Team Key for below teams is missing in Dim Team',str)

             #checking single rep entry
             single_rep_entry  = spark.sql("""
             select count(*) as cnt from (
             select distinct ldap,count(distinct team_key,manager,TSM) from stage_df 
             --where quarter = '{curr_qtr}'
             group by ldap
             having count(distinct team_key) > 1) """)
             rep_count = single_rep_entry.first()['cnt']

             if rep_count > 0:
                rep_count_df = spark.sql("""
                select distinct ldap from stage_df 
                --where quarter = '{curr_qtr}'
                group by ldap
                having count(distinct team_key) > 1
                """)
                str = rep_count_df.toPandas()
                str = str.to_html()
                mail(from_addr,to_addr,'Dim Files Alert:Dim Agent Alert - Rep With different Manager,Team and TSM','Below rep has muliple Manager, TSM or Team',str)           

             #checking team key with 1 sfdc_flag, 1 csam_init_flag
             flag_check = spark.sql("""
             select count(*) as cnt from (
             select distinct team_key,count(distinct SFDC_Flag,CSAM_INIT_FLAG )  from stage_df
             group by team_key
             having count(distinct SFDC_Flag,CSAM_INIT_FLAG ) > 1)""")
             team_count = flag_check.first()['cnt']

             if team_count > 0:
                flag_check_df = spark.sql("""
                select distinct team_key from stage_df
                group by team_key
                having count(distinct SFDC_Flag,CSAM_INIT_FLAG ) > 1""")
                str = flag_check_df.toPandas()
                str = str.to_html()
                mail(from_addr,to_addr,'Dim Files Alert:Dim Agent Alert - Team With Multiple SFDC_Flag,CSAM_INIT_FLAG','Below Team has entry with multiple SFDC_Flag,CSAM_INIT_FLAG ',str)           
             
             #checking sales center with standard values
             sales_center_flag = 0
             sales_center = spark.sql("""select distinct Sales_Center from stage_df""")
             for i in sales_center.collect():
                if i[0] not in sales_center_list:
                    sales_center_flag = 1
             
             if sales_center_flag > 0:
                mail(from_addr,to_addr,'Dim Files Alert:Dim Agent Alert - Sales Center Not Matched','Sales Center should be one of the standard Value ',"'JAPAN','LATAM','APAC','GBD','EBD' ")  
                  
             #checking geo with standard values
             geo_flag = 0
             geo = spark.sql("""select distinct geo from stage_df""")
             for i in geo.collect():
                if i[0] not in geo_list:
                    geo_flag = 1

             if geo_flag > 0:
                mail(from_addr,to_addr,'Dim Files Alert:Dim Agent Alert - Geo Not Matched','Geo should be one of the standard Value ',"'AMER', 'ASIA', 'EMEA', 'JPN'")

             #Checking Null values in primary key columns
             null_value = spark.sql("""select 
             count(*) as cnt
             from stage_df 
             where (upper(Sales_Center) = 'NULL') or (upper(Geo) = 'NULL') or 
             (upper(Rep_name) = 'NULL') or (upper(Ldap) = 'NULL') or (upper(TSM) = 'NULL') or (upper(TSM_Ldap) = 'NULL') or 
             (upper(Manager) = 'NULL') or (upper(Manager_ldap) = 'NULL') or (upper(Final_Team) = 'NULL') or
             (upper(Team_Category) = 'NULL') or (upper(ActiveFlag) = 'NULL') or (upper(SFDC_Flag) = 'NULL') or (upper(CSAM_INIT_FLAG) = 'NULL') or (upper(Quarter) = 'NULL')""")
             null_value_count = null_value.first()['cnt']

             if null_value_count > 0:
                mail(from_addr,to_addr,'Dim Files Alert:Dim Agent Alert - Blank Value Found','Null Found for one of the key columns',"Sales_Center, Geo, Rep_name, Ldap, TSM, TSM_Ldap, Team_category, Active_flag, SFDC_Flag, CSAM_INIT_FLAG, Quarter")

             #checking team with multiple manager
             rep_team_with_duplicate_manager = spark.sql(""" select count(*) as cnt from (select distinct standard_team_name,count(distinct manager) from stage_df 
             group by standard_team_name
             having count(distinct manager) > 1 )""")
             rep_team_with_duplicate_manager_count =  rep_team_with_duplicate_manager.first()['cnt']

             if rep_team_with_duplicate_manager_count > 0:
                rep_team_with_duplicate_manager = spark.sql(""" select distinct standard_team_name from stage_df 
                group by standard_team_name
                having count(distinct manager) > 1 """)
                str = rep_team_with_duplicate_manager.toPandas()
                str = str.to_html()
                mail(from_addr,to_addr,'Dim Files Alert:Dim Agent Alert - Team With different Manager','Below Team has muliple Manager',str)

             
             stage_df.repartition("Quarter").write.format("parquet").mode("overwrite").insertInto("%s"%TGT1_TBL,overwrite=True)
                 
             try:
                 dbutils.notebook.exit("SUCCESS")   
             except Exception as e:                 
                 print("exception:",e)
        except Exception as e:
             dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()
