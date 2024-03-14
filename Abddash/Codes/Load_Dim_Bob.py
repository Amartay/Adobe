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

             backup_df = spark.sql("""select * from b2b_phones.dim_bob""")
             backup_df.repartition("Fiscal_yr_and_qtr_desc").write.format("parquet").mode("overwrite").insertInto("%s"%BKP_TBL,overwrite=True)

             df_bob_temp = spark.sql(""" select  distinct * from {SRC_TBL} """.format(SRC_TBL = SRC_TBL))
             df_bob_temp.createOrReplaceTempView("df_bob_temp")  

             #Checking entry in staging table
             cnt_df  = spark.sql("""select count(*) as cnt from df_bob_temp""")
             cnt_test = cnt_df.first()['cnt']

             if cnt_test == 0:
                mail(from_addr,to_addr,'Dim Files Alert:Dim BoB Alert - No Entry in Staging table','Staging Table b2b_phones.dim_bob_stg is Empty.','Re-run the process')
                raise Exception("No Entry Found in dim_bob_stg ")

                
             stage_df = spark.sql("""
             select a.Contract_ID,
             a.CSAM_Name,
             a.CSAM_Team,
             a.CSAM_Email,
             a.Group,
             a.CSAM_BOB_FLAG,
             a.CSAM,
             a.CSAM_Market_Area,
             a.exclusion_flag,
             b.Legacy_team_name,
             b.Standard_team_name,
             b.Team_key,
             a.Fiscal_yr_and_qtr_desc
             from df_bob_temp a
             left outer join(select distinct Legacy_team_name,Standard_team_name,Team_key from {DIM_TEAM}) b
             on upper(a.CSAM_Team) = upper(b.Legacy_team_name) """.format(DIM_TEAM = DIM_TEAM))
             stage_df.createOrReplaceTempView("stage_df")

             #checking entry in dim team
             check_df = spark.sql("""select count(*) as cnt from stage_df where Team_key is null""")
             final_count=check_df.first()['cnt']
             if final_count > 0:
                team_df = spark.sql("""select distinct CSAM_Team from stage_df where Team_key is null""")
                str = team_df.toPandas()
                str = str.to_html()
                mail(from_addr,to_addr,'Dim Files Alert:Dim BOB Alert - Team Not Found in dim Team','Team Key for below teams is missing in Dim Team',str)      
             
             #checking team key with 1 exclusion_flag
             flag_check = spark.sql("""
             select count(*) as cnt from (
             select distinct team_key,count(distinct exclusion_flag)  from stage_df
             group by team_key
             having count(distinct exclusion_flag) > 1)""")
             team_count = flag_check.first()['cnt']

             if team_count > 0:
                flag_check_df = spark.sql("""
                select distinct team_key from stage_df
                group by team_key
                having count(distinct exclusion_flag) > 1""")
                str = flag_check_df.toPandas()
                str = str.to_html()
                mail(from_addr,to_addr,'Dim Files Alert:Dim BOB Alert - Team With Multiple Exclusion flag','Below Team has entry with multiple exclustion flag',str)
             
             #checking team in roaster
             agent_check = spark.sql("""
             select count(*) as cnt from (                      
             select distinct bob.ldap from 
             (select distinct Fiscal_yr_and_qtr_desc,Team_key,split(CSAM_Email,'@')[0] as ldap from stage_df) bob
             left outer join (select distinct Quarter,team_key,ldap from b2b_phones.dim_agent_details) agent
             on upper(bob.ldap) = upper(agent.ldap)
             and bob.Fiscal_yr_and_qtr_desc = agent.Quarter
             where bob.team_key <> agent.team_key )""")
             agent_count = agent_check.first()['cnt']   
             
             if agent_count > 0:
                agent_check_df = spark.sql("""                     
                select distinct bob.ldap from 
                (select distinct Fiscal_yr_and_qtr_desc,Team_key,split(CSAM_Email,'@')[0] as ldap from stage_df) bob
                left outer join (select distinct Quarter,team_key,ldap from b2b_phones.dim_agent_details) agent
                on upper(bob.ldap) = upper(agent.ldap)
                and bob.Fiscal_yr_and_qtr_desc = agent.Quarter
                where bob.team_key <> agent.team_key """)
                str = agent_check_df.toPandas()
                str = str.to_html()
                mail(from_addr,to_addr,'Dim Files Alert:Dim BOB Alert - Agent Team in BoB mismatched with Agent Roaster.','For Below Agents team is Different in Agent Roaster.',str)


             #checking Tsm team in roaster
             agent_check = spark.sql("""
             select count(*) as cnt from (                      
             select distinct bob.ldap from 
             (select distinct Fiscal_yr_and_qtr_desc,Team_key,split(CSAM_Email,'@')[0] as ldap from stage_df) bob
             left outer join (select distinct Quarter,team_key,TSM_ldap from b2b_phones.dim_agent_details) agent
             on upper(bob.ldap) = upper(agent.TSM_ldap)
             and bob.Fiscal_yr_and_qtr_desc = agent.Quarter
             where bob.team_key <> agent.team_key )""")
             agent_count = agent_check.first()['cnt']   

             if agent_count > 0:
                agent_check_df = spark.sql("""                     
                select distinct bob.ldap from 
                (select distinct Fiscal_yr_and_qtr_desc,Team_key,split(CSAM_Email,'@')[0] as ldap from stage_df) bob
                left outer join (select distinct Quarter,team_key,TSM_ldap from b2b_phones.dim_agent_details) agent
                on upper(bob.ldap) = upper(agent.TSM_ldap)
                and bob.Fiscal_yr_and_qtr_desc = agent.Quarter
                where bob.team_key <> agent.team_key """)
                str = agent_check_df.toPandas()
                str = str.to_html()
                mail(from_addr,to_addr,'Dim Files Alert:Dim BOB Alert - TSM Team in BoB mismatched with Agent Roaster.','For Below TSMs team is Different in Agent Roaster.',str)

             #checking Manager team in roaster
             agent_check = spark.sql("""
             select count(*) as cnt from (                      
             select distinct bob.ldap from 
             (select distinct Fiscal_yr_and_qtr_desc,Team_key,split(CSAM_Email,'@')[0] as ldap from stage_df) bob
             left outer join (select distinct Quarter,team_key,Manager_ldap from b2b_phones.dim_agent_details) agent
             on upper(bob.ldap) = upper(agent.Manager_ldap)
             and bob.Fiscal_yr_and_qtr_desc = agent.Quarter
             where bob.team_key <> agent.team_key )""")
             agent_count = agent_check.first()['cnt']   

             if agent_count > 0:
                agent_check_df = spark.sql("""                     
                select distinct bob.ldap from 
                (select distinct Fiscal_yr_and_qtr_desc,Team_key,split(CSAM_Email,'@')[0] as ldap from stage_df) bob
                left outer join (select distinct Quarter,team_key,Manager_ldap from b2b_phones.dim_agent_details) agent
                on upper(bob.ldap) = upper(agent.Manager_ldap)
                and bob.Fiscal_yr_and_qtr_desc = agent.Quarter
                where bob.team_key <> agent.team_key """)
                str = agent_check_df.toPandas()
                str = str.to_html()
                mail(from_addr,to_addr,'Dim Files Alert:Dim BOB Alert - Manager Team in BoB mismatched with Agent Roaster.','For Below Managers team is Different in Agent Roaster.',str)

             #Checking Multiple entry of a contract ID
             contract_check = spark.sql(""" 
             select count(*) as cnt from(
             select Contract_ID,count(distinct *) as cnt1 from stage_df 
             group by Contract_ID
             having cnt1 > 1 )""") 
             contract_count = contract_check.first()['cnt']

             if contract_count > 0:
                contract_check_df = spark.sql(""" 
                select Contract_ID,count(distinct *) as number_of_times from stage_df 
                group by Contract_ID
                having number_of_times > 1 """)
            
                str = contract_check_df.toPandas()
                str = str.to_html()
                mail(from_addr,to_addr,'Dim Files Alert:Dim BOB Alert - Multiple Entries for Contract ID.','Multiple Entries for below contract IDs.',str)
             
             #Checking Null values in primary key columns
             null_value = spark.sql("""select 
             count(*) as cnt
             from stage_df 
             where (upper(Contract_ID) = 'NULL') or (upper(CSAM_Team) = 'NULL') or (upper(exclusion_flag) = 'NULL') or (upper(Fiscal_yr_and_qtr_desc) = 'NULL') """)
             null_value_count = null_value.first()['cnt']

             if null_value_count > 0:
                mail(from_addr,to_addr,'Dim Files Alert:Dim BoB Alert - Blank Value Found','Null Found for one of the key columns',"Contract_ID ,CSAM_Team, exclusion_flag, Fiscal_yr_and_qtr_desc")

             stage_df.repartition("Fiscal_yr_and_qtr_desc").write.format("parquet").mode("overwrite").insertInto("%s"%TGT1_TBL,overwrite=True)
                 
             try:
                 dbutils.notebook.exit("SUCCESS")   
             except Exception as e:                 
                 print("exception:",e)
        except Exception as e:
             dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()
