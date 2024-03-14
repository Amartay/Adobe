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
class main() :
    def __init__(self):
        try :
            
            spark = SparkSession.builder \
                 .enableHiveSupport() \
                 .config('hive.exec.dynamic.partition', 'true') \
                 .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
                 .config('hive.exec.max.dynamic.partitions', '10000') \
                 .getOrCreate()
             #PRINT ('CREATED SPARK SESSSION 20')
            log4j = spark._jvm.org.apache.log4j
            log4j.LogManager.getRootLogger().setLevel(log4j.Level.ERROR)
            spark.sql('SET hive.warehouse.data.skiptrash=true;')
            spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
            spark.conf.set('spark.sql.cbo.enabled', True)
            spark.conf.set('spark.sql.cbo.join reorder.enabled', True)
            spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
            spark.sql('set spark.sql.parquet.enableVectorizedReader=false')
            spark.sql('set spark.sql.sources.partitionOverwriteMode=dynamic')
            spark.sql("set spark.databricks.sql.files.prorateMaxPartitionBytes.enabled=false")
            spark.sql("set spark.sql.adaptive.coalescePartitions.enabled=false")
            spark.sql("set spark.sql.adaptive.enabled=false")

            dbutils.widgets.text("TGT1_TBL", "")        
            dbutils.widgets.text("TGT2_TBL", "")        
            dbutils.widgets.text("STG2_TBL", "") 
            dbutils.widgets.text("STG3_TBL", "") 
            dbutils.widgets.text("FROM_DT", "") 
            dbutils.widgets.text("TO_DT", "") 
            dbutils.widgets.text("Custom_Settings","")

            TGT1_TBL = dbutils.widgets.get("TGT1_TBL")
            TGT2_TBL = dbutils.widgets.get("TGT2_TBL")
            STG2_TBL = dbutils.widgets.get("STG2_TBL")
            STG3_TBL = dbutils.widgets.get("STG3_TBL")
            FROM_DT = dbutils.widgets.get("FROM_DT")
            TO_DT = dbutils.widgets.get("TO_DT")
            #PRINT ('CREATED SPARK SESSSION 45')
            Settings = dbutils.widgets.get("Custom_Settings")
            Set_list = Settings.split(',')
            if len(Set_list)>0:
                for i in Set_list:
                    if i != "":
                        print("spark.sql(+i+)")
                        spark.sql("""{i}""".format(i=i))
            
            print("before attr_val")
            
            attr_val = spark.sql("""select distinct cast(split(fiscal_yr_and_wk_desc,'-')[0] as int) as fiscal_yr_and_wk_desc from ids_coredata.dim_date where cast(split(fiscal_yr_and_wk_desc,'-')[1] as int)='53' and cast(split(fiscal_yr_and_wk_desc,'-')[0] as int) >= '2016'""")
            print("attr_val ")
            a = attr_val.collect()
            print("collected attr_val")
            b = [int(i.fiscal_yr_and_wk_desc) for i in a]
            print("assigned b")
            years_with_53_weeks = str(b).replace("[", "(").replace("]", ")")
            print("assigned 53")
            #years_with_53_weeks = "2016, 2021, 2027"
            print(" 53 WEEKS")
            
            df_rtb_lookup_table_1 = spark.sql(""" select
        max_qtr_date            ,
        fiscal_wk_ending_date   ,
        fiscal_yr_and_wk_desc   ,
    fiscal_yr_and_wk_desc_actual,
        fiscal_yr_and_wk_desc_derived,
        week_no                 ,
        wkname                  ,
        fiscal_yr_and_wk_desc_lwk       ,
        CASE WHEN cast(split(fiscal_yr_and_wk_in_lqtr_interim,'-')[1] as int) < 10
        THEN concat (cast(split(fiscal_yr_and_wk_in_lqtr_interim,'-')[0] as int) ,'-0',cast(split(fiscal_yr_and_wk_in_lqtr_interim,'-')[1] as int))
        else fiscal_yr_and_wk_in_lqtr_interim
        end as fiscal_yr_and_wk_in_lqtr,
        CASE WHEN cast(split(fiscal_yr_and_wk_ly_interim,'-')[1] as int) < 10
        THEN concat (cast(split(fiscal_yr_and_wk_ly_interim,'-')[0] as int) ,'-0',cast(split(fiscal_yr_and_wk_ly_interim,'-')[1] as int))
        else fiscal_yr_and_wk_ly_interim
        end as fiscal_yr_and_wk_ly,
        CASE WHEN cast(split(fiscal_yr_and_wk_in_lylq_interim,'-')[1] as int) < 10
        THEN concat (cast(split(fiscal_yr_and_wk_in_lylq_interim,'-')[0] as int) ,'-0',cast(split(fiscal_yr_and_wk_in_lylq_interim,'-')[1] as int))
        else fiscal_yr_and_wk_in_lylq_interim
        end as fiscal_yr_and_wk_in_lylq,
        fiscal_wk_in_qtr        ,
        fiscal_yr_and_qtr_flag  ,
        fiscal_yr_and_qtr       ,
        fiscal_yr_and_qtr_desc  ,
        quarter_no              ,
        fiscal_yr_and_qtr_desc_lqtr     ,
        fiscal_yr_and_qtr_ly    ,
        fiscal_yr_and_qtr_lylq  ,
        fiscal_prv_wk_flag      ,
        fiscal_yr_and_wk_flag   ,
        week_select_flag        ,
        fiscal_yr_and_wk        ,
        day_name                ,
        baked_weeks             ,
        baked_weeks_new
        from            
        (
        select
        max(date_date) over (partition by fiscal_yr_and_qtr_desc order by NULL) as max_qtr_date,
        date_date fiscal_wk_ending_date,
        fiscal_yr_and_wk_desc,
        L1.fiscal_yr_and_wk_desc_actual,
        L1.fiscal_yr_and_wk_desc_derived,
        concat(split(fiscal_yr_and_wk_desc,'-')[0],split(fiscal_yr_and_wk_desc,'-')[1])as week_no,
        concat('Q',cast(fiscal_yr_and_qtr%10 as int),'W',fiscal_wk_in_qtr) as wkname,
        CASE
        WHEN cast(split(fiscal_yr_and_wk_desc,'-')[0] -1 as int) in {years_with_53_weeks}
        THEN CASE WHEN split(fiscal_yr_and_wk_desc,'-')[1]='01' THEN concat(cast(split(fiscal_yr_and_wk_desc,'-')[0] -1 as int),'-','53')
		WHEN split(fiscal_yr_and_wk_desc,'-')[1] in ('02','03','04','05','06','07','08','09','10')
        THEN concat(cast(split(fiscal_yr_and_wk_desc,'-')[0] as int),'-0',cast(split(fiscal_yr_and_wk_desc,'-')[1] -1 as int))
        ELSE concat(cast(split(fiscal_yr_and_wk_desc,'-')[0] as int),'-',cast(split(fiscal_yr_and_wk_desc,'-')[1] -1 as int)) END
        ELSE
        CASE WHEN split(fiscal_yr_and_wk_desc,'-')[1]='01' THEN concat(cast(split(fiscal_yr_and_wk_desc,'-')[0] -1 as int),'-','52')
        WHEN split(fiscal_yr_and_wk_desc,'-')[1] in ('02','03','04','05','06','07','08','09','10')
        THEN concat(cast(split(fiscal_yr_and_wk_desc,'-')[0] as int),'-0',cast(split(fiscal_yr_and_wk_desc,'-')[1] -1 as int))
        ELSE concat(cast(split(fiscal_yr_and_wk_desc,'-')[0] as int),'-',cast(split(fiscal_yr_and_wk_desc,'-')[1] -1 as int)) END
        END as fiscal_yr_and_wk_desc_lwk,
        CASE
		WHEN cast(split(fiscal_yr_and_wk_desc,'-')[0]  as int) in {years_with_53_weeks}
        THEN
         CASE
         WHEN split(fiscal_yr_and_qtr_desc,'-')[1]='Q1'
         THEN concat(cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] +39 as int))
         ELSE
          CASE
          WHEN cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -13 as int) <= 13
          THEN
          concat(cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] as int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -13 as int))
          ELSE
          concat(cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] as int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -12 as int))
          END
         END
        ELSE
          CASE
      WHEN cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int) in {years_with_53_weeks} AND split(fiscal_yr_and_qtr_desc,'-')[1]='Q1'
      THEN concat(cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] +40 as int))
                WHEN split(fiscal_yr_and_qtr_desc,'-')[1]='Q1'
                THEN concat(cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] +39 as int))
          ELSE
           concat(cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] as int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -13 as int))
          END
        END as fiscal_yr_and_wk_in_lqtr_interim,
        CASE
        WHEN cast(split(fiscal_yr_and_wk_desc,'-')[0] as int)  in {years_with_53_weeks} AND cast(split(fiscal_yr_and_wk_desc,'-')[1]as int) > 13
        THEN concat(cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] as int))
        WHEN cast(split(fiscal_yr_and_wk_desc,'-')[0] -1 as int)  in {years_with_53_weeks} AND cast(split(fiscal_yr_and_wk_desc,'-')[1]as int) > 13
        THEN
        CASE WHEN cast(split(fiscal_yr_and_wk_desc,'-')[1] -13 as int) <= 0
        THEN concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[1] as int))
        ELSE concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[1] +1 as int))
        END
        ELSE
        CASE WHEN cast(split(fiscal_yr_and_wk_desc,'-')[1] -13 as int) <= 0
        THEN concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[1] as int))
        ELSE concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[1] as int))
        END
        END as fiscal_yr_and_wk_ly_interim,
CASE
     WHEN CAST(split(fiscal_yr_and_wk_desc,'-')[1] -13 AS int) <= 0
     THEN
     CASE WHEN cast(split(fiscal_yr_and_wk_desc,'-')[0] -2 AS int)  IN {years_with_53_weeks}
     THEN concat(cast(split(L1.fiscal_yr_and_wk_desc_actual,'-')[0] -2 AS int),'-',cast(split(fiscal_yr_and_wk_desc_actual,'-')[1] +40 AS int))
     ELSE concat(cast(split(L1.fiscal_yr_and_wk_desc_actual,'-')[0] -2 AS int),'-',cast(split(fiscal_yr_and_wk_desc_actual,'-')[1] +39 AS int))
     END
     ELSE
    CASE WHEN cast(split(fiscal_yr_and_wk_desc,'-')[0] AS int)  IN {years_with_53_weeks}
    THEN concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 AS int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -13 AS int))
       ELSE
     CASE WHEN cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -12 AS int) <= 14
     THEN concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 AS int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -13 AS int))
     ELSE
      CASE WHEN cast(split(fiscal_yr_and_wk_desc,'-')[0] -1 AS int) IN {years_with_53_weeks}
      THEN concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 AS int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -12 AS int))
      ELSE concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 AS int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -13 AS int))
      END
     END
    END
END AS fiscal_yr_and_wk_in_lylq_interim,
        fiscal_wk_in_qtr,
        fiscal_yr_and_qtr_flag,
        fiscal_yr_and_qtr,
        fiscal_yr_and_qtr_desc,
        concat(split(fiscal_yr_and_qtr_desc,'-')[0],split(fiscal_yr_and_qtr_desc,'-')[1])as quarter_no,
        CASE WHEN split(fiscal_yr_and_qtr_desc,'-')[1]='Q1' THEN concat(cast(split(fiscal_yr_and_qtr_desc,'-')[0] -1 as int),'-','Q4')
        ELSE concat(cast(split(fiscal_yr_and_qtr_desc,'-')[0] as int),'-Q',cast(substr(split(fiscal_yr_and_qtr_desc,'-')[1],2,1) -1 as int)) END as fiscal_yr_and_qtr_desc_lqtr,
        concat(cast(split(fiscal_yr_and_qtr_desc,'-')[0] -1 as int),'-',split(fiscal_yr_and_qtr_desc,'-')[1]) as fiscal_yr_and_qtr_ly,
        CASE WHEN split(fiscal_yr_and_qtr_desc,'-')[1]='Q1' THEN concat(cast(split(fiscal_yr_and_qtr_desc,'-')[0] -2 as int),'-','Q4')
        ELSE concat(cast(split(fiscal_yr_and_qtr_desc,'-')[0] -1 as int),'-Q',cast(substr(split(fiscal_yr_and_qtr_desc,'-')[1],2,1) -1 as int)) END as fiscal_yr_and_qtr_lylq,
        fiscal_prv_wk_flag,
        fiscal_yr_and_wk_flag,
        case when to_date(D1.fiscal_wk_ending_date) < current_date() then 1 else 0 end as week_select_flag,
        D1.fiscal_yr_and_wk,
        D1.day_name,
        datediff (to_date(b.fiscal_wk_ending_date), to_date(D1.fiscal_wk_ending_date))/7 as baked_weeks,
        datediff (to_date(c.fiscal_wk_ending_date), to_date(D1.fiscal_wk_ending_date))/7 as baked_weeks_new
        from
        {STG2_TBL} D1
        inner join {STG3_TBL} L1
        on D1.fiscal_yr_and_wk_desc = L1.fiscal_yr_and_wk_desc_actual
        cross join (select abc.fiscal_wk_ending_date
        from
        {STG2_TBL} abc
        where abc.day_name='FRIDAY'
        and abc.fiscal_prv_wk_flag = 'Y'
        ) b
        cross join 
        (
        select distinct fiscal_wk_ending_date from {STG2_TBL} where date_date = current_date()
        ) c
WHERE D1.date_date BETWEEN "{FROM_DT}" AND "{TO_DT}" and D1.fiscal_yr_and_wk_flag='Y') XYZ """.format(TGT1_TBL=TGT1_TBL,years_with_53_weeks=years_with_53_weeks,STG2_TBL=STG2_TBL,STG3_TBL=STG3_TBL,FROM_DT=FROM_DT,TO_DT=TO_DT))
            #print(df_rtb_lookup_table_1.show(200))
            print("AFTER select")
            df_rtb_lookup_table_1.registerTempTable("df_rtb_lookup_table_1")
            df_rtb_lookup_table_1.write.format("parquet").mode("overwrite").insertInto("%s"%TGT1_TBL,overwrite=True)
            
            '''df_rtb_lookup_table_2 = spark.sql(""" select
        max_qtr_date            ,
        fiscal_wk_ending_date   ,
        fiscal_yr_and_wk_desc   ,
    fiscal_yr_and_wk_desc_actual,
        fiscal_yr_and_wk_desc_derived,
        week_no                 ,
        wkname                  ,
        fiscal_yr_and_wk_desc_lwk       ,
        CASE WHEN cast(split(fiscal_yr_and_wk_in_lqtr_interim,'-')[1] as int) < 10
        THEN concat (cast(split(fiscal_yr_and_wk_in_lqtr_interim,'-')[0] as int) ,'-0',cast(split(fiscal_yr_and_wk_in_lqtr_interim,'-')[1] as int))
        else fiscal_yr_and_wk_in_lqtr_interim
        end as fiscal_yr_and_wk_in_lqtr,
        CASE WHEN cast(split(fiscal_yr_and_wk_ly_interim,'-')[1] as int) < 10
        THEN concat (cast(split(fiscal_yr_and_wk_ly_interim,'-')[0] as int) ,'-0',cast(split(fiscal_yr_and_wk_ly_interim,'-')[1] as int))
        else fiscal_yr_and_wk_ly_interim
        end as fiscal_yr_and_wk_ly,
        CASE WHEN cast(split(fiscal_yr_and_wk_in_lylq_interim,'-')[1] as int) < 10
        THEN concat (cast(split(fiscal_yr_and_wk_in_lylq_interim,'-')[0] as int) ,'-0',cast(split(fiscal_yr_and_wk_in_lylq_interim,'-')[1] as int))
        else fiscal_yr_and_wk_in_lylq_interim
        end as fiscal_yr_and_wk_in_lylq,
        fiscal_wk_in_qtr        ,
        fiscal_yr_and_qtr_flag  ,
        fiscal_yr_and_qtr       ,
        fiscal_yr_and_qtr_desc  ,
        quarter_no              ,
        fiscal_yr_and_qtr_desc_lqtr     ,
        fiscal_yr_and_qtr_ly    ,
        fiscal_yr_and_qtr_lylq  ,
        fiscal_prv_wk_flag      ,
        fiscal_yr_and_wk_flag   ,
        week_select_flag        ,
        fiscal_yr_and_wk        ,
        day_name                ,
        baked_weeks
        from            
        (
        select
        max(date_date) over (partition by fiscal_yr_and_qtr_desc order by NULL) as max_qtr_date,
        date_date fiscal_wk_ending_date,
        fiscal_yr_and_wk_desc,
        L1.fiscal_yr_and_wk_desc_actual,
        L1.fiscal_yr_and_wk_desc_derived,
        concat(split(fiscal_yr_and_wk_desc,'-')[0],split(fiscal_yr_and_wk_desc,'-')[1])as week_no,
        concat('Q',cast(fiscal_yr_and_qtr%10 as int),'W',fiscal_wk_in_qtr) as wkname,
        CASE
        WHEN cast(split(fiscal_yr_and_wk_desc,'-')[0] -1 as int) in {years_with_53_weeks}
        THEN CASE WHEN split(fiscal_yr_and_wk_desc,'-')[1]='01' THEN concat(cast(split(fiscal_yr_and_wk_desc,'-')[0] -1 as int),'-','53')
		WHEN split(fiscal_yr_and_wk_desc,'-')[1] in ('02','03','04','05','06','07','08','09','10')
        THEN concat(cast(split(fiscal_yr_and_wk_desc,'-')[0] as int),'-0',cast(split(fiscal_yr_and_wk_desc,'-')[1] -1 as int))
        ELSE concat(cast(split(fiscal_yr_and_wk_desc,'-')[0] as int),'-',cast(split(fiscal_yr_and_wk_desc,'-')[1] -1 as int)) END
        ELSE
        CASE WHEN split(fiscal_yr_and_wk_desc,'-')[1]='01' THEN concat(cast(split(fiscal_yr_and_wk_desc,'-')[0] -1 as int),'-','52')
        WHEN split(fiscal_yr_and_wk_desc,'-')[1] in ('02','03','04','05','06','07','08','09','10')
        THEN concat(cast(split(fiscal_yr_and_wk_desc,'-')[0] as int),'-0',cast(split(fiscal_yr_and_wk_desc,'-')[1] -1 as int))
        ELSE concat(cast(split(fiscal_yr_and_wk_desc,'-')[0] as int),'-',cast(split(fiscal_yr_and_wk_desc,'-')[1] -1 as int)) END
        END as fiscal_yr_and_wk_desc_lwk,
        CASE
		WHEN cast(split(fiscal_yr_and_wk_desc,'-')[0]  as int) in {years_with_53_weeks}
        THEN
         CASE
         WHEN split(fiscal_yr_and_qtr_desc,'-')[1]='Q1'
         THEN concat(cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] +39 as int))
         ELSE
          CASE
          WHEN cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -13 as int) <= 13
          THEN
          concat(cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] as int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -13 as int))
          ELSE
          concat(cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] as int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -12 as int))
          END
         END
        ELSE
          CASE
      WHEN cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int) in {years_with_53_weeks} AND split(fiscal_yr_and_qtr_desc,'-')[1]='Q1'
      THEN concat(cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] +40 as int))
                WHEN split(fiscal_yr_and_qtr_desc,'-')[1]='Q1'
                THEN concat(cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] +39 as int))
          ELSE
           concat(cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] as int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -13 as int))
          END
        END as fiscal_yr_and_wk_in_lqtr_interim,
        CASE
        WHEN cast(split(fiscal_yr_and_wk_desc,'-')[0] as int)  in {years_with_53_weeks} AND cast(split(fiscal_yr_and_wk_desc,'-')[1]as int) > 13
        THEN concat(cast(split(fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] as int))
        WHEN cast(split(fiscal_yr_and_wk_desc,'-')[0] -1 as int)  in {years_with_53_weeks} AND cast(split(fiscal_yr_and_wk_desc,'-')[1]as int) > 13
        THEN
        CASE WHEN cast(split(fiscal_yr_and_wk_desc,'-')[1] -13 as int) <= 0
        THEN concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[1] as int))
        ELSE concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[1] +1 as int))
        END
        ELSE
        CASE WHEN cast(split(fiscal_yr_and_wk_desc,'-')[1] -13 as int) <= 0
        THEN concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[1] as int))
        ELSE concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 as int),'-',cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[1] as int))
        END
        END as fiscal_yr_and_wk_ly_interim,
CASE
     WHEN CAST(split(fiscal_yr_and_wk_desc,'-')[1] -13 AS int) <= 0
     THEN
     CASE WHEN cast(split(fiscal_yr_and_wk_desc,'-')[0] -2 AS int)  IN {years_with_53_weeks}
     THEN concat(cast(split(L1.fiscal_yr_and_wk_desc_actual,'-')[0] -2 AS int),'-',cast(split(fiscal_yr_and_wk_desc_actual,'-')[1] +40 AS int))
     ELSE concat(cast(split(L1.fiscal_yr_and_wk_desc_actual,'-')[0] -2 AS int),'-',cast(split(fiscal_yr_and_wk_desc_actual,'-')[1] +39 AS int))
     END
     ELSE
    CASE WHEN cast(split(fiscal_yr_and_wk_desc,'-')[0] AS int)  IN {years_with_53_weeks}
    THEN concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 AS int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -13 AS int))
       ELSE
     CASE WHEN cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -12 AS int) <= 14
     THEN concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 AS int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -13 AS int))
     ELSE
      CASE WHEN cast(split(fiscal_yr_and_wk_desc,'-')[0] -1 AS int) IN {years_with_53_weeks}
      THEN concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 AS int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -12 AS int))
      ELSE concat(cast(split(L1.fiscal_yr_and_wk_desc_derived,'-')[0] -1 AS int),'-',cast(split(fiscal_yr_and_wk_desc_derived,'-')[1] -13 AS int))
      END
     END
    END
END AS fiscal_yr_and_wk_in_lylq_interim,
        fiscal_wk_in_qtr,
        fiscal_yr_and_qtr_flag,
        fiscal_yr_and_qtr,
        fiscal_yr_and_qtr_desc,
        concat(split(fiscal_yr_and_qtr_desc,'-')[0],split(fiscal_yr_and_qtr_desc,'-')[1])as quarter_no,
        CASE WHEN split(fiscal_yr_and_qtr_desc,'-')[1]='Q1' THEN concat(cast(split(fiscal_yr_and_qtr_desc,'-')[0] -1 as int),'-','Q4')
        ELSE concat(cast(split(fiscal_yr_and_qtr_desc,'-')[0] as int),'-Q',cast(substr(split(fiscal_yr_and_qtr_desc,'-')[1],2,1) -1 as int)) END as fiscal_yr_and_qtr_desc_lqtr,
        concat(cast(split(fiscal_yr_and_qtr_desc,'-')[0] -1 as int),'-',split(fiscal_yr_and_qtr_desc,'-')[1]) as fiscal_yr_and_qtr_ly,
        CASE WHEN split(fiscal_yr_and_qtr_desc,'-')[1]='Q1' THEN concat(cast(split(fiscal_yr_and_qtr_desc,'-')[0] -2 as int),'-','Q4')
        ELSE concat(cast(split(fiscal_yr_and_qtr_desc,'-')[0] -1 as int),'-Q',cast(substr(split(fiscal_yr_and_qtr_desc,'-')[1],2,1) -1 as int)) END as fiscal_yr_and_qtr_lylq,
        fiscal_prv_wk_flag,
        fiscal_yr_and_wk_flag,
        case when to_date(D1.fiscal_wk_ending_date) < current_date() then 1 else 0 end as week_select_flag,
        D1.fiscal_yr_and_wk,
        D1.day_name,
        datediff (to_date(b.fiscal_wk_ending_date), to_date(D1.fiscal_wk_ending_date))/7 as baked_weeks
        from
        {STG2_TBL} D1
        inner join {STG3_TBL} L1
        on D1.fiscal_yr_and_wk_desc = L1.fiscal_yr_and_wk_desc_actual
        cross join (select abc.fiscal_wk_ending_date
        from
        {STG2_TBL} abc
        where abc.day_name='FRIDAY'
        and abc.fiscal_prv_wk_flag = 'Y'
        ) b
WHERE D1.date_date BETWEEN "{FROM_DT_2}" AND "{TO_DT}" and D1.fiscal_yr_and_wk_flag='Y') XYZ """.format(TGT2_TBL=TGT2_TBL,years_with_53_weeks=years_with_53_weeks,STG2_TBL=STG2_TBL,STG3_TBL=STG3_TBL,FROM_DT_2=FROM_DT_2,TO_DT=TO_DT))'''
            df_rtb_lookup_table_2 = spark.sql("""SELECT * FROM df_rtb_lookup_table_1 WHERE fiscal_yr_and_qtr_desc IN (SELECT DISTINCT fiscal_yr_and_qtr_desc FROM df_rtb_lookup_table_1 ORDER BY fiscal_yr_and_qtr_desc DESC LIMIT 2)""")
            #print(df_rtb_lookup_table_2.show(200))
            print("AFTER select")
            df_rtb_lookup_table_2.registerTempTable("df_rtb_lookup_table_2")
            df_rtb_lookup_table_2.write.format("parquet").mode("overwrite").insertInto("%s"%TGT2_TBL,overwrite=True)
            
            dbutils.notebook.exit("SUCCESS")   
        except Exception as e:
            dbutils.notebook.exit(e)

if __name__ == '__main__': 
        main()
