################################################################################################################################################
# READ ME                                                                                                                                      #
#                                                                                                                                              #
# FULL_LOAD FLAG PLEASE PUT 'N' FOR INCREMENTAL AND 'Y' FOR FULL LOAD                                                                          #
# 'Y' -- full_params for historical loads                                                                                                      #
# 'N' -- incremental_params for incremental loads                                                                                              #
#                                                                                                                                              #
# Values for 'Type':                                                                                                                           #
# 'STATIC' --  static value                                                                                                                    #
#  eg : '2021-12-13'                                                                                                                           #
# 'SQL'    --  sql query ( Please make sure the query returns only one value and datetime object are casted as string)                         #
#  eg : 'select cast(max(activity_date) as string)  as date from dbxpoc.fact_user_activity'                                                    #
# 'PYTHON' --  python command                                                                                                                  #
#  eg : 'datetime.today().strftime('%Y-%m-%d')'                                                                                                #
# 'SHELL' --  shell command                                                                                                                    #
#  eg : (date +'%Y-%m-%d' -d 'yesterday')                                                                                                      #
# Values for 'Dtype':                                                                                                                          #
# 'STR' --  String values                                                                                                                      #
# 'QSTR' --  Quoted String values ('19-01-2022')                                                                                               #
# 'INT'    --  Integer Values                                                                                                                  #
#                                                                                                                                              #
# Custom_settings will contain comma separated list job specific spark configs that will run before the query triggers #                       #
# Example                                                                                                                                      #
# 'set spark.sql.adaptive.coalescePartitions.enabled=false , set spark.databricks.sql.files.prorateMaxPartitionBytes.enabled=false'            #
#                                                                                                                                              #
# Post processing query can hold sql command which will run at the end of the job #                                                            #
# Example                                                                                                                                      #
# 'update table tbl_name set last_run_date=<today_date> where job_id=123;'                                                                     #
#                                                                                                                                              #
################################################################################################################################################

import json

flag_dict ={
'FULL_LOAD':'N'}

incremental_params={'Custom_Settings': {'Dtype': 'STR', 'Type': 'STATIC', 'Value': 'set spark.sql.parquet.enableVectorizedReader=false,SET hive.warehouse.data.skiptrash=true'},
 'FROM_DT': {'Dtype': 'STR', 'Type': 'SQL', 'Value': "select date_format(to_date(cast(min_date as string), 'yyyyMMdd'), 'yyyy-MM-dd') from (select min(date_key) as min_date from ids_coredata.dim_date A join (select distinct fiscal_yr_and_qtr from ids_coredata.dim_date where date_key <= (select date_format(CURRENT_DATE, 'yyyyMMdd')) order by 1 desc limit 9)B ON A.fiscal_yr_and_qtr =B.fiscal_yr_and_qtr)"},
 'TGT1_TBL': {'Dtype': 'STR', 'Type': 'STATIC', 'Value': 'b2b_phones.rtb_lookup_table_history'},
 'TGT2_TBL': {'Dtype': 'STR', 'Type': 'STATIC', 'Value': 'b2b_phones.rtb_lookup_table'},
 'STG2_TBL': {'Dtype': 'STR', 'Type': 'STATIC', 'Value': 'ids_coredata.dim_date'},
 'STG3_TBL': {'Dtype': 'STR', 'Type': 'STATIC', 'Value': 'b2b_phones.leap_year_lookup_table'},
 'TO_DT': {'Dtype': 'STR', 'Type': 'SQL', 'Value': "select date_format(to_date(cast(max_date as string), 'yyyyMMdd'), 'yyyy-MM-dd') from (select max(date_key) as max_date from ids_coredata.dim_date A join (select distinct fiscal_yr_and_qtr from ids_coredata.dim_date where date_key <= (select date_format(CURRENT_DATE, 'yyyyMMdd')) order by 1 desc limit 2)B ON A.fiscal_yr_and_qtr =B.fiscal_yr_and_qtr)"}}

full_params={'Custom_Settings': {'Dtype': 'STR', 'Type': 'STATIC', 'Value': 'set spark.sql.parquet.enableVectorizedReader=false,SET hive.warehouse.data.skiptrash=true'},
 'FROM_DT': {'Dtype': 'STR', 'Type': 'STATIC', 'Value': '2021-09-04'},
 'TGT1_TBL': {'Dtype': 'STR', 'Type': 'STATIC', 'Value': 'b2b_phones.rtb_lookup_table_history'},
 'TGT2_TBL': {'Dtype': 'STR', 'Type': 'STATIC', 'Value': 'b2b_phones.rtb_lookup_table'},
 'STG2_TBL': {'Dtype': 'STR', 'Type': 'STATIC', 'Value': 'ids_coredata.dim_date'},
 'STG3_TBL': {'Dtype': 'STR', 'Type': 'STATIC', 'Value': 'b2b_phones.leap_year_lookup_table'},
 'TO_DT': {'Dtype': 'STR', 'Type': 'STATIC', 'Value': '2023-09-10'}}
 

post_processing_query=['']

dct_values={
'flag_dict':flag_dict,
'incremental_params':incremental_params,
'full_params':full_params,
'post_processing_query':post_processing_query}

dbutils.notebook.exit(dct_values)
