################################################################################################################################################
# READ ME																																		 #
#																																				 #
# FULL_LOAD FLAG PLEASE PUT 'N' FOR INCREMENTAL AND 'Y' FOR FULL LOAD																			 #
# 'Y' -- full_params for historical loads																										 #
# 'N' -- incremental_params for incremental loads																								 #
#																																				 #
# Values for 'Type':																															 #
# 'STATIC' --	 static value																													 #
#	 eg : '2021-12-13'																															 #
# 'SQL'	 --	 sql query ( Please make sure the query returns only one value and datetime object are casted as string)						 #
#	 eg : 'select cast(max(activity_date) as string)  as date from dbxpoc.fact_user_activity'													 #
# 'PYTHON' --	 python command																													 #
#	 eg : 'datetime.today().strftime('%Y-%m-%d')'																								 #
# 'SHELL' --	shell command																													 #
#	 eg : (date +'%Y-%m-%d' -d 'yesterday')																										 #
# Values for 'Dtype':																															 #
# 'STR' --  String values																														 #
# 'QSTR' --  Quoted String values ('19-01-2022')																								 #
# 'INT'	 --	 Integer Values																													 #
#																																				 #
# Custom_settings will contain comma separated list job specific spark configs that will run before the query triggers #						 #
# Example																																		 #
# 'set spark.sql.adaptive.coalescePartitions.enabled=false , set spark.databricks.sql.files.prorateMaxPartitionBytes.enabled=false'			 #
#																																				 #
# Post processing query can hold sql command which will run at the end of the job #															 #
# Example																																		 #
# 'update table tbl_name set last_run_date=<today_date> where job_id=123;'																	 #
#																																				 #
################################################################################################################################################

import json

flag_dict = {
'FULL_LOAD':'N'
}

incremental_params={'TGT1_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_orders_negotiated'},
'BKP_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_orders_negotiated_bkp'}, 
'SRC_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_negotiaded_stg'},
'DIM_TEAM': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_team'},                    
'FILE_PATH': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': '/FileStore/tables/B2B_phones/Dim_latam/'},
'TO_ADDR': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'tat80177@adobe.com'},
'FROM_ADDR': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'tat80177@adobe.com'},
'Custom_Settings': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'set spark.sql.parquet.enableVectorizedReader=false,SET hive.warehouse.data.skiptrash=true'}}

full_params={'TGT1_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_orders_negotiated'},
'BKP_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_orders_negotiated_bkp'},
'SRC_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_negotiaded_stg'},
'DIM_TEAM': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_team'},              
'FILE_PATH': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': '/FileStore/tables/B2B_phones/Dim_latam/'},
'TO_ADDR': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'tat80177@adobe.com'},
'FROM_ADDR': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'tat80177@adobe.com'},
'Custom_Settings': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'set spark.sql.parquet.enableVectorizedReader=false,SET hive.warehouse.data.skiptrash=true'}}

post_processing_query = ['']

dct_values = {
'flag_dict' : flag_dict,
'incremental_params' : incremental_params,
'full_params' : full_params,
'post_processing_query' : post_processing_query
}

dbutils.notebook.exit(dct_values)
