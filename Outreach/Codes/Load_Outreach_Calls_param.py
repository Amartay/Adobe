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
'FULL_LOAD':'Y'
}

incremental_params={'TGT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.Outreach_calls'},
'OUTREACH_BASE': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.outreach_base'},
'DIM_DATE': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ids_coredata.dim_date'},
'EVENTS': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'prospects,users,accounts,sequenceSteps,sequenceTemplates,sequences,tasks,opportunities,calls,snippets,templates'},
'FROM_DT': {'Type': 'SQL', 'Dtype': 'STR', 'Value': '''select date_add(current_date,-3)'''},
'TO_DT': {'Type': 'SQL', 'Dtype': 'STR', 'Value': '''select date_add(current_date,-1)'''},
'PAGE_SIZE': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': '500'},
'TO_ADDR': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'tat80177@adobe.com,saksangal@adobe.com'},
'FROM_ADDR': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'tat80177@adobe.com'},
'INCREMENTAL_FLAG': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'Y'},
'Custom_Settings': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'set spark.sql.parquet.enableVectorizedReader=false,SET hive.warehouse.data.skiptrash=true'}}

full_params={'TGT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.Outreach_calls'},
'OUTREACH_BASE': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.outreach_base'}, 
'DIM_DATE': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ids_coredata.dim_date'},             
'EVENTS': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'prospects,users,accounts,sequenceStates,sequenceSteps,sequences,tasks,opportunities,calls,snippets,templates'},
'FROM_DT': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': '2023-12-02'},
'TO_DT': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': '2024-02-20'},
'PAGE_SIZE': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': '500'},
'TO_ADDR': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'tat80177@adobe.com,saksangal@adobe.com'},
'FROM_ADDR': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'tat80177@adobe.com'},
'INCREMENTAL_FLAG': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'N'},
'Custom_Settings': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'set spark.sql.parquet.enableVectorizedReader=false,SET hive.warehouse.data.skiptrash=true'}}

post_processing_query = ['']

dct_values = {
'flag_dict' : flag_dict,
'incremental_params' : incremental_params,
'full_params' : full_params,
'post_processing_query' : post_processing_query
}

dbutils.notebook.exit(dct_values)
