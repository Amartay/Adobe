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

incremental_params={
'TGT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.Fact_I2T_Adjustments'},
'SRC_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.abdashbase_rules'},
'LVT_PROFILE_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ocf_analytics.dim_user_lvt_profile'},
'FIRMOGRAPHICS_SMB_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b.sp_firmographics_smb_contract_details'}, 
'DIM_SEAT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ocf_analytics.dim_seat'},
'SCD_LICENSE_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ocf_analytics.scd_license'},
'PIVOT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'csmb.vw_ccm_pivot4_all'},
'DIM_CONTRACT': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ocf_analytics.dim_contract'}, 
'GENERIC_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b.generic_domains'}, 
'FROM_DT': {'Type':'SQL','Dtype':'STR','Value':'''select  min(date_date) from ids_coredata.dim_date where fiscal_yr_and_qtr = (select fiscal_yr_and_qtr from ids_coredata.dim_date where date_key = (select date_Format(current_date,'yyyyMMdd'))) '''},  
'TO_DT': {'Type': 'SQL', 'Dtype': 'STR', 'Value': '''select current_date'''}, 
'Custom_Settings': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'set spark.sql.parquet.enableVectorizedReader=false,SET hive.warehouse.data.skiptrash=true'}}

full_params={
'TGT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.Fact_I2T_Adjustments'},
'SRC_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.abdashbase_rules'},
'LVT_PROFILE_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ocf_analytics.dim_user_lvt_profile'},
'FIRMOGRAPHICS_SMB_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b.sp_firmographics_smb_contract_details'}, 
'DIM_SEAT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ocf_analytics.dim_seat'},
'SCD_LICENSE_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ocf_analytics.scd_license'},
'PIVOT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'csmb.vw_ccm_pivot4_all'},
'DIM_CONTRACT': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ocf_analytics.dim_contract'}, 
'GENERIC_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b.generic_domains'}, 
'FROM_DT': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': '2023-09-02'}, 
'TO_DT': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': '2024-01-18'}, 
'Custom_Settings': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'set spark.sql.parquet.enableVectorizedReader=false,SET hive.warehouse.data.skiptrash=true'}}

post_processing_query = ['']

dct_values = {
'flag_dict' : flag_dict,
'incremental_params' : incremental_params,
'full_params' : full_params,
'post_processing_query' : post_processing_query
}

dbutils.notebook.exit(dct_values)
