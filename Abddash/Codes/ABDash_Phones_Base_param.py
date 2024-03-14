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

incremental_params={'TGT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.ABDashBase_tmp'},  
'file_type': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'CTR'},
'archive_path': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'abfs://or1-prod-data@azr6665prddpaascxia.dfs.core.windows.net/user/hive/warehouse/cxia/ce_analytics/contact/filearchival/'},
'TO_DT': {'Type': 'SQL', 'Dtype': 'QSTR', 'Value': '''select current_date()'''},
'FROM_DT': {'Type': 'SQL', 'Dtype': 'QSTR', 'Value': '''select  min(date_date) from ids_coredata.dim_date where fiscal_yr_and_qtr = (select fiscal_yr_and_qtr from ids_coredata.dim_date where date_key = (select date_Format(current_date,'yyyyMMdd')))'''},
'TGT_TBL_RULES': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.abdashbase_rules'},
'PIVOT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'csmb.vw_ccm_pivot4_all'},
'BOB_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_bob'},
'LATAM_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_orders_negotiated'},
'HENDRIX_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ocf.vw_csui_report'},
'AGENT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_agent_details'},
'DATE_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ids_coredata.dim_date'},
'SEAT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ocf_analytics.dim_seat'},
'LIC_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ocf_analytics.scd_license'},
'OPPORTUNITY_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b.uda_replicn_sf_corp_uda_vw_opportunity'},
'USER_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b.uda_replicn_sf_corp_uda_vw_user'},
'LEAD_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b.uda_replicn_sf_corp_uda_vw_lead'},
'CAMPAIGN_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b.uda_replicn_sf_corp_uda_vw_campaign'},
'TEAM_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_team'},
'SALES_ITEM': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ids_can_analytics.sales_item'},
'PRE_CHECK_EXECUTION': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'Y'},
'TO_ADDR':{'Type': 'STATIC', 'Dtype': 'STR','Value': "ksantra@adobe.com,saimukhe@adobe.com,kabansal@adobe.com,ankitasthana@adobe.com,tat80177@adobe.com"},
'FROM_ADDR':{'Type': 'STATIC','Dtype': 'STR', 'Value': "do-not-reply-dpaas-dbx@adobe.com"},
'AGENT_TBL_SCD': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_agent_details_scd'},
'Custom_Settings': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'set spark.sql.parquet.enableVectorizedReader=false,SET hive.warehouse.data.skiptrash=true'}}

full_params={'TGT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.ABDashBase_tmp'},  
'file_type': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'CTR'},
'archive_path': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'abfs://or1-prod-data@azr6665prddpaascxia.dfs.core.windows.net/user/hive/warehouse/cxia/ce_analytics/contact/filearchival/'},
'TO_DT': {'Type': 'STATIC', 'Dtype': 'QSTR', 'Value': '2023-12-01'},
'FROM_DT': {'Type': 'STATIC', 'Dtype': 'QSTR', 'Value': '2023-09-02'},
'TGT_TBL_RULES': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.abdashbase_rules'},
'PIVOT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'csmb.vw_ccm_pivot4_all'},
'BOB_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_bob'},
'LATAM_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_orders_negotiated'},
'HENDRIX_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ocf.vw_csui_report'},
'AGENT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_agent_details'},
'DATE_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ids_coredata.dim_date'},
'SEAT_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ocf_analytics.dim_seat'},
'LIC_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ocf_analytics.scd_license'},
'OPPORTUNITY_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b.uda_replicn_sf_corp_uda_vw_opportunity'},
'USER_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b.uda_replicn_sf_corp_uda_vw_user'},
'LEAD_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b.uda_replicn_sf_corp_uda_vw_lead'},
'CAMPAIGN_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b.uda_replicn_sf_corp_uda_vw_campaign'},
'TEAM_TBL': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_team'},
'SALES_ITEM': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'ids_can_analytics.sales_item'},
'PRE_CHECK_EXECUTION': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'N'},
'TO_ADDR':{'Type': 'STATIC', 'Dtype': 'STR','Value': "ksantra@adobe.com,saimukhe@adobe.com,kabansal@adobe.com,ankitasthana@adobe.com,tat80177@adobe.com"},
'FROM_ADDR':{'Type': 'STATIC','Dtype': 'STR', 'Value': "do-not-reply-dpaas-dbx@adobe.com"},
'AGENT_TBL_SCD': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'b2b_phones.dim_agent_details_scd'},
'Custom_Settings': {'Type': 'STATIC', 'Dtype': 'STR', 'Value': 'set spark.sql.parquet.enableVectorizedReader=false,SET hive.warehouse.data.skiptrash=true'}}

post_processing_query = ['']

dct_values = {
'flag_dict' : flag_dict,
'incremental_params' : incremental_params,
'full_params' : full_params,
'post_processing_query' : post_processing_query
}

dbutils.notebook.exit(dct_values)
