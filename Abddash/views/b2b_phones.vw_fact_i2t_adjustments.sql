drop VIEW b2b_phones.vw_fact_i2t_adjustments ;
CREATE VIEW b2b_phones.vw_fact_i2t_adjustments (
sales_document_item,
CREATED_BY,
CRM_CUSTOMER_GUID,
ENTITLEMENT_TYPE,
date_date,
cc_phone_vs_web,
GEO,
MARKET_AREA,
MARKET_SEGMENT,
offer_type_description,
PRODUCT_CONFIG,
product_config_description,
PRODUCT_NAME,
product_name_description,
PROMO_TYPE,
PROMOTION,
Region,
SALES_DOCUMENT,
DYLAN_ORDER_NUMBER,
STYPE,
SUBS_OFFER,
SUBSCRIPTION_ACCOUNT_GUID,
VIP_CONTRACT,
addl_purchase_diff,
addl_purchase_same,
gross_cancel_arr_cfx,
gross_new_arr_cfx,
gross_new_subs,
init_purchase_arr_cfx,
migrated_from_arr_cfx,
migrated_to_arr_cfx,
net_cancelled_arr_cfx,
net_new_arr_cfx,
net_new_subs,
net_purchases_arr_cfx,
reactivated_arr_cfx,
returns_arr_cfx,
ecc_customer_id,
SALES_DISTRICT,
contract_start_date_veda,
contract_end_date_veda,
contract_id,
renewal_from_arr_cfx,
renewal_to_arr_cfx,
projected_dme_gtm_segment,
gtm_acct_segment,
route_to_market,
cc_segment,
net_new_arr,
Sales_Center,
AgentGeo,
RepName,
RepLdap,
con_RepLdap,
RepTSM,
RepManger,
RepTeam,
cross_team_agent,
cross_sell_team,
Flag,
AgentMapFlag,
Txns,
SFDC_opportunity_id,
SFDC_opportunity_created_date,
SFDC_closed_date,
SFDC_email,
SFDC_ecc_salesordernumber,
SFDC_campaignid,
SFDC_name,
SFDC_cum_campaignid,
SFDC_min_date,
SFDC_min_fiscal_yr_and_qtr_desc,
SFDC_min_fiscal_yr_and_wk_desc,
SFDC_Flag,
Bob_Flag,
TeamKey,
gross_new_arr_cfx_unclaimed,
net_purchases_arr_cfx_unclaimed,
unclaimed_ldap,
unclaimed_team,
unclaimed_TSM,
unclaimed_Manager,
agent_max_quarter,
agent_curr_qtr,
created_curr_qtr,
agent_TSM_Ldap,
created_TSM_Ldap,
agent_max_quarter_TSM,
created_max_quarter_TSM,
agent_TSM_team,
created_TSM_team,
agent_TSM_team_key,
created_TSM_team_key,
agent_TSM_flag,
created_TSM_flag,
ABD_Flag,
agent_ABD_Flag,
TSM_ABD_Flag,
TSM_agent_ABD_Flag,
gross_cancellations,
net_cancelled_subs,
migrated_from,
migrated_to,
renewal_from,
renewal_to,
Custom_Flag,
bob_ABD_flag,
lat_ABD_flag,
same_wk_cncl_flag,
cross_team_agent_name,
cross_sell_team_key,
net_value_usd,
acct_name,
customer_email_domain,
payment_method,
net_purchase_agg,
cancel_date_list,
sum_cancel_arr,
net_purchase_arr_incremental,
domain,
org_name,
email,
Generic_vs_buisness,
final_cancel_amt,
final_cancelled_subs_amt,
IN_Cancelled_List,
event,
FISCAL_YR_AND_QTR_DESC,
FISCAL_YR_AND_WK_DESC)
TBLPROPERTIES (
'bucketing_version' = '2',
'transient_lastDdlTime' = '1705309182')
AS select 
RULES.sales_document_item
, RULES.CREATED_BY
, RULES.CRM_CUSTOMER_GUID
, RULES.ENTITLEMENT_TYPE
, RULES.date_date
, RULES.cc_phone_vs_web
, RULES.GEO
, RULES.MARKET_AREA
, RULES.MARKET_SEGMENT
, RULES.offer_type_description
, RULES.PRODUCT_CONFIG
, RULES.product_config_description
, RULES.PRODUCT_NAME
, RULES.product_name_description
, RULES.PROMO_TYPE
, RULES.PROMOTION
, RULES.Region
, RULES.SALES_DOCUMENT
, RULES.DYLAN_ORDER_NUMBER
, RULES.STYPE
, RULES.SUBS_OFFER
, RULES.SUBSCRIPTION_ACCOUNT_GUID
, RULES.VIP_CONTRACT
, RULES.addl_purchase_diff
, RULES.addl_purchase_same
, RULES.gross_cancel_arr_cfx
, RULES.gross_new_arr_cfx
, RULES.gross_new_subs
, RULES.init_purchase_arr_cfx
, RULES.migrated_from_arr_cfx
, RULES.migrated_to_arr_cfx
, RULES.net_cancelled_arr_cfx
, RULES.net_new_arr_cfx
, RULES.net_new_subs
, RULES.net_purchases_arr_cfx
, RULES.reactivated_arr_cfx
, RULES.returns_arr_cfx
, RULES.ecc_customer_id
, RULES.SALES_DISTRICT
, RULES.contract_start_date_veda
, RULES.contract_end_date_veda
, RULES.contract_id
, RULES.renewal_from_arr_cfx
, RULES.renewal_to_arr_cfx
, RULES.projected_dme_gtm_segment
, RULES.gtm_acct_segment
, RULES.route_to_market
, RULES.cc_segment
, RULES.net_new_arr
, RULES.Sales_Center
, RULES.AgentGeo
, RULES.RepName
, RULES.RepLdap
, RULES.con_RepLdap
, RULES.RepTSM
, RULES.RepManger
, RULES.RepTeam
, RULES.cross_team_agent
, RULES.cross_sell_team
, RULES.Flag
, RULES.AgentMapFlag
, RULES.Txns
, RULES.SFDC_opportunity_id
, RULES.SFDC_opportunity_created_date
, RULES.SFDC_closed_date
, RULES.SFDC_email
, RULES.SFDC_ecc_salesordernumber
, RULES.SFDC_campaignid
, RULES.SFDC_name
, RULES.SFDC_cum_campaignid
, RULES.SFDC_min_date
, RULES.SFDC_min_fiscal_yr_and_qtr_desc
, RULES.SFDC_min_fiscal_yr_and_wk_desc
, RULES.SFDC_Flag
, RULES.Bob_Flag
, RULES.TeamKey
, RULES.gross_new_arr_cfx_unclaimed
, RULES.net_purchases_arr_cfx_unclaimed
, RULES.unclaimed_ldap
, RULES.unclaimed_team
, RULES.unclaimed_TSM
, RULES.unclaimed_Manager
, RULES.agent_max_quarter
, RULES.agent_curr_qtr
, RULES.created_curr_qtr
, RULES.agent_TSM_Ldap
, RULES.created_TSM_Ldap
, RULES.agent_max_quarter_TSM
, RULES.created_max_quarter_TSM
, RULES.agent_TSM_team
, RULES.created_TSM_team
, RULES.agent_TSM_team_key
, RULES.created_TSM_team_key
, RULES.agent_TSM_flag
, RULES.created_TSM_flag
, RULES.ABD_Flag
, RULES.agent_ABD_Flag
, RULES.TSM_ABD_Flag
, RULES.TSM_agent_ABD_Flag
, RULES.gross_cancellations
, RULES.net_cancelled_subs
, RULES.migrated_from
, RULES.migrated_to
, RULES.renewal_from
, RULES.renewal_to
, RULES.Custom_Flag
, RULES.bob_ABD_flag
, RULES.lat_ABD_flag
, RULES.same_wk_cncl_flag
, RULES.cross_team_agent_name
, RULES.cross_sell_team_key
, RULES.net_value_usd
, RULES.acct_name
, RULES.customer_email_domain
, RULES.payment_method
, RULES.net_purchase_agg
, RULES.cancel_date_list
, RULES.sum_cancel_arr
, RULES.net_purchase_arr_incremental
, RULES.domain	
, RULES.org_name
, I2T.email
, I2T.Generic_vs_buisness
, I2T.final_cancel_amt
, I2T.final_cancelled_subs_amt	
, I2T.IN_Cancelled_List
, RULES.event
, RULES.FISCAL_YR_AND_QTR_DESC
, RULES.FISCAL_YR_AND_WK_DESC from ( select
a.*,
case when trim(a.customer_email_domain) is null or trim(a.customer_email_domain) = '' or trim(upper(a.customer_email_domain)) = 'NULL' 
then coalesce(h.domain,e.domain,i.domain,j.domain,c.domain,a.customer_email_domain)  
when trim(upper(a.customer_email_domain)) = h.domain then h.domain
when trim(upper(a.customer_email_domain)) = e.domain then e.domain
when trim(upper(a.customer_email_domain)) = i.domain then i.domain
when trim(upper(a.customer_email_domain)) = j.domain then j.domain
when trim(upper(a.customer_email_domain)) = c.domain then c.domain
else coalesce(h.domain,e.domain,i.domain,j.domain,c.domain,a.customer_email_domain) end as domain, 
case when trim(a.customer_email_domain) is null or trim(a.customer_email_domain) = '' or trim(upper(a.customer_email_domain)) = 'NULL' 
then coalesce(h.org_name,e.org_name,i.org_name,j.org_name,c.org_name)
when trim(upper(a.customer_email_domain)) = h.domain then h.org_name
when trim(upper(a.customer_email_domain)) = e.domain then e.org_name
when trim(upper(a.customer_email_domain)) = i.domain then i.org_name
when trim(upper(a.customer_email_domain)) = j.domain then j.org_name
when trim(upper(a.customer_email_domain)) = c.domain then c.org_name
else coalesce(h.org_name,e.org_name,i.org_name,j.org_name,c.org_name) end as org_name
from
(( select * from
( select *
from b2b_phones.abdashbase_rules 
where STYPE = 'TM' and txns in ('Initial Purchase','Rep-Add on') 
and TeamKey <> 'WEB NOT APPLICABLE' and event = 'RULES'
)) a 
left outer join 
ocf_analytics.dim_contract b on upper(a.contract_id) = upper(b.contract_id) 
left outer join 
( select * from 
( select *,row_number() over (partition by enrollee_id order by null)rn from b2b.sp_firmographics_smb_contract_details
) where rn = 1 ) c on upper(b.enrollee_id) = upper(c.enrollee_id)
left outer join 
ocf_analytics.dim_user_lvt_profile d on upper(a.CRM_CUSTOMER_GUID) = upper(d.user_guid)
left outer join 
( select * from 
( select *,row_number() over (partition by domain order by null)rn from b2b.sp_firmographics_smb_contract_details
) where rn = 1 )e 
on SUBSTR(UPPER(d.pers_email), INSTR(UPPER(d.pers_email), '@') + 1)=upper(e.domain)
left outer join 
( select * from 
( select *,row_number() over (partition by contract_id order by null)rn from ocf_analytics.dim_seat
) where rn = 1 )f on upper(a.contract_id) = upper(f.contract_id) 
left outer join 
( select * from 
( select *,row_number() over (partition by user_guid order by row_update_dttm)rn from ocf_analytics.dim_user_lvt_profile
) where rn = 1 )g on upper(f.member_guid) = upper(g.user_guid)
left outer join 
( select * from 
( select *,row_number() over (partition by domain order by null)rn from b2b.sp_firmographics_smb_contract_details
) where rn = 1 )h 
on SUBSTR(UPPER(g.pers_email), INSTR(UPPER(g.pers_email), '@') + 1)=upper(h.domain)
left outer join 
( select * from 
( select *,row_number() over (partition by subscription_account_guid order by null) rn from b2b.sp_firmographics_smb_contract_details
) where rn = 1 ) i on upper(a.subscription_account_guid)=upper(i.subscription_account_guid)
left outer join  
( select * from 
( select *,row_number() over (partition by contract_id order by null)rn from b2b.sp_firmographics_smb_contract_details
) where rn = 1 )j on upper(a.subscription_account_guid)=upper(j.contract_id))) RULES
left outer join 
(select * from b2b_phones.Fact_I2T_Adjustments where Generic_vs_buisness = 'non_generic') I2T
on lower(trim(RULES.domain)) = lower(trim(I2T.domain))
and lower(trim(RULES.org_name)) = lower(trim(I2T.org_name))
and lower(trim(RULES.SALES_DOCUMENT)) = lower(trim(I2T.SALES_DOCUMENT))
and lower(trim(RULES.sales_document_item)) = lower(trim(I2T.sales_document_item))
and lower(trim(RULES.date_date)) = lower(trim(I2T.date_date))
union ALL
select 
RULES.sales_document_item
, RULES.CREATED_BY
, RULES.CRM_CUSTOMER_GUID
, RULES.ENTITLEMENT_TYPE
, RULES.date_date
, RULES.cc_phone_vs_web
, RULES.GEO
, RULES.MARKET_AREA
, RULES.MARKET_SEGMENT
, RULES.offer_type_description
, RULES.PRODUCT_CONFIG
, RULES.product_config_description
, RULES.PRODUCT_NAME
, RULES.product_name_description
, RULES.PROMO_TYPE
, RULES.PROMOTION
, RULES.Region
, RULES.SALES_DOCUMENT
, RULES.DYLAN_ORDER_NUMBER
, RULES.STYPE
, RULES.SUBS_OFFER
, RULES.SUBSCRIPTION_ACCOUNT_GUID
, RULES.VIP_CONTRACT
, RULES.addl_purchase_diff
, RULES.addl_purchase_same
, RULES.gross_cancel_arr_cfx
, RULES.gross_new_arr_cfx
, RULES.gross_new_subs
, RULES.init_purchase_arr_cfx
, RULES.migrated_from_arr_cfx
, RULES.migrated_to_arr_cfx
, RULES.net_cancelled_arr_cfx
, RULES.net_new_arr_cfx
, RULES.net_new_subs
, RULES.net_purchases_arr_cfx
, RULES.reactivated_arr_cfx
, RULES.returns_arr_cfx
, RULES.ecc_customer_id
, RULES.SALES_DISTRICT
, RULES.contract_start_date_veda
, RULES.contract_end_date_veda
, RULES.contract_id
, RULES.renewal_from_arr_cfx
, RULES.renewal_to_arr_cfx
, RULES.projected_dme_gtm_segment
, RULES.gtm_acct_segment
, RULES.route_to_market
, RULES.cc_segment
, RULES.net_new_arr
, RULES.Sales_Center
, RULES.AgentGeo
, RULES.RepName
, RULES.RepLdap
, RULES.con_RepLdap
, RULES.RepTSM
, RULES.RepManger
, RULES.RepTeam
, RULES.cross_team_agent
, RULES.cross_sell_team
, RULES.Flag
, RULES.AgentMapFlag
, RULES.Txns
, RULES.SFDC_opportunity_id
, RULES.SFDC_opportunity_created_date
, RULES.SFDC_closed_date
, RULES.SFDC_email
, RULES.SFDC_ecc_salesordernumber
, RULES.SFDC_campaignid
, RULES.SFDC_name
, RULES.SFDC_cum_campaignid
, RULES.SFDC_min_date
, RULES.SFDC_min_fiscal_yr_and_qtr_desc
, RULES.SFDC_min_fiscal_yr_and_wk_desc
, RULES.SFDC_Flag
, RULES.Bob_Flag
, RULES.TeamKey
, RULES.gross_new_arr_cfx_unclaimed
, RULES.net_purchases_arr_cfx_unclaimed
, RULES.unclaimed_ldap
, RULES.unclaimed_team
, RULES.unclaimed_TSM
, RULES.unclaimed_Manager
, RULES.agent_max_quarter
, RULES.agent_curr_qtr
, RULES.created_curr_qtr
, RULES.agent_TSM_Ldap
, RULES.created_TSM_Ldap
, RULES.agent_max_quarter_TSM
, RULES.created_max_quarter_TSM
, RULES.agent_TSM_team
, RULES.created_TSM_team
, RULES.agent_TSM_team_key
, RULES.created_TSM_team_key
, RULES.agent_TSM_flag
, RULES.created_TSM_flag
, RULES.ABD_Flag
, RULES.agent_ABD_Flag
, RULES.TSM_ABD_Flag
, RULES.TSM_agent_ABD_Flag
, RULES.gross_cancellations
, RULES.net_cancelled_subs
, RULES.migrated_from
, RULES.migrated_to
, RULES.renewal_from
, RULES.renewal_to
, RULES.Custom_Flag
, RULES.bob_ABD_flag
, RULES.lat_ABD_flag
, RULES.same_wk_cncl_flag
, RULES.cross_team_agent_name
, RULES.cross_sell_team_key
, RULES.net_value_usd
, RULES.acct_name
, RULES.customer_email_domain
, RULES.payment_method
, RULES.net_purchase_agg
, RULES.cancel_date_list
, RULES.sum_cancel_arr
, RULES.net_purchase_arr_incremental
, RULES.domain	
, RULES.org_name
, I2T.email
, I2T.Generic_vs_buisness
, I2T.final_cancel_amt
, I2T.final_cancelled_subs_amt	
, I2T.IN_Cancelled_List
, RULES.event
, RULES.FISCAL_YR_AND_QTR_DESC
, RULES.FISCAL_YR_AND_WK_DESC from ( select
a.*,
coalesce(c.email,e.email) as email,
case when trim(a.customer_email_domain) is null or trim(a.customer_email_domain) = '' or trim(upper(a.customer_email_domain)) = 'NULL'  
then coalesce(c.domain,e.domain,a.customer_email_domain)  
when trim(upper(a.customer_email_domain)) = c.domain then c.domain
when trim(upper(a.customer_email_domain)) = e.domain then e.domain
else coalesce(c.domain,e.domain,a.customer_email_domain) end as domain,
case when trim(a.customer_email_domain) is null or trim(a.customer_email_domain) = '' or trim(upper(a.customer_email_domain)) = 'NULL' 
then coalesce(c.org_name,e.org_name)
when trim(upper(a.customer_email_domain)) = c.domain then c.org_name
when trim(upper(a.customer_email_domain)) = e.domain then e.org_name
else coalesce(c.org_name,e.org_name) end as org_name
from
(( select * from
( select *
from b2b_phones.abdashbase_rules 
where STYPE = 'TM' and txns in ('Initial Purchase','Rep-Add on') 
and TeamKey <> 'WEB NOT APPLICABLE' and event = 'RULES'
)) a 
left outer join 
ocf_analytics.dim_contract b on upper(a.contract_id) = upper(b.contract_id) 
left outer join 
( select * from 
( select *,row_number() over (partition by enrollee_id order by null)rn from  b2b.sp_firmographics_smb_contract_details
) where rn = 1 ) c on upper(b.enrollee_id) = upper(c.enrollee_id)
left outer join 
ocf_analytics.dim_user_lvt_profile d on upper(a.CRM_CUSTOMER_GUID) = upper(d.user_guid)
left outer join 
( select * from 
( select *,row_number() over (partition by domain order by null)rn from  b2b.sp_firmographics_smb_contract_details
) where rn = 1 ) e 
on UPPER(d.pers_email)=upper(e.email))) RULES
left outer join 
(select * from b2b_phones.Fact_I2T_Adjustments where Generic_vs_buisness = 'generic') I2T
on lower(trim(RULES.domain)) = lower(trim(I2T.domain))
and lower(trim(RULES.org_name)) = lower(trim(I2T.org_name))
and lower(trim(RULES.SALES_DOCUMENT)) = lower(trim(I2T.SALES_DOCUMENT))
and lower(trim(RULES.sales_document_item)) = lower(trim(I2T.sales_document_item))
and lower(trim(RULES.date_date)) = lower(trim(I2T.date_date))
