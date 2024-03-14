%sql
CREATE VIEW b2b_phones.abddash_agent_360 (
  date,
  cc_phone_vs_web,
  Geo,
  Sales_Center,
  Sales_document,
  dylan_order_number,
  sales_document_item,
  contract_id,
  Txns,
  route_to_market,
  PROMO_TYPE,
  cc_segment,
  product_group,
  sub_product_group,
  stype,
  market_area_description,
  AgentMapFlag,
  cross_sell_team,
  original_cross_sell_team,
  cross_team_agent,
  RepLdap,
  created_ldap,
  RepName,
  attained_RepName,
  created_RepName,
  RepTSM,
  attained_TSM,
  created_TSM,
  RepManger,
  attained_Manager,
  created_Manager,
  original_RepManger,
  original_attained_Manager,
  original_created_Manager,
  rep_manager_ldap,
  attained_manager_ldap,
  created_manager_ldap,
  original_rep_manager_ldap,
  original_attained_manager_ldap,
  orginal_created_manager_ldap,
  repteam,
  attained_team,
  created_team,
  original_repteam,
  original_attained_team,
  original_created_team,
  display_team_category_attained,
  display_team_category_created,
  Team_Category,
  flag,
  attained_flag,
  unattained_flag,
  customer_email_domain,
  acct_name,
  net_purchases_arr_cfx_attained,
  net_cancelled_arr_cfx_attained,
  reactivated_arr_cfx_attained,
  net_migration_arr_cfx_attained,
  returns_arr_cfx_attained,
  net_new_subs_attained,
  gross_new_subs_attained,
  net_purchases_arr_cfx_created,
  net_cancelled_arr_cfx_created,
  reactivated_arr_cfx_created,
  net_migration_arr_cfx_created,
  returns_arr_cfx_created,
  net_new_subs_created,
  gross_new_subs_created,
  net_purchases_deal_band_item,
  net_cancelled_deal_band_item,
  net_purchases_deal_band_arr_overall,
  net_cancelled_band_arr_overall,
  net_purchases_deal_band_overall,
  net_cancelled_deal_band_overall,
  refresh_date,
  cw_flag,
  cw_latest_flag,
  lw_flag,
  FISCAL_YR_AND_QTR_DESC,
  FISCAL_YR_AND_WK_DESC)
TBLPROPERTIES (
  'bucketing_version' = '2',
  'transient_lastDdlTime' = '1704377016')
AS select 
date,
cc_phone_vs_web,
Geo,
Sales_Center,
Sales_document,
dylan_order_number,
sales_document_item,
contract_id,
Txns,
route_to_market,
PROMO_TYPE,
cc_segment,
product_group,
sub_product_group,
stype,
market_area_description,
AgentMapFlag,
display_cross_sell_team.Display_team_name as cross_sell_team,
cross_sell_team as original_cross_sell_team,
cross_team_agent,
RepLdap,
created_ldap,
RepName,
attained_RepName,
created_RepName,
RepTSM,
attained_TSM,
created_TSM,
display_repteam.Display_team_manager as RepManger,
display_attainedteam.Display_team_manager as attained_Manager,
display_createdteam.Display_team_manager as created_Manager,
RepManger as original_RepManger,
attained_Manager as original_attained_Manager,
created_Manager as original_created_Manager,
display_repteam.Display_team_manager_ldap as rep_manager_ldap,
display_attainedteam.Display_team_manager_ldap as attained_manager_ldap,
display_createdteam.Display_team_manager_ldap as created_manager_ldap,
rep_manager_ldap as original_rep_manager_ldap,
attained_manager_ldap as original_attained_manager_ldap,
created_manager_ldap as orginal_created_manager_ldap,
display_repteam.Display_team_name as repteam,
display_attainedteam.Display_team_name as attained_team,
display_createdteam.Display_team_name as created_team,
repteam as original_repteam,
attained_team as original_attained_team,
created_team as original_created_team,
display_team_category_attained,
display_team_category_created,
Team_Category,
flag,
case when upper(trim(rep_manager_ldap)) = upper(trim(attained_manager_ldap)) then 'Y' else 'N' end as attained_flag,
case when upper(trim(rep_manager_ldap)) = upper(trim(created_manager_ldap)) and upper(trim(rep_manager_ldap)) <> upper(trim(attained_manager_ldap)) then 'Y' else 'N' end as unattained_flag,
customer_email_domain,
acct_name,
net_purchases_arr_cfx_attained ,
net_cancelled_arr_cfx_attained ,
reactivated_arr_cfx_attained ,
net_migration_arr_cfx_attained ,
returns_arr_cfx_attained,
net_new_subs_attained,
gross_new_subs_attained,
net_purchases_arr_cfx_created ,
net_cancelled_arr_cfx_created ,
reactivated_arr_cfx_created ,
net_migration_arr_cfx_created ,
returns_arr_cfx_created,
net_new_subs_created,
gross_new_subs_created,
net_purchases_deal_band_item,
net_cancelled_deal_band_item,
net_purchases_deal_band_arr_overall,
net_cancelled_band_arr_overall,
net_purchases_deal_band_overall,
net_cancelled_deal_band_overall,
refresh_date,
vw_calender_flags.cw_flag,
vw_calender_flags.cw_flag_new as cw_latest_flag,
vw_calender_flags.lw_flag,
FISCAL_YR_AND_QTR_DESC,
FISCAL_YR_AND_WK_DESC
from b2b_phones.fact_abddash_rep_performance a
left outer join
b2b_phones.vw_calender_flags vw_calender_flags
on a.fiscal_yr_and_wk_desc = vw_calender_flags.week
left outer JOIN
(select DISTINCT Standard_team_name,Display_team_manager,Display_team_name,Display_team_manager_ldap from b2b_phones.dim_team) display_repteam
on upper(trim(a.repteam)) = upper(trim(display_repteam.Standard_team_name))
left outer JOIN
(select DISTINCT Standard_team_name,Display_team_manager,Display_team_name,Display_team_manager_ldap from b2b_phones.dim_team) display_attainedteam
on upper(trim(a.attained_team))  = upper(trim(display_attainedteam.Standard_team_name))
left outer JOIN
(select DISTINCT Standard_team_name,Display_team_manager,Display_team_name,Display_team_manager_ldap from b2b_phones.dim_team) display_createdteam
on upper(trim(a.created_team))  = upper(trim(display_createdteam.Standard_team_name))
left outer JOIN
(select DISTINCT Standard_team_name,Display_team_manager,Display_team_name from b2b_phones.dim_team) display_cross_sell_team
on upper(trim(a.cross_sell_team)) = upper(trim(display_cross_sell_team.Standard_team_name))