%sql
CREATE VIEW b2b_phones.vw_abddash
as
select 
financial.GEO,
financial.MARKET_AREA,
dim_country.market_area_description as market_area_description,
financial.MARKET_SEGMENT,
financial.cc_phone_vs_web,
financial.sales_document_item,
financial.CREATED_BY,
financial.CRM_CUSTOMER_GUID,
financial.ENTITLEMENT_TYPE,
financial.date_date,
financial.FISCAL_YR_AND_QTR_DESC,
financial.projected_dme_gtm_segment,
financial.gtm_acct_segment,
financial.route_to_market,
financial.PROMOTION,
financial.PROMO_TYPE,
financial.SALES_DISTRICT,
coalesce(upper(trim(financial.Sales_Center)),'UNKNOWN') as Sales_Center,
financial.RepName,
financial.con_RepLdap as RepLdap,
financial.RepTSM,
dim_team.Display_team_manager as RepManager,
dim_team.Display_team_manager_ldap as RepManagerLdap,
case when financial.RepManger = 'UNKNOWN' then coalesce(team_unknown_manager.Manager,'UNKNOWN') else financial.RepManger end Original_RepManager,
dim_team.Display_team_name as  repteam,
dim_team.Standard_team_name as Original_repteam,
dim_team.Team_category,
dim_team.Display_Team_Category,
financial.addl_purchase_diff,
financial.addl_purchase_same,
financial.gross_cancel_arr_cfx,
financial.gross_new_arr_cfx,
financial.gross_new_subs,
financial.init_purchase_arr_cfx,
financial.migrated_from_arr_cfx,
financial.migrated_to_arr_cfx,
financial.net_cancelled_arr_cfx,
--financial.net_new_arr_cfx,
financial.net_new_subs,
financial.net_purchases_arr_cfx,
financial.net_migration_arr_cfx,
((financial.net_purchases_arr_cfx + financial.net_migration_arr_cfx)-financial.net_cancelled_arr_cfx) as net_new_arr_cfx,
financial.reactivated_arr_cfx,
financial.returns_arr_cfx,
financial.offer_type_description,
financial.PRODUCT_CONFIG,
financial.product_config_description,
financial.PRODUCT_NAME,
financial.product_name_description,
financial.Region,
financial.SALES_DOCUMENT,
financial.DYLAN_ORDER_NUMBER,
financial.STYPE,
financial.SUBS_OFFER,
financial.SUBSCRIPTION_ACCOUNT_GUID,
financial.VIP_CONTRACT,
financial.payment_method,
financial.ecc_customer_id,
financial.contract_start_date_veda,
financial.contract_end_date_veda,
financial.contract_id,
financial.renewal_from_arr_cfx,
financial.renewal_to_arr_cfx,
financial.cc_segment,
financial.product_group,
financial.sub_product_group,
--financial.net_new_arr,
financial.AgentGeo,
--financial.RepManger,
--financial.RepLdap,
--financial.RepTeam,
financial.cross_team_agent,
financial.cross_sell_team,
financial.Flag,
financial.AgentMapFlag,
financial.Txns,
financial.SFDC_opportunity_id,
financial.SFDC_opportunity_created_date,
financial.SFDC_closed_date,
financial.SFDC_email,
financial.SFDC_ecc_salesordernumber,
financial.SFDC_campaignid,
financial.SFDC_name,
financial.SFDC_cum_campaignid,
financial.SFDC_min_date,
financial.SFDC_min_fiscal_yr_and_qtr_desc,
financial.SFDC_min_fiscal_yr_and_wk_desc,
financial.SFDC_Flag,
financial.Bob_Flag,
financial.TeamKey,
financial.gross_new_arr_cfx_unclaimed,
financial.net_purchases_arr_cfx_unclaimed,
financial.unclaimed_ldap,
financial.unclaimed_team,
financial.unclaimed_TSM,
financial.unclaimed_Manager,
financial.agent_max_quarter,
financial.agent_curr_qtr,
financial.created_curr_qtr,
financial.agent_TSM_Ldap,
financial.created_TSM_Ldap,
financial.agent_max_quarter_TSM,
financial.created_max_quarter_TSM,
financial.agent_TSM_team,
financial.created_TSM_team,
financial.agent_TSM_team_key,
financial.created_TSM_team_key,
financial.agent_TSM_flag,
financial.created_TSM_flag,
financial.ABD_Flag,
financial.agent_ABD_Flag,
financial.TSM_ABD_Flag,
financial.TSM_agent_ABD_Flag,
financial.gross_cancellations,
financial.net_cancelled_subs,
financial.migrated_from,
financial.migrated_to,
financial.renewal_from,
financial.renewal_to,
financial.Custom_Flag,
financial.bob_ABD_flag,
financial.lat_ABD_flag,
financial.same_wk_cncl_flag,
financial.cross_team_agent_name,
financial.cross_sell_team_key,
financial.FISCAL_YR_AND_WK_DESC
from
(select 
sales_document_item,
CREATED_BY,
CRM_CUSTOMER_GUID,
ENTITLEMENT_TYPE,
date_date,
FISCAL_YR_AND_QTR_DESC,
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
payment_method,
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
((migrated_to_arr_cfx - migrated_from_arr_cfx) + (renewal_to_arr_cfx - renewal_from_arr_cfx)) as net_migration_arr_cfx,
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
case when cc_segment = 'SIGN' then 'SIGN'
when cc_segment = 'ACROBAT DC' then 'ACROBAT'
when cc_segment = 'ACROBAT CC' then 'ACROBAT'
when cc_segment = 'STUDENT' then 'K-12'
when cc_segment = 'TEAM' and upper(PRODUCT_NAME) = 'SBST' then 'SUBSTANCE'
when cc_segment = 'TEAM' and upper(PRODUCT_NAME) = 'FFLY' then 'FIREFLY'
when cc_segment = 'TEAM' then 'CCT'
when cc_segment = 'K12+EEA' then 'K-12'
when cc_segment = 'STOCK' then 'STOCK'
when cc_segment = 'INDIVIDUAL' and upper(PRODUCT_NAME) = 'FFLY' then 'FIREFLY'
when cc_segment = 'INDIVIDUAL' then 'CONSUMER'
else 'OTHERS'
end as product_group,
case 
--when cc_segment = 'SIGN' and cc_phone_vs_web = 'VIP-PHONE' and PROMO_TYPE = 'AWS INTRO 20%' then 'SIGN Starter Pack'
--when cc_segment = 'SIGN' and cc_phone_vs_web = 'VIP-PHONE' and PROMO_TYPE = 'AZURE INTRO 20%' then 'SIGN Starter Pack'
--when cc_segment = 'SIGN' and cc_phone_vs_web = 'VIP-PHONE' and PROMO_TYPE = 'AWS 3K INTRO' then 'SIGN Starter Pack'
when cc_segment = 'SIGN' and cc_phone_vs_web = 'VIP-PHONE' and PROMO_TYPE in ('AWS INTRO 20%','AZURE INTRO 20%','AWS 3K INTRO') then 'SIGN Starter Pack'
when cc_segment = 'SIGN' then 'SIGN'
when cc_segment = 'ACROBAT DC' and PROMO_TYPE = 'TEAM 50PK PROMO' then 'Acrobat 50 PK'
when cc_segment = 'ACROBAT DC' and PROMO_TYPE = 'TEAM 25PK PROMO' then 'Acrobat 25 PK'
when cc_segment = 'ACROBAT CC'  and PROMO_TYPE = 'TEAM 50PK PROMO' then 'Acrobat 50 PK'
when cc_segment = 'ACROBAT CC' and PROMO_TYPE = 'TEAM 25PK PROMO' then 'Acrobat 25 PK'
when cc_segment = 'ACROBAT DC' then 'ACROBAT' 
when cc_segment = 'ACROBAT CC' then 'ACROBAT' 
when cc_segment = 'TEAM' and PRODUCT_NAME = 'SBST' then 'SUBSTANCE'
when cc_segment = 'TEAM' and PRODUCT_NAME = 'FFLY' then 'FIREFLY'
when cc_segment = 'TEAM' and PRODUCT_NAME = 'CCEX' then 'Express'
when cc_segment = 'TEAM' then 'CCT'
when cc_segment = 'STOCK' and PRODUCT_NAME = 'STEL' then 'Stock Credit Packs'
when cc_segment = 'STOCK' then 'STOCK'
when cc_segment = 'INDIVIDUAL' and PRODUCT_NAME = 'CCEX' then 'Express'
when cc_segment = 'INDIVIDUAL' and PRODUCT_NAME = 'FFLY' then 'FIREFLY'
when cc_segment = 'INDIVIDUAL' then 'CONSUMER'
when cc_segment = 'STUDENT' and PRODUCT_NAME = 'CCEX' then 'Express'
when cc_segment = 'STUDENT' then 'K-12'
when cc_segment = 'K12+EEA' then 'K-12'
when PRODUCT_NAME = 'CCEX' then 'Express'
else 'OTHERS'
end as sub_product_group,
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
FISCAL_YR_AND_WK_DESC from b2b_phones.abdashbase_rules where TeamKey <> 'WEB NOT APPLICABLE' and event = 'RULES') financial
left outer join 
(select distinct Team_key,Standard_team_name, Team_category,Display_Team_Category,Display_team_name,Display_team_manager,Display_team_manager_ldap from b2b_phones.dim_team ) dim_team
on financial.TeamKey = dim_team.Team_key
left outer join 
(select distinct country_code_iso2,market_area_description from ids_masterdata.dim_country) dim_country
on financial.SALES_DISTRICT = dim_country.country_code_iso2
left outer join
(
select Team_Key, Manager,Quarter,rn from (
select row_number() over (partition by Team_Key,Quarter order by Team_Key) as rn , Team_Key, Manager,Quarter  from 
(select distinct Team_Key, Manager,Quarter from b2b_phones.dim_agent_details )
) where rn = 1 
) team_unknown_manager
on upper(trim(financial.TeamKey)) = upper(trim(team_unknown_manager.Team_Key)) 
and financial.fiscal_yr_and_qtr_desc = team_unknown_manager.Quarter
