%sql
CREATE TABLE spark_catalog.b2b_phones.fact_abddash_rep_performance (
  date STRING,
  cc_phone_vs_web STRING,
  Geo STRING,
  Sales_Center STRING,
  Sales_document STRING,
  dylan_order_number STRING,
  sales_document_item STRING,
  contract_id STRING,
  Txns STRING,
  route_to_market STRING,
  PROMO_TYPE STRING,
  cc_segment STRING,
  product_name_description STRING,
  product_group STRING,
  sub_product_group STRING,
  stype STRING,
  market_area_description STRING,
  AgentMapFlag STRING,
  cross_sell_team STRING,
  cross_team_agent STRING,
  RepLdap STRING,
  created_ldap STRING,
  RepName STRING,
  created_RepName STRING,
  attained_RepName STRING,
  RepTSM STRING,
  attained_TSM STRING,
  created_TSM STRING,
  RepManger STRING,
  attained_Manager STRING,
  created_Manager STRING,
  rep_manager_ldap STRING,
  attained_manager_ldap STRING,
  created_manager_ldap STRING,
  repteam STRING,
  attained_team STRING,
  created_team STRING,
  display_team_category_attained STRING,
  display_team_category_created STRING,
  Team_Category STRING,
  flag STRING,
  customer_email_domain STRING,
  acct_name STRING,
  net_purchases_arr_cfx_attained DOUBLE,
  net_cancelled_arr_cfx_attained DOUBLE,
  reactivated_arr_cfx_attained DOUBLE,
  net_migration_arr_cfx_attained DOUBLE,
  returns_arr_cfx_attained DOUBLE,
  net_new_subs_attained DOUBLE,
  gross_new_subs_attained DOUBLE,
  net_purchases_arr_cfx_created DOUBLE,
  net_cancelled_arr_cfx_created DOUBLE,
  reactivated_arr_cfx_created DOUBLE,
  net_migration_arr_cfx_created DOUBLE,
  returns_arr_cfx_created DOUBLE,
  net_new_subs_created DOUBLE,
  gross_new_subs_created DOUBLE,
  net_purchases_deal_band_item STRING,
  net_cancelled_deal_band_item STRING,
  net_purchases_deal_band_arr_overall DOUBLE,
  net_cancelled_band_arr_overall DOUBLE,
  band_row_num DOUBLE,
  net_purchases_deal_band_overall STRING,
  net_cancelled_deal_band_overall STRING,
  refresh_date STRING,
  FISCAL_YR_AND_QTR_DESC STRING,
  FISCAL_YR_AND_WK_DESC STRING)
USING parquet
PARTITIONED BY (FISCAL_YR_AND_QTR_DESC, FISCAL_YR_AND_WK_DESC)
LOCATION 'abfs://or1-prod-data@azr6665prddpaasb2bdna.dfs.core.windows.net/user/hive/warehouse/b2bdna/b2b_phones.db/fact_abddash_rep_performance'
TBLPROPERTIES (
  'bucketing_version' = '2')
