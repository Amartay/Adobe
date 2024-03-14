CREATE TABLE spark_catalog.b2b_phones.abddash_summary (
  geo STRING,
  market_area STRING,
  market_area_description STRING,
  market_segment STRING,
  cc_phone_vs_web STRING,
  cc_segment STRING,
  product_group STRING,
  sub_product_group STRING,
  STYPE STRING,
  projected_dme_gtm_segment STRING,
  gtm_acct_segment STRING,
  route_to_market STRING,
  promotion STRING,
  promo_type STRING,
  sales_district STRING,
  sales_center STRING,
  repname STRING,
  RepLdap STRING,
  reptsm STRING,
  repmanger STRING,
  repteam STRING,
  Team_category STRING,
  transaction_type STRING,
  net_purchases_deal_band STRING,
  net_cancelled_deal_band STRING,
  net_purchases_deal_band_overall STRING,
  net_cancelled_deal_band_overall STRING,
  Language STRING,
  Channel STRING,
  Surface_app_id STRING,
  queue STRING,
  queue_type STRING,
  transfer_in_Queue STRING,
  transfer_in_Agent STRING,
  transfer_out_Queue STRING,
  transfer_out_Agent STRING,
  entered_actuals DOUBLE,
  entered_lq DOUBLE,
  entered_ly DOUBLE,
  entered_lylq DOUBLE,
  entered_lwk DOUBLE,
  offered_actuals BIGINT,
  offered_lq BIGINT,
  offered_ly BIGINT,
  offered_lylq BIGINT,
  offered_lwk BIGINT,
  accepeted_actuals DOUBLE,
  accepeted_lq DOUBLE,
  accepeted_ly DOUBLE,
  accepeted_lylq DOUBLE,
  accepeted_lwk DOUBLE,
  abandoned_actuals DOUBLE,
  abandoned_lq DOUBLE,
  abandoned_ly DOUBLE,
  abandoned_lylq DOUBLE,
  abandoned_lwk DOUBLE,
  Non_Interactive_actuals DOUBLE,
  Non_Interactive_lq DOUBLE,
  Non_Interactive_ly DOUBLE,
  Non_Interactive_lylq DOUBLE,
  Non_Interactive_lwk DOUBLE,
  Wrap_time_min_actuals DOUBLE,
  Wrap_time_min_lq DOUBLE,
  Wrap_time_min_ly DOUBLE,
  Wrap_time_min_lylq DOUBLE,
  Wrap_time_min_lwk DOUBLE,
  Talk_time_min_actuals DOUBLE,
  Talk_time_min_lq DOUBLE,
  Talk_time_min_ly DOUBLE,
  Talk_time_min_lylq DOUBLE,
  Talk_time_min_lwk DOUBLE,
  transfer_in_actuals BIGINT,
  transfer_in_lq BIGINT,
  transfer_in_ly BIGINT,
  transfer_in_lylq BIGINT,
  transfer_in_lwk BIGINT,
  transfer_out_actuals BIGINT,
  transfer_out_lq BIGINT,
  transfer_out_ly BIGINT,
  transfer_out_lylq BIGINT,
  transfer_out_lwk BIGINT,
  incremental_arr_actuals DOUBLE,
  incremental_arr_lq DOUBLE,
  incremental_arr_ly DOUBLE,
  incremental_arr_lylq DOUBLE,
  incremental_arr_lwk DOUBLE,
  Incremental_arr_customer_addon_actuals DOUBLE,
  Incremental_arr_customer_addon_lq DOUBLE,
  Incremental_arr_customer_addon_ly DOUBLE,
  Incremental_arr_customer_addon_lylq DOUBLE,
  Incremental_arr_customer_addon_lwk DOUBLE,
  addl_purchase_diff_actuals DOUBLE,
  addl_purchase_diff_lq DOUBLE,
  addl_purchase_diff_ly DOUBLE,
  addl_purchase_diff_lylq DOUBLE,
  addl_purchase_diff_lwk DOUBLE,
  addl_purchase_same_actuals DOUBLE,
  addl_purchase_same_lq DOUBLE,
  addl_purchase_same_ly DOUBLE,
  addl_purchase_same_lylq DOUBLE,
  addl_purchase_same_lwk DOUBLE,
  gross_cancel_arr_cfx_actuals DOUBLE,
  gross_cancel_arr_cfx_lq DOUBLE,
  gross_cancel_arr_cfx_ly DOUBLE,
  gross_cancel_arr_cfx_lylq DOUBLE,
  gross_cancel_arr_cfx_lwk DOUBLE,
  gross_new_arr_cfx_actuals DOUBLE,
  gross_new_arr_cfx_lq DOUBLE,
  gross_new_arr_cfx_ly DOUBLE,
  gross_new_arr_cfx_lylq DOUBLE,
  gross_new_arr_cfx_lwk DOUBLE,
  gross_new_subs_actuals DOUBLE,
  gross_new_subs_lq DOUBLE,
  gross_new_subs_ly DOUBLE,
  gross_new_subs_lylq DOUBLE,
  gross_new_subs_lwk DOUBLE,
  init_purchase_arr_cfx_actuals DOUBLE,
  init_purchase_arr_cfx_lq DOUBLE,
  init_purchase_arr_cfx_ly DOUBLE,
  init_purchase_arr_cfx_lylq DOUBLE,
  init_purchase_arr_cfx_lwk DOUBLE,
  migrated_from_arr_cfx_actuals DOUBLE,
  migrated_from_arr_cfx_lq DOUBLE,
  migrated_from_arr_cfx_ly DOUBLE,
  migrated_from_arr_cfx_lylq DOUBLE,
  migrated_from_arr_cfx_lwk DOUBLE,
  migrated_to_arr_cfx_actuals DOUBLE,
  migrated_to_arr_cfx_lq DOUBLE,
  migrated_to_arr_cfx_ly DOUBLE,
  migrated_to_arr_cfx_lylq DOUBLE,
  migrated_to_arr_cfx_lwk DOUBLE,
  net_cancelled_arr_cfx_actuals DOUBLE,
  net_cancelled_arr_cfx_lq DOUBLE,
  net_cancelled_arr_cfx_ly DOUBLE,
  net_cancelled_arr_cfx_lylq DOUBLE,
  net_cancelled_arr_cfx_lwk DOUBLE,
  net_cancelled_arr_cfx_qrf DOUBLE,
  net_new_arr_cfx_actuals DOUBLE,
  net_new_arr_cfx_lq DOUBLE,
  net_new_arr_cfx_ly DOUBLE,
  net_new_arr_cfx_lylq DOUBLE,
  net_new_arr_cfx_lwk DOUBLE,
  net_new_subs_actuals DOUBLE,
  net_new_subs_lq DOUBLE,
  net_new_subs_ly DOUBLE,
  net_new_subs_lylq DOUBLE,
  net_new_subs_lwk DOUBLE,
  net_purchases_arr_cfx_actuals DOUBLE,
  net_purchases_arr_cfx_lq DOUBLE,
  net_purchases_arr_cfx_ly DOUBLE,
  net_purchases_arr_cfx_lylq DOUBLE,
  net_purchases_arr_cfx_lwk DOUBLE,
  net_purchases_arr_qrf DOUBLE,
  net_purchases_arr_buffer_qrf DOUBLE,
  net_purchases_arr_gt_5K_actuals DOUBLE,
  net_purchases_arr_gt_5K_lq DOUBLE,
  net_purchases_arr_gt_5K_ly DOUBLE,
  net_purchases_arr_gt_5K_lylq DOUBLE,
  net_purchases_arr_gt_5K_lwk DOUBLE,
  net_purchases_arr_ls_5K_actuals DOUBLE,
  net_purchases_arr_ls_5K_lq DOUBLE,
  net_purchases_arr_ls_5K_ly DOUBLE,
  net_purchases_arr_ls_5K_lylq DOUBLE,
  net_purchases_arr_ls_5K_lwk DOUBLE,
  orders_gt_5K_actuals BIGINT,
  orders_gt_5K_lq BIGINT,
  orders_gt_5K_ly BIGINT,
  orders_gt_5K_lylq BIGINT,
  orders_gt_5K_lwk BIGINT,
  orders_ls_5K_actuals BIGINT,
  orders_ls_5K_lq BIGINT,
  orders_ls_5K_ly BIGINT,
  orders_ls_5K_lylq BIGINT,
  orders_ls_5K_lwk BIGINT,
  reactivated_arr_cfx_actuals DOUBLE,
  reactivated_arr_cfx_lq DOUBLE,
  reactivated_arr_cfx_ly DOUBLE,
  reactivated_arr_cfx_lylq DOUBLE,
  reactivated_arr_cfx_lwk DOUBLE,
  returns_arr_cfx_actuals DOUBLE,
  returns_arr_cfx_lq DOUBLE,
  returns_arr_cfx_ly DOUBLE,
  returns_arr_cfx_lylq DOUBLE,
  returns_arr_cfx_lwk DOUBLE,
  renewal_from_arr_cfx_actuals DOUBLE,
  renewal_from_arr_cfx_lq DOUBLE,
  renewal_from_arr_cfx_ly DOUBLE,
  renewal_from_arr_cfx_lylq DOUBLE,
  renewal_from_arr_cfx_lwk DOUBLE,
  renewal_to_arr_cfx_actuals DOUBLE,
  renewal_to_arr_cfx_lq DOUBLE,
  renewal_to_arr_cfx_ly DOUBLE,
  renewal_to_arr_cfx_lylq DOUBLE,
  renewal_to_arr_cfx_lwk DOUBLE,
  net_purchases_arr_cfx_unclaimed_actuals DOUBLE,
  net_purchases_arr_cfx_unclaimed_lq DOUBLE,
  net_purchases_arr_cfx_unclaimed_ly DOUBLE,
  net_purchases_arr_cfx_unclaimed_lylq DOUBLE,
  net_purchases_arr_cfx_unclaimed_lwk DOUBLE,
  gross_new_arr_cfx_unclaimed_actuals DOUBLE,
  gross_new_arr_cfx_unclaimed_lq DOUBLE,
  gross_new_arr_cfx_unclaimed_ly DOUBLE,
  gross_new_arr_cfx_unclaimed_lylq DOUBLE,
  gross_new_arr_cfx_unclaimed_lwk DOUBLE,
  net_migration_arr_cfx_actuals DOUBLE,
  net_migration_arr_cfx_ly BIGINT,
  net_migration_arr_cfx_lq DOUBLE,
  net_migration_arr_cfx_lylq DOUBLE,
  net_migration_arr_cfx_lwk DOUBLE,
  net_migration_arr_qrf DOUBLE,
  refresh_date TIMESTAMP,
  fiscal_yr_and_wk_desc STRING,
  fiscal_yr_and_qtr_desc STRING)
USING parquet
PARTITIONED BY (fiscal_yr_and_wk_desc, fiscal_yr_and_qtr_desc)
LOCATION 'abfs://or1-prod-data@azr6665prddpaasb2bdna.dfs.core.windows.net/user/hive/warehouse/b2bdna/b2b_phones.db/abddash_summary'
TBLPROPERTIES (
  'bucketing_version' = '2',
  'parquet.compression' = 'zstd')
