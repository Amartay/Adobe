%sql
CREATE VIEW b2b_phones.vw_abddash_summary (
  geo,
  market_area,
  market_area_description,
  market_segment,
  cc_phone_vs_web,
  cc_segment,
  product_group,
  sub_product_group,
  stype,
  projected_dme_gtm_segment,
  gtm_acct_segment,
  offering,
  route_to_market,
  promotion,
  promo_type,
  sales_district,
  sales_center,
  repname,
  RepLdap,
  reptsm,
  repmanger,
  repteam,
  managerLdap,
  Original_repmanger,
  Original_repteam,
  Team_category,
  Display_team_category,
  transaction_type,
  net_purchases_deal_band,
  net_cancelled_deal_band,
  net_purchases_deal_band_overall,
  net_cancelled_deal_band_overall,
  interaction_type,
  Language,
  Channel,
  Surface_app_id,
  queue,
  queue_type,
  transfer_in_Queue,
  transfer_in_Agent,
  transfer_out_Queue,
  transfer_out_Agent,
  entered_actuals,
  entered_lq,
  entered_ly,
  entered_lylq,
  entered_lwk,
  offered_actuals,
  offered_lq,
  offered_ly,
  offered_lylq,
  offered_lwk,
  accepeted_actuals,
  accepeted_lq,
  accepeted_ly,
  accepeted_lylq,
  accepeted_lwk,
  abandoned_actuals,
  abandoned_lq,
  abandoned_ly,
  abandoned_lylq,
  abandoned_lwk,
  Non_Interactive_actuals,
  Non_Interactive_lq,
  Non_Interactive_ly,
  Non_Interactive_lylq,
  Non_Interactive_lwk,
  Wrap_time_min_actuals,
  Wrap_time_min_lq,
  Wrap_time_min_ly,
  Wrap_time_min_lylq,
  Wrap_time_min_lwk,
  Talk_Time_Min_actuals,
  Talk_Time_Min_lq,
  Talk_time_min_ly,
  Talk_time_min_lylq,
  Talk_time_min_lwk,
  transfer_in_actuals,
  transfer_in_lq,
  transfer_in_ly,
  transfer_in_lylq,
  transfer_in_lwk,
  transfer_out_actuals,
  transfer_out_lq,
  transfer_out_ly,
  transfer_out_lylq,
  transfer_out_lwk,
  incremental_arr_actuals,
  incremental_arr_lq,
  incremental_arr_ly,
  incremental_arr_lylq,
  incremental_arr_lwk,
  addl_purchase_diff_actuals,
  addl_purchase_diff_lq,
  addl_purchase_diff_ly,
  addl_purchase_diff_lylq,
  addl_purchase_diff_lwk,
  addl_purchase_same_actuals,
  addl_purchase_same_lq,
  addl_purchase_same_ly,
  addl_purchase_same_lylq,
  addl_purchase_same_lwk,
  gross_cancel_arr_cfx_actuals,
  gross_cancel_arr_cfx_lq,
  gross_cancel_arr_cfx_ly,
  gross_cancel_arr_cfx_lylq,
  gross_cancel_arr_cfx_lwk,
  gross_new_arr_cfx_actuals,
  gross_new_arr_cfx_lq,
  gross_new_arr_cfx_ly,
  gross_new_arr_cfx_lylq,
  gross_new_arr_cfx_lwk,
  gross_new_subs_actuals,
  gross_new_subs_lq,
  gross_new_subs_ly,
  gross_new_subs_lylq,
  gross_new_subs_lwk,
  init_purchase_arr_cfx_actuals,
  init_purchase_arr_cfx_lq,
  init_purchase_arr_cfx_ly,
  init_purchase_arr_cfx_lylq,
  init_purchase_arr_cfx_lwk,
  migrated_from_arr_cfx_actuals,
  migrated_from_arr_cfx_lq,
  migrated_from_arr_cfx_ly,
  migrated_from_arr_cfx_lylq,
  migrated_from_arr_cfx_lwk,
  migrated_to_arr_cfx_actuals,
  migrated_to_arr_cfx_lq,
  migrated_to_arr_cfx_ly,
  migrated_to_arr_cfx_lylq,
  migrated_to_arr_cfx_lwk,
  net_cancelled_arr_cfx_actuals,
  net_cancelled_arr_cfx_lq,
  net_cancelled_arr_cfx_ly,
  net_cancelled_arr_cfx_lylq,
  net_cancelled_arr_cfx_lwk,
  net_cancelled_arr_cfx_qrf,
  net_cancelled_arr_gt_5K_actuals,
  net_cancelled_arr_gt_5K_lq,
  net_cancelled_arr_gt_5K_ly,
  net_cancelled_arr_gt_5K_lylq,
  net_cancelled_arr_gt_5K_lwk,
  net_cancelled_arr_ls_5K_actuals,
  net_cancelled_arr_ls_5K_lq,
  net_cancelled_arr_ls_5K_ly,
  net_cancelled_arr_ls_5K_lylq,
  net_cancelled_arr_ls_5K_lwk,
  net_new_arr_cfx_actuals,
  net_new_arr_cfx_lq,
  net_new_arr_cfx_ly,
  net_new_arr_cfx_lylq,
  net_new_arr_cfx_lwk,
  net_new_arr_qrf,
  net_new_subs_actuals,
  net_new_subs_lq,
  net_new_subs_ly,
  net_new_subs_lylq,
  net_new_subs_lwk,
  net_purchases_arr_cfx_actuals,
  net_purchases_arr_cfx_lq,
  net_purchases_arr_cfx_ly,
  net_purchases_arr_cfx_lylq,
  net_purchases_arr_cfx_lwk,
  net_purchases_arr_qrf,
  net_purchases_arr_gt_5K_actuals,
  net_purchases_arr_gt_5K_lq,
  net_purchases_arr_gt_5K_ly,
  net_purchases_arr_gt_5K_lylq,
  net_purchases_arr_gt_5K_lwk,
  net_purchases_arr_ls_5K_actuals,
  net_purchases_arr_ls_5K_lq,
  net_purchases_arr_ls_5K_ly,
  net_purchases_arr_ls_5K_lylq,
  net_purchases_arr_ls_5K_lwk,
  orders_gt_5K_actuals,
  orders_gt_5K_lq,
  orders_gt_5K_ly,
  orders_gt_5K_lylq,
  orders_gt_5K_lwk,
  orders_ls_5K_actuals,
  orders_ls_5K_lq,
  orders_ls_5K_ly,
  orders_ls_5K_lylq,
  orders_ls_5K_lwk,
  net_purchases_arr_buffer_qrf,
  reactivated_arr_cfx_actuals,
  reactivated_arr_cfx_lq,
  reactivated_arr_cfx_ly,
  reactivated_arr_cfx_lylq,
  reactivated_arr_cfx_lwk,
  returns_arr_cfx_actuals,
  returns_arr_cfx_lq,
  returns_arr_cfx_ly,
  returns_arr_cfx_lylq,
  returns_arr_cfx_lwk,
  renewal_from_arr_cfx_actuals,
  renewal_from_arr_cfx_lq,
  renewal_from_arr_cfx_ly,
  renewal_from_arr_cfx_lylq,
  renewal_from_arr_cfx_lwk,
  renewal_to_arr_cfx_actuals,
  renewal_to_arr_cfx_lq,
  renewal_to_arr_cfx_ly,
  renewal_to_arr_cfx_lylq,
  renewal_to_arr_cfx_lwk,
  net_purchases_arr_cfx_unclaimed_actuals,
  net_purchases_arr_cfx_unclaimed_lq,
  net_purchases_arr_cfx_unclaimed_ly,
  net_purchases_arr_cfx_unclaimed_lylq,
  net_purchases_arr_cfx_unclaimed_lwk,
  gross_new_arr_cfx_unclaimed_actuals,
  gross_new_arr_cfx_unclaimed_lq,
  gross_new_arr_cfx_unclaimed_ly,
  gross_new_arr_cfx_unclaimed_lylq,
  gross_new_arr_cfx_unclaimed_lwk,
  net_migration_arr_cfx_actuals,
  net_migration_arr_cfx_ly,
  net_migration_arr_cfx_lq,
  net_migration_arr_cfx_lylq,
  net_migration_arr_cfx_lwk,
  net_migration_arr_qrf,
  net_migration_arr_gt_5K_actuals,
  net_migration_arr_gt_5K_lq,
  net_migration_arr_gt_5K_ly,
  net_migration_arr_gt_5K_lylq,
  net_migration_arr_gt_5K_lwk,
  net_migration_arr_ls_5K_actuals,
  net_migration_arr_ls_5K_lq,
  net_migration_arr_ls_5K_ly,
  net_migration_arr_ls_5K_lylq,
  net_migration_arr_ls_5K_lwk,
  Head_count,
  Head_count_qtr_begin,
  rn,
  fiscal_yr_and_wk_desc,
  fiscal_yr_and_qtr_desc,
  refresh_date,
  cw_flag,
  cw_latest_flag,
  lw_flag,
  week_number,
  rolling_qtr,
  fiscal_yr_and_qtr_desc_lqtr)
TBLPROPERTIES (
  'bucketing_version' = '2',
  'transient_lastDdlTime' = '1704377608')
AS select
geo,
market_area,
market_area_description,
market_segment,
cc_phone_vs_web,
cc_segment,
product_group,
sub_product_group,
stype,
projected_dme_gtm_segment,
gtm_acct_segment,
offering,
route_to_market,
promotion,
promo_type,
sales_district,
sales_center,
repname,
RepLdap,
reptsm,
repmanger,
repteam,
managerLdap,
Original_repmanger,
Original_repteam,
Team_category,
Display_team_category,
transaction_type,
net_purchases_deal_band,
net_cancelled_deal_band,
net_purchases_deal_band_overall,
net_cancelled_deal_band_overall,
interaction_type,
Language,
Channel,
Surface_app_id,
queue,
queue_type,
transfer_in_Queue,
transfer_in_Agent,
transfer_out_Queue,
transfer_out_Agent,
entered_actuals,
entered_lq,
entered_ly,
entered_lylq,
entered_lwk,
offered_actuals,
offered_lq,
offered_ly,
offered_lylq,
offered_lwk,
accepeted_actuals,
accepeted_lq,
accepeted_ly,
accepeted_lylq,
accepeted_lwk,
abandoned_actuals,
abandoned_lq,
abandoned_ly,
abandoned_lylq,
abandoned_lwk,
Non_Interactive_actuals,
Non_Interactive_lq,
Non_Interactive_ly,
Non_Interactive_lylq,
Non_Interactive_lwk,
Wrap_time_min_actuals,
Wrap_time_min_lq,
Wrap_time_min_ly,
Wrap_time_min_lylq,
Wrap_time_min_lwk,
Talk_Time_Min_actuals,
Talk_Time_Min_lq,
Talk_time_min_ly,
Talk_time_min_lylq,
Talk_time_min_lwk,
transfer_in_actuals,
transfer_in_lq,
transfer_in_ly,
transfer_in_lylq,
transfer_in_lwk,
transfer_out_actuals,
transfer_out_lq,
transfer_out_ly,
transfer_out_lylq,
transfer_out_lwk,
incremental_arr_actuals,
incremental_arr_lq,
incremental_arr_ly,
incremental_arr_lylq,
incremental_arr_lwk,
addl_purchase_diff_actuals,
addl_purchase_diff_lq,
addl_purchase_diff_ly,
addl_purchase_diff_lylq,
addl_purchase_diff_lwk,
addl_purchase_same_actuals,
addl_purchase_same_lq,
addl_purchase_same_ly,
addl_purchase_same_lylq,
addl_purchase_same_lwk,
gross_cancel_arr_cfx_actuals,
gross_cancel_arr_cfx_lq,
gross_cancel_arr_cfx_ly,
gross_cancel_arr_cfx_lylq,
gross_cancel_arr_cfx_lwk,
gross_new_arr_cfx_actuals,
gross_new_arr_cfx_lq,
gross_new_arr_cfx_ly,
gross_new_arr_cfx_lylq,
gross_new_arr_cfx_lwk,
gross_new_subs_actuals,
gross_new_subs_lq,
gross_new_subs_ly,
gross_new_subs_lylq,
gross_new_subs_lwk,
init_purchase_arr_cfx_actuals,
init_purchase_arr_cfx_lq,
init_purchase_arr_cfx_ly,
init_purchase_arr_cfx_lylq,
init_purchase_arr_cfx_lwk,
migrated_from_arr_cfx_actuals,
migrated_from_arr_cfx_lq,
migrated_from_arr_cfx_ly,
migrated_from_arr_cfx_lylq,
migrated_from_arr_cfx_lwk,
migrated_to_arr_cfx_actuals,
migrated_to_arr_cfx_lq,
migrated_to_arr_cfx_ly,
migrated_to_arr_cfx_lylq,
migrated_to_arr_cfx_lwk,
net_cancelled_arr_cfx_actuals,
net_cancelled_arr_cfx_lq,
net_cancelled_arr_cfx_ly,
net_cancelled_arr_cfx_lylq,
net_cancelled_arr_cfx_lwk,
net_cancelled_arr_cfx_qrf,
net_cancelled_arr_gt_5K_actuals,
net_cancelled_arr_gt_5K_lq,
net_cancelled_arr_gt_5K_ly,
net_cancelled_arr_gt_5K_lylq,
net_cancelled_arr_gt_5K_lwk,
net_cancelled_arr_ls_5K_actuals,
net_cancelled_arr_ls_5K_lq,
net_cancelled_arr_ls_5K_ly,
net_cancelled_arr_ls_5K_lylq,
net_cancelled_arr_ls_5K_lwk,
net_new_arr_cfx_actuals,
net_new_arr_cfx_lq,
net_new_arr_cfx_ly,
net_new_arr_cfx_lylq,
net_new_arr_cfx_lwk,
net_new_arr_qrf,
net_new_subs_actuals,
net_new_subs_lq,
net_new_subs_ly,
net_new_subs_lylq,
net_new_subs_lwk,
net_purchases_arr_cfx_actuals,
net_purchases_arr_cfx_lq,
net_purchases_arr_cfx_ly,
net_purchases_arr_cfx_lylq,
net_purchases_arr_cfx_lwk,
net_purchases_arr_qrf,
net_purchases_arr_gt_5K_actuals,
net_purchases_arr_gt_5K_lq,
net_purchases_arr_gt_5K_ly,
net_purchases_arr_gt_5K_lylq,
net_purchases_arr_gt_5K_lwk,
net_purchases_arr_ls_5K_actuals,
net_purchases_arr_ls_5K_lq,
net_purchases_arr_ls_5K_ly,
net_purchases_arr_ls_5K_lylq,
net_purchases_arr_ls_5K_lwk,
orders_gt_5K_actuals,
orders_gt_5K_lq,
orders_gt_5K_ly,
orders_gt_5K_lylq,
orders_gt_5K_lwk,
orders_ls_5K_actuals,
orders_ls_5K_lq,
orders_ls_5K_ly,
orders_ls_5K_lylq,
orders_ls_5K_lwk,
net_purchases_arr_buffer_qrf,
reactivated_arr_cfx_actuals,
reactivated_arr_cfx_lq,
reactivated_arr_cfx_ly,
reactivated_arr_cfx_lylq,
reactivated_arr_cfx_lwk,
returns_arr_cfx_actuals,
returns_arr_cfx_lq,
returns_arr_cfx_ly,
returns_arr_cfx_lylq,
returns_arr_cfx_lwk,
renewal_from_arr_cfx_actuals,
renewal_from_arr_cfx_lq,
renewal_from_arr_cfx_ly,
renewal_from_arr_cfx_lylq,
renewal_from_arr_cfx_lwk,
renewal_to_arr_cfx_actuals,
renewal_to_arr_cfx_lq,
renewal_to_arr_cfx_ly,
renewal_to_arr_cfx_lylq,
renewal_to_arr_cfx_lwk,
net_purchases_arr_cfx_unclaimed_actuals,
net_purchases_arr_cfx_unclaimed_lq,
net_purchases_arr_cfx_unclaimed_ly,
net_purchases_arr_cfx_unclaimed_lylq,
net_purchases_arr_cfx_unclaimed_lwk,
gross_new_arr_cfx_unclaimed_actuals,
gross_new_arr_cfx_unclaimed_lq,
gross_new_arr_cfx_unclaimed_ly,
gross_new_arr_cfx_unclaimed_lylq,
gross_new_arr_cfx_unclaimed_lwk,
net_migration_arr_cfx_actuals,
net_migration_arr_cfx_ly,
net_migration_arr_cfx_lq,
net_migration_arr_cfx_lylq,
net_migration_arr_cfx_lwk,
net_migration_arr_qrf,
net_migration_arr_gt_5K_actuals,
net_migration_arr_gt_5K_lq,
net_migration_arr_gt_5K_ly,
net_migration_arr_gt_5K_lylq,
net_migration_arr_gt_5K_lwk,
net_migration_arr_ls_5K_actuals,
net_migration_arr_ls_5K_lq,
net_migration_arr_ls_5K_ly,
net_migration_arr_ls_5K_lylq,
net_migration_arr_ls_5K_lwk,
sum(CASE WHEN rn = 1 THEN Head_count ELSE 0 END) over (PARTITION BY repmanger,repteam,fiscal_yr_and_qtr_desc,geo,sales_center) as Head_count,
sum(CASE WHEN rn = 1 THEN Head_count_qtr_begin ELSE 0 END) over (PARTITION BY repmanger,repteam,fiscal_yr_and_qtr_desc,geo,sales_center) as Head_count_qtr_begin,
rn,
fiscal_yr_and_wk_desc,
fiscal_yr_and_qtr_desc,
refresh_date,
cw_flag,
cw_latest_flag,
lw_flag,
week_number,
rolling_qtr,
fiscal_yr_and_qtr_desc_lqtr

from 
(
select 
coalesce(a.geo,'UNKNOWN') as geo,
coalesce(a.market_area,'UNKNOWN') as market_area,
coalesce(a.market_area_description,'UNKNOWN') as market_area_description,
coalesce(a.market_segment,'UNKNOWN') as market_segment,
coalesce(a.cc_phone_vs_web,'UNKNOWN') as cc_phone_vs_web,
coalesce(a.cc_segment,'UNKNOWN') as cc_segment,
coalesce(a.product_group,'UNKNOWN') as product_group,
coalesce(a.sub_product_group,'UNKNOWN') as sub_product_group,
coalesce(a.stype,'UNKNONW') as stype,
coalesce(a.projected_dme_gtm_segment,'UNKNOWN') as projected_dme_gtm_segment,
coalesce(a.gtm_acct_segment,'UNKNOWN') as gtm_acct_segment,
'UNKNOWN' as offering,
coalesce(a.route_to_market,'UNKNOWN') as route_to_market,
coalesce(a.promotion,'UNKNOWN') as promotion,
coalesce(a.promo_type) as promo_type,
coalesce(a.sales_district,'UNKNOWN') as sales_district,
coalesce(a.sales_center,'UNKNOWN') as sales_center,
coalesce(a.repname,'UNKNOWN') as repname,
coalesce(a.RepLdap ,'UNKNOWN') as RepLdap,
coalesce(a.reptsm,'UNKNOWN') as reptsm,
coalesce(c.Display_team_manager,'UNKNOWN') as repmanger,
coalesce(c.Display_team_name,'UNKNOWN') as repteam,
coalesce(c.Display_team_manager_ldap,'UNKNOWN') as managerLdap,
coalesce(a.repmanger,'UNKNOWN') as Original_repmanger,
coalesce(a.repteam,'UNKNOWN') as Original_repteam,
coalesce(a.Team_category,'UNKNOWN') as Team_category,
coalesce(c.Display_team_category,'UNKNOWN') as Display_team_category,
coalesce(upper(a.transaction_type),'UNKNOWN') as transaction_type,
coalesce(a.net_purchases_deal_band , 'UNKNOWN') as net_purchases_deal_band,
coalesce(a.net_cancelled_deal_band , 'UNKNOWN') as net_cancelled_deal_band,

coalesce(a.net_purchases_deal_band_overall , 'UNKNOWN') as net_purchases_deal_band_overall,
coalesce(a.net_cancelled_deal_band_overall , 'UNKNOWN') as net_cancelled_deal_band_overall,

'UNKNOWN' as interaction_type,
coalesce(a.Language,'UNKNOWN') as Language,
coalesce(a.Channel,'UNKNOWN')as Channel,
coalesce(a.Surface_app_id,'UNKNOWN') as Surface_app_id,
coalesce(a.queue,'UNKNOWN')as queue,
coalesce(a.queue_type,'UNKNOWN') as queue_type,
coalesce(a.transfer_in_Queue,'UNKNOWN') as transfer_in_Queue,
coalesce(a.transfer_in_Agent,'UNKNOWN') as transfer_in_Agent,
coalesce(a.transfer_out_Queue,'UNKNOWN') as transfer_out_Queue,
coalesce(a.transfer_out_Agent,'UNKNOWN') as transfer_out_Agent,

a.entered_actuals as entered_actuals,
a.entered_lq as entered_lq,
a.entered_ly as entered_ly,
a.entered_lylq as entered_lylq,
a.entered_lwk as entered_lwk,
a.offered_actuals as offered_actuals,
a.offered_lq as offered_lq,
a.offered_ly as offered_ly,
a.offered_lylq as offered_lylq,
a.offered_lwk as offered_lwk,
a.accepeted_actuals as accepeted_actuals,
a.accepeted_lq as accepeted_lq,
a.accepeted_ly as accepeted_ly,
a.accepeted_lylq as accepeted_lylq,
a.accepeted_lwk as accepeted_lwk,
a.abandoned_actuals as abandoned_actuals,
a.abandoned_lq as abandoned_lq,
a.abandoned_ly as abandoned_ly,
a.abandoned_lylq as abandoned_lylq,
a.abandoned_lwk as abandoned_lwk,
a.Non_Interactive_actuals as Non_Interactive_actuals,
a.Non_Interactive_lq as Non_Interactive_lq,
a.Non_Interactive_ly as Non_Interactive_ly,
a.Non_Interactive_lylq as Non_Interactive_lylq,
a.Non_Interactive_lwk as Non_Interactive_lwk,
a.Wrap_time_min_actuals as Wrap_time_min_actuals,
a.Wrap_time_min_lq as Wrap_time_min_lq,
a.Wrap_time_min_ly as Wrap_time_min_ly,
a.Wrap_time_min_lylq as Wrap_time_min_lylq,
a.Wrap_time_min_lwk as Wrap_time_min_lwk,
a.Talk_time_min_actuals as Talk_Time_Min_actuals,
a.Talk_time_min_lq as Talk_Time_Min_lq,
a.Talk_time_min_ly as Talk_time_min_ly,
a.Talk_time_min_lylq as Talk_time_min_lylq,
a.Talk_time_min_lwk as Talk_time_min_lwk,
a.transfer_in_actuals as transfer_in_actuals,
a.transfer_in_lq as transfer_in_lq,
a.transfer_in_ly as transfer_in_ly,
a.transfer_in_lylq as transfer_in_lylq,
a.transfer_in_lwk as transfer_in_lwk,
a.transfer_out_actuals as transfer_out_actuals,
a.transfer_out_lq as transfer_out_lq,
a.transfer_out_ly as transfer_out_ly,
a.transfer_out_lylq as transfer_out_lylq,
a.transfer_out_lwk as transfer_out_lwk,
a.incremental_arr_actuals,
a.incremental_arr_lq,
a.incremental_arr_ly,
a.incremental_arr_lylq,
a.incremental_arr_lwk,
a.addl_purchase_diff_actuals,
a.addl_purchase_diff_lq,
a.addl_purchase_diff_ly,
a.addl_purchase_diff_lylq,
a.addl_purchase_diff_lwk,
a.addl_purchase_same_actuals,
a.addl_purchase_same_lq,
a.addl_purchase_same_ly,
a.addl_purchase_same_lylq,
a.addl_purchase_same_lwk,
a.gross_cancel_arr_cfx_actuals,
a.gross_cancel_arr_cfx_lq,
a.gross_cancel_arr_cfx_ly,
a.gross_cancel_arr_cfx_lylq,
a.gross_cancel_arr_cfx_lwk,
a.gross_new_arr_cfx_actuals,
a.gross_new_arr_cfx_lq,
a.gross_new_arr_cfx_ly,
a.gross_new_arr_cfx_lylq,
a.gross_new_arr_cfx_lwk,
a.gross_new_subs_actuals,
a.gross_new_subs_lq,
a.gross_new_subs_ly,
a.gross_new_subs_lylq,
a.gross_new_subs_lwk,
a.init_purchase_arr_cfx_actuals,
a.init_purchase_arr_cfx_lq,
a.init_purchase_arr_cfx_ly,
a.init_purchase_arr_cfx_lylq,
a.init_purchase_arr_cfx_lwk,
a.migrated_from_arr_cfx_actuals,
a.migrated_from_arr_cfx_lq,
a.migrated_from_arr_cfx_ly,
a.migrated_from_arr_cfx_lylq,
a.migrated_from_arr_cfx_lwk,
a.migrated_to_arr_cfx_actuals,
a.migrated_to_arr_cfx_lq,
a.migrated_to_arr_cfx_ly,
a.migrated_to_arr_cfx_lylq,
a.migrated_to_arr_cfx_lwk,
a.net_cancelled_arr_cfx_actuals,
a.net_cancelled_arr_cfx_lq,
a.net_cancelled_arr_cfx_ly,
a.net_cancelled_arr_cfx_lylq,
a.net_cancelled_arr_cfx_lwk,
a.net_cancelled_arr_cfx_qrf,

0 as net_cancelled_arr_gt_5K_actuals,
0 as net_cancelled_arr_gt_5K_lq,
0 as net_cancelled_arr_gt_5K_ly,
0 as net_cancelled_arr_gt_5K_lylq,
0 as net_cancelled_arr_gt_5K_lwk,

0 as net_cancelled_arr_ls_5K_actuals,
0 as net_cancelled_arr_ls_5K_lq,
0 as net_cancelled_arr_ls_5K_ly,
0 as net_cancelled_arr_ls_5K_lylq,
0 as net_cancelled_arr_ls_5K_lwk,

a.net_new_arr_cfx_actuals,
a.net_new_arr_cfx_lq,
a.net_new_arr_cfx_ly,
a.net_new_arr_cfx_lylq,
a.net_new_arr_cfx_lwk,
0 as net_new_arr_qrf,
a.net_new_subs_actuals,
a.net_new_subs_lq,
a.net_new_subs_ly,
a.net_new_subs_lylq,
a.net_new_subs_lwk,

--a.net_purchases_arr_cfx_actuals,
--a.net_purchases_arr_cfx_lq,
--a.net_purchases_arr_cfx_ly,
--a.net_purchases_arr_cfx_lylq,
--a.net_purchases_arr_cfx_lwk,

(a.net_purchases_arr_cfx_actuals - a.incremental_arr_actuals + a.Incremental_arr_customer_addon_actuals) as net_purchases_arr_cfx_actuals,
(a.net_purchases_arr_cfx_lq - a.incremental_arr_lq + a.Incremental_arr_customer_addon_lq)as net_purchases_arr_cfx_lq,
(a.net_purchases_arr_cfx_ly - a.incremental_arr_ly + a.Incremental_arr_customer_addon_ly) as net_purchases_arr_cfx_ly,
(a.net_purchases_arr_cfx_lylq - a.incremental_arr_lylq + a.Incremental_arr_customer_addon_lylq) as net_purchases_arr_cfx_lylq,
(a.net_purchases_arr_cfx_lwk - a.incremental_arr_lwk + a.Incremental_arr_customer_addon_lwk) as net_purchases_arr_cfx_lwk,

a.net_purchases_arr_qrf,

a.net_purchases_arr_gt_5K_actuals as net_purchases_arr_gt_5K_actuals,
a.net_purchases_arr_gt_5K_lq as net_purchases_arr_gt_5K_lq,
a.net_purchases_arr_gt_5K_ly as net_purchases_arr_gt_5K_ly,
a.net_purchases_arr_gt_5K_lylq as net_purchases_arr_gt_5K_lylq,
a.net_purchases_arr_gt_5K_lwk as net_purchases_arr_gt_5K_lwk,

a.net_purchases_arr_ls_5K_actuals as net_purchases_arr_ls_5K_actuals,
a.net_purchases_arr_ls_5K_lq as net_purchases_arr_ls_5K_lq,
a.net_purchases_arr_ls_5K_ly as net_purchases_arr_ls_5K_ly,
a.net_purchases_arr_ls_5K_lylq as net_purchases_arr_ls_5K_lylq,
a.net_purchases_arr_ls_5K_lwk as net_purchases_arr_ls_5K_lwk,
a.orders_gt_5K_actuals as orders_gt_5K_actuals,
a.orders_gt_5K_lq as orders_gt_5K_lq,
a.orders_gt_5K_ly as orders_gt_5K_ly,
a.orders_gt_5K_lylq as orders_gt_5K_lylq,
a.orders_gt_5K_lwk as orders_gt_5K_lwk,
a.orders_ls_5K_actuals as orders_ls_5K_actuals,
a.orders_ls_5K_lq as orders_ls_5K_lq,
a.orders_ls_5K_ly as orders_ls_5K_ly,
a.orders_ls_5K_lylq as orders_ls_5K_lylq,
a.orders_ls_5K_lwk as orders_ls_5K_lwk,
 
a.net_purchases_arr_buffer_qrf,
a.reactivated_arr_cfx_actuals,
a.reactivated_arr_cfx_lq,
a.reactivated_arr_cfx_ly,
a.reactivated_arr_cfx_lylq,
a.reactivated_arr_cfx_lwk,
a.returns_arr_cfx_actuals,
a.returns_arr_cfx_lq,
a.returns_arr_cfx_ly,
a.returns_arr_cfx_lylq,
a.returns_arr_cfx_lwk,
a.renewal_from_arr_cfx_actuals,
a.renewal_from_arr_cfx_lq,
a.renewal_from_arr_cfx_ly,
a.renewal_from_arr_cfx_lylq,
a.renewal_from_arr_cfx_lwk,
a.renewal_to_arr_cfx_actuals,
a.renewal_to_arr_cfx_lq,
a.renewal_to_arr_cfx_ly,
a.renewal_to_arr_cfx_lylq,
a.renewal_to_arr_cfx_lwk,
a.net_purchases_arr_cfx_unclaimed_actuals,
a.net_purchases_arr_cfx_unclaimed_lq,
a.net_purchases_arr_cfx_unclaimed_ly,
a.net_purchases_arr_cfx_unclaimed_lylq,
a.net_purchases_arr_cfx_unclaimed_lwk,
a.gross_new_arr_cfx_unclaimed_actuals,
a.gross_new_arr_cfx_unclaimed_lq,
a.gross_new_arr_cfx_unclaimed_ly,
a.gross_new_arr_cfx_unclaimed_lylq,
a.gross_new_arr_cfx_unclaimed_lwk,
a.net_migration_arr_cfx_actuals,
a.net_migration_arr_cfx_ly,
a.net_migration_arr_cfx_lq,
a.net_migration_arr_cfx_lylq,
a.net_migration_arr_cfx_lwk,
a.net_migration_arr_qrf,

0 as net_migration_arr_gt_5K_actuals,
0 as net_migration_arr_gt_5K_lq,
0 as net_migration_arr_gt_5K_ly,
0 as net_migration_arr_gt_5K_lylq,
0 as net_migration_arr_gt_5K_lwk,

0 as net_migration_arr_ls_5K_actuals,
0 as net_migration_arr_ls_5K_lq,
0 as net_migration_arr_ls_5K_ly,
0 as net_migration_arr_ls_5K_lylq,
0 as net_migration_arr_ls_5K_lwk,
coalesce(d.cnt,0) as Head_count,
coalesce(e.cnt,0) as Head_count_qtr_begin,
row_number() over (PARTITION BY d.manager,d.Standard_team_name,d.Quarter,d.geo,d.sales_center order by repmanger) as rn,
a.fiscal_yr_and_wk_desc,
a.fiscal_yr_and_qtr_desc,
a.refresh_date,
b.cw_flag as cw_flag,
b.cw_flag_new as cw_latest_flag,
b.lw_flag as lw_flag,
b.wkno as week_number,
b.rolling_qtr,
b.fiscal_yr_and_qtr_desc_lqtr
from b2b_phones.abddash_summary a
left outer join
b2b_phones.vw_calender_flags b
on a.fiscal_yr_and_wk_desc = b.week
left outer join
(select distinct Standard_team_name,Display_Team_Category,Display_team_name,Display_team_manager,Display_team_manager_ldap from b2b_phones.dim_team) c
on upper(a.repteam) = upper(c.Standard_team_name)
left outer join
(select distinct Manager,Standard_team_name,Quarter,geo,sales_center,COUNT(*) OVER (PARTITION BY Manager,Standard_team_name,Quarter,geo,sales_center) as cnt from b2b_phones.dim_agent_details) d 
on upper(trim(a.repmanger)) = upper(trim(d.Manager)) and a.fiscal_yr_and_qtr_desc = d.Quarter and a.repteam = d.Standard_team_name and upper(trim(a.geo)) = upper(trim(d.geo)) and upper(trim(a.sales_center)) = upper(trim(d.sales_center))
left outer join
(select distinct Manager,Standard_team_name,Quarter,geo,sales_center,COUNT(*) OVER (PARTITION BY Manager,Standard_team_name,Quarter,geo,sales_center) as cnt from b2b_phones.dim_agent_details_qtr_begin ) e
on upper(trim(a.repmanger)) = upper(trim(e.Manager)) and a.fiscal_yr_and_qtr_desc = e.Quarter and a.repteam = e.Standard_team_name and upper(trim(a.geo)) = upper(trim(e.geo)) and upper(trim(a.sales_center)) = upper(trim(e.sales_center))
)
