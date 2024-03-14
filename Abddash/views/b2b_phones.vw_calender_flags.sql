CREATE VIEW b2b_phones.vw_calender_flags (
  quarter,
  week,
  baked_weeks,
  wkname,
  wkno,
  fiscal_wk_ending_date,
  week2_flag,
  week2_cw_flag,
  week2_lw_flag,
  week5_flag,
  week5_cw_flag,
  week5_lw_flag,
  week10_flag,
  week10_cw_flag,
  week10_lw_flag,
  week4_flag,
  week4_cw_flag,
  week4_lw_flag,
  week_prev_flag,
  week_prev_cw_flag,
  week_prev_lw_flag,
  week_select_flag,
  cw_flag,
  cw_flag_new,
  lw_flag,
  week9_flag,
  week9_cw_flag,
  week9_lw_flag,
  rolling_qtr,
  fiscal_yr_and_qtr_desc_lqtr)
TBLPROPERTIES (
  'bucketing_version' = '2',
  'transient_lastDdlTime' = '1692963827')
AS select
a.fiscal_yr_and_qtr_desc as quarter,a.fiscal_yr_and_wk_desc as week,a.baked_weeks, a.wkname, week_no as wkno, a.fiscal_wk_ending_date,
case when a.baked_weeks >= 2 then 1 else 0 end week2_flag,
case when a.baked_weeks = 2 then 1
when a.baked_weeks > 2 and a.week_no = 13 and a.number_weeks_in_quarter =13  then 1
when a.baked_weeks > 2 and a.week_no = 14 and a.number_weeks_in_quarter =14  then 1
else 0 end week2_cw_flag,
case when a.baked_weeks = 3 then 1
when a.baked_weeks > 3 and a.week_no = 12 and a.number_weeks_in_quarter = 13  then 1
when a.baked_weeks > 3 and a.week_no = 13 and a.number_weeks_in_quarter =14 then 1
else 0 end week2_lw_flag,
case when a.baked_weeks >= 5 then 1 else 0 end week5_flag,
case when a.baked_weeks = 5 then 1
when a.baked_weeks > 5 and a.week_no = 13 and a.number_weeks_in_quarter =13  then 1
when a.baked_weeks > 5 and a.week_no = 14 and a.number_weeks_in_quarter =14  then 1 else 0
end week5_cw_flag,
case when a.baked_weeks = 6 then 1
when a.baked_weeks > 6 and a.week_no = 12 and a.number_weeks_in_quarter = 13  then 1
when a.baked_weeks > 6 and a.week_no = 13 and a.number_weeks_in_quarter = 14  then 1
else 0 end week5_lw_flag,
case when a.baked_weeks >= 10 then 1 else 0 end week10_flag,
case when a.baked_weeks = 10 then 1
when a.baked_weeks > 10 and a.week_no = 13 and a.number_weeks_in_quarter =13  then 1
when a.baked_weeks > 10 and a.week_no = 14 and a.number_weeks_in_quarter =14  then 1
else 0 end week10_cw_flag,
case when a.baked_weeks = 11 then 1
when a.baked_weeks > 11 and a.week_no = 12 and a.number_weeks_in_quarter = 13  then 1
when a.baked_weeks > 11 and a.week_no = 13 and a.number_weeks_in_quarter = 14  then 1
else 0 end week10_lw_flag,
case when a.baked_weeks >= 4 then 1 else 0 end week4_flag,
case when a.baked_weeks = 4 then 1
when a.baked_weeks > 4 and a.week_no = 13 and a.number_weeks_in_quarter =13  then 1
when a.baked_weeks > 4 and a.week_no = 14 and a.number_weeks_in_quarter =14  then 1
else 0 end week4_cw_flag,
case when a.baked_weeks = 5 then 1
when a.baked_weeks > 5 and a.week_no = 12 and a.number_weeks_in_quarter = 13  then 1
when a.baked_weeks > 5 and a.week_no = 13 and a.number_weeks_in_quarter = 14  then 1
else 0 end week4_lw_flag,
case when a.baked_weeks >= 1 then 1 else 0 end week_prev_flag,
case when a.baked_weeks = 1 then 1
when a.baked_weeks > 1 and a.week_no = 13 and  a.number_weeks_in_quarter = 13  then 1
when a.baked_weeks > 1 and a.week_no = 14  and a.number_weeks_in_quarter = 14  then 1
else 0 end week_prev_cw_flag,
case when a.baked_weeks >2 and a.week_no = 12  and  a.number_weeks_in_quarter = 13  then 1
when a.baked_weeks >2 and a.week_no = 13  and  a.number_weeks_in_quarter = 14  then 1
when a.baked_weeks = 2 and a.week_no < 13 and a.number_weeks_in_quarter = 13  then 1
when a.baked_weeks = 2 and a.week_no < 14 and a.number_weeks_in_quarter = 14  then 1
else 0 end week_prev_lw_flag,
a.week_select_flag,
case when a.baked_weeks > 0 and a.week_no = 13 and  a.number_weeks_in_quarter = 13  then 1
when a.baked_weeks > 0 and a.week_no = 14 and  a.number_weeks_in_quarter = 14  then 1
when a.baked_weeks = 0 then 1
else 0 end cw_flag,
case when a.baked_weeks_new > 0 and a.week_no = 13 and  a.number_weeks_in_quarter = 13  then 1
when a.baked_weeks_new > 0 and a.week_no = 14 and  a.number_weeks_in_quarter = 14  then 1
when a.baked_weeks_new = 0 then 1
else 0 end cw_flag_new,
case when a.baked_weeks > 1 and a.week_no = 12 and a.number_weeks_in_quarter = 13  then 1
when a.baked_weeks > 1 and a.week_no = 13 and a.number_weeks_in_quarter = 14  then 1
when a.baked_weeks = 1 and a.week_no < 13 and a.number_weeks_in_quarter = 13 then 1
when a.baked_weeks = 1 and a.week_no < 14 and a.number_weeks_in_quarter = 14 then 1
else 0 end lw_flag,
case when a.baked_weeks >= 9 then 1 else 0 end week9_flag,
case when a.baked_weeks = 9 then 1
when a.baked_weeks > 9 and a.week_no = 13 and a.number_weeks_in_quarter = 13  then 1
when a.baked_weeks > 9 and a.week_no = 14 and a.number_weeks_in_quarter = 14  then 1
else 0 end week9_cw_flag,
case when a.baked_weeks = 10 then 1
when a.baked_weeks > 10 and a.week_no = 12 and a.number_weeks_in_quarter = 13  then 1
when a.baked_weeks > 10 and a.week_no = 13 and a.number_weeks_in_quarter = 14  then 1
else 0 end week9_lw_flag,
concat(a.fiscal_yr_and_qtr_desc,',' ,a.fiscal_yr_and_qtr_desc_lqtr) as rolling_qtr,
a.fiscal_yr_and_qtr_desc_lqtr as fiscal_yr_and_qtr_desc_lqtr
from (SELECT
lkp.fiscal_yr_and_qtr_desc, lkp.fiscal_yr_and_qtr_desc_lqtr,lkp.fiscal_yr_and_wk_desc,lkp.baked_weeks,lkp.baked_weeks_new, lkp.wkname, lkp.fiscal_wk_ending_date, SUBSTRING(lkp.wkname,4,2) AS week_no,
lkp.week_select_flag, max(SUBSTRING(lkp.wkname,4,2)) over (partition by lkp.fiscal_yr_and_qtr_desc) as number_weeks_in_quarter FROM b2b_phones.rtb_lookup_table_history lkp) a
