CREATE TABLE spark_catalog.b2b_phones.ib_manual (
  View STRING,
  geo_code STRING,
  Sales_Center STRING,
  Market_area STRING,
  Market_Area_Code STRING,
  Market_Area_Desc STRING,
  Language STRING,
  rep_team STRING,
  Ldap STRING,
  Name STRING,
  Channel STRING,
  Initiation_method STRING,
  Surface_app_ID STRING,
  interaction_type STRING,
  queue_type STRING,
  queue STRING,
  Entered STRING,
  accepeted STRING,
  Non_Interactive STRING,
  abandoned STRING,
  Talk_time_Sec STRING,
  Wrap_time_Sec STRING,
  fiscal_yr_and_qtr_desc STRING,
  fiscal_yr_and_wk_desc STRING)
USING parquet
PARTITIONED BY (fiscal_yr_and_qtr_desc, fiscal_yr_and_wk_desc)
LOCATION 'abfs://or1-prod-data@azr6665prddpaasb2bdna.dfs.core.windows.net/user/hive/warehouse/b2bdna/b2b_phones.db/ib_manual'
TBLPROPERTIES (
  'bucketing_version' = '2')
