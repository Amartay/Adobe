CREATE TABLE spark_catalog.b2b_phones.dim_qrf (
  KPI STRING,
  Fiscal_WK STRING,
  Geo STRING,
  Final_Team STRING,
  Process STRING,
  Sales_centre STRING,
  PHONE_VS_WEB STRING,
  Product_Mapping STRING,
  Team_grouping STRING,
  TSM STRING,
  Rep STRING,
  Txn STRING,
  flag STRING,
  GROSS STRING,
  MIGRATION STRING,
  CANCEL STRING,
  Buffer STRING,
  Incremental_arr STRING,
  Incremental_arr_customer_addon STRING,
  Standard_team_name STRING,
  Team_key STRING,
  Team_category STRING,
  Manager STRING,
  Quarter STRING)
USING parquet
PARTITIONED BY (Quarter)
LOCATION 'abfs://or1-prod-data@azr6665prddpaasb2bdna.dfs.core.windows.net/user/hive/warehouse/b2bdna/b2b_phones.db/dim_qrf'
TBLPROPERTIES (
  'bucketing_version' = '2')
