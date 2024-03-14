CREATE TABLE spark_catalog.b2b_phones.dim_orders_negotiated (
  Week STRING,
  Date STRING,
  CRMGUID STRING,
  Sales_Document STRING,
  Assignee_Name STRING,
  LDAP STRING,
  `Subs offer` STRING,
  Units STRING,
  Direction STRING,
  Country STRING,
  base STRING,
  Phone_vs_web STRING,
  Contract_ID STRING,
  cc_segment STRING,
  Legacy_team_name STRING,
  Standard_team_name STRING,
  Team_key STRING,
  Sales_Center STRING,
  fiscal_yr_and_qtr_desc STRING)
USING PARQUET
PARTITIONED BY (Sales_Center, fiscal_yr_and_qtr_desc)
LOCATION 'abfs://or1-prod-data@azr6665prddpaasb2bdna.dfs.core.windows.net/user/hive/warehouse/b2bdna/b2b_phones.db/dim_orders_negotiated'
TBLPROPERTIES (
  'bucketing_version' = '2')
