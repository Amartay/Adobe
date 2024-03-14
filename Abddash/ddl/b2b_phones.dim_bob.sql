CREATE TABLE spark_catalog.b2b_phones.dim_bob (
  Contract_ID STRING,
  CSAM_Name STRING,
  CSAM_Team STRING,
  CSAM_Email STRING,
  Group STRING,
  CSAM_BOB_FLAG STRING,
  CSAM STRING,
  CSAM_Market_Area STRING,
  exclusion_flag STRING,
  Legacy_team_name STRING,
  Standard_team_name STRING,
  Team_key STRING,
  Fiscal_yr_and_qtr_desc STRING)
USING PARQUET
PARTITIONED BY (Fiscal_yr_and_qtr_desc)
LOCATION 'abfs://or1-prod-data@azr6665prddpaasb2bdna.dfs.core.windows.net/user/hive/warehouse/b2bdna/b2b_phones.db/dim_bob'
TBLPROPERTIES (
  'bucketing_version' = '2')
