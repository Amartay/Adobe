%sql
CREATE TABLE spark_catalog.b2b_phones.dim_agent_details_scd_active (
  agent_roster_key STRING,
  Active_record_flag STRING,
  start_date STRING,
  end_date STRING,
  Sales_Center STRING,
  Geo STRING,
  Rep_name STRING,
  Ldap STRING,
  TSM STRING,
  TSM_Ldap STRING,
  Manager STRING,
  Manager_ldap STRING,
  Final_team STRING,
  Team_Category STRING,
  ActiveFlag STRING,
  SFDC_Flag STRING,
  CSAM_INIT_FLAG STRING,
  Shift_Time STRING,
  Time_Zone STRING,
  Legacy_team_name STRING,
  Standard_team_name STRING,
  Team_key STRING,
  Quarter STRING)
USING PARQUET
PARTITIONED BY (Quarter)
LOCATION 'abfs://or1-prod-data@azr6665prddpaasb2bdna.dfs.core.windows.net/user/hive/warehouse/b2bdna/b2b_phones.db/dim_agent_details_scd_active'
TBLPROPERTIES (
  'bucketing_version' = '2')
