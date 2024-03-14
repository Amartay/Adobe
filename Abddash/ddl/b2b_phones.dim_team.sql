%sql
CREATE TABLE b2b_phones.dim_team (
  Legacy_team_name STRING,
  Standard_team_name STRING,
  Team_key STRING,
  ABD_Flag STRING,
  Team_category STRING,
  Display_Team_Category STRING,
  Display_team_name string,
  Display_team_manager string,
  Display_team_manager_ldap string)
USING parquet
LOCATION 'abfs://or1-prod-data@azr6665prddpaasb2bdna.dfs.core.windows.net/user/hive/warehouse/b2bdna/b2b_phones.db/dim_team'
TBLPROPERTIES (
  'bucketing_version' = '2')
