%sql
CREATE TABLE b2b_phones.dim_team_stg (
  Legacy_team_name string,
  Standard_team_name string,
  ABD_Flag string,
  Team_category string,
  Display_Team_Category string,
  Display_team_name string,
  Display_team_manager string,
  Display_team_manager_ldap string
)
  ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ',' ESCAPED BY '\\'
LINES TERMINATED BY '\n'
stored as textfile
location 'abfs://or1-prod-data@azr6665prddpaasb2bdna.dfs.core.windows.net/user/hive/warehouse/b2bdna/b2b_phones.db/dim_team_stg'
