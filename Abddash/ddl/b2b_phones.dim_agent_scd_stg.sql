%sql
CREATE TABLE b2b_phones.dim_agent_scd_stg (
  Sales_Center STRING,
  Geo STRING,
  Rep_name STRING,
  Ldap STRING,
  TSM STRING,
  TSM_Ldap STRING,
  Manager STRING,
  Manager_ldap STRING,
  Final_Team STRING,
  ActiveFlag STRING,
  SFDC_Flag STRING,
  CSAM_INIT_FLAG STRING,
  Shift_Time STRING,
  Time_Zone STRING,
  agent_start_date STRING,
  Quarter STRING)
  ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
stored as textfile
location 'abfs://or1-prod-data@azr6665prddpaasb2bdna.dfs.core.windows.net/user/hive/warehouse/b2bdna/b2b_phones.db/dim_agent_scd_stg'
