--- CDD DATA ---

-- CDC records, external table as gateway to csv files
DROP TABLE IF EXISTS group3a.cdc_records_init;
CREATE EXTERNAL TABLE group3a.cdc_records_init (
`timestamp` float,
dttm_utc timestamp,
value float,
estimated float,
anomaly string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/group3a/raw/v1/csv'
tblproperties("skip.header.line.count"="1");

-- cdc records tables stored in text format
DROP TABLE IF EXISTS group3a.cdc_records_txt;
CREATE TABLE group3a.cdc_records_txt 
(
site_id int,
`timestamp` float,
dttm_utc timestamp,
value float,
estimated float,
anomaly string 
)
STORED AS TEXTFILE;

-- cdc records tables stored in orc format
DROP TABLE IF EXISTS group3a.cdc_records_orc;
CREATE TABLE group3a.cdc_records_orc 
(
site_id int,
`timestamp` float,
dttm_utc timestamp,
value float,
estimated float,
anomaly string 
)
STORED AS ORC;

--- SITES METADATA ---

DROP TABLE IF EXISTS group3a.sites;
CREATE EXTERNAL TABLE group3a.sites (
site_id int,
industry string,
sub_industry string,
sq_ft string,
lat string,
lng string,
time_zone string,
tz_offset string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/group3a/raw/v1/meta'
tblproperties("skip.header.line.count"="1");

--- COMBINED DATA ---

-- see here for joins : https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Joins
DROP TABLE IF EXISTS group3a.cdc_records_sites_orc;
CREATE TABLE group3a.cdc_records_sites_orc
(site_id int,
`timestamp` float,
dttm_utc timestamp,
value float,
estimated float,
anomaly string,
industry string,
sub_industry string,
sq_ft string,
lat string,
lng string,
time_zone string,
tz_offset string)
STORED AS ORC;