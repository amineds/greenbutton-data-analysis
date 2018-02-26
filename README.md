EnerNOC GreenButton Data Analysis

### Commandes Hadoop

```
hadoop fs -mkdir -p /group3a/raw/v1/csv
hadoop fs -mkdir -p /group3a/raw/v1/meta

hadoop fs -put ./csv/*.csv /group3a/raw/v1/csv
hadoop fs -put ./meta/all_sites.csv /group3a/raw/v1/meta

hadoop fs -chown hive:hive /group3a/raw/v1/csv
hadoop fs -chown hive:hive /group3a/raw/v1/meta

```

### HIVE DDL

#### Records Data

```
DROP TABLE IF EXISTS csv_table;
CREATE EXTERNAL TABLE csv_table (
`timestamp` float,
dttm_utc timestamp,
value float,
estimated float,
anomaly string 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '/group3a/raw/v1/csv'
tblproperties("skip.header.line.count"="1");
```

```
DROP VIEW IF EXISTS data_view;
CREATE VIEW data_view
as select cast(REGEXP_EXTRACT(INPUT__FILE__NAME, '.*/(.*)/([0-9]*)', 2) as int) AS side_id,
`timestamp`,
dttm_utc,
value,
estimated,
anomaly	
from csv_table;
```

#### Sites

```
DROP TABLE IF EXISTS meta_table;
CREATE EXTERNAL TABLE meta_table (
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
```

### HIVE DML

#### Calculer la CdC totale des 100 sites (pas de temps 5 minutes)
```
DROP TABLE IF EXISTS CDC_ALL_SITES_5_MIN;
CREATE TABLE CDC_ALL_SITES_5_MIN 
AS SELECT dttm_utc, SUM(value) from data_view 
GROUP BY (dttm_utc);
```

