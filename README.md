EnerNOC GreenButton - Data Analytics

Below a detailed report of data analytics works operated on EnerNOC GreenButton data


# Table of Contents
1. [Getting Data into Hadoop](#getting-data-into-hadoop)
2. [Hive DDL](#hive-ddl)
3. [Hive DML](#hive-dml)
4. [Load Curve Calculation](#load-curve-calculation)
	1. [Load Curve Calculation via HIVE](#load-curve-calculation-via-hive)
	2. [Load Curve Calculation via SPARK](#load-curve-calculation-via-spark)

### Getting Data into Hadoop

After getting and unarchiving data into the cluster, we move all data files into HDFS
 
```bash
hadoop fs -mkdir -p /group3a/raw/v1/csv
hadoop fs -mkdir -p /group3a/raw/v1/meta

hadoop fs -put ./csv/*.csv /group3a/raw/v1/csv
hadoop fs -put ./meta/all_sites.csv /group3a/raw/v1/meta

hadoop fs -chown hive:hdfs /group3a/raw/v1/csv
hadoop fs -chown hive:hdfs /group3a/raw/v1/meta
```

### HIVE DDL

We create our own database to avoid any issue using the default schema
```sql
CREATE DATABASE IF NOT EXISTS GROUP3A;
```

We start creating 2 external tables that point on CSV files

```sql
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
```

We create then a table that combine all the data. Note that we'll create a intermediate table to fill in Site IDs.

We choose ORC as storage format : data will be compacted, hence faster to query. 

```sql
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
``` 

### HIVE DML

Once Data created, we load data

```sql
INSERT OVERWRITE TABLE group3a.cdc_records_orc 
SELECT cast(REGEXP_EXTRACT(INPUT__FILE__NAME, '.*/(.*)/([0-9]*)', 2) as int) AS site_id,
`timestamp`,
dttm_utc,
value,
estimated,
anomaly	
FROM group3a.cdc_records_init;

INSERT OVERWRITE TABLE group3a.cdc_records_sites_orc
SELECT
cdc.site_id,
cdc.`timestamp`,
cdc.dttm_utc,
cdc.value,
cdc.estimated,
cdc.anomaly,
sit.industry,
sit.sub_industry,
sit.sq_ft,
sit.lat,
sit.lng,
sit.time_zone,
sit.tz_offset
FROM group3a.cdc_records_orc cdc LEFT OUTER JOIN group3a.sites sit ON (cdc.site_id = sit.site_id);
```

### Load Curve Calculation
We'll do this calculation following two approaches : HIVE and SPARK

#### Load Curve Calculation via HIVE

All Sites, Timeframe : 5 min 
```sql
DROP VIEW IF EXISTS group3a.cdc_all_sites_5min;
CREATE VIEW group3a.cdc_all_sites_5min 
AS SELECT dttm_utc, ROUND(SUM(value),4) as total_cdc
FROM group3a.cdc_records_sites_orc
GROUP BY dttm_utc
ORDER BY dttm_utc ASC;
```

Average per industry, Timeframe : 5 min 
```sql
DROP VIEW IF EXISTS group3a.cdc_industry_avg_5min;
CREATE VIEW group3a.cdc_industry_avg_5min
AS SELECT industry, dttm_utc, ROUND(AVG(value),4) as avg_cdc
FROM group3a.cdc_records_sites_orc
GROUP BY industry,dttm_utc;
```

All Sites, Timeframe : week 
```sql
DROP VIEW IF EXISTS group3a.cdc_all_sites_week;
CREATE VIEW group3a.cdc_all_sites_week
AS SELECT WEEKOFYEAR(dttm_utc) as week, ROUND(SUM(value),4) as total_cdc
FROM group3a.cdc_records_sites_orc
GROUP BY WEEKOFYEAR(dttm_utc)
ORDER BY week ASC;
```

Average per industry, Timeframe : week 
```sql
DROP VIEW IF EXISTS group3a.cdc_industry_avg_week;
CREATE VIEW group3a.cdc_industry_avg_week
AS SELECT industry, WEEKOFYEAR(dttm_utc) as week, ROUND(AVG(value),4) as avg_cdc
FROM group3a.cdc_records_sites_orc
GROUP BY industry,WEEKOFYEAR(dttm_utc);
```

#### Load Curve Calculation via SPARK

Load Data into Data Frame
```scala
val df = spark.table("group3a.cdc_records_sites_orc").withColumn("week",weekofyear($"dttm_utc")).withColumn("day",date_format($"dttm_utc","dd.MM.yyyy"))
```

All Sites, Timeframe : 5 min 
```scala
val cdc_all_5min = df.groupBy("dttm_utc").agg(round(sum("value"),4) as "total_cdc")
```

Average per industry, Timeframe : 5 min 
```scala
val cdc_inds_avg_5min = df.groupBy("industry","dttm_utc").agg(round(avg("value"),4) as "avg_cdc")
```

All Sites, Timeframe : week 
```scala
val cdc_all_week = df.groupBy("week").agg(round(sum("value"),4) as "total_cdc")
```

Average per industry, Timeframe : week 
```scala
val cdc_inds_avg_week = df.groupBy("industry","week").agg(round(avg("value"),4) as "avg_cdc")
```

### Industry Ranking

First let's calculate the sum of load curve and surface per industry and week

```scala
val cdc_inds_sum_week_sqft = df.groupBy("industry","week").agg(round(sum("value"),4).as("sum_cdc"),sum("sq_ft").as("sum_sq_ft"))
```

Creata a new column to store intensity

```scala
val cdc_inds_week_intensity = cdc_inds_sum_week_sqft.withColumn("intensity",round($"sum_cdc"/$"sum_sq_ft",4))
```

**Rank then industry comparing max intensity over the year**

```scala
val industry_intensity_rank = cdc_inds_week_intensity.groupBy("industry").agg(max("intensity").as("max_int")).orderBy("max_int")
industry_intensity_rank.write.mode("overwrite").saveAsTable("group3a.industry_intensity_rank")
```

Rank then industry comparing max intensity per week

```scala
val indus_intensity_week_rank = cdc_inds_week_intensity.groupBy("industry","week").agg(max("intensity").as("max_int_week")).orderBy("max_int_week")
indus_intensity_week_rank.write.mode("overwrite").saveAsTable("group3a.indus_intensity_week_rank")
```

Calculate season based on week number

```scala
val indus_intensity_week_rank_season = indus_intensity_week_rank.withColumn("season", when($"week" < 14, "WINTER").when($"week" < 27, "SPRING").when($"week" < 40, "SUMMER").otherwise("AUTOMN"))
```

**Rank then industry comparing max intensity per season**

```scala
val indus_intensity_season_rank = indus_intensity_week_rank_season.groupBy("industry","season").agg(max("max_int_week").as("max_int_season")).orderBy("season","max_int_season")
indus_intensity_season_rank.write.mode("overwrite").saveAsTable("group3a.indus_intensity_season_rank")
val indus_intensity_season_rank_2 = indus_intensity_season_rank.withColumn("max_int_season_w",$"max_int_season"*1000)
indus_intensity_season_rank_2.write.mode("overwrite").saveAsTable("group3a.indus_intensity_season_rank_2")
```

### Highest Energy Day

```scala
val df2 = df.select("site_id","value","day")
val cdc_site_day_sum = df2.groupBy("site_id","day").agg(round(sum("value"),4).as("sum_value_site"))
val cdc_site_day_sum_rep = cdc_site_day_sum.repartition("site_id")
val cdc_site_high_day = cdc_site_day_sum_rep.groupBy("site_id","day").max("sum_value_site")
```
