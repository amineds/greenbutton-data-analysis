 
-- All Sites, Timeframe : 5 min 
DROP VIEW IF EXISTS group3a.cdc_all_sites_5min;
CREATE VIEW group3a.cdc_all_sites_5min 
AS SELECT dttm_utc, ROUND(SUM(value),4) as total_cdc
FROM group3a.cdc_records_sites_orc
GROUP BY dttm_utc
ORDER BY dttm_utc ASC;

-- Avergae per industry, Timeframe : 5 min 
DROP VIEW IF EXISTS group3a.cdc_industry_avg_5min;
CREATE VIEW group3a.cdc_industry_avg_5min
AS SELECT industry, dttm_utc, ROUND(AVG(value),4) as avg_cdc
FROM group3a.cdc_records_sites_orc
GROUP BY industry,dttm_utc;

-- All Sites, Timeframe : week 
DROP VIEW IF EXISTS group3a.cdc_all_sites_week;
CREATE VIEW group3a.cdc_all_sites_week
AS SELECT WEEKOFYEAR(dttm_utc) as week, ROUND(SUM(value),4) as total_cdc
FROM group3a.cdc_records_sites_orc
GROUP BY WEEKOFYEAR(dttm_utc)
ORDER BY week ASC;

-- Avergae per industry, Timeframe : week 
DROP VIEW IF EXISTS group3a.cdc_industry_avg_week;
CREATE VIEW group3a.cdc_industry_avg_week
AS SELECT industry, WEEKOFYEAR(dttm_utc) as week, ROUND(AVG(value),4) as avg_cdc
FROM group3a.cdc_records_sites_orc
GROUP BY industry,WEEKOFYEAR(dttm_utc);



