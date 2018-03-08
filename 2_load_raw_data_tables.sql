-- cdc records, txt format
INSERT OVERWRITE TABLE group3a.cdc_records_txt 
SELECT cast(REGEXP_EXTRACT(INPUT__FILE__NAME, '.*/(.*)/([0-9]*)', 2) as int) AS site_id,
`timestamp`,
dttm_utc,
value,
estimated,
anomaly	
FROM group3a.cdc_records_init; 
-- Time taken: 260.002 seconds
-- Number of Rows 	10531288
-- Raw Data Size 	509125273
-- Total Size 	519656561

-- cdc records, orc format
INSERT OVERWRITE TABLE group3a.cdc_records_orc 
SELECT cast(REGEXP_EXTRACT(INPUT__FILE__NAME, '.*/(.*)/([0-9]*)', 2) as int) AS site_id,
`timestamp`,
dttm_utc,
value,
estimated,
anomaly	
FROM group3a.cdc_records_init; 
-- Time taken: 196.585 seconds
-- Number of Rows 	10531288
-- Raw Data Size 	1474380320
-- Total Size 	25967592

-- cdc records combined with sites metadata, orc format
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
-- Time taken: 70.622 seconds
-- numRows=10531288
-- rawDataSize=8530035313
-- totalSize=26181527




