import spark.implicits._

// Loading Data and adding week information
val df = spark.table("group3a.cdc_records_sites_orc").withColumn("week",weekofyear($"dttm_utc"))

// All Sites CDC, Timeframe : 5 min 
val cdc_all_5min = df.groupBy("dttm_utc").agg(round(sum("value"),4) as "total_cdc")

// Average per industry, Timeframe : 5 min 
val cdc_inds_avg_5min = df.groupBy("industry","dttm_utc").agg(round(avg("value"),4) as "avg_cdc")

// All Sites CDC, Timeframe : week 
val cdc_all_week = df.groupBy("week").agg(round(sum("value"),4) as "total_cdc")

// Average per industry, Timeframe : week
val cdc_inds_avg_week = df.groupBy("industry","week").agg(round(avg("value"),4) as "avg_cdc")