SELECT impressions AS impressions, simple_first_by AS simple_first_by, simple_last_by AS simple_last_by 
FROM (SELECT impressions AS impressions, simple_first_by AS simple_first_by, simple_last_by AS simple_last_by 
FROM (SELECT sum(impressions) AS impressions, simple_first_by AS simple_first_by, simple_last_by AS simple_last_by 
FROM (SELECT impressions, simple_first_by, simple_last_by 
FROM (SELECT impressions, simple_first_by, simple_last_by 
FROM (SELECT sum(coalesce(q0.impressions,0)) as impressions, q0.simple_first_by AS simple_first_by, q0.simple_last_by AS simple_last_by 
FROM (SELECT 'adwords' AS __data_source, sum(impressions) AS impressions, simple_first_by AS simple_first_by, simple_last_by AS simple_last_by 
FROM (SELECT impressions AS impressions, simple_first_by AS simple_first_by, simple_last_by AS simple_last_by 
FROM (SELECT db_schema_metric_table_hourly_0f32eb13951cfe30.impressions AS impressions, FIRST_VALUE(db_schema_metric_table_hourly_0f32eb13951cfe30.ad_id) OVER (PARTITION BY db_schema_metric_table_hourly_0f32eb13951cfe30.ad_id, db_schema_metric_table_hourly_0f32eb13951cfe30.start_time ORDER BY db_schema_metric_table_hourly_0f32eb13951cfe30.objective ASC NULLS LAST) AS simple_first_by, LAST_VALUE(db_schema_metric_table_hourly_0f32eb13951cfe30.ad_id) OVER (PARTITION BY db_schema_metric_table_hourly_0f32eb13951cfe30.ad_id, db_schema_metric_table_hourly_0f32eb13951cfe30.start_time ORDER BY db_schema_metric_table_hourly_0f32eb13951cfe30.objective ASC NULLS LAST) AS simple_last_by 
FROM db.schema.metric_table_hourly AS db_schema_metric_table_hourly_0f32eb13951cfe30  
WHERE db_schema_metric_table_hourly_0f32eb13951cfe30.account_id = '123')) GROUP BY __data_source, simple_first_by, simple_last_by) AS q0 GROUP BY q0.simple_first_by, q0.simple_last_by))) GROUP BY simple_first_by, simple_last_by))
 LIMIT 100