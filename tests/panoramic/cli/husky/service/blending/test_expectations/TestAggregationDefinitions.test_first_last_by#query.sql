SELECT impressions AS impressions, simple_first_by AS simple_first_by, simple_last_by AS simple_last_by 
FROM (SELECT impressions AS impressions, simple_first_by AS simple_first_by, simple_last_by AS simple_last_by 
FROM (SELECT sum(impressions) AS impressions, simple_first_by AS simple_first_by, simple_last_by AS simple_last_by 
FROM (SELECT impressions, simple_first_by, simple_last_by 
FROM (SELECT impressions, simple_first_by, simple_last_by 
FROM (SELECT sum(coalesce(q0.impressions,0)) as impressions, q0.simple_first_by AS simple_first_by, q0.simple_last_by AS simple_last_by 
FROM (SELECT 'adwords' AS __data_source, sum(impressions) AS impressions, simple_first_by AS simple_first_by, simple_last_by AS simple_last_by 
FROM (SELECT impressions AS impressions, simple_first_by AS simple_first_by, simple_last_by AS simple_last_by 
FROM (SELECT schema_metric_table_hourly_4ba9264ca9b14f09.impressions AS impressions, FIRST_VALUE(schema_metric_table_hourly_4ba9264ca9b14f09.ad_id) OVER (PARTITION BY schema_metric_table_hourly_4ba9264ca9b14f09.ad_id, schema_metric_table_hourly_4ba9264ca9b14f09.start_time ORDER BY schema_metric_table_hourly_4ba9264ca9b14f09.objective ASC NULLS LAST) AS simple_first_by, LAST_VALUE(schema_metric_table_hourly_4ba9264ca9b14f09.ad_id) OVER (PARTITION BY schema_metric_table_hourly_4ba9264ca9b14f09.ad_id, schema_metric_table_hourly_4ba9264ca9b14f09.start_time ORDER BY schema_metric_table_hourly_4ba9264ca9b14f09.objective ASC NULLS LAST) AS simple_last_by 
FROM schema.metric_table_hourly AS schema_metric_table_hourly_4ba9264ca9b14f09  
WHERE schema_metric_table_hourly_4ba9264ca9b14f09.account_id = '123')) GROUP BY __data_source, simple_first_by, simple_last_by) AS q0 GROUP BY q0.simple_first_by, q0.simple_last_by))) GROUP BY simple_first_by, simple_last_by))
 LIMIT 100