SELECT impressions AS impressions, simple_count_all AS simple_count_all, simple_count_distinct AS simple_count_distinct, simple_first_by AS simple_first_by, simple_last_by AS simple_last_by, simple_max AS simple_max, simple_min AS simple_min 
FROM (SELECT impressions AS impressions, simple_count_all AS simple_count_all, simple_count_distinct AS simple_count_distinct, simple_first_by AS simple_first_by, simple_last_by AS simple_last_by, simple_max AS simple_max, simple_min AS simple_min 
FROM (SELECT sum(impressions) AS impressions, sum(simple_count_all) AS simple_count_all, sum(simple_count_distinct) AS simple_count_distinct, simple_first_by AS simple_first_by, simple_last_by AS simple_last_by, simple_max AS simple_max, simple_min AS simple_min 
FROM (SELECT impressions, simple_count_all, simple_count_distinct, simple_first_by, simple_last_by, simple_max, simple_min 
FROM (SELECT impressions, simple_count_all, simple_count_distinct, simple_first_by, simple_last_by, simple_max, simple_min 
FROM (SELECT sum(coalesce(q0.impressions,0)) as impressions, sum(coalesce(q0.simple_count_all,0)) as simple_count_all, sum(coalesce(q0.simple_count_distinct,0)) as simple_count_distinct, q0.simple_first_by AS simple_first_by, q0.simple_last_by AS simple_last_by, q0.simple_max AS simple_max, q0.simple_min AS simple_min 
FROM (SELECT 'adwords' AS __data_source, sum(impressions) AS impressions, count(simple_count_all) AS simple_count_all, count(DISTINCT simple_count_distinct) AS simple_count_distinct, simple_first_by AS simple_first_by, simple_last_by AS simple_last_by, max(simple_max) AS simple_max, min(simple_min) AS simple_min 
FROM (SELECT impressions AS impressions, simple_count_all AS simple_count_all, simple_count_distinct AS simple_count_distinct, simple_first_by AS simple_first_by, simple_last_by AS simple_last_by, simple_max AS simple_max, simple_min AS simple_min 
FROM (SELECT schema_metric_table_hourly_4ba9264ca9b14f09.impressions AS impressions, schema_metric_table_hourly_4ba9264ca9b14f09.ad_id AS simple_count_all, schema_metric_table_hourly_4ba9264ca9b14f09.ad_id AS simple_count_distinct, FIRST_VALUE(schema_metric_table_hourly_4ba9264ca9b14f09.ad_id) OVER (PARTITION BY schema_metric_table_hourly_4ba9264ca9b14f09.ad_id, schema_metric_table_hourly_4ba9264ca9b14f09.start_time ORDER BY schema_metric_table_hourly_4ba9264ca9b14f09.objective ASC NULLS LAST) AS simple_first_by, LAST_VALUE(schema_metric_table_hourly_4ba9264ca9b14f09.ad_id) OVER (PARTITION BY schema_metric_table_hourly_4ba9264ca9b14f09.ad_id, schema_metric_table_hourly_4ba9264ca9b14f09.start_time ORDER BY schema_metric_table_hourly_4ba9264ca9b14f09.objective ASC NULLS LAST) AS simple_last_by, schema_metric_table_hourly_4ba9264ca9b14f09.ad_id AS simple_max, schema_metric_table_hourly_4ba9264ca9b14f09.ad_id AS simple_min 
FROM schema.metric_table_hourly AS schema_metric_table_hourly_4ba9264ca9b14f09  
WHERE schema_metric_table_hourly_4ba9264ca9b14f09.account_id = '123')) GROUP BY __data_source, simple_first_by, simple_last_by) AS q0 GROUP BY q0.simple_first_by, q0.simple_last_by, q0.simple_max, q0.simple_min))) GROUP BY simple_first_by, simple_last_by, simple_max, simple_min))
 LIMIT 100