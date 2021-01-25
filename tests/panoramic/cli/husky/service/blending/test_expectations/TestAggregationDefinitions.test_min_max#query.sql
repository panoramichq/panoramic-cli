SELECT account_id AS account_id, impressions AS impressions, simple_max AS simple_max, simple_min AS simple_min 
FROM (SELECT account_id AS account_id, impressions AS impressions, simple_max AS simple_max, simple_min AS simple_min 
FROM (SELECT account_id AS account_id, sum(impressions) AS impressions, simple_max AS simple_max, simple_min AS simple_min 
FROM (SELECT account_id, impressions, simple_max, simple_min 
FROM (SELECT account_id, impressions, simple_max, simple_min 
FROM (SELECT q0.account_id AS account_id, sum(coalesce(q0.impressions,0)) as impressions, q0.simple_max AS simple_max, q0.simple_min AS simple_min 
FROM (SELECT 'adwords' AS __data_source, account_id AS account_id, sum(impressions) AS impressions, max(simple_max) AS simple_max, min(simple_min) AS simple_min 
FROM (SELECT schema_metric_table_hourly_4ba9264ca9b14f09.account_id AS account_id, schema_metric_table_hourly_4ba9264ca9b14f09.impressions AS impressions, schema_metric_table_hourly_4ba9264ca9b14f09.ad_id AS simple_max, schema_metric_table_hourly_4ba9264ca9b14f09.ad_id AS simple_min 
FROM schema.metric_table_hourly AS schema_metric_table_hourly_4ba9264ca9b14f09  
WHERE schema_metric_table_hourly_4ba9264ca9b14f09.account_id = '123') GROUP BY __data_source, account_id) AS q0 GROUP BY q0.account_id, q0.simple_max, q0.simple_min))) GROUP BY account_id, simple_max, simple_min))
 LIMIT 100