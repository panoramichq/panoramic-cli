SELECT account_id AS account_id, impressions AS impressions, simple_max AS simple_max, simple_min AS simple_min 
FROM (SELECT account_id AS account_id, impressions AS impressions, simple_max AS simple_max, simple_min AS simple_min 
FROM (SELECT account_id AS account_id, sum(impressions) AS impressions, simple_max AS simple_max, simple_min AS simple_min 
FROM (SELECT account_id, impressions, simple_max, simple_min 
FROM (SELECT account_id, impressions, simple_max, simple_min 
FROM (SELECT q0.account_id AS account_id, sum(coalesce(q0.impressions,0)) as impressions, q0.simple_max AS simple_max, q0.simple_min AS simple_min 
FROM (SELECT 'adwords' AS __data_source, account_id AS account_id, sum(impressions) AS impressions, max(simple_max) AS simple_max, min(simple_min) AS simple_min 
FROM (SELECT db_schema_metric_table_hourly_0f32eb13951cfe30.account_id AS account_id, db_schema_metric_table_hourly_0f32eb13951cfe30.impressions AS impressions, db_schema_metric_table_hourly_0f32eb13951cfe30.ad_id AS simple_max, db_schema_metric_table_hourly_0f32eb13951cfe30.ad_id AS simple_min 
FROM db.schema.metric_table_hourly AS db_schema_metric_table_hourly_0f32eb13951cfe30  
WHERE db_schema_metric_table_hourly_0f32eb13951cfe30.account_id = '123') GROUP BY __data_source, account_id) AS q0 GROUP BY q0.account_id, q0.simple_max, q0.simple_min))) GROUP BY account_id, simple_max, simple_min))
 LIMIT 100