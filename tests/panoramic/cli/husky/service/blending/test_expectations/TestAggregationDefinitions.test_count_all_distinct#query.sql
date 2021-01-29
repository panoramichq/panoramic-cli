SELECT account_id AS account_id, impressions AS impressions, simple_count_all AS simple_count_all, simple_count_distinct AS simple_count_distinct 
FROM (SELECT account_id AS account_id, impressions AS impressions, simple_count_all AS simple_count_all, simple_count_distinct AS simple_count_distinct 
FROM (SELECT account_id AS account_id, sum(impressions) AS impressions, sum(simple_count_all) AS simple_count_all, sum(simple_count_distinct) AS simple_count_distinct 
FROM (SELECT account_id, impressions, simple_count_all, simple_count_distinct 
FROM (SELECT account_id, impressions, simple_count_all, simple_count_distinct 
FROM (SELECT q0.account_id AS account_id, sum(coalesce(q0.impressions,0)) as impressions, sum(coalesce(q0.simple_count_all,0)) as simple_count_all, sum(coalesce(q0.simple_count_distinct,0)) as simple_count_distinct 
FROM (SELECT 'adwords' AS __data_source, account_id AS account_id, sum(impressions) AS impressions, count(simple_count_all) AS simple_count_all, count(DISTINCT simple_count_distinct) AS simple_count_distinct 
FROM (SELECT db_schema_metric_table_hourly_0f32eb13951cfe30.account_id AS account_id, db_schema_metric_table_hourly_0f32eb13951cfe30.impressions AS impressions, db_schema_metric_table_hourly_0f32eb13951cfe30.ad_id AS simple_count_all, db_schema_metric_table_hourly_0f32eb13951cfe30.ad_id AS simple_count_distinct 
FROM db.schema.metric_table_hourly AS db_schema_metric_table_hourly_0f32eb13951cfe30  
WHERE db_schema_metric_table_hourly_0f32eb13951cfe30.account_id = '123') GROUP BY __data_source, account_id) AS q0 GROUP BY q0.account_id))) GROUP BY account_id))
 LIMIT 100