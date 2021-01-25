SELECT account_id AS account_id, impressions AS impressions, simple_count_all AS simple_count_all, simple_count_distinct AS simple_count_distinct 
FROM (SELECT account_id AS account_id, impressions AS impressions, simple_count_all AS simple_count_all, simple_count_distinct AS simple_count_distinct 
FROM (SELECT account_id AS account_id, sum(impressions) AS impressions, sum(simple_count_all) AS simple_count_all, sum(simple_count_distinct) AS simple_count_distinct 
FROM (SELECT account_id, impressions, simple_count_all, simple_count_distinct 
FROM (SELECT account_id, impressions, simple_count_all, simple_count_distinct 
FROM (SELECT q0.account_id AS account_id, sum(coalesce(q0.impressions,0)) as impressions, sum(coalesce(q0.simple_count_all,0)) as simple_count_all, sum(coalesce(q0.simple_count_distinct,0)) as simple_count_distinct 
FROM (SELECT 'adwords' AS __data_source, account_id AS account_id, sum(impressions) AS impressions, count(simple_count_all) AS simple_count_all, count(DISTINCT simple_count_distinct) AS simple_count_distinct 
FROM (SELECT schema_metric_table_hourly_4ba9264ca9b14f09.account_id AS account_id, schema_metric_table_hourly_4ba9264ca9b14f09.impressions AS impressions, schema_metric_table_hourly_4ba9264ca9b14f09.ad_id AS simple_count_all, schema_metric_table_hourly_4ba9264ca9b14f09.ad_id AS simple_count_distinct 
FROM schema.metric_table_hourly AS schema_metric_table_hourly_4ba9264ca9b14f09  
WHERE schema_metric_table_hourly_4ba9264ca9b14f09.account_id = '123') GROUP BY __data_source, account_id) AS q0 GROUP BY q0.account_id))) GROUP BY account_id))
 LIMIT 100