SELECT 'data-source' AS __data_source, ad_id AS ad_id, sum(impressions) AS impressions 
FROM (SELECT schema_metric_table_hourly_4ba9264ca9b14f09.ad_id AS ad_id, schema_metric_table_hourly_4ba9264ca9b14f09.impressions AS impressions 
FROM schema.metric_table_hourly AS schema_metric_table_hourly_4ba9264ca9b14f09 ) GROUP BY __data_source, ad_id