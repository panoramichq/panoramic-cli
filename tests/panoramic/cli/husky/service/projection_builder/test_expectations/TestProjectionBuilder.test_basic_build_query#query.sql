SELECT 'data-source' AS __data_source, ad_id AS ad_id, sum(impressions) AS impressions 
FROM (SELECT db_schema_metric_table_hourly_0f32eb13951cfe30.ad_id AS ad_id, db_schema_metric_table_hourly_0f32eb13951cfe30.impressions AS impressions 
FROM db.schema.metric_table_hourly AS db_schema_metric_table_hourly_0f32eb13951cfe30 ) GROUP BY __data_source, ad_id