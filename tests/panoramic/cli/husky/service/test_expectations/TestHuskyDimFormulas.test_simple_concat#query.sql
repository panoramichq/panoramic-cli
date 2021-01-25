SELECT __1, 'mock_data_source' AS __data_source, account_id AS account_id, ad_name AS ad_name 
FROM (SELECT concat(schema_entity_table_83a8d33392b3cf56.ad_name,'xx') AS __1, schema_entity_table_83a8d33392b3cf56.account_id AS account_id, schema_entity_table_83a8d33392b3cf56.ad_name AS ad_name 
FROM schema.entity_table AS schema_entity_table_83a8d33392b3cf56  
WHERE schema_entity_table_83a8d33392b3cf56.account_id = 'abc') GROUP BY __1, __data_source, account_id, ad_name