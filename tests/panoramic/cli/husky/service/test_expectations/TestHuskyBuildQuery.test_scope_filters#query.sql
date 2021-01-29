SELECT 'mock_data_source' AS __data_source, account_id AS account_id, ad_name AS ad_name 
FROM (SELECT db_schema_entity_table_84641c8dee7d8b60.account_id AS account_id, db_schema_entity_table_84641c8dee7d8b60.ad_name AS ad_name 
FROM db.schema.entity_table AS db_schema_entity_table_84641c8dee7d8b60  
WHERE (db_schema_entity_table_84641c8dee7d8b60.account_id = '595126134331606')) GROUP BY __data_source, account_id, ad_name