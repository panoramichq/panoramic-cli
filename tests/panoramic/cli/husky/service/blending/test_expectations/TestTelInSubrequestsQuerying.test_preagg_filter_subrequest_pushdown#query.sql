SELECT spend AS spend 
FROM (SELECT spend AS spend 
FROM (SELECT sum(spend) AS spend 
FROM (SELECT spend 
FROM (SELECT spend 
FROM (SELECT sum(coalesce(q0.spend,0)+coalesce(q1.spend,0)) as spend 
FROM (SELECT twitter_column_mock, __data_source 
FROM twitter_table_mock) AS q0 FULL OUTER JOIN (SELECT facebook_column_mock, __data_source 
FROM facebook_table_mock) AS q1 ON q0.__data_source = q1.__data_source)))))
 LIMIT 100