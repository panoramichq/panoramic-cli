SELECT cpm AS cpm, date AS date, spend AS spend 
FROM (SELECT __cpm1 / nullif(__cpm2, 0) AS cpm, date AS date, spend AS spend 
FROM (SELECT sum(1000 * spend) AS __cpm1, sum(impressions) AS __cpm2, date AS date, sum(spend) AS spend 
FROM (SELECT date, impressions, spend 
FROM (SELECT date, impressions, spend 
FROM (SELECT coalesce(q0.date, q1.date) AS date, sum(coalesce(q0.impressions,0)+coalesce(q1.impressions,0)) as impressions, sum(coalesce(q0.spend,0)+coalesce(q1.spend,0)) as spend 
FROM (SELECT adwords_column_mock, __data_source 
FROM adwords_table_mock) AS q0 FULL OUTER JOIN (SELECT facebook_column_mock, __data_source 
FROM facebook_table_mock) AS q1 ON q0.__data_source = q1.__data_source GROUP BY coalesce(q0.date, q1.date)))) GROUP BY date)) ORDER BY date ASC NULLS LAST, date ASC NULLS LAST
 LIMIT 100