SELECT comparison_cpm_d3fa58a941286552 AS comparison_cpm_d3fa58a941286552, cpm AS cpm, date AS date, spend AS spend 
FROM (SELECT __comparison_cpm1_2d896dd702f7d8cd / nullif(__comparison_cpm2_5d8099c8e1b3d8ca, 0) AS comparison_cpm_d3fa58a941286552, __cpm1 / nullif(__cpm2, 0) AS cpm, date AS date, spend AS spend 
FROM (SELECT sum(1000 * comparison_spend_5eb761e62a53b4c0) AS __comparison_cpm1_2d896dd702f7d8cd, sum(comparison_impressions_2b66864e9ec66eff) AS __comparison_cpm2_5d8099c8e1b3d8ca, sum(1000 * spend) AS __cpm1, sum(impressions) AS __cpm2, date AS date, sum(spend) AS spend 
FROM (SELECT comparison_dataframe.comparison_impressions_2b66864e9ec66eff AS comparison_impressions_2b66864e9ec66eff, comparison_dataframe.comparison_spend_5eb761e62a53b4c0 AS comparison_spend_5eb761e62a53b4c0, data_dataframe.date AS date, data_dataframe.impressions AS impressions, data_dataframe.objective AS objective, data_dataframe.spend AS spend 
FROM (SELECT date, impressions, objective, spend 
FROM (SELECT date, impressions, objective, spend 
FROM (SELECT coalesce(q0.date, q1.date) AS date, sum(coalesce(q0.impressions,0)+coalesce(q1.impressions,0)) as impressions, coalesce(q0.objective, q1.objective) AS objective, sum(coalesce(q0.spend,0)+coalesce(q1.spend,0)) as spend 
FROM (SELECT adwords_column_mock, __data_source 
FROM adwords_table_mock) AS q0 FULL OUTER JOIN (SELECT facebook_column_mock, __data_source 
FROM facebook_table_mock) AS q1 ON q0.__data_source = q1.__data_source GROUP BY coalesce(q0.date, q1.date), coalesce(q0.objective, q1.objective)))) AS data_dataframe LEFT OUTER JOIN (SELECT impressions AS comparison_impressions_2b66864e9ec66eff, spend AS comparison_spend_5eb761e62a53b4c0, objective AS objective 
FROM (SELECT impressions, objective, spend 
FROM (SELECT sum(coalesce(q0.impressions,0)+coalesce(q1.impressions,0)) as impressions, coalesce(q0.objective, q1.objective) AS objective, sum(coalesce(q0.spend,0)+coalesce(q1.spend,0)) as spend 
FROM (SELECT adwords_comparison_column_mock, __data_source 
FROM adwords_comparison_table_mock) AS q0 FULL OUTER JOIN (SELECT facebook_comparison_column_mock, __data_source 
FROM facebook_comparison_table_mock) AS q1 ON q0.__data_source = q1.__data_source GROUP BY coalesce(q0.objective, q1.objective)))) AS comparison_dataframe ON data_dataframe.objective = comparison_dataframe.objective OR (data_dataframe.objective IS NULL AND comparison_dataframe.objective IS NULL)) GROUP BY date)) ORDER BY date ASC NULLS LAST
 LIMIT 100