SELECT date AS date, enhanced_spend AS enhanced_spend, gender AS gender, generic_spend AS generic_spend 
FROM (SELECT date AS date, enhanced_spend AS enhanced_spend, gender AS gender, generic_spend AS generic_spend 
FROM (SELECT date AS date, sum(CASE WHEN (__enhanced_spend1 = 'LINK_CLICKS') THEN adwords_spend_a4a9ec96df6a4664 * 1.5 ELSE adwords_spend_a4a9ec96df6a4664 END) AS enhanced_spend, gender AS gender, sum(adwords_spend_a4a9ec96df6a4664) AS generic_spend 
FROM (SELECT objective AS __enhanced_spend1, gender, generic_impressions, generic_spend, objective 
FROM (SELECT gender, generic_impressions, generic_spend, objective 
FROM (SELECT q0.gender AS gender, sum(coalesce(q0.generic_impressions,0)) as generic_impressions, sum(coalesce(q0.generic_spend,0)) as generic_spend, q0.objective AS objective 
FROM (SELECT adwords_column_mock, __data_source 
FROM adwords_table_mock) AS q0 GROUP BY q0.gender, q0.objective))) GROUP BY date, gender))
 LIMIT 100