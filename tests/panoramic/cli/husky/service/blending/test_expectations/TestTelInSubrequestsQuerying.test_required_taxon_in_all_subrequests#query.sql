SELECT fb_adwords_spend_all_required AS fb_adwords_spend_all_required 
FROM (SELECT fb_adwords_spend_all_required AS fb_adwords_spend_all_required 
FROM (SELECT sum(coalesce(facebook_ads_spend_5811c78c7c741b5a, 0) + coalesce(adwords_spend_a4a9ec96df6a4664, 0)) AS fb_adwords_spend_all_required 
FROM (SELECT adwords_spend_a4a9ec96df6a4664, facebook_ads_spend_5811c78c7c741b5a 
FROM (SELECT adwords_spend_a4a9ec96df6a4664, facebook_ads_spend_5811c78c7c741b5a 
FROM (SELECT sum(q0.adwords_spend_a4a9ec96df6a4664) as adwords_spend_a4a9ec96df6a4664, sum(q1.facebook_ads_spend_5811c78c7c741b5a) as facebook_ads_spend_5811c78c7c741b5a 
FROM (SELECT adwords_column_mock, __data_source 
FROM adwords_table_mock) AS q0 FULL OUTER JOIN (SELECT facebook_column_mock, __data_source 
FROM facebook_table_mock) AS q1 ON q0.__data_source = q1.__data_source)))))
 LIMIT 100