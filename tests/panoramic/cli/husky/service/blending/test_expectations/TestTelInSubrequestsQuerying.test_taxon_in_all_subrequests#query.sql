SELECT fb_tw_adwords_impressions_all_optional AS fb_tw_adwords_impressions_all_optional, fb_tw_adwords_spend_all_optional AS fb_tw_adwords_spend_all_optional 
FROM (SELECT fb_tw_adwords_impressions_all_optional AS fb_tw_adwords_impressions_all_optional, fb_tw_adwords_spend_all_optional AS fb_tw_adwords_spend_all_optional 
FROM (SELECT sum(coalesce(facebook_ads_impressions_0bf2e36fb4e71190, 0) + coalesce(adwords_impressions_c62a8b11347285de, 0)) AS fb_tw_adwords_impressions_all_optional, sum(coalesce(facebook_ads_spend_5811c78c7c741b5a, 0) + coalesce(adwords_spend_a4a9ec96df6a4664, 0)) AS fb_tw_adwords_spend_all_optional 
FROM (SELECT adwords_impressions_c62a8b11347285de, facebook_ads_spend_5811c78c7c741b5a 
FROM (SELECT adwords_impressions_c62a8b11347285de, facebook_ads_spend_5811c78c7c741b5a 
FROM (SELECT sum(q0.adwords_impressions_c62a8b11347285de) as adwords_impressions_c62a8b11347285de, sum(q1.facebook_ads_spend_5811c78c7c741b5a) as facebook_ads_spend_5811c78c7c741b5a 
FROM (SELECT adwords_column_mock, __data_source 
FROM adwords_table_mock) AS q0 FULL OUTER JOIN (SELECT facebook_column_mock, __data_source 
FROM facebook_table_mock) AS q1 ON q0.__data_source = q1.__data_source)))))
 LIMIT 100