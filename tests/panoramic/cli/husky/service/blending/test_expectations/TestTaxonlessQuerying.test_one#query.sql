SELECT __m_generic_spend___generic_impressions_a2e54989d79c801e AS __m_generic_spend___generic_impressions_a2e54989d79c801e, enhanced_cpm AS enhanced_cpm, spend AS spend 
FROM (SELECT ___m_generic_spend___generic_impressions1_6f8b30e101070048 / nullif(___m_generic_spend___generic_impressions2_daf151af22e38fae, 0) AS __m_generic_spend___generic_impressions_a2e54989d79c801e, __enhanced_cpm2 / nullif(__enhanced_cpm3, 0) AS enhanced_cpm, spend AS spend 
FROM (SELECT sum(coalesce(facebook_ads_spend_5811c78c7c741b5a, 0) + coalesce(adwords_spend_a4a9ec96df6a4664, 0)) AS ___m_generic_spend___generic_impressions1_6f8b30e101070048, sum(coalesce(facebook_ads_impressions_0bf2e36fb4e71190, 0) + coalesce(adwords_impressions_c62a8b11347285de, 0)) AS ___m_generic_spend___generic_impressions2_daf151af22e38fae, sum(1000 * CASE WHEN (__enhanced_cpm1 = 'LINK_CLICKS') THEN (coalesce(facebook_ads_spend_5811c78c7c741b5a, 0) + coalesce(adwords_spend_a4a9ec96df6a4664, 0)) * 1.5 ELSE coalesce(facebook_ads_spend_5811c78c7c741b5a, 0) + coalesce(adwords_spend_a4a9ec96df6a4664, 0) END) AS __enhanced_cpm2, sum(coalesce(facebook_ads_impressions_0bf2e36fb4e71190, 0) + coalesce(adwords_impressions_c62a8b11347285de, 0)) AS __enhanced_cpm3, sum(spend) AS spend 
FROM (SELECT objective AS __enhanced_cpm1, generic_impressions, generic_spend 
FROM (SELECT generic_impressions, generic_spend 
FROM (SELECT sum(coalesce(q0.generic_impressions,0)+coalesce(q1.generic_impressions,0)) as generic_impressions, sum(coalesce(q0.generic_spend,0)+coalesce(q1.generic_spend,0)) as generic_spend 
FROM (SELECT adwords_column_mock, __data_source 
FROM adwords_table_mock) AS q0 FULL OUTER JOIN (SELECT facebook_column_mock, __data_source 
FROM facebook_table_mock) AS q1 ON q0.__data_source = q1.__data_source)))))
 LIMIT 100