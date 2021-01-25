SELECT ad_id, campaign_id, impressions 
FROM (SELECT coalesce(q0.ad_id, q1.ad_id, q2.ad_id) AS ad_id, coalesce(q1.campaign_id, q2.campaign_id) AS campaign_id, sum(coalesce(q0.impressions,0)+coalesce(q1.impressions,0)+coalesce(q2.impressions,0)) as impressions 
FROM (SELECT ad_id, impressions, __data_source 
FROM table1) AS q0 FULL OUTER JOIN (SELECT ad_id, campaign_id, impressions, __data_source 
FROM table2) AS q1 ON q0.__data_source = q1.__data_source FULL OUTER JOIN (SELECT ad_id, campaign_id, impressions, __data_source 
FROM table3) AS q2 ON q0.__data_source = q2.__data_source GROUP BY coalesce(q0.ad_id, q1.ad_id, q2.ad_id), coalesce(q1.campaign_id, q2.campaign_id))