SELECT age_bucket, impressions, objective 
FROM (SELECT q1.age_bucket AS age_bucket, sum(coalesce(q0.impressions,0)+coalesce(q1.impressions,0)) as impressions, q0.objective AS objective 
FROM (SELECT objective, impressions, __data_source 
FROM table1) AS q0 FULL OUTER JOIN (SELECT age, impressions, __data_source 
FROM table2) AS q1 ON q0.__data_source = q1.__data_source GROUP BY q1.age_bucket, q0.objective)