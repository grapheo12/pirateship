SELECT time_bucket('2.000s',"time") AS "time", 
 percentile_cont(0.50) within group (order by response_time) as "50 percentile"
FROM request
GROUP BY 1
ORDER BY 1, 2

-------


SELECT a.time, a.med + 7 * b.med as "50 percentile", a.med as "gettoken_time", b.med as "store_or_recover"
FROM
(SELECT time_bucket('5.000s',"time") AS "time", name,
 percentile_cont(0.50) within group (order by response_time) as "med"
FROM request
WHERE name = '/gettoken'
GROUP BY 1, name
ORDER BY 1, 2) a
JOIN
(SELECT time_bucket('5.000s',"time") AS "time", name,
 percentile_cont(0.50) within group (order by response_time) as "med"
FROM request
WHERE name LIKE '%/recoversecret'
GROUP BY 1, name
ORDER BY 1, 2) b

ON a.time = b.time;

----

SELECT time_bucket('2.000s',"time") AS "time", 
 avg(response_time)
FROM request 
GROUP BY 1
ORDER BY 1, 2