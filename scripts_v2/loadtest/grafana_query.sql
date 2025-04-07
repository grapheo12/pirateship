SELECT time_bucket('2.000s',"time") AS "time", 
 percentile_cont(0.50) within group (order by response_time) as "50 percentile"
FROM request
GROUP BY 1
ORDER BY 1, 2