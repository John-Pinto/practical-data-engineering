SELECT
	*
FROM
	green_taxi_2025_11
LIMIT
	10;

SELECT
	z."LocationID",
	z."Zone",
	SUM(g.total_amount)
FROM
	green_taxi_2025_11 AS g
	INNER JOIN zone_taxi AS z ON g."PULocationID" = z."LocationID"
WHERE
	g.lpep_pickup_datetime::date = '2025-11-18'
GROUP BY
	1,
	2
ORDER BY
	3 DESC;