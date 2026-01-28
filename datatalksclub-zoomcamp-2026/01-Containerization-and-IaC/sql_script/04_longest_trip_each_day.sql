SELECT
	lpep_pickup_datetime::date,
	SUM(trip_distance)
FROM
	green_taxi_2025_11
WHERE
	trip_distance <= 100
GROUP BY
	1
ORDER BY
	2 DESC;