SELECT
	lpep_pickup_datetime,
	trip_distance
FROM
	green_taxi_2025_11
WHERE
	(
		lpep_pickup_datetime >= '2025-11-01'
		AND lpep_pickup_datetime < '2025-12-01'
	)
	AND trip_distance <= 1;