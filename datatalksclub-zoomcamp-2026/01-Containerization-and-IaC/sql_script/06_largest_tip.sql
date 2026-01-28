SELECT
	*
FROM
	green_taxi_2025_11
LIMIT
	10;

WITH
	drop_location_tip AS (
		SELECT
			g."DOLocationID",
			MAX(g.tip_amount)
		FROM
			green_taxi_2025_11 AS g
			INNER JOIN zone_taxi AS z ON g."PULocationID" = z."LocationID"
		WHERE
			z."Zone" ILIKE 'East Harlem North'
		GROUP BY
			1
	)
SELECT
	z."Zone",
	max
FROM
	drop_location_tip AS d
	INNER JOIN zone_taxi AS z ON d."DOLocationID" = z."LocationID"
ORDER BY
	max DESC
LIMIT
	1;