SELECT count(*)
FROM yellow_taxi_table
WHERE CAST(yellow_taxi_table.tpep_pickup_datetime AS DATE)= '2021-01-15' ;

SELECT MAX(tip_amount) AS max_tip,
       DATE(yellow_taxi_table.tpep_pickup_datetime)
FROM yellow_taxi_table
GROUP BY DATE(yellow_taxi_table.tpep_pickup_datetime)
ORDER BY max_tip DESC

SELECT avg(a.fare_amount) AS amount,
       concat_ws('//', b."Zone", c."Zone") AS LOCATION
FROM yellow_taxi_table a,
     zones b,
     zones c
WHERE a."PULocationID"=b."LocationID"
  AND a."DOLocationID"=c."LocationID"
GROUP BY concat_ws('//', b."Zone", c."Zone")
ORDER BY amount DESC