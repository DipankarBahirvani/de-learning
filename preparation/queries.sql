select count(*) from yellow_taxi_table  where CAST(yellow_taxi_table.tpep_pickup_datetime AS DATE)= '2021-01-15' ;

select MAX(tip_amount) as max_tip, DATE(yellow_taxi_table.tpep_pickup_datetime)  from yellow_taxi_table  GROUP BY DATE(yellow_taxi_table.tpep_pickup_datetime) order by max_tip desc

select avg(a.fare_amount) as amount,concat_ws('//',b."Zone",c."Zone") as location from yellow_taxi_table a, zones b, zones c where a."PULocationID"=b."LocationID" and
a."DOLocationID"=c."LocationID" group by concat_ws('//',b."Zone",c."Zone") order by amount desc