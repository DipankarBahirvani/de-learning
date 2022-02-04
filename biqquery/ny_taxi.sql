SELECT * FROM `de-bootcamp-dipankar.ny_trips.yellow_tripdata_all_three_years_external` ORDER BY tpep_pickup_datetime DESC;


## CREATE an internal table
CREATE OR REPLACE TABLE de-bootcamp-dipankar.ny_trips.yellow_tripdata_all_three_years AS
SELECT * FROM de-bootcamp-dipankar.ny_trips.yellow_tripdata_all_three_years_external ;

## Run a vendor id query
SELECT DISTINCT(VendorID)
FROM de-bootcamp-dipankar.ny_trips.yellow_tripdata_all_three_years
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';


## Create a partioned table
## CREATE an internal table
CREATE OR REPLACE TABLE de-bootcamp-dipankar.ny_trips.yellow_tripdata_all_three_years_partioned PARTITION BY
DATE(tpep_pickup_datetime) AS SELECT * FROM de-bootcamp-dipankar.ny_trips.yellow_tripdata_all_three_years_external ;


## Run a vendor id query
SELECT DISTINCT(VendorID)
FROM de-bootcamp-dipankar.ny_trips.yellow_tripdata_all_three_years_partioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

SELECT table_name, partition_id, total_rows
FROM `ny_trips.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_all_three_years_partioned'
ORDER BY total_rows DESC;


##create a partioned and clustered table
CREATE OR REPLACE TABLE de-bootcamp-dipankar.ny_trips.yellow_tripdata_all_three_years_partioned_clustered PARTITION BY
DATE(tpep_pickup_datetime) CLUSTER BY VendorID  AS SELECT * FROM de-bootcamp-dipankar.ny_trips.yellow_tripdata_all_three_years_external ;

SELECT count(*) as trips
FROM  de-bootcamp-dipankar.ny_trips.yellow_tripdata_all_three_years_partioned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;


SELECT count(*) as trips
FROM  de-bootcamp-dipankar.ny_trips.yellow_tripdata_all_three_years_partioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;