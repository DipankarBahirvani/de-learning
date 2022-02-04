
CREATE OR REPLACE EXTERNAL TABLE `de-bootcamp-dipankar.ny_trips.ny_trips_fhv_2019`
OPTIONS (
  format = 'parquet',
  uris = ['gs://test-buck-dip/raw/output_fhv_tripdata_2019-*.parquet']
);

select count(distinct (dispatching_base_num)) from de-bootcamp-dipankar.ny_trips.ny_trips_fhv_2019;

CREATE OR REPLACE  TABLE `de-bootcamp-dipankar.ny_trips.ny_trips_fhv_2019_internal`
as select * from de-bootcamp-dipankar.ny_trips.ny_trips_fhv_2019;

select count(*) from de-bootcamp-dipankar.ny_trips.ny_trips_fhv_2019_internal where dispatching_base_num in ('B00987', 'B02060', 'B02279') and
 DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31';

CREATE OR REPLACE  TABLE `de-bootcamp-dipankar.ny_trips.ny_trips_fhv_2019_internal_partioned_clustered` PARTITION BY
DATE(dropoff_datetime) CLUSTER BY  dispatching_base_num
as select * from de-bootcamp-dipankar.ny_trips.ny_trips_fhv_2019;

select count(*) from de-bootcamp-dipankar.ny_trips.ny_trips_fhv_2019_internal_partioned_clustered where dispatching_base_num in ('B00987', 'B02060', 'B02279') and
 DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31';