-- Question 1: What is count of records for the 2022 Green Taxi Data??
--SELECT count(*) FROM `de-zoom-camp-411609.green_taxi_2022.green_taxi_2022_external`

-- Question 3: How many records have a fare_amount of 0?
SELECT count(*) FROM `de-zoom-camp-411609.green_taxi_2022.green_taxi_2022_external`
WHERE fare_amount=0;

-- Question 4
CREATE OR REPLACE TABLE `de-zoom-camp-411609.green_taxi_2022.green_taxi_2022_partitoned_clustered`
PARTITION BY pickup_date
CLUSTER BY PUlocationID AS
SELECT DATE(TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64))) AS pickup_date, *
FROM `de-zoom-camp-411609.green_taxi_2022.green_taxi_2022_external`;

-- Question 5: Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)
-- with materialized table
SELECT count(distinct PUlocationID)
FROM (select Date(TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64))) as day, * from `de-zoom-camp-411609.green_taxi_2022.green_taxi_2022`)
where day between '2022-01-06' and '2022-06-30';

-- with partitioned and clustered table
SELECT count(distinct PUlocationID)
FROM `de-zoom-camp-411609.green_taxi_2022.green_taxi_2022_partitoned_clustered`
where pickup_date between '2022-01-06' and '2022-06-30';