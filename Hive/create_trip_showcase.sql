set hive.execution.engine=tez;

create table yellow_taxi.trip_showcase
(
payment_type string,
dt date,
avg_tip_amount double,
total_passenger_count int
)
stored as parquet
location 's3a://a-tagirov/trip_showcase/';
