use yellow_taxi;

set hive.execution.engine=tez;

create external table ny_taxi
(
vendor_id string,
tpep_pickup_datetime timestamp,
tpep_dropoff_datetime timestamp,
passenger_count int,
trip_distance double,
ratecode_id int,
store_and_fwd_flag string,
pulocation_id int,
dolocation_id int,
payment_type int,
fare_amount double,
extra double,
mta_tax double,
tip_amount double,
tolls_amount double,
improvement_surcharge double,
total_amount double,
congestion_surcharge double
)
row format delimited
fields terminated by ','
lines terminated by '\n'
location 's3a://a-tagirov/ny-taxi'
TBLPROPERTIES ("skip.header.line.count"="1");



create external table yellow_taxi.dim_vendor
(
    id int,
    name string
)
stored as parquet;
-- 1= Creative Mobile Technologies, LLC;
-- 2= VeriFone Inc;

insert into dim_vendor
select 1, 'Creative Mobile Technologies'
union all
select 2, 'VeriFone Inc';



create external table yellow_taxi.rates
(
    id int,
    name string
)
stored as parquet;

-- 1= Standard rate;
-- 2=JFK;
-- 3=Newark;
-- 4=Nassau or Westchester;
-- 5=Negotiated fare;
-- 6=Group ride;

insert into rates
select 1, 'Standard rate'
union all
select 2, 'JFK'
union all
select 3, 'Newark'
union all
select 4, 'Nassau or Westchester'
union all
select 5, 'Negotiated fare'
union all
select 6, 'Group ride';



create external table yellow_taxi.payment
(
    id int,
    name string
)
stored as parquet;

-- 1= Credit card
-- 2= Cash
-- 3= No charge
-- 4= Dispute
-- 5= Unknown
-- 6= Voided trip

insert into yellow_taxi.payment
select 1, 'Credit card'
union all
select 2, 'Cash'
union all
select 3, 'No charge'
union all
select 4, 'Dispute'
union all
select 5, 'Unknown'
union all
select 6, 'Voided trip';



create external table yellow_taxi.flag
(
    id string,
    name string
)
stored as parquet;
-- Y= store and forward trip
-- N= not a store and forward trip

insert into yellow_taxi.flag
select 'Y', 'store and forward trip'
union all
select 'N', 'not a store and forward trip';


create external table yellow_taxi.trip
(
vendor_id string,
tpep_pickup_datetime timestamp,
tpep_dropoff_datetime timestamp,
passenger_count int,
trip_distance double,
ratecode_id int,
store_and_fwd_flag string,
pulocation_id int,
dolocation_id int,
payment_type int,
fare_amount double,
extra double,
mta_tax double,
tip_amount double,
tolls_amount double,
improvement_surcharge double,
total_amount double,
congestion_surcharge double
)
partitioned by (dt date)
stored as parquet
location 's3a://a-tagirov/trip/';


with t as
(
    select *, to_date(tpep_pickup_datetime) as dt
    from ny_taxi
    where to_date(tpep_pickup_datetime) between '2020-01-01' and '2020-12-31'
)
insert into yellow_taxi.trip2 partition(dt) select * from t;

select *, to_date(tpep_pickup_datetime) as dt  from ny_taxi;


insert into yellow_taxi.trip partition(dt) select *, to_date(tpep_pickup_datetime) as dt from ny_taxi nt where to_date(nt.tpep_pickup_datetime) between '2020-01-01' and '2020-12-31';


insert into yellow_taxi.trip_p partition(dt='2020-01-01')
select * from ny_taxi t where to_date(t.tpep_pickup_datetime) = '2020-01-01';


create view yellow_taxi.trip_showcase_view as
(
    select p.name,
    t.dt,
    round(avg(t.tip_amount), 2) as avg_tip_amount,
    sum(t.passenger_count) as total_passenger_count
    from yellow_taxi.trip t join yellow_taxi.payment p on t.payment_type=p.id
    group by p.name, t.dt
);

with cte as (select
    p.name,
    t.dt,
    round(avg(t.tip_amount), 2) as avg_ta,
    sum(t.passenger_count) as total_pc
from yellow_taxi.trip t join yellow_taxi.payment p on t.payment_type_id = p.id
group by p.name, t.dt)


create table yellow_taxi.trip_showcase
(
payment_type string,
dt date,
avg_tip_amount double,
total_passenger_count int
)
stored as parquet
location 's3a://a-tagirov/trip_showcase/';


insert into yellow_taxi.trip_showcase
select * from trip_showcase_view;