set hive.execution.engine=tez;
set hive.exec.dynamic.partition.mode=nonstrict;


insert into yellow_taxi.trip partition(dt)
select *, to_date(tpep_pickup_datetime) as dt
from yellow_taxi.ny_taxi nt
where to_date(nt.tpep_pickup_datetime) between '2020-01-01' and '2020-12-31';
