set hive.execution.engine=tez;

insert into yellow_taxi.trip_showcase
select * from yellow_taxi.trip_showcase_view;
