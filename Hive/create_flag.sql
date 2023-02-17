set hive.execution.engine=tez;

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
