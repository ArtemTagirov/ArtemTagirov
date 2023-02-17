set hive.execution.engine=tez;

create external table yellow_taxi.dim_vendor
(
    id int,
    name string
)
stored as parquet;
-- 1= Creative Mobile Technologies, LLC;
-- 2= VeriFone Inc;

insert into yellow_taxi.dim_vendor
select 1, 'Creative Mobile Technologies'
union all
select 2, 'VeriFone Inc';
