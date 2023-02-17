set hive.execution.engine=tez;

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

insert into yellow_taxi.rates
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
