set hive.execution.engine=tez;

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
