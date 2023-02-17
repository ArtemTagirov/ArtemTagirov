set hive.execution.engine=tez;

create view yellow_taxi.trip_showcase_view as
(
    select p.name,
    t.dt,
    round(avg(t.tip_amount), 2) as avg_tip_amount,
    sum(t.passenger_count) as total_passenger_count
    from yellow_taxi.trip t join yellow_taxi.payment p on t.payment_type=p.id
    group by p.name, t.dt
);
