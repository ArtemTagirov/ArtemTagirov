#!/bin/bash

hive -f create_dim_vendor.sql
hive -f create_flag.sql
hive -f create_payment.sql
hive -f create_rates.sql
hive -f create_trip.sql
hive -f insert_data_to_trip.sql
hive -f create_trip_showcase_view.sql
hive -f create_trip_showcase.sql
hive -f insert_data_to_trip_showcase.sql
