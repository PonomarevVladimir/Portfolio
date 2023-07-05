select * from stg.apisystem_couriers limit 10;
select count(1) from stg.apisystem_couriers where _id is null;
select count(1) from stg.apisystem_couriers where name is null;
select _id from stg.apisystem_couriers group by _id having count(1) > 1;

select * from stg.apisystem_deliveries limit 10;
select count(1) from stg.apisystem_deliveries where order_id is null;
select order_id from stg.apisystem_deliveries group by order_id having count(1) > 1;
select count(1) from stg.apisystem_deliveries where order_ts is null;
select count(1) from stg.apisystem_deliveries where courier_id is null;
select count(1) from stg.apisystem_deliveries where rate is null;
select count(1) from stg.apisystem_deliveries where sum is null;
select count(1) from stg.apisystem_deliveries where tip is null;

select courier_id from dds.dm_couriers group by courier_id having count(1) > 1;

select ts from dds.dm_timestamps group by ts having count(1) > 1;

select order_id from dds.dm_deliveries group by order_id having count(1) > 1;
select courier_id, time_id from dds.dm_deliveries group by courier_id, time_id, rate, sum, tip having count(1) > 1;

select courier_id from cdm.dm_courier_ledger group by courier_id, settlement_year, settlement_month having count(1) > 1;