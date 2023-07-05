with a as (select row_number() over (partition by load_table order by current_offset desc), current_offset as co  from stg.loads_check where load_table = 'apisystem_deliveries'),
b as (select a.co from a where a.row_number = 2),
c as (select row_number() over (), order_id, order_ts::timestamp, courier_id, cast(rate as int), cast(sum as numeric(14,2)), cast(tip_sum as numeric(14,2)), b.co from stg.apisystem_deliveries, b where order_id is not null and order_ts is not null and courier_id is not null, and rate is not null and sum is not null and cast is not null) 
insert into dds.dm_deliveries (order_id, courier_id, time_id, rate, sum, tip) 
select c.order_id, d.id, t.id, c.rate, c.sum, c.tip_sum from c
inner join (select id, courier_id from dds.dm_couriers) d on c.courier_id = d.courier_id 
inner join (select id, ts from dds.dm_timestamps dt) t on c.order_ts = t.ts
where c.row_number > c.co;

select * from dds.dm_deliveries order by id desc limit 10;