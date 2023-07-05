with a as (select row_number() over (partition by load_table order by current_offset desc), current_offset as co  from stg.loads_check where load_table = 'apisystem_deliveries'),
b as (select a.co from a where a.row_number = 2),
t as (select distinct row_number() over (), order_ts::timestamp as ts from stg.apisystem_deliveries where order_ts is not null) 
insert into dds.dm_timestamps (ts, year, month, day, time, date) 
select t.ts, extract(year from t.ts), extract(month from t.ts), extract(day from t.ts), t.ts::time, t.ts::date from t, b where t.row_number > b.co;

select * from dds.dm_timestamps order by id desc limit 10;