drop table if exists public_test.stg_deliveries;

create table public_test.stg_deliveries as (select * from stg.apisystem_deliveries) with no data;

drop table if exists public_test.dds_deliveries;

CREATE TABLE public_test.dds_deliveries (
	id serial NOT NULL,
	order_id text NULL,
	courier_id int NULL,
	time_id int NULL,
	rate int NOT NULL,
	sum numeric(14, 2) NOT NULL,
	tip numeric(14, 2) NOT NULL,
	CONSTRAINT dm_deliveries_order_id_key UNIQUE (order_id),
	CONSTRAINT dm_deliveries_pkey PRIMARY KEY (id),
	CONSTRAINT dm_deliveries_rate_check CHECK (((rate >= 1) AND (rate <= 5))),
	CONSTRAINT dm_deliveries_sum_check CHECK ((sum > 0)),
	CONSTRAINT dm_deliveries_tip_check CHECK ((tip >= 0))
);

insert into public_test.stg_deliveries values (null, null, null, null, null, null, null, null, null), ('6325a795bd84556ffd18552c', '2022-10-18 08:35:17.452', null , '0vcepmfub8u90otke2e2seq', null, null, '1', '1', '1');


with c as (select  order_id, order_ts::timestamp, courier_id, cast(rate as int), cast(sum as numeric(14,2)), cast(tip_sum as numeric(14,2)) from public_test.stg_deliveries where order_id is not null and order_ts is not null and courier_id is not null and rate is not null and sum is not null and tip_sum is not null) 
insert into public_test.dds_deliveries (order_id, courier_id, time_id, rate, sum, tip) 
select c.order_id, d.id, t.id, c.rate, c.sum, c.tip_sum from c
inner join (select id, courier_id from dds.dm_couriers) d on c.courier_id = d.courier_id 
inner join (select id, ts from dds.dm_timestamps dt) t on c.order_ts = t.ts;

select * from public_test.dds_deliveries;