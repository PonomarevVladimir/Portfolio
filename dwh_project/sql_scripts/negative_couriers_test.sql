drop table if exists public_test.dds_timestamps;

CREATE TABLE public_test.dds_couriers (
	id serial NOT NULL,
	courier_id text NOT NULL,
	courier_name text NOT NULL,
	CONSTRAINT dm_couriers_courier_id_key UNIQUE (courier_id),
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id)
);

insert into public_test.stg_couriers values (null, null), (null, 'negative name'), ('negative id', null), ('positive id', 'positive name');

insert into public_test.dds_couriers (courier_id, courier_name) 
select _id, name from public_test.stg_couriers where _id is not null and name is not null;

select * from public_test.dds_couriers;