drop table if exists stg.loads_check;

create table stg.loads_check(
	id serial primary key,
	load_table varchar not null,
	current_offset bigint not null,
	load_comment varchar not null
);

insert into stg.loads_check(load_table, current_offset, load_comment)
values ('apisystem_couriers', 0, 'initial value'), ('apisystem_deliveries', 0, 'initial value');