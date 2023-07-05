DROP TABLE IF EXISTS stg.apisystem_couriers;

CREATE TABLE stg.apisystem_couriers (
	_id textL,
	name text
);

DROP TABLE IF EXISTS stg.apisystem_deliveries;

CREATE TABLE stg.apisystem_deliveries (
	order_id text,
	order_ts text,
	delivery_id text,
	courier_id text,
	address text,
	delivery_ts text,
	rate text,
	sum text,
	tip_sum text
);