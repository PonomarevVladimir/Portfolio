DROP TABLE IF EXISTS dds.dm_couriers;

CREATE TABLE dds.dm_couriers (
	id serial NOT NULL,
	courier_id text NOT NULL,
	courier_name text NOT NULL,
	CONSTRAINT dm_couriers_courier_id_key UNIQUE (courier_id),
	CONSTRAINT dm_couriers_pkey PRIMARY KEY (id)
);

DROP TABLE IF EXISTS dds.dm_timestamps;

CREATE TABLE dds.dm_timestamps (
	id serial NOT NULL,
	ts timestamp NOT NULL,
	year smallint NOT NULL,
	month smallint NOT NULL,
	day smallint NOT NULL,
	time time NOT NULL,
	date date NOT NULL,
	CONSTRAINT dm_timestamps_day_check CHECK (((day >= 1) AND (day <= 31))),
	CONSTRAINT dm_timestamps_month_check CHECK (((month >= 1) AND (month <= 12))),
	CONSTRAINT dm_timestamps_pkey PRIMARY KEY (id),
	CONSTRAINT dm_timestamps_ts_key UNIQUE (ts),
	CONSTRAINT dm_timestamps_year_check CHECK (((year >= 2022) AND (year < 2500)))
);

DROP TABLE IF EXISTS dds.dm_deliveries;

CREATE TABLE dds.dm_deliveries (
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

ALTER TABLE dds.dm_deliveries ADD CONSTRAINT dm_deliveries_courier_foreign_key FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id);
ALTER TABLE dds.dm_deliveries ADD CONSTRAINT dm_deliveries_time_foreign_key FOREIGN KEY (time_id) REFERENCES dds.dm_timestamps(id);

