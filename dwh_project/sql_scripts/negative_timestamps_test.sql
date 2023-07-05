drop table if exists public_test.dds_timestamps;

CREATE TABLE public_test.dds_timestamps (
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

with t as (select distinct order_ts::timestamp as ts from public_test.stg_deliveries where order_ts is not null) 
insert into public_test.dds_timestamps (ts, year, month, day, time, date) 
select t.ts, extract(year from t.ts), extract(month from t.ts), extract(day from t.ts), t.ts::time, t.ts::date from t;

select * from public_test.dds_timestamps;