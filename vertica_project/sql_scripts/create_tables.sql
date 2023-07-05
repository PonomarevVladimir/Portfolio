CREATE TABLE BOBA1997YANDEXRU__STAGING.transactions (
	operation_id varchar(60),
	account_number_from int,
	account_number_to int,
	currency_code int,
	country varchar(30),
	status varchar(30),
	transaction_type varchar(30),
	amount int,
	transaction_dt TIMESTAMP(3)
)
SEGMENTED BY hash(operation_id, trunc(transaction_dt, 'DD')) ALL NODES;

CREATE PROJECTION BOBA1997YANDEXRU__STAGING.transactions_dates (
	operation_id,
	account_number_from,
	account_number_to,
	currency_code,
	country,
	status,
	transaction_type,
	amount,
	transaction_dt,
	trn_date
) AS SELECT *, trunc(transaction_dt, 'DD') as trn_date from BOBA1997YANDEXRU__STAGING.transactions
ORDER BY trn_date
SEGMENTED BY hash(operation_id, trunc(transaction_dt, 'DD')) ALL NODES;

CREATE TABLE BOBA1997YANDEXRU__STAGING.currencies (
	date_update TIMESTAMP(0),
	currency_code int,
	currency_code_with int,
	currency_code_div numeric(5,3)
)
ORDER BY date_update;

CREATE TABLE BOBA1997YANDEXRU__DWH.global_metrics (
	date_update DATE not null,
	currency_from int not null,
	amount_total numeric(15,3) not null,
	cnt_transactions int not null,
	avg_transactions_per_account float not null,
	cnt_accounts_make_transactions int not null,
	CONSTRAINT METRICS_PRIMARY PRIMARY KEY (date_update, currency_from) DISABLED
)
ORDER BY date_update;
