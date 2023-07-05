import contextlib
import logging
import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from airflow.decorators import dag

import pandas as pd
import pendulum
import vertica_python

vertica_conn_info = {'host': Variable.get('vertica_host'), 
        'port': Variable.get('vertica_port'),
        'user': Variable.get('vertica_user'),       
        'password': Variable.get('vertica_pass'),
        'database': Variable.get('vertica_db'),
        'autocommit': True}

def load_dataset_to_vertica(
    schema_from: str,
    schema_to: str,
    table_trn: str,
    table_cur: str,
    datamart: str,
):
    log = logging.getLogger('airflow.task')
    vertica_conn = vertica_python.connect(
        **vertica_conn_info
    )

    with contextlib.closing(vertica_conn.cursor()) as cur:
        log.info("vertica connected")
        dat = Variable.get("mart_upload_date")
        next_dat = datetime.datetime.strptime(dat, "%Y-%m-%d").date() + datetime.timedelta(days=1)
        Variable.set("mart_upload_date", next_dat.strftime('%Y-%m-%d'))

        sql_file = open("sql_mart.txt", 'r')
        
        insert_expr = f"""INSERT INTO {schema_to}.{datamart} (date_update,
	                    currency_from,
	                    amount_total,
	                    cnt_transactions,
	                    avg_transactions_per_account,
	                    cnt_accounts_make_transactions) 
                        {sql_file.read().format(dat, schema_from, table_trn, table_cur)}"""
        
        cur.execute(insert_expr)
        cur.execute("commit")


    log.info("mart calculated")
    vertica_conn.close()

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

with DAG(
        'stg_to_mart_one_day',
        description='',
        default_args=args,
        start_date=pendulum.parse('2023-06-04'),
        schedule_interval='@daily',
        max_active_runs = 1,
) as dag:
    prepare_mart = PythonOperator(
        task_id='prepare_mart',
        python_callable=load_dataset_to_vertica,
        op_kwargs={
            'schema_from': 'BOBA1997YANDEXRU__STAGING',
            'schema_to': 'BOBA1997YANDEXRU__DWH',
            'table_trn': 'transactions',
            'table_cur': 'currencies',
            'datamart': 'global_metrics',
        }
    )  

    (
    prepare_mart
    )