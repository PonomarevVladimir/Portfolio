import contextlib
from typing import Dict, List, Optional
import logging
import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    postgres_conn_id: str,
    schema: str,
    table: str,
    columns: List[str],
    type_override: Optional[Dict[str, str]] = None,
):
    log = logging.getLogger('airflow.task')
    pg_hook = PostgresHook(postgres_conn_id)
    log.info("postgres connected")
    vertica_conn = vertica_python.connect(
        **vertica_conn_info
    )
    log.info("vertica connected")
    columns = ', '.join(columns)
    copy_expr = f"""
    COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY '''' ABORT ON ERROR
    """
    
    dat = Variable.get("upload_date")

    if table == "transactions":
        dt_column = "transaction_dt"
    else:
        dt_column = "date_update"
        next_dat = datetime.datetime.strptime(dat, "%Y-%m-%d").date() + datetime.timedelta(days=1)
        Variable.set("upload_date", next_dat.strftime('%Y-%m-%d'))

    with contextlib.closing(vertica_conn.cursor()) as cur:
        df_len = pd.DataFrame()

        sql_dates = f"""
        SELECT *, 
               date_trunc('day', {dt_column}) as trn_date
          FROM public.{table}
        """

        sql_len = f"""
        with a as ({sql_dates})
        SELECT count(1)
          FROM a 
         WHERE trn_date='{dat}'
        """
        
        df_len = pg_hook.get_pandas_df(sql=sql_len)
        table_len = df_len.iloc[0,0]
        for i in range(0, table_len, 100000):
            log.info(i)
            df = pd.DataFrame()

            sql_df = f"""
            with b as ({sql_dates}),
                 a as (SELECT *, 
                              row_number() over(partition by trn_date) as rn 
                              FROM b) 
            SELECT *
              FROM a 
             WHERE trn_date='{dat}' 
               and rn between {i} and {i}+100000")
            """

            df = pg_hook.get_pandas_df(sql=sql_df)
            df.drop("trn_date", axis = 1, inplace=True)
            df.drop("rn", axis = 1, inplace=True)
            cur.copy(copy_expr, df.to_csv(header = None, index = False))

        
    log.info("table copied")
    vertica_conn.close()


args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

with DAG(
        'dag_to_stg_one_day',
        description='',
        default_args=args,
        start_date=pendulum.parse('2023-06-04'),
        schedule_interval='@daily',
        max_active_runs = 1,
) as dag:
    load_transactions = PythonOperator(
        task_id='load_transactions',
        python_callable=load_dataset_to_vertica,
        op_kwargs={
            'postgres_conn_id': 'postgresql_de',
            'schema': 'BOBA1997YANDEXRU__STAGING',
            'table': 'transactions',
            'columns': ['operation_id',
	                    'account_number_from',
	                    'account_number_to',
	                    'currency_code',
	                    'country',
	                    'status',
	                    'transaction_type',
	                    'amount',
	                    'transaction_dt'],
        }
    ),
    load_currencies = PythonOperator(
        task_id='load_currencies',
        python_callable=load_dataset_to_vertica,
        op_kwargs={
            'postgres_conn_id': 'postgresql_de',
            'schema': 'BOBA1997YANDEXRU__STAGING',
            'table': 'currencies',
            'columns': ['date_update',
	                    'currency_code',
	                    'currency_code_with',
	                    'currency_code_div'],
        }
    )   

    (
    [load_currencies, load_transactions]
    )