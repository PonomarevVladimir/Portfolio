import requests
import json
import pandas as pd
import logging

from datetime import datetime
from airflow import DAG
from airflow import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

postgres_conn_id = 'postgresql_de'

nickname = 'Vova'
cohort = '5'
api_key = Variable.get("api_key_deliveries")

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-API-KEY': api_key
}

d_url = Variable.get("deliveries_url")

def upload(url, headers,conn):
    pg_hook = PostgresHook(conn)
    log = logging.getLogger('airflow.task')
    offset=int(pg_hook.get_pandas_df(sql="select current_offset from stg.loads_check where load_table = 'apisystem_deliveries' order by id desc;")['current_offset'].iloc[[0]])
    df = pd.DataFrame()
    log.info(offset)
    while True:
        response= requests.get(url+str(offset), headers=headers)
        temp = response.content
        temp = json.loads(temp)
        df1 = pd.DataFrame(temp)
        if df1.empty:
            break
        df = pd.concat([df,df1])
        offset += df1.shape[0]
        log.info(offset)
    pg_hook.insert_rows(table='stg.loads_check', rows = [('apisystem_deliveries', offset, 'new load')], target_fields = ['load_table', 'current_offset', 'load_comment'])
    pg_hook.insert_rows(table='stg.apisystem_deliveries', rows = df.values.tolist(), target_fields = ['order_id', 'order_ts', 'delivery_id', 'courier_id', 'address', 'delivery_ts', 'rate', 'sum', 'tip_sum'])

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

business_dt = '{{ ds }}'

with DAG(
        'deliveries_get',
        default_args=args,
        description='sprint5_project',
        start_date=datetime.today(),
        schedule_interval='@daily',
        catchup=True,
        max_active_runs = 1,
) as dag:
    deliveries_get = PythonOperator(
        task_id = 'get_deliveries_from_api',
        python_callable = upload,
        op_kwargs={'url': d_url,
                   'headers': headers,
                   'conn': postgres_conn_id}
    )
    (
        deliveries_get   
    )
