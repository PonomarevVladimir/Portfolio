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
api_key = Variable.get("api_key_couriers")

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-API-KEY': api_key
}

d_url = Variable.get("couriers_url")

def upload(url, headers, conn):
    pg_hook = PostgresHook(conn)
    log = logging.getLogger('airflow.task')
    offset=int(pg_hook.get_pandas_df(sql="select current_offset from stg.loads_check where load_table = 'apisystem_couriers' order by id desc;")['current_offset'].iloc[[0]])
    log.info(offset)
    df = pd.DataFrame()
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
    pg_hook.insert_rows(table='stg.loads_check', rows = [('apisystem_couriers', offset, 'new load')], target_fields = ['load_table', 'current_offset', 'load_comment'])
    pg_hook.insert_rows(table='stg.apisystem_couriers', rows = df.values.tolist(), target_fields = ['_id', 'name'])

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

business_dt = '{{ ds }}'

with DAG(
        'couriers_get',
        default_args=args,
        description='sprint5_project',
        start_date=datetime.today(),
        schedule_interval='@daily',
        catchup=True,
        max_active_runs = 1,
) as dag:
    couriers_get = PythonOperator(
        task_id = 'get_couriers_from_api',
        python_callable = upload,
        op_kwargs={'url': d_url,
                   'headers': headers,
                   'conn': postgres_conn_id}
    )
    (
        couriers_get   
    )
