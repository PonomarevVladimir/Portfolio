from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

postgres_conn_id = 'postgresql_de'

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

with DAG(
        'dds_to_cdm',
        default_args=args,
        description='sprint5_project',
        catchup=True,
        start_date=datetime.today(),
        schedule_interval='0 1 1 * *',
        max_active_runs = 1,
) as dag:
    courier_ledger = PostgresOperator(
        task_id='insert_into_dm_courier_ledger',
        postgres_conn_id=postgres_conn_id,
        sql="insert_ledger.sql"
    )
    (
        courier_ledger
    )