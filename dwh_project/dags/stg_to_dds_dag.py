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
        'stg_to_dds',
        default_args=args,
        description='sprint5_project',
        catchup=True,
        start_date=datetime.today(),
        schedule_interval='30 0 * * *',
        max_active_runs = 1,
) as dag:
    couriers = PostgresOperator(
        task_id='insert_into_dm_couriers',
        postgres_conn_id=postgres_conn_id,
        sql="insert_couriers.sql"
    )
    timestamps = PostgresOperator(
        task_id='insert_into_dm_timestamps',
        postgres_conn_id=postgres_conn_id,
        sql="insert_timestamps.sql"
    )
    deliveries = PostgresOperator(
        task_id='insert_into_dm_deliveries',
        postgres_conn_id=postgres_conn_id,
        sql="insert_deliveries.sql"
    )
    (
        [couriers, timestamps]
        >> deliveries
    )