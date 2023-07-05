from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag
from airflow.models import Variable
from datetime import date
import pendulum

input_path = Variable.get('INPUT_PATH') 

with DAG(
        'dag_zs',
        description='sprint7_project',
        start_date=pendulum.parse('2023-02-06'),
        schedule_interval="@weekly",
        max_active_runs = 1,
) as dag:
    task = BashOperator(
        task_id='prepare_zone_stats',
        bash_command = f"spark-submit scripts\\zs.py '{date.today()}' '37' 'append' '{input_path}'",
        dag = dag
    )

    (
    task
    )
