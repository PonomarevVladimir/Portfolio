from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag
from airflow.models import Variable
from datetime import date
from datetime import datetime
import pendulum

input_path = Variable.get('INPUT_PATH') # hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/vladimirpo/data/geo/

def calculate_number_of_days():
    DATA_START_DATE =  datetime.strptime(Variable.get('DATA_START_DATE'), "YYYY-mm-dd").date()
    oper_dt = date.today()
    return oper_dt - DATA_START_DATE

with DAG(
        'dag_get_file',
        description='sprint7_project',
        start_date=pendulum.parse('2023-02-06'),
        schedule_interval=None,
        max_active_runs = 1,
) as dag:
    task = BashOperator(
        task_id="zone_stats",
        bash_command = f"spark-submit src\\scripts\\zs.py '{date.today()}' '{calculate_number_of_days}' 'overwrite' '{input_path}'",
	    dag = dag
    )

    (
    task
    )