from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import dag
from airflow.models import Variable
from datetime import date
from datetime import datetime
import pendulum


input_path = Variable.get('INPUT_PATH') 

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
        task_id="rec",
        bash_command = f"spark-submit src\\scripts\\rec.py '{date.today()}' '{calculate_number_of_days}' 'overwrite' '{input_path}'",
	    dag = dag
    )

    (
    task
    )
