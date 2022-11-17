from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
}

with DAG(
    'dag_test_raizen',  #nome da dag
    default_args=default_args,
    description='Test Raizen',
    schedule_interval=None,
    start_date=days_ago(2),
    tags=[],
) as dag:
  
#Function
    task_extractor = BashOperator( 
      task_id='extracao', 
      bash_command='python c:/airflow-docker/extractor/main.py', 
      dag=dag,
      )
    task_transformer = BashOperator( 
      task_id='transformacao', 
      bash_command='python c:/airflow-docker/transformer/main.py', 
      dag=dag,
      )
    task_extractor >> task_transformer
