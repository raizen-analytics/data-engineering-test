# Dag modules
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from raizen import Raizen
# from raizen import *

default_args = {
    'owner': 'vitor',
    'start_date': days_ago(2),
    'provide_context': True,
}

r = Raizen()

dag = DAG('raizen-etl',
          default_args=default_args,
          description='ANP Brasil data ETL',
          schedule_interval="@once")

t0 = PythonOperator(task_id='import_configs',
                    python_callable=r.import_configs,
                    dag=dag)

t1 = PythonOperator(task_id='fetch_data',
                    python_callable=r.fetch_data,
                    dag=dag)

t2 = PythonOperator(task_id='convert_data',
                    python_callable=r.convert_data,
                    dag=dag)

t3 = PythonOperator(task_id='transform_data',
                    python_callable=r.transform_data,
                    dag=dag)

t4 = PythonOperator(task_id='load_data',
                    python_callable=r.load_data,
                    dag=dag)

t0 >> t1 >> t2 >> t3 >> t4
