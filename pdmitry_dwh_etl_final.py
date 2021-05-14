from datetime import timedelta, datetime
from random import randint
import json

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'pdmitry'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl',
    default_args=default_args,
    description='DWH ETL tasks for final project',
    max_active_runs=1,
    schedule_interval="0 0 1 1 *",
)

def get_tasks_list(phase, source):
    tasks = []
    for task in source[phase]:
        tasks.append(
            PostgresOperator(
            task_id=f'{phase}_{task}_final',
            dag=dag,
            sql=f' """{source[phase][task]}""" '
            )
        )
    return tasks

with open('sample.json', 'r') as f:
    data = json.load(f)

ods_loaded = DummyOperator(task_id="ods_loaded", dag=dag)

get_tasks_list('clear_ods', data) >>get_tasks_list('fill_ods', data)>> get_tasks_list('fill_hashed', data) >> ods_loaded


all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)


ods_loaded >> get_tasks_list('hub', data) >> all_hubs_loaded


all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)

all_hubs_loaded >> get_tasks_list('link', data) >> all_links_loaded


all_sats_loaded = DummyOperator(task_id="all_sats_loaded", dag=dag)

all_links_loaded >> get_tasks_list('sat', data) >> all_sats_loaded



