from datetime import timedelta, datetime
from random import randint
import json

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'pdmitry'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2010, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_dwh_etl_final',
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
            task_id='{}_{}_final'.format(phase, task),
            dag=dag,
            sql='{}'.format(source[phase][task])
            )
        )
    return tasks

with open('/root/airflow/dags/pdmitry/etl_final.json', 'r') as f:
    data = json.load(f)

ods_loaded = DummyOperator(task_id="ods_loaded", dag=dag)
ods_cleared = DummyOperator(task_id="ods_cleared", dag=dag)
ods_filled = DummyOperator(task_id="ods_filled", dag=dag)

get_tasks_list('clear_ods', data) >> ods_cleared >> get_tasks_list('fill_ods', data)>> ods_filled >> get_tasks_list('fill_hashed', data) >> ods_loaded


all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)


ods_loaded >> get_tasks_list('hub', data) >> all_hubs_loaded


all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)

all_hubs_loaded >> get_tasks_list('link', data) >> all_links_loaded


all_sats_loaded = DummyOperator(task_id="all_sats_loaded", dag=dag)

all_links_loaded >> get_tasks_list('sat', data) >> all_sats_loaded

payment_report_temp_created = DummyOperator(task_id="payment_report_temp_created", dag=dag)

payment_report_dim_loaded = DummyOperator(task_id="payment_report_dim_loaded", dag=dag)

payment_report_fct_loaded = DummyOperator(task_id="payment_report_fct_loaded", dag=dag)

all_sats_loaded >>  get_tasks_list('payment_report_temp', data) >> payment_report_temp_created >> get_tasks_list('payment_report_dim', data) >> payment_report_dim_loaded >> get_tasks_list('payment_report_fct', data) >> payment_report_fct_loaded >> get_tasks_list('drop_payment_report_temp', data)




