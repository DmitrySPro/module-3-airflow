from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataProcHiveOperator

USERNAME = 'pdmitry'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_data_lake_etl',
    default_args=default_args,
    description='Data Lake ETL tasks',
    schedule_interval="0 0 1 1 *",
)

ods_billing = DataProcHiveOperator(
    task_id='ods_billing',
    dag=dag,
    query="""
        insert overwrite table pdmitry.ods_billing partition (year='{{ execution_date.year }}') 
        select user_id, billing_period, service, tariff, cast(sum as FLOAT), cast(created_at as DATE) from pdmitry.stg_billing where year(cast(created_at as DATE)) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_billing_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)
ods_traffic = DataProcHiveOperator(
    task_id='ods_traffic',
    dag=dag,
    query="""
        insert overwrite table pdmitry.ods_traffic partition (year='{{ execution_date.year }}') 
        select user_id, cast(`timestamp` as TIMESTAMP), device_id, device_ip_addr, bytes_sent, bytes_received from pdmitry.stg_traffic where year(cast(`timestamp` as TIMESTAMP)) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_traffic_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)
ods_issue = DataProcHiveOperator(
    task_id='ods_issue',
    dag=dag,
    query="""
        insert overwrite table pdmitry.ods_issue partition (year='{{ execution_date.year }}') 
        select cast(user_id as INT), cast(start_time as TIMESTAMP), cast(end_time as TIMESTAMP), title, description, service from pdmitry.stg_issue where year(cast(start_time as TIMESTAMP)) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_issue_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)
ods_payment = DataProcHiveOperator(
    task_id='ods_payment',
    dag=dag,
    query="""
        insert overwrite table pdmitry.ods_payment partition (year='{{ execution_date.year }}') 
        select user_id, pay_doc_type, cast(pay_doc_num as INT), account, phone, billing_period, cast(pay_date as DATE), cast(sum as FLOAT) from pdmitry.stg_payment where year(cast(pay_date as DATE)) = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_ods_payment_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

dm_traffic = DataProcHiveOperator(
    task_id='dm_traffic',
    dag=dag,
    query="""
        insert overwrite table pdmitry.dm_traffic partition (year='{{ execution_date.year }}') 
        select user_id, MIN(bytes_received) as MIN_bytes, AVG(bytes_received) as AVG_bytes, MAX(bytes_received) as MAX_bytes from pdmitry.ods_traffic where year = {{ execution_date.year }};
    """,
    cluster_name='cluster-dataproc',
    job_name=USERNAME + '_dm_traffic_{{ execution_date.year }}_{{ params.job_suffix }}',
    params={"job_suffix": randint(0, 100000)},
    region='europe-west3',
)

ods_billing>>ods_traffic>>ods_issue>>ods_payment>>dm_traffic
