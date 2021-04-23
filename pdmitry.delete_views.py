from datetime import timedelta, datetime
from random import randint

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator

USERNAME = 'pdmitry'

default_args = {
    'owner': USERNAME,
    'start_date': datetime(2013, 1, 1, 0, 0, 0)
}

dag = DAG(
    USERNAME + '_my_views',
    default_args=default_args,
    description='DWH ETL tasks',
    schedule_interval="0 0 1 1 *",
)
ods_year_temp_view_update = PostgresOperator(
 task_id="ods_year_temp_view",
    dag=dag,
    sql="""
   alter view pdmitry.ods_v_payment_2020 owner to pdmitry;
   alter view pdmitry.ods_v_payment_2019 owner to pdmitry;
   alter view pdmitry.ods_v_payment_2018 owner to pdmitry;
   alter view pdmitry.ods_v_payment_2017 owner to pdmitry;
   alter view pdmitry.ods_v_payment_2016 owner to pdmitry;
   alter view pdmitry.ods_v_payment_2015 owner to pdmitry;
   alter view pdmitry.ods_v_payment_2014 owner to pdmitry;
   alter view pdmitry.ods_v_payment_2013 owner to pdmitry;
    """
)
