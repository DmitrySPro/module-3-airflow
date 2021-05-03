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
    USERNAME + '_DM',
    default_args=default_args,
    description='DM_etl',
    max_active_runs=1,
    schedule_interval="0 0 1 1 *",
)
create_temp_table = PostgresOperator(
    task_id="create_temp_table",
    dag=dag,
    sql="""
    drop table if exists pdmitry.payment_report_temp_{{ execution_date.year }};
    create table pdmitry.payment_report_temp_{{ execution_date.year }} as
    with raw_data as (
        select
            legal_type,
            district,
            extract (year from registered_at) as registration_year,
            is_vip,
            extract (year from to_date(billing_period_key, 'YYYY-MM')) as billing_year,
            billing_period_key,
            sum as billing_sum
        from pdmitry.dds_link_payment_v2 p
        join pdmitry.dds_hub_billing_period_v2 hbp on p.billing_period_pk=hbp.billing_period_pk
        join pdmitry.dds_hub_user_v2 usr on p.user_pk=usr.user_pk
        join pdmitry.dds_sat_link_payment_v2 pmnt on p.pay_pk=pmnt.pay_pk
        join pdmitry.dds_sat_mdm mdm on usr.user_pk=mdm.user_pk
    )
    select billing_year, legal_type, district, registration_year, is_vip, sum(billing_sum)
    from raw_data
    where billing_year = {{ execution_date.year }}
    group by billing_year, legal_type, district, registration_year, is_vip
    order by billing_year, legal_type, district, registration_year, is_vip;
    """
)
