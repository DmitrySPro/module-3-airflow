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
        join pdmitry.dds_sat_mdm_v2 mdm on usr.user_pk=mdm.user_pk
    )
    select billing_year, legal_type, district, registration_year, is_vip, sum(billing_sum)
    from raw_data
    where billing_year = {{ execution_date.year }}
    group by billing_year, legal_type, district, registration_year, is_vip
    order by billing_year, legal_type, district, registration_year, is_vip;
    """
)

temp_table_created = DummyOperator(task_id="temp_table_created", dag=dag)

create_temp_table >> temp_table_created

load_dimensions = PostgresOperator(
    task_id="load_dimensions",
    dag=dag,
    sql="""
insert into pdmitry.payment_report_dim_billing_year(billing_year_key)
select distinct billing_year as billing_year_key from pdmitry.payment_report_temp_{{ execution_date.year }}
left join pdmitry.payment_report_dim_billing_year on billing_year_key=billing_year
where billing_year_key is null;

insert into pdmitry.payment_report_dim_district(district_key)
select distinct district as district_key from pdmitry.payment_report_temp_{{ execution_date.year }}
left join pdmitry.payment_report_dim_district on district_key=district
where district_key is null;

insert into pdmitry.payment_report_dim_legal_type(legal_type_key)
select distinct legal_type as legal_type_key from pdmitry.payment_report_temp_{{ execution_date.year }}
left join pdmitry.payment_report_dim_legal_type on legal_type_key=legal_type
where legal_type_key is null;

insert into pdmitry.payment_report_dim_registration_year(registration_year_key)
select distinct registration_year as registration_year_key from pdmitry.payment_report_temp_{{ execution_date.year }}
left join pdmitry.payment_report_dim_registration_year on registration_year_key=registration_year
where registration_year_key is null;
    """
)
dimensions_loaded = DummyOperator(task_id="dimensions_loaded", dag=dag)

temp_table_created >> load_dimensions >> dimensions_loaded

load_facts_table = PostgresOperator(
    task_id="load_facts_table",
    dag=dag,
    sql="""
INSERT INTO pdmitry.payment_report_fct(billing_year_id, legal_type_id, district_id, registration_year_id, is_vip, sum)
select biy.id, lt.id, d.id, ry.id, is_vip, raw.sum
from pdmitry.payment_report_temp_{{ execution_date.year }} raw
join pdmitry.payment_report_dim_billing_year biy on raw.billing_year=biy.billing_year_key
join pdmitry.payment_report_dim_legal_type lt on raw.legal_type=lt.legal_type_key
join pdmitry.payment_report_dim_district d on raw.district=d.district_key
join pdmitry.payment_report_dim_registration_year ry on raw.registration_year=ry.registration_year_key;
    """
)
drop_temp_table = PostgresOperator(
    task_id="drop_temp_table",
    dag=dag,
    sql="""
    drop table if exists pdmitry.payment_report_temp_{{ execution_date.year }};
    """
)

dimensions_loaded >> load_facts_table >> drop_temp_table



