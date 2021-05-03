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
    USERNAME + '_bad_dwh_etl',
    default_args=default_args,
    description='DWH ETL tasks',
    max_active_runs=1,
    schedule_interval="0 0 1 1 *",
)
clear_ods = PostgresOperator(
    task_id="clear_ods",
    dag=dag,
    sql="""
        DELETE FROM pdmitry.ods_payment WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)

fill_ods = PostgresOperator(
    task_id="fill_ods",
    dag=dag,
    sql="""
        INSERT INTO pdmitry.ods_payment
        SELECT * FROM pdmitry.stg_payment 
        WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)

clear_ods_hashed = PostgresOperator(
    task_id="clear_ods_hashed",
    dag=dag,
    sql="""
        DELETE FROM pdmitry.ods_payment_hashed WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)

fill_ods_hashed = PostgresOperator(
    task_id="fill_ods_hashed",
    dag=dag,
    sql="""
        INSERT INTO pdmitry.ods_payment_hashed
        SELECT *, '{{ execution_date }}'::TIMESTAMP AS LOAD_DATE FROM pdmitry.ods_v_payment 
        WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)

ods_loaded = DummyOperator(task_id="ods_loaded", dag=dag)

clear_ods >> fill_ods >> clear_ods_hashed >> fill_ods_hashed >> ods_loaded

dds_hub_user = PostgresOperator(
    task_id="dds_hub_user",
    dag=dag,
    sql="""
        INSERT INTO pdmitry.dds_hub_user ("user_pk", "user_key", "load_date", "record_source")
        SELECT "user_pk", "user_key", "load_date", "record_source"
        FROM pdmitry.view_hub_user_etl
    """
)

dds_hub_billing_period = PostgresOperator(
    task_id="dds_hub_billing_period",
    dag=dag,
    sql="""
        insert into pdmitry.dds_hub_billing_period (BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE)
        SELECT BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE
        FROM pdmitry.view_hub_billing_period_etl
    """
)

dds_hub_account = PostgresOperator(
    task_id="dds_hub_account",
    dag=dag,
    sql="""
        insert into pdmitry.dds_hub_account (ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE)
        SELECT ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE
        FROM pdmitry.view_hub_account_etl

    """
)

dds_hub_payment = PostgresOperator(
    task_id="dds_hub_payment",
    dag=dag,
    sql="""
        insert into pdmitry.dds_hub_payment (PAYMENT_PK, PAY_DOC_TYPE_KEY, PAY_DOC_NUM_KEY, LOAD_DATE, RECORD_SOURCE)
        SELECT PAYMENT_PK, PAY_DOC_TYPE_KEY, PAY_DOC_NUM_KEY, LOAD_DATE, RECORD_SOURCE
        FROM pdmitry.view_hub_payment_etl

    """
)



all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)


ods_loaded >> dds_hub_user >> dds_hub_billing_period >> dds_hub_account >> dds_hub_payment >> all_hubs_loaded

dds_link_user_account_billing_payment = PostgresOperator(
    task_id="dds_link_user_account_billing_payment",
    dag=dag,
    sql="""
    insert into pdmitry.dds_link_user_account_billing_payment (PAY_PK, USER_PK, ACCOUNT_PK, PAYMENT_PK, BILLING_PERIOD_PK, LOAD_DATE, RECORD_SOURCE)
    SELECT PAY_PK, USER_PK, ACCOUNT_PK, PAYMENT_PK, BILLING_PERIOD_PK, LOAD_DATE, RECORD_SOURCE
    FROM pdmitry.view_link_user_account_billing_payment_etl
    """
)


all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)

all_hubs_loaded >> dds_link_user_account_billing_payment >> all_links_loaded


dds_sat_user = PostgresOperator(
    task_id="dds_sat_user",
    dag=dag,
    sql="""
        insert into pdmitry.dds_sat_user (user_pk, user_hashdif, phone, effective_from, load_date, record_source)
        with source_data as (
        select a.USER_PK, a.USER_HASHDIF, a.phone, a.EFFECTIVE_FROM, a.LOAD_DATE, a.RECORD_SOURCE from pdmitry.ods_payment_hashed as a
        WHERE a.LOAD_DATE = '{{ execution_date }}'::TIMESTAMP
        ),
        update_records as (
        select a.USER_PK, a.USER_HASHDIF, a.phone, a.EFFECTIVE_FROM, a.LOAD_DATE, a.RECORD_SOURCE from pdmitry.dds_sat_user as a
        join source_data as b on a.USER_PK = b.USER_PK AND a.LOAD_DATE <= (SELECT max(LOAD_DATE) from source_data)
        ),
        latest_records as (
         select * from (
                       select c.USER_PK, c.USER_HASHDIF, c.LOAD_DATE,
                       CASE WHEN RANK() OVER (PARTITION BY c.USER_PK ORDER BY c.LOAD_DATE DESC) = 1
                       THEN 'Y' ELSE 'N' END AS latest
                FROM update_records as c
            ) as s
            WHERE latest = 'Y'
            ),
        records_to_insert as (
         select distinct e.USER_PK, e.USER_HASHDIF, e.phone, e.EFFECTIVE_FROM, e.LOAD_DATE, e.RECORD_SOURCE
         from source_data as e
         left join latest_records on latest_records.USER_HASHDIF=e.USER_HASHDIF and latest_records.USER_PK=e.USER_PK
         where latest_records.USER_HASHDIF is null
          )
     select * from records_to_insert
    """
)
        
dds_sat_payment = PostgresOperator(
    task_id="dds_sat_payment",
    dag=dag,
    sql="""
    insert into pdmitry.dds_sat_payment (PAYMENT_pk, PAYMENT_hashdif, pay_date, sum, effective_from, load_date, record_source)
    with source_data as (
    select a.PAYMENT_PK, a.PAYMENT_HASHDIF, a.pay_date,a.sum, a.EFFECTIVE_FROM, a.LOAD_DATE, a.RECORD_SOURCE from pdmitry.ods_payment_hashed as a
    WHERE a.LOAD_DATE = '{{ execution_date }}'::TIMESTAMP
    ),
     update_records as (
        select a.PAYMENT_PK, a.PAYMENT_HASHDIF, a.pay_date, a.sum, a.EFFECTIVE_FROM, a.LOAD_DATE, a.RECORD_SOURCE from pdmitry.dds_sat_payment as a
        join source_data as b on a.PAYMENT_PK = b.PAYMENT_PK AND a.LOAD_DATE <= (SELECT max(LOAD_DATE) from source_data)
     ),
     latest_records as (
         select * from (
                       select c.PAYMENT_PK, c.PAYMENT_HASHDIF, c.LOAD_DATE,
                       CASE WHEN RANK() over (partition by c.PAYMENT_PK order by c.LOAD_DATE DESC) = 1
                       THEN 'Y' ELSE 'N' END AS latest
                FROM update_records as c
            ) as s
            WHERE latest = 'Y'
            ),

     records_to_insert as (
         select distinct e.PAYMENT_PK, e.PAYMENT_HASHDIF, e.pay_date, e.sum, e.EFFECTIVE_FROM, e.LOAD_DATE, e.RECORD_SOURCE
         from source_data as e
         left join latest_records on latest_records.PAYMENT_HASHDIF=e.PAYMENT_HASHDIF and latest_records.PAYMENT_PK=e.PAYMENT_PK
         where latest_records.PAYMENT_HASHDIF is null
          )
     select * from records_to_insert
    """
)   
dds_sat_mdm = PostgresOperator(
    task_id="dds_sat_mdm",
    dag=dag,
    sql="""
    insert into pdmitry.dds_sat_mdm (USER_PK, legal_type, district, registered_at, billing_mode, is_vip, mdm_hashdif, LOAD_DATE, RECORD_SOURCE)
    with source_data as (
    select a.USER_PK, a.legal_type, a.district, a.registered_at, a.billing_mode, a.is_vip, a.mdm_hashdif, a.LOAD_DATE, a.RECORD_SOURCE from pdmitry.ods_mdm_hashed as a
    WHERE a.LOAD_DATE = '{{ execution_date }}'::TIMESTAMP
    ),
     update_records as (
        select a.USER_PK, a.legal_type, a.district, a.registered_at, a.billing_mode, a.is_vip, a.mdm_hashdif, a.LOAD_DATE, a.RECORD_SOURCE from pdmitry.dds_sat_mdm as a
        join source_data as b on a.USER_PK = b.USER_PK AND a.LOAD_DATE <= (SELECT max(LOAD_DATE) from source_data)
     ),
     latest_records as (
         select * from (
                       select c.USER_PK, c.MDM_HASHDIF, c.LOAD_DATE,
                       CASE WHEN RANK() over (partition by c.USER_PK order by c.LOAD_DATE DESC) = 1
                       THEN 'Y' ELSE 'N' END AS latest
                FROM update_records as c
            ) as s
            WHERE latest = 'Y'
            ),
     records_to_insert as (
         select distinct e.USER_PK, e.legal_type, e.district, e.registered_at, e.billing_mode, e.is_vip, e.mdm_hashdif, e.LOAD_DATE, e.RECORD_SOURCE
         from source_data as e
         left join latest_records on latest_records.MDM_HASHDIF=e.MDM_HASHDIF and latest_records.USER_PK=e.USER_PK
         where latest_records.MDM_HASHDIF is null
          )
     select * from records_to_insert
    """
)
all_links_loaded >> dds_sat_user >> dds_sat_payment >> dds_sat_mdm
