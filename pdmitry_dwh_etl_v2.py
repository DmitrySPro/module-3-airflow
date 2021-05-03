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
    USERNAME + '_dwh_etl',
    default_args=default_args,
    description='DWH ETL tasks',
    max_active_runs=1,
    schedule_interval="0 0 1 1 *",
)
clear_ods = PostgresOperator(
    task_id="clear_ods_payment",
    dag=dag,
    sql="""
        DELETE FROM pdmitry.ods_payment WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)
clear_ods_mdm = PostgresOperator(
    task_id="clear_ods_mdm_user",
    dag=dag,
    sql="""
        DELETE FROM pdmitry.ods_mdm_user WHERE EXTRACT(YEAR FROM registered_at::DATE) = {{ execution_date.year }}
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

fill_ods_mdm = PostgresOperator(
    task_id="fill_ods_mdm",
    dag=dag,
    sql="""
        INSERT INTO pdmitry.ods_mdm_user
        SELECT * FROM mdm.user 
        WHERE EXTRACT(YEAR FROM registered_at::DATE) = {{ execution_date.year }}
    """
)


clear_ods_hashed = PostgresOperator(
    task_id="clear_ods_hashed_v2",
    dag=dag,
    sql="""
        DELETE FROM pdmitry.ods_payment_hashed_v2 WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)

clear_ods_mdm_hashed = PostgresOperator(
    task_id="clear_ods_mdm_hashed_v2",
    dag=dag,
    sql="""
        DELETE FROM pdmitry.ods_mdm_hashed_v2 WHERE EXTRACT(YEAR FROM registered_at::DATE) = {{ execution_date.year }}
    """
)


fill_ods_hashed = PostgresOperator(
    task_id="fill_ods_hashed_v2",
    dag=dag,
    sql="""
        INSERT INTO pdmitry.ods_payment_hashed_v2
        SELECT *, '{{ execution_date }}'::TIMESTAMP AS LOAD_DATE FROM pdmitry.ods_v_payment_v2 
        WHERE EXTRACT(YEAR FROM pay_date::DATE) = {{ execution_date.year }}
    """
)
fill_ods_mdm_hashed = PostgresOperator(
    task_id="fill_ods_mdm_hashed_v2",
    dag=dag,
    sql="""
        INSERT INTO pdmitry.ods_mdm_hashed_v2
        SELECT *, '{{ execution_date }}'::TIMESTAMP AS LOAD_DATE FROM pdmitry.mdm_v_payment_v2 
        WHERE EXTRACT(YEAR FROM registered_at::DATE) = {{ execution_date.year }}
    """
)
ods_loaded = DummyOperator(task_id="ods_loaded", dag=dag)

clear_ods >>clear_ods_mdm>> fill_ods >> fill_ods_mdm >> clear_ods_hashed >>clear_ods_mdm_hashed >> fill_ods_hashed >> fill_ods_mdm_hashed>> ods_loaded

dds_hub_user = PostgresOperator(
    task_id="dds_hub_user_v2",
    dag=dag,
    sql="""
        INSERT INTO pdmitry.dds_hub_user_v2 (user_pk, user_key, load_date, record_source)
        SELECT user_pk, user_key, load_date, record_source
        FROM pdmitry.view_hub_user_etl_v2
    """
)

dds_hub_billing_period = PostgresOperator(
    task_id="dds_hub_billing_period_v2",
    dag=dag,
    sql="""
        insert into pdmitry.dds_hub_billing_period_v2 (BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE)
        SELECT BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE
        FROM pdmitry.view_hub_billing_period_etl_v2
    """
)

dds_hub_account = PostgresOperator(
    task_id="dds_hub_account_v2",
    dag=dag,
    sql="""
        insert into pdmitry.dds_hub_account_v2 (ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE)
        SELECT ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE
        FROM pdmitry.view_hub_account_etl_v2
    """
)

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)


ods_loaded >> dds_hub_user >> dds_hub_billing_period >> dds_hub_account >> all_hubs_loaded

dds_link_payment = PostgresOperator(
    task_id="dds_link_payment_v2",
    dag=dag,
    sql="""
    insert into pdmitry.dds_link_payment_v2 (PAY_PK, USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, LOAD_DATE, RECORD_SOURCE)
    SELECT PAY_PK, USER_PK, ACCOUNT_PK, BILLING_PERIOD_PK, LOAD_DATE, RECORD_SOURCE
    FROM pdmitry.view_link_payment_etl_v2
    """
)


all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)

all_hubs_loaded >> dds_link_payment >> all_links_loaded


dds_sat_user_v2 = PostgresOperator(
    task_id="dds_sat_user_v2",
    dag=dag,
    sql="""
        insert into pdmitry.dds_sat_user_v2 (user_pk, user_hashdif, phone, effective_from, load_date, record_source)
        with source_data as (
        select a.USER_PK, a.USER_HASHDIF, a.phone, a.EFFECTIVE_FROM, a.LOAD_DATE, a.RECORD_SOURCE from pdmitry.ods_payment_hashed_v2 as a
        WHERE EXTRACT(YEAR FROM a.pay_date::DATE) = {{ execution_date.year }}
        ),
        update_records as (
        select a.USER_PK, a.USER_HASHDIF, a.phone, a.EFFECTIVE_FROM, a.LOAD_DATE, a.RECORD_SOURCE from pdmitry.dds_sat_user_v2 as a
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
     select * from records_to_insert;

    """
)
        
dds_sat_link_payment_v2 = PostgresOperator(
    task_id="dds_sat_link_payment_v2",
    dag=dag,
    sql="""
    insert into pdmitry.dds_sat_link_payment_v2 (PAY_PK, pay_doc_num, pay_doc_type, PAYMENT_HASHDIF, pay_date, sum, EFFECTIVE_FROM, LOAD_DATE, RECORD_SOURCE)
    with source_data as (
    select a.PAY_PK, a.pay_doc_num, a.pay_doc_type, a.PAYMENT_HASHDIF, a.pay_date, a.sum, a.EFFECTIVE_FROM, a.LOAD_DATE, a.RECORD_SOURCE
    from pdmitry.ods_payment_hashed_v2 as a
    WHERE EXTRACT(YEAR FROM a.pay_date::DATE) = {{ execution_date.year }}
    ),
     update_records as (
        select a.PAY_PK, a.PAYMENT_HASHDIF, a.pay_date, a.sum, a.EFFECTIVE_FROM, a.LOAD_DATE, a.RECORD_SOURCE from pdmitry.dds_sat_link_payment_v2 as a
        join source_data as b on a.PAY_PK = b.PAY_PK AND a.LOAD_DATE <= (SELECT max(LOAD_DATE) from source_data)
     ),
     latest_records as (
         select * from (
                       select c.PAY_PK, c.PAYMENT_HASHDIF, c.LOAD_DATE,
                       CASE WHEN RANK() over (partition by c.PAY_PK order by c.LOAD_DATE DESC) = 1
                       THEN 'Y' ELSE 'N' END AS latest
                FROM update_records as c
            ) as s
            WHERE latest = 'Y'
            ),
     records_to_insert as (
         select distinct e.PAY_PK, e.pay_doc_num, e.pay_doc_type, e.PAYMENT_HASHDIF, e.pay_date, e.sum, e.EFFECTIVE_FROM, e.LOAD_DATE, e.RECORD_SOURCE
         from source_data as e
         left join latest_records on latest_records.PAYMENT_HASHDIF=e.PAYMENT_HASHDIF and latest_records.PAY_PK=e.PAY_PK
         where latest_records.PAYMENT_HASHDIF is null
          )
     select * from records_to_insert;

    """
)   

dds_sat_mdm_v2 = PostgresOperator(
    task_id="dds_sat_mdm_v2",
    dag=dag,
    sql="""
    insert into pdmitry.dds_sat_mdm_v2 (USER_PK, legal_type, district, registered_at, billing_mode, is_vip, mdm_hashdif, effective_from, LOAD_DATE, RECORD_SOURCE)
    with source_data as (
    select a.USER_PK, a.legal_type, a.district, a.registered_at, a.billing_mode, a.is_vip, a.mdm_hashdif, a.effective_from, a.LOAD_DATE, a.RECORD_SOURCE from pdmitry.ods_mdm_hashed_v2 as a
    WHERE EXTRACT(YEAR FROM a.effective_from) = {{ execution_date.year }}
    ),
     update_records as (
        select a.USER_PK, a.legal_type, a.district, a.registered_at, a.billing_mode, a.is_vip, a.mdm_hashdif, a.effective_from, a.LOAD_DATE, a.RECORD_SOURCE from pdmitry.dds_sat_mdm_v2 as a
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
         select distinct e.USER_PK, e.legal_type, e.district, e.registered_at, e.billing_mode, e.is_vip, e.mdm_hashdif, e.effective_from, e.LOAD_DATE, e.RECORD_SOURCE
         from source_data as e
         left join latest_records on latest_records.MDM_HASHDIF=e.MDM_HASHDIF and latest_records.USER_PK=e.USER_PK
         where latest_records.MDM_HASHDIF is null
          )
     select * from records_to_insert;
    """
)   

all_links_loaded >> dds_sat_user_v2 >> dds_sat_link_payment_v2 >> dds_sat_mdm_v2
