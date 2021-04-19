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
    schedule_interval="0 0 1 1 *",
)
ods_year_temp_view_update = PostgresOperator(
 task_id="ods_year_temp_view",
    dag=dag,
    sql="""
    create or replace view pdmitry.ods_v_payment_{{ execution_date.year }} as (
    with derived_columns as (
        select user_id,
               pay_doc_type,
               pay_doc_num,
               account,
               phone,
               billing_period,
               pay_date,
               sum,
               user_id::text              as USER_KEY,
               account::text              as ACCOUNT_KEY,
               pay_doc_type::text as PAY_DOC_TYPE_KEY,
               pay_doc_num::text as PAY_DOC_NUM_KEY,
               billing_period::text       as BILLING_PERIOD_KEY,
               'Payment -Data Lake'::text as RECORD_SOURCE,
               current_timestamp          as LOAD_DATE,
               pay_date                   as EFFECTIVE_FROM
        from pdmitry.ods_payment where EXTRACT(year from pay_date) = {{ execution_date.year }}),

        hashed_columns as (
            select user_id,
                   pay_doc_type,
                   pay_doc_num,
                   account,
                   phone,
                   billing_period,
                   pay_date,
                   sum,
                   USER_KEY,
                   ACCOUNT_KEY,
                   PAY_DOC_TYPE_KEY,
                   PAY_DOC_NUM_KEY,
                   BILLING_PERIOD_KEY,
                   RECORD_SOURCE,
                   LOAD_DATE,
                   EFFECTIVE_FROM,
                   cast((MD5(NULLIF(upper(trim(cast(user_id as varchar))), ''))) as text) as USER_PK,
                   cast((MD5(NULLIF(upper(trim(cast(account as varchar))), ''))) as text) as ACCOUNT_PK,

                   cast(MD5(NULLIF(CONCAT_WS('||',
                                             coalesce(NULLIF(upper(trim(cast(pay_doc_type as varchar))), ''), '^^'),
                                             coalesce(NULLIF(upper(trim(cast(pay_doc_num as varchar))), ''), '^^')),
                                             '^^||^^')) as text) as PAYMENT_PK,

                   cast((MD5(NULLIF(upper(trim(cast(billing_period as varchar))), ''))) as text) as BILLING_PERIOD_PK,

                   cast(MD5(NULLIF(CONCAT_WS('||',
                                             coalesce(NULLIF(upper(trim(cast(pay_doc_num as varchar))), ''), '^^'),
                                             coalesce(NULLIF(upper(trim(cast(account as varchar))), ''), '^^'),
                                             coalesce(NULLIF(upper(trim(cast(billing_period as varchar))), ''), '^^')
                                       ),
                                   '^^||^^||^^')) as text) as PAY_PK,
                   cast(MD5(NULLIF(CONCAT_WS('||',
                                             coalesce(NULLIF(upper(trim(cast(user_id as varchar))), ''), '^^'),
                                             coalesce(NULLIF(upper(trim(cast(account as varchar))), ''), '^^')
                                               ),
                                   '^^||^^')) as text) as USER_ACCOUNT_PK,

                   cast(MD5(CONCAT_WS('||',
                                      COALESCE(NULLIF(upper(trim(cast(phone as varchar))), ''), '^^'))) as text) as USER_HASHDIF,
                   cast(MD5(CONCAT_WS('||',
                                    COALESCE(NULLIF(upper(trim(cast(pay_date as varchar))), ''), '^^'),
                                    COALESCE(NULLIF(upper(trim(cast(sum as varchar))), ''), '^^'),
                                    COALESCE(NULLIF(upper(trim(cast(EFFECTIVE_FROM as varchar))), ''), '^^')
                       )) as text) as PAYMENT_HASHDIF

            from derived_columns)

select * from hashed_columns);;
    """
)
dds_hub_user = PostgresOperator(
    task_id="dds_hub_user",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
        insert into pdmitry.dds_hub_user
        select user_pk, user_key, load_date, record_source from (
            with row_rank_1 as (
            select * from (select USER_PK, USER_KEY, LOAD_DATE, RECORD_SOURCE,
                              row_number() over (partition by USER_PK order by LOAD_DATE asc) as row_number
        from pdmitry.ods_v_payment_{{ execution_date.year }}) as h
        where row_number=1),
        records_to_insert as (select a.USER_PK, a.USER_KEY, a.LOAD_DATE, a.RECORD_SOURCE from row_rank_1 as a
            left join pdmitry.dds_hub_user as d on a.USER_PK = d.USER_PK where d.USER_PK is NULL)
        select * from records_to_insert
        ) as dti;
    """
)

dds_hub_billing_period = PostgresOperator(
    task_id="dds_hub_billing_period",
    dag=dag,
    sql="""
        insert into pdmitry.dds_hub_billing_period select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE from (
        with row_rank_1 as (
        select * from (select BILLING_PERIOD_PK, BILLING_PERIOD_KEY, LOAD_DATE, RECORD_SOURCE,
                              row_number() over (partition by BILLING_PERIOD_PK order by LOAD_DATE asc) as row_number
            from pdmitry.ods_v_payment_{{ execution_date.year }}) as h
        where row_number=1),

        records_to_insert as (select a.BILLING_PERIOD_PK, a.BILLING_PERIOD_KEY, a.LOAD_DATE, a.RECORD_SOURCE from row_rank_1 as a
            left join pdmitry.dds_hub_billing_period as d on a.BILLING_PERIOD_PK = d.BILLING_PERIOD_PK where d.BILLING_PERIOD_PK is NULL)
        select * from records_to_insert
        ) as dti;
    """
)

dds_hub_account = PostgresOperator(
    task_id="dds_hub_account",
    dag=dag,
    sql="""
        insert into pdmitry.dds_hub_account select ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE from (
        with row_rank_1 as (
        select * from (select ACCOUNT_PK, ACCOUNT_KEY, LOAD_DATE, RECORD_SOURCE,
                              row_number() over (partition by ACCOUNT_PK order by LOAD_DATE asc) as row_number
            from pdmitry.ods_v_payment_{{ execution_date.year }}) as h
        where row_number=1),

        records_to_insert as (select a.ACCOUNT_PK, a.ACCOUNT_KEY, a.LOAD_DATE, a.RECORD_SOURCE from row_rank_1 as a
            left join pdmitry.dds_hub_account as d on a.ACCOUNT_PK = d.ACCOUNT_PK where d.ACCOUNT_PK is NULL)
        select * from records_to_insert
        ) as dti;
    """
)

dds_hub_payment = PostgresOperator(
    task_id="dds_hub_payment",
    dag=dag,
    sql="""
        insert into pdmitry.dds_hub_payment select PAYMENT_PK, PAY_DOC_TYPE_KEY,PAY_DOC_NUM_KEY, load_date, record_source from (
        with row_rank_1 as (
        select * from (select PAYMENT_PK, PAY_DOC_TYPE_KEY, PAY_DOC_NUM_KEY, LOAD_DATE, RECORD_SOURCE,
                              row_number() over (partition by PAYMENT_PK order by LOAD_DATE asc) as row_number
        from pdmitry.ods_v_payment_{{ execution_date.year }}) as h
        where row_number=1),

        records_to_insert as (select a.PAYMENT_PK, a.PAY_DOC_TYPE_KEY, a.PAY_DOC_NUM_KEY, a.LOAD_DATE, a.RECORD_SOURCE from row_rank_1 as a
            left join pdmitry.dds_hub_payment as d on a.PAYMENT_PK = d.PAYMENT_PK where d.PAYMENT_PK is NULL)
        select * from records_to_insert
        ) as dti;
    """
)

all_hubs_loaded = DummyOperator(task_id="all_hubs_loaded", dag=dag)

ods_year_temp_view_update >> dds_hub_user >> dds_hub_billing_period >> dds_hub_account >> dds_hub_payment >> all_hubs_loaded

dds_link_user_account = PostgresOperator(
    task_id="dds_link_user_account",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
    insert into pdmitry.dds_link_user_account select USER_ACCOUNT_PK, USER_PK, ACCOUNT_PK, LOAD_DATE, RECORD_SOURCE from (
    select distinct stg.USER_ACCOUNT_PK, stg.USER_PK, stg.ACCOUNT_PK, stg.LOAD_DATE, stg.RECORD_SOURCE
        from pdmitry.ods_v_payment_{{ execution_date.year }} as stg
        left join pdmitry.dds_link_user_account as tgt on stg.USER_ACCOUNT_PK = tgt.USER_ACCOUNT_PK where tgt.USER_ACCOUNT_PK is NULL
    ) as dti;
    """
)
dds_link_account_billing_payment = PostgresOperator(
    task_id="dds_link_account_billing_payment",
    dag=dag,
    # postgres_conn_id="postgres_default",
    sql="""
    insert into pdmitry.dds_link_account_billing_payment select PAY_PK, ACCOUNT_PK, PAYMENT_PK, BILLING_PERIOD_PK, LOAD_DATE, RECORD_SOURCE from (
        select distinct stg.PAY_PK, stg.ACCOUNT_PK, stg.PAYMENT_PK, stg.BILLING_PERIOD_PK, stg.LOAD_DATE, stg.RECORD_SOURCE
        from pdmitry.ods_v_payment_{{ execution_date.year }} as stg
        left join pdmitry.dds_link_account_billing_payment as tgt on stg.PAY_PK = tgt.PAY_PK where tgt.PAY_PK is NULL
     ) as dti;
    """
)
all_links_loaded = DummyOperator(task_id="all_links_loaded", dag=dag)

all_hubs_loaded >> dds_link_user_account >> dds_link_account_billing_payment >> all_links_loaded

dds_sat_user = PostgresOperator(
    task_id="dds_sat_user",
    dag=dag,
    sql="""
insert into pdmitry.dds_sat_user select user_pk, user_hashdif, phone, effective_from, load_date, record_source from (
with source_data as (
    select a.USER_PK, a.USER_HASHDIF, a.phone, a.EFFECTIVE_FROM, a.LOAD_DATE, a.RECORD_SOURCE from pdmitry.ods_v_payment_{{ execution_date.year }} as a
),
     update_records as (
        select a.USER_PK, a.USER_HASHDIF, a.phone, a.EFFECTIVE_FROM, a.LOAD_DATE, a.RECORD_SOURCE from pdmitry.dds_sat_user as a
        join source_data as b on a.USER_PK = b.USER_PK
     ),
     latest_records as (
         select * from (
                       select c.USER_PK, c.USER_HASHDIF, c.LOAD_DATE,
                       rank() over (partition by c.USER_PK order by c.LOAD_DATE DESC) as rank_1
                       from update_records as c) as s where rank_1 = 1),

     records_to_insert as (
         select distinct e.USER_PK, e.USER_HASHDIF, e.phone, e.EFFECTIVE_FROM, e.LOAD_DATE, e.RECORD_SOURCE
         from source_data as e
         left join latest_records on latest_records.USER_HASHDIF=e.USER_HASHDIF
         where latest_records.USER_HASHDIF is null
          )
     select * from records_to_insert) as dti;
    """
)

dds_sat_payment = PostgresOperator(
    task_id="dds_sat_payment",
    dag=dag,
    sql="""
insert into pdmitry.dds_sat_payment select PAYMENT_pk, PAYMENT_hashdif, pay_date, sum, effective_from, load_date, record_source from (
with source_data as (
    select a.PAYMENT_PK, a.PAYMENT_HASHDIF, a.pay_date,a.sum, a.EFFECTIVE_FROM, a.LOAD_DATE, a.RECORD_SOURCE from pdmitry.ods_v_payment_{{ execution_date.year }} as a
),
     update_records as (
        select a.PAYMENT_PK, a.PAYMENT_HASHDIF, a.pay_date, a.sum, a.EFFECTIVE_FROM, a.LOAD_DATE, a.RECORD_SOURCE from pdmitry.dds_sat_payment as a
        join source_data as b on a.PAYMENT_PK = b.PAYMENT_PK
     ),
     latest_records as (
         select * from (
                       select c.PAYMENT_PK, c.PAYMENT_HASHDIF, c.LOAD_DATE,
                       rank() over (partition by c.PAYMENT_PK order by c.LOAD_DATE DESC) as rank_1
                       from update_records as c) as s where rank_1 = 1),

     records_to_insert as (
         select distinct e.PAYMENT_PK, e.PAYMENT_HASHDIF, e.pay_date, e.sum, e.EFFECTIVE_FROM, e.LOAD_DATE, e.RECORD_SOURCE
         from source_data as e
         left join latest_records on latest_records.PAYMENT_HASHDIF=e.PAYMENT_HASHDIF
         where latest_records.PAYMENT_HASHDIF is null
          )
     select * from records_to_insert) as dti;
    """
)

all_links_loaded >> dds_sat_user >> dds_sat_payment
