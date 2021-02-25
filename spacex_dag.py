from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2018, 1, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

rockets = ['all','falcon1', 'falcon9', 'falconheavy']

dag = DAG("spacex", default_args=default_args, schedule_interval="0 0 1 1 *")


for i, r in enumerate(rockets):
        
    t1_+f'{i}' = BashOperator(
        task_id="get_data", 
        bash_command="python3 /root/airflow/dags/spacex/load_launches.py -y {{ execution_date.year }} -o /var/data -r {{ params.rocket }}", 
        params={"rocket": f'{r}'}
        dag=dag
    )

    t2_+f'{i}' = BashOperator(
        task_id="print_data", 
        bash_command="cat /var/data/year={{ execution_date.year }}/rocket={{ params.rocket }}/data.csv", 
        params={"rocket": f'{r}'}, # falcon1/falcon9/falconheavy
        dag=dag
    )

    t1_+f'{i}' >> t2_+f'{i}'
