from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def firstname():
    print("Sohan")
    return None

def lastname():
    print("adamsmith")
    return None



default_args={
    "owner":"airflow",
    "retries":1,
    "retry_delay":timedelta(minutes=1),
}


with DAG(
    dag_id="testing_2_dag",
    default_args=default_args,
    description="testing second dag tamplate",
    schedule_interval="@daily",
    start_date=datetime(2026,1,1),
    catchup=False,
) as dag:

    task1=PythonOperator(
        task_id="task1",
        python_callable=firstname
    )

    task2=PythonOperator(
        task_id="task2",
        python_callable=lastname
    )


    task1>>task2