from airflow import DAG
from datetime import datetime
from airflow.operators.dummy import DummyOperator

with DAG(dag_id="meu_dag",
         start_date=datetime(2022, 6, 23),
         schedule_interval="@hourly",
         catchup=False) as dag:
        
    firstTask = DummyOperator(
        task_id="task1"
    )
        
    secondTask = DummyOperator(
        task_id="task2"
    )
        
    thirdTask = DummyOperator(
        task_id="task2"
    )

    firstTask >> secondTask >> thirdTask