from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Definir o DAG
with DAG(
    dag_id="example_dag",
    schedule_interval="0 * * * *",  # Roda todo início de hora
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Definir as tarefas
    task_1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    task_2 = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello, Airflow!'",
    )

    # Dependências
    task_1 >> task_2
