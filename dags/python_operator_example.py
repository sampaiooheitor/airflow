from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Função Python para ser executada pelo PythonOperator
def print_hello():
    print("Hello from PythonOperator!")

def calculate_sum(a, b):
    result = a + b
    print(f"The sum of {a} and {b} is {result}")
    return result

# Definir o DAG
with DAG(
    dag_id="python_operator_example",
    schedule_interval="@daily",  # Executa diariamente
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Primeira tarefa: imprimir mensagem
    task_1 = PythonOperator(
        task_id="say_hello",
        python_callable=print_hello,  # Passa a função para ser executada
    )

    # Segunda tarefa: calcular a soma
    task_2 = PythonOperator(
        task_id="calculate_sum",
        python_callable=calculate_sum,  # Passa a função para ser executada
        op_kwargs={"a": 10, "b": 20},  # Argumentos para a função
    )

    # Dependências
    task_1 >> task_2
