from airflow import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator
from datetime import datetime

# Configuração básica do DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
}

dag = DAG(
    'orchestrate_notebooks',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Caminho dos notebooks
notebook_path = "C:\Users\giova\OneDrive\Documents\Case Data Engineer - Bees\medallion_architecture"
output_path = "/tmp"

# Task 1: Execute bronze.ipynb
bronze_task = PapermillOperator(
    task_id="run_bronze_notebook",
    input_nb=f"/opt/airflow/notebooks/bronze.ipynb",
    output_nb=f"/opt/airflow/logs/bronze_output.ipynb",
    parameters={},
    dag=dag,
)

# Task 2: Execute silver.ipynb
silver_task = PapermillOperator(
    task_id="run_silver_notebook",
    input_nb=f"/opt/airflow/notebooks/silver.ipynb",
    output_nb=f"/opt/airflow/logs/silver_output.ipynb",
    parameters={},
    dag=dag,
)

# Task 3: Execute gold.ipynb
gold_task = PapermillOperator(
    task_id="run_gold_notebook",
    input_nb=f"/opt/airflow/notebooks/gold.ipynb",
    output_nb=f"/opt/airflow/logs/gold_output.ipynb",
    parameters={},
    dag=dag,
)

# Define execution order
bronze_task >> silver_task >> gold_task