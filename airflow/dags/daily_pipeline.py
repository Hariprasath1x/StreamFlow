from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

# This is a simple educational DAG demonstrating batch orchestration.
# It simulates a daily process that might run alongside our real-time streaming architecture.

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def validate_data():
    """Dummy function to represent a batch data validation step."""
    print("Validating batch historical data for data quality...")
    return True

with DAG(
    'daily_historical_batch_pipeline',
    default_args=default_args,
    description='A simple batch workflow to demonstrate Airflow orchestration',
    schedule=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['streamflow', 'batch'],
) as dag:

    # Task 1: Generate historical data (calling our existing generator script)
    # Note: In a real system this might be "Extract from Source"
    generate_historical_task = BashOperator(
        task_id='generate_historical_data',
        bash_command='cd /Users/hariprasathc/Projects/StreamFlow && .venv/bin/python producer/generator.py',
    )

    # Task 2: Validate the data using a Python function
    validate_data_task = PythonOperator(
        task_id='validate_historical_data',
        python_callable=validate_data,
    )

    # Task 3: Load into Warehouse (Dummy step for demonstration)
    load_warehouse_task = BashOperator(
        task_id='load_into_postgres',
        bash_command='echo "Loading batch data into PostgreSQL Warehouse..."',
    )

    # Define the dependency workflow
    generate_historical_task >> validate_data_task >> load_warehouse_task
