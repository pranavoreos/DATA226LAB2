from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Set paths as Airflow Variables
DBT_PROFILES_DIR = Variable.get("DBT_PROFILES_DIR", "/path/to/profiles.yml")
DBT_PROJECT_DIR = Variable.get("DBT_PROJECT_DIR", "/path/to/dbt/project")

# Define the DAG
dag = DAG(
    'stock_analytics_dbt_pipeline',
    description='DBT Pipeline for Stock Analytics',
    schedule_interval=None,  # No schedule, triggered manually
    start_date=days_ago(1),
    catchup=False
)

# Task to start the pipeline
start = DummyOperator(
    task_id='start',
    dag=dag
)

# DBT Run Task
dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}",
    dag=dag
)

# DBT Test Task
dbt_test = BashOperator(
    task_id="dbt_test",
    bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}",
    dag=dag
)

# DBT Snapshot Task
dbt_snapshot = BashOperator(
    task_id="dbt_snapshot",
    bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}",
    dag=dag
)

# Task to end the pipeline
end = DummyOperator(
    task_id='end',
    dag=dag
)

# Set task dependencies
start >> dbt_run >> dbt_test >> dbt_snapshot >> end
