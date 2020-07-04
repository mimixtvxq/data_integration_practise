from airflow import DAG
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020,06,30),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "mchindasook@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Basic DAG object with the minimum required parameters
# we use the "with" python keyword to ensure that the DAG object is well closed once we are done using it
with DAG(dag_id="forex_data_pipeline",
         schedule_interval="@daily",
         default_args=default_args,
    # to prevent from running past dag runs, catchup = False
         catchup=False) as dag:
    None


#What is an operator?
# An operator determines what actually gets done! - is a single task in a pipeline
# DAGs basically help ensure that operators run in the correct order
# a list of all operators can be found at https://airflow_apache.org/_api/airflow/operators/index.html