from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.http_sensor import HttpSensor
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
import json
import csv
import requests

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020,6,30),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "mchindasook@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Download forex rates according to the currencies we want to watch
# described in the file forex_currencies.csv
def download_rates():
    with open('/usr/local/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for row in reader:
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get('https://api.exchangeratesapi.io/latest?base=' + base).json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/usr/local/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

# Basic DAG object with the minimum required parameters
# we use the "with" python keyword to ensure that the DAG object is well closed once we are done using it
with DAG(dag_id="forex_data_pipeline",
         schedule_interval="@daily",
         default_args=default_args,
    # to prevent from running past dag runs, catchup = False
         catchup=False) as dag:

    # check if the rates at the link are available using the httpsensor
    is_forex_rates_available = HttpSensor(
        task_id = "is_forex_rates_available",
        method="GET",
        # for conn_id you need to put the link of the connection that you are connecting to
        # we use the name forex_api because we will create a connection in airflow that is called forex_api
        # the connection we refer to here refers to the connections available in http: // localhost:8080 / admin / connection /
        http_conn_id = "forex_api",
        endpoint = "latest",
        # we have to give a lambda function that returns true when you get a response from the http sensor
        # this lambda function basically returns True if the field rates is available/returned in the response.text
        response_check=lambda response: "rates" in response.text,
        # the http sensor should send an http request every 5 seconds
        poke_interval = 5,
        # for at most 20 seconds before it times out
        timeout = 20
    )

    # check if the currencies pair file is available in the directory
    is_forex_currencies_file_available = FileSensor(
        task_id="is_forex_currencies_file_available",
        fs_conn_id="forex_path",
        filepath="forex_currencies.csv",
        poke_interval=5,
        timeout=20
    )

    # use the python operator to call a python function that makes a request from the online API
    request_rates_api = PythonOperator(
        task_id="request_rates_api",
        python_callable=download_rates
    )

    #the bash operator allows you to execute bash commands (eg. ls or pwd)
    # in this DAG we will use the bash operator to save rates into the HDFS
    saving_rates = BashOperator(
        task_id="saving_rates",
        bash_command='''
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
        '''
    )

    # create a table in the Hive DWh
    creating_forex_rates_table = HiveOperator(
        task_id="creating_forex_rates_table",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )


# What is an operator?
# An operator determines what actually gets done! - is a single task in a pipeline
# DAGs basically help ensure that operators run in the correct order
# a list of all operators can be found at https://airflow_apache.org/_api/airflow/operators/index.html


# HTTP Sensor - sends an HTTP request at random intervals to see if the HTTP link is available


# verify that your DAG is clean by exiting the docker CLI and typing in:
# docker logs f577ceeff2d0

# when testing, always test that each task works by using
# docker exec -it f577ceeff2d0 /bin/bash , followed by:
# airflow test forex_data_pipeline is_forex_rates_available 2019-01-01


