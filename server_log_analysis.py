# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 10:37:17 2019

@author: CA20072599
"""

# Here we read the config.yaml file to get necessary values used to define the DAG
import yaml

with open("config.yaml", 'r') as stream:
    yaml_data = yaml.safe_load(stream)

# Here we define the DAG and its args
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

args = {
    'owner': 'ca20072599',
    'start_date': datetime(2015, 6, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    yaml_data['dag_id'],
    schedule_interval="@once",
    user_defined_macros=dict(
            input_path=yaml_data['input_path'], 
            output_path=yaml_data['output_path']),
    default_args=args)

create_table = HiveOperator(
    hql='hive_task/create_table.hql',
    hive_cli_conn_id=yaml_data['hive_connection'],
    hiveconf_jinja_translate=True,
    task_id='create_table',
    dag=dag)

load_table = HiveOperator(
    hql='hive_task/load_table.hql',
    hive_cli_conn_id=yaml_data['hive_connection'],
    hiveconf_jinja_translate=True,
    task_id='create_table',
    dag=dag)