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
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# Here we define the Hive Connection

from airflow import models
from airflow.settings import Session
import logging
import json

logging.info('Creating connections, pool and sql path')

session = Session()

def create_new_conn(session, attributes):
    new_conn = models.Connection()
    new_conn.conn_id = attributes.get("conn_id")
    new_conn.conn_type = attributes.get('conn_type')
    new_conn.host = attributes.get('host')
    new_conn.port = attributes.get('port')
    new_conn.schema = attributes.get('schema')
    new_conn.login = attributes.get('login')
    new_conn.set_extra(attributes.get('extra'))
    new_conn.set_password(attributes.get('password'))

    session.add(new_conn)
    session.commit()

create_new_conn(session,
                {"conn_id": "hive_staging",
                 "conn_type": "hive_cli",
                 "host": "localhost",
                 "schema": "default",
                 "port": 10000,
                 "login": "ec2-user",
                 "password": "",
                 "extra": json.dumps(
                    {"hive_cli_params": "",
                     "auth": "none",
                     "use_beeline": "true"})})

new_var = models.Variable()
new_var.key = "hive_sql_path"
new_var.set_val("/usr/local/airflow/hql")
session.add(new_var)
session.commit()

session.close()

# Below we define the DAG itself

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
    task_id='load_table',
    dag=dag)

#top_10_city = BashOperator(
#    task_id='top_10_city',
#    dag=dag,
#    bash_command='java some.jar {{ input_path }} {{ output_path }}')