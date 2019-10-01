# -*- coding: utf-8 -*-
"""
Created on Fri Sep 27 10:37:17 2019

@author: CA20072599
"""

# Here we read the config.yaml file to get necessary values used to define the DAG
import yaml

with open("config.yaml", 'r') as stream:
    yaml_data = yaml.safe_load(stream)
 
yaml_dag_id=yaml_data['dag_id']
yaml_hive_connection=yaml_data['hive_connection']
yaml_input_path=yaml_data['input_path']
yaml_output_path=yaml_data['output_path']

# Here we define the DAG and its args
import airflow
from airflow.operators.hive_operator import HiveOperator
#from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

# Below we define the DAG itself

args = {
    'owner': 'ca20072599',
    'start_date': airflow.utils.dates.days_ago(1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    yaml_dag_id,
    schedule_interval="@once",
    user_defined_macros=dict(
            input_path=yaml_input_path, 
            output_path=yaml_output_path),
    default_args=args)

create_table = HiveOperator(
    hql='hive_task/create_table.hql',
    hive_cli_conn_id=yaml_hive_connection,
    hiveconf_jinja_translate=True,
    task_id='create_table',
    dag=dag)

load_table = HiveOperator(
    hql='hive_task/load_table.hql',
    hive_cli_conn_id=yaml_hive_connection,
    hiveconf_jinja_translate=True,
    task_id='load_table',
    dag=dag)

#top_10_city = BashOperator(
#    task_id='top_10_city',
#    dag=dag,
#    bash_command='java somecity.jar {{ input_path }} {{ output_path }}')

#top_10_country = BashOperator(
#    task_id='top_10_country',
#    dag=dag,
#    bash_command='java somecountry.jar {{ input_path }} {{ output_path }}')

from airflow.hooks.hive_hooks import HiveServer2Hook
from airflow.utils.decorators import apply_defaults
from airflow.utils.operator_helpers import context_to_airflow_vars

class CsvHiveOp(HiveOperator):
    @apply_defaults
    def __init__(
            self, hql,
            hive_cli_conn_id='hive_cli_default',
            schema='default',
            hiveconfs=None,
            hiveconf_jinja_translate=False,
            script_begin_tag=None,
            run_as_owner=False,
            mapred_queue=None,
            mapred_queue_priority=None,
            mapred_job_name=None,
            output_filepath='/output/default.csv'
            *args, **kwargs):

        super(CsvHiveOp, self).__init__(*args, **kwargs)
        self.hql = hql
        self.hive_cli_conn_id = hive_cli_conn_id
        self.schema = schema
        self.hiveconfs = hiveconfs or {}
        self.hiveconf_jinja_translate = hiveconf_jinja_translate
        self.script_begin_tag = script_begin_tag
        self.run_as = None
        if run_as_owner:
            self.run_as = self.dag.owner
        self.mapred_queue = mapred_queue
        self.mapred_queue_priority = mapred_queue_priority
        self.mapred_job_name = mapred_job_name
        self.output_filepath = output_filepath
        
    def get_hook(self):
        return HiveServer2Hook(
            hiveserver2_conn_id=self.hive_cli_conn_id)
        
    def execute(self, context):
        self.log.info('Executing: %s', self.hql)
        self.hook = self.get_hook()
        self.conn = self.hook.get_conn()
        
        # set the mapred_job_name if it's not set with dag, task, execution time info
        if not self.mapred_job_name:
            ti = context['ti']
            self.hook.mapred_job_name = 'Airflow HiveOperator task for {}.{}.{}.{}'\
                .format(ti.hostname.split('.')[0], ti.dag_id, ti.task_id,
                        ti.execution_date.isoformat())

        if self.hiveconf_jinja_translate:
            self.hiveconfs = context_to_airflow_vars(context)
        else:
            self.hiveconfs.update(context_to_airflow_vars(context))

        self.log.info('Passing HiveConf: %s', self.hiveconfs)
        self.conn.to_csv(
            hql=self.hql, 
            csv_filepath=self.output_filepath, 
            schema='default',
            delimiter=',',
            lineterminator='\r\n',
            output_header=True,
            fetch_size=1000,
            hive_conf=None)

top_10_resource = CsvHiveOp(
    hql='hive_task/top_10_resource.hql',
    hive_cli_conn_id=yaml_hive_connection,
    hiveconf_jinja_translate=True,
    task_id='top_10_resource',
    dag=dag,
    output_filepath=yaml_output_path+'/top_10_resource.csv')

top_10_failed_request = CsvHiveOp(
    hql='hive_task/top_10_failed_request.hql',
    hive_cli_conn_id=yaml_hive_connection,
    hiveconf_jinja_translate=True,
    task_id='top_10_failed_request',
    dag=dag,
    output_filepath=yaml_output_path+'/top_10_failed_request.csv')

top_10_forbidden_request = CsvHiveOp(
    hql='hive_task/top_10_forbidden_request.hql',
    hive_cli_conn_id=yaml_hive_connection,
    hiveconf_jinja_translate=True,
    task_id='top_10_forbidden_request',
    dag=dag,
    output_filepath=yaml_output_path+'/top_10_forbidden_request.csv')

top_10_forbidden_host = CsvHiveOp(
    hql='hive_task/top_10_forbidden_host.hql',
    hive_cli_conn_id=yaml_hive_connection,
    hiveconf_jinja_translate=True,
    task_id='top_10_forbidden_host',
    dag=dag,
    output_filepath=yaml_output_path+'/top_10_forbidden_host.csv')

request_by_day_of_week = CsvHiveOp(
    hql='hive_task/request_by_day_of_week.hql',
    hive_cli_conn_id=yaml_hive_connection,
    hiveconf_jinja_translate=True,
    task_id='request_by_day_of_week',
    dag=dag,
    output_filepath=yaml_output_path+'/request_by_day_of_week.csv')

request_by_hour_of_day = CsvHiveOp(
    hql='hive_task/request_by_hour_of_day.hql',
    hive_cli_conn_id=yaml_hive_connection,
    hiveconf_jinja_translate=True,
    task_id='request_by_hour_of_day',
    dag=dag,
    output_filepath=yaml_output_path+'/request_by_hour_of_day.csv')