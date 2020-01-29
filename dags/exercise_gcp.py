# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example DAG demonstrating the usage of the BashOperator."""

#from datetime import timedelta
#from datetime import date
import datetime


import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor

args = {
    'owner': 'Airflow',
    #'start_date': airflow.utils.dates.days_ago(2),
    'start_date': datetime.datetime(2020, 1, 27),
}

dag = DAG(
    dag_id='exercise_gcp_dag',
    default_args=args,
    schedule_interval="0 0 * * *",
    dagrun_timeout=datetime.timedelta(minutes=60),
)

sensor = GoogleCloudStorageObjectSensor(
    task_id='sensor',
    bucket='test_bucket312312',
    timeout=3600,
    object='test/{{ ds_nodash }}.csv',
    dag=dag,
)

copy_to_bq = GoogleCloudStorageToBigQueryOperator(
    task_id = 'copy_to_bq',
    dag=dag,
    bucket='test_bucket312312',
    source_objects=['test/{{ ds_nodash }}.csv'],
    destination_project_dataset_table = 'airflowbolcom-jan2829-2ad52563.test_dataset.test_table',
    skip_leading_rows = 1,
    write_disposition='WRITE_TRUNCATE',
    autodetect=True,

)

execute_query = BigQueryOperator(
    task_id='execute_query',
    dag=dag,
    use_legacy_sql=False,
    destination_dataset_table = 'airflowbolcom-jan2829-2ad52563.test_dataset.test_table_results',
    bql="SELECT date, forecast+1 as new_forecast FROM `airflowbolcom-jan2829-2ad52563.test_dataset.test_table`",
    write_disposition = 'WRITE_TRUNCATE',
)


sensor >> copy_to_bq >> execute_query