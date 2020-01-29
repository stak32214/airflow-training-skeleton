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
from airflow.contrib.sensor.gcs_sensor import GoogleCloudStorageObjectSensor

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
    object='test/{{ ds_nodash }}.csv',
    dag=dag,
)

sensor