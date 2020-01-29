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
from airflow.operators.postgres_to_gcs import PostgresToGCSOperator

from airflow.hooks.base_hook import BaseHook

args = {
    'owner': 'Airflow',
    #'start_date': airflow.utils.dates.days_ago(2),
    'start_date': datetime.datetime(2020, 1, 27),
}

dag = DAG(
    dag_id='exercise_hooks_dag',
    default_args=args,
    schedule_interval="0 0 * * *",
    dagrun_timeout=datetime.timedelta(minutes=60),
)


get_data = PostgresToGoogleCloudStorageOperator(
    postgres_conn_id="test_connection”,
    bucket = 'test_bucket312312',
    file = "land_registry_price_paid_uk/{{ ds_nodash }}/test_{}.csv"
    sql = "SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
    task_id = 'get_data',
    dag = dag,
)

get_data

