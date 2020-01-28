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

args = {
    'owner': 'Airflow',
    #'start_date': airflow.utils.dates.days_ago(2),
    'start_date': datetime.datetime(2020, 27, 1),
}

dag = DAG(
    dag_id='exercise_python_dag',
    default_args=args,
    schedule_interval="0 0 * * *",
    dagrun_timeout=datetime.timedelta(minutes=60),
)




def python_method(ds, **context):
    print(context['execution_date'])

print_execution_date = PythonOperator(
    task_id='print_execution_date',
    provide_context=True,
    python_callable=python_method,
    dag=dag)

print_execution_date >> wait_1


wait_1 = BashOperator(
    task_id='wait_1',
    bash_command='sleep 1',
    dag=dag,
)

print_execution_date >> wait_5

wait_5 = BashOperator(
    task_id='wait_5',
    bash_command='sleep 5',
    dag=dag,
)

print_execution_date >> wait_10

wait_10 = BashOperator(
    task_id='wait_10',
    bash_command='sleep 10',
    dag=dag,
)

[wait_1, wait_5, wait_10] >> the_end

the_end = DummyOperator(
    task_id='the_end',
    dag=dag,
)


