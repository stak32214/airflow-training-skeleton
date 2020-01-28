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
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator


args = {
    'owner': 'Airflow',
    #'start_date': airflow.utils.dates.days_ago(2),
    'start_date': datetime.datetime(2020, 1, 27),
}

dag = DAG(
    dag_id='exercise_branching',
    default_args=args,
    schedule_interval="0 0 * * *",
    dagrun_timeout=datetime.timedelta(minutes=60),
)


def get_weekday(execution_date, **context):
    print(execution_date.strftime("%a"))


def choose_person(execution_date, **context):
    if execution_date.strftime("%a") == 'Mon':
        return 'email_bob'
    elif execution_date.strftime("%a") == 'Tue':
        return 'email_alice'
    else:
        return 'email_joe'



branching = BranchPythonOperator(
    task_id="branching",
    python_callable=choose_person,
    provide_context=True,
    dag=dag
)

get_weekday >> branching

days = ['alice','bob','joe']
for name in names:
    branching >> DummyOperator(task_id=f"email_{name}", dag=dag)


final_task = BashOperator(
    task_id='final_task',
    trigger_rule="one_success",
    bash_command='sleep 1',
    dag=dag,
)


branching >> final_task

