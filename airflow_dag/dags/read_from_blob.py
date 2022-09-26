import itertools
import logging
import os
import shutil
from enum import Enum

import srsly

from datetime import datetime, timedelta

from airflow.utils.task_group import TaskGroup
from bson import json_util # this is used instead of json because mongodb records are bson
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.dates import days_ago
from azure.storage.blob import BlobServiceClient, BlobClient

args = {
    'owner': 'deepak',
    'start_date': days_ago(2),
}


def log(**kwargs):
    logging.info(kwargs['msg'])


def init_temp_file_and_copy_content(base_local_temp_dir, local_temp_file_path, blob_client: BlobClient):
    if not os.path.exists(base_local_temp_dir):
        os.makedirs(base_local_temp_dir, mode=0o777)
    os.chmod(base_local_temp_dir, mode=0o777)

    with open(local_temp_file_path, 'wb') as f:
        b = blob_client.download_blob()
        b.readinto(f)

    content = json_util.dumps(srsly.read_jsonl(local_temp_file_path))

    shutil.rmtree(base_local_temp_dir)
    print(f"PRINTING FOR TESTING{content}")
    return content


def init_temp_file_and_copy_content_op(dag, environment,
                                       base_local_temp_dir, local_temp_file_path,
                                       blob_client):
    return PythonOperator(
        task_id=f'init_temp_file_and_copy_content_{environment}',
        python_callable=init_temp_file_and_copy_content,
        op_kwargs={
            "base_local_temp_dir": base_local_temp_dir,
            "local_temp_file_path": local_temp_file_path,
            "blob_client": blob_client,
        },
        dag=dag)



def update_json(environment, task_instance):#task_instance is passed automatically by airflow
    json_dump = task_instance.xcom_pull(# this takes the latest output of task ids from the last run of pipeline and return it. It doesn not call the task again.This is how data is passed between tasks
        task_ids=[
            f'init_temp_file_and_copy_content_{environment}'])[0]
    json_dump_lst = json_util.loads(json_dump)
    json_dump_lst += ['happy airflowing']
    print(f"PRINTING FOR TESTING{json_dump_lst}")
    return json_util.dumps(json_dump_lst)


def update_json_op(dag, environment):
    return PythonOperator(
        task_id=f'update_json_{environment}',
        python_callable=update_json,
        op_kwargs={"environment": environment  
        },
        dag=dag)


def write_to_blob(blob_client: BlobClient, local_temp_file_path, base_local_temp_dir, environment, task_instance):
    tasks_dump = task_instance.xcom_pull(
        task_ids=[f'update_json_{environment}'])[0]
    
    print(f"PRINTING FOR TESTING{tasks_dump}")
    if not os.path.exists(base_local_temp_dir):
        os.makedirs(base_local_temp_dir, mode=0o777)
    os.chmod(base_local_temp_dir, mode=0o777)

    #with open(local_temp_file_path, 'wb') as f:
    #    b = blob_client.download_blob()
    #    b.readinto(f)

    srsly.write_jsonl(append=True, path=local_temp_file_path, lines=tasks_dump)
    with open(local_temp_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    shutil.rmtree(base_local_temp_dir)


def write_to_blob_op(dag, environment, blob_client: BlobClient, local_temp_file_path, base_local_temp_dir):
    return PythonOperator(
        task_id=f'write_to_blob_{environment}',
        python_callable=write_to_blob,
        op_kwargs={
            "blob_client": blob_client,
            "local_temp_file_path": local_temp_file_path,
            "base_local_temp_dir": base_local_temp_dir,
            "environment": environment
        },
        dag=dag)


def generate_dag(environment='dev', scheduling_expression="@daily", tags=['test']  ):
    dag = DAG('test_model_{}'.format(environment), schedule_interval=scheduling_expression,
              default_args=args, tags=tags, start_date=days_ago(0.5), catchup=False)

    storage_hook = WasbHook(wasb_conn_id=AZURE_BS_CONN_ID)
    bs_connection_string = storage_hook.get_connection(conn_id=storage_hook.conn_id).extra_dejson.get(
        "connection_string")
    blob_client = BlobServiceClient.from_connection_string(bs_connection_string)
    container_client = blob_client.get_container_client(container='base')

    source_file_name = 'train_data.json'
    sink_file_name = 'train_data_updated.json'
    base_local_temp_dir = f'temp_{datetime.now().strftime("%Y_%m_%d_%H-%M-%S")}/'
    blob_folder_path = f'data/train/'

    source_blob_file_path = os.path.join(blob_folder_path, source_file_name)
    source_local_file_path = os.path.join(base_local_temp_dir, source_file_name)

    sink_blob_file_path = os.path.join(blob_folder_path, sink_file_name)
    sink_local_file_path = os.path.join(base_local_temp_dir, sink_file_name)

    source_blob_client = container_client.get_blob_client(blob=source_blob_file_path)
    sink_blob_client = container_client.get_blob_client(blob=sink_blob_file_path)

    start_task = PythonOperator(
        task_id='start_task_{}'.format(environment),
        op_kwargs={'msg': 'Workflow started...'},
        python_callable=log,
        dag=dag
    )
    init_temp_file_and_copy_content_task = init_temp_file_and_copy_content_op(dag=dag,
                                                                              environment=environment,
                                                                              base_local_temp_dir=base_local_temp_dir,
                                                                              local_temp_file_path=source_local_file_path,
                                                                              blob_client=source_blob_client
                                                                              )
    update_json_task = update_json_op(dag=dag, 
                                      environment=environment)
    write_to_blob_task = write_to_blob_op(dag=dag,
                                          environment=environment,
                                          blob_client=sink_blob_client,
                                          local_temp_file_path=sink_local_file_path,
                                          base_local_temp_dir=base_local_temp_dir)
    end_task = PythonOperator(
        task_id='end_task_{}'.format(environment),
        op_kwargs={'msg': 'Workflow ended...'},
        python_callable=log,
        dag=dag
    )

    start_task >> init_temp_file_and_copy_content_task >> update_json_task >> write_to_blob_task >> end_task
    return dag


AZURE_BS_CONN_ID = 'azure_default' #connection id defined in the airflow UI. install necessary providers


uat_dag = generate_dag(environment='uat',
                       tags=['property'])

dev_dag = generate_dag(environment='dev',
                       tags=['property'],
                       scheduling_expression=timedelta(days=1, hours=1))
