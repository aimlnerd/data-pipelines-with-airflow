import itertools
import logging
import os
import shutil
from enum import Enum

import srsly
import spacy

from datetime import datetime, timedelta

from airflow.utils.task_group import TaskGroup
from bson import json_util # this is used instead of json because mongodb records are bson
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.utils.dates import days_ago
from azure.storage.blob import BlobServiceClient, BlobClient

args = {
    'owner': 'deepak',
    'start_date': days_ago(2),
}


class LineOfBusiness(str, Enum):
    marine = "marine"
    property = "property"


class CountryCode(str, Enum):
    belgium = "be"
    netherlands = "nl"


MONGO_DB_NAME = 'db'
USER_MONGO_COLLECTION = 'users'
PROPERTY_BE_MONGO_COLLECTION = 'emails'
COUNTRY_CODE = CountryCode.belgium
LINE_OF_BUSINESS = LineOfBusiness.property

nlp = spacy.load('nl_core_news_sm',
                 exclude=["tagger", "parser", "ner",
                          "attribute_ruler", "lemmatizer",
                          "morphologizer", "senter"])


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


def get_underwriter_users_emails(mongo_conn, mongo_db, user_mongo_collection, country_code, line_of_business, env):
    mongo_user_collection = mongo_conn.get_collection(mongo_collection=user_mongo_collection,
                                                      mongo_db=mongo_db)
    if env == "uat":
        uw_user = mongo_user_collection.find({'role': 'underwriter',
                                              'country_code': country_code,
                                              'line_of_business': line_of_business})

        users_list = [x['email'] for x in uw_user]
    else:
        users_list = ["aravindan.anna_mohanram@allianz.be"]
    return json_util.dumps(users_list)


def get_underwriter_users_emails_op(dag, environment, mongo_conn, mongo_db, user_mongo_collection, country_code,
                                    line_of_business):
    return PythonOperator(
        task_id=f'get_underwriter_users_emails_task_{environment}',
        python_callable=get_underwriter_users_emails,
        op_kwargs={
            "mongo_conn": mongo_conn,
            "user_mongo_collection": user_mongo_collection,
            "country_code": country_code,
            "line_of_business": line_of_business,
            "mongo_db": mongo_db,
            "env": environment
        },
        dag=dag)


def get_emails(mongo_conn, users_email_list: list, mongo_db, property_mongo_collection):
    current_date = datetime.now()
    starting_date = current_date - timedelta(days=1)
    mongo_property_collection = mongo_conn.get_collection(mongo_collection=property_mongo_collection,
                                                          mongo_db=mongo_db)
    query_data_extraction = {'versioned_data.processed': True, 'versioned_data.from_email': {'$in': users_email_list},
                             'change_date': {"$lt": current_date, '$gte': starting_date}}
    emails = mongo_property_collection.find(query_data_extraction)
    return emails


def get_uw_annotations(mongo_conn, environment, task_instance, mongo_db, property_mongo_collection):#task_instance is passed automatically by airflow
    users_email_list_dump = task_instance.xcom_pull(# this takes the latest output of task ids from the last run of pipeline and return it. It doesn not call the task again.This is how data is passed between tasks
        task_ids=[
            f'initialization.initialization_mongo_annotation.get_underwriter_users_emails_task_{environment}'])[0]
    users_email_list = json_util.loads(users_email_list_dump)
    emails = get_emails(mongo_conn=mongo_conn, users_email_list=users_email_list,
                        property_mongo_collection=property_mongo_collection,
                        mongo_db=mongo_db
                        )
    annotations = []
    for email in list(emails):
        email_dict = {'user': email['versioned_data']['from_email'], 'id': email['_id']}
        body_text = email['versioned_data']['text']
        body_annot = [(body_text[annotation['start']:annotation['end']].strip(), annotation['label']) for annotation in
                      email['versioned_data']['annotations'] if
                      annotation["source"] == "user"]
        attachment_annot = [(attachment['text'][annotation['start']:annotation['end']].strip(), annotation['label']) for
                            attachment in email['versioned_data']['attachments'] for annotation in
                            attachment['annotations'] if annotation['source'] == "user"]
        email_dict['body_annotations'] = body_annot
        email_dict['attachment_annotation'] = attachment_annot
        annotations.append(email_dict)
    return json_util.dumps(annotations)


def get_uw_annotations_op(dag, environment, mongo_conn, mongo_db, property_mongo_collection):
    return PythonOperator(
        task_id=f'get_uw_annotations_task_{environment}',
        python_callable=get_uw_annotations,
        op_kwargs={
            "mongo_conn": mongo_conn,
            "environment": environment,
            "property_mongo_collection": property_mongo_collection,
            "mongo_db": mongo_db
        },
        dag=dag)


def get_broker_policyholder_list_from_json(environment, task_instance):
    file_content_dump = task_instance.xcom_pull(
        task_ids=[
            f'init_temp_file_and_copy_content_{environment}'])[0]

    rules = json_util.loads(file_content_dump)
    old_policyholders = []
    old_brokers = []
    for rule in rules:
        entity_name = " ".join([pattern['LOWER'] for pattern in rule['pattern']]).lower().strip()
        if rule['label'] == 'POLICYHOLDER':
            old_policyholders.append(entity_name)
        else:
            old_brokers.append(entity_name)
    return json_util.dumps(dict(policyholder=old_policyholders,
                                broker=old_brokers))


def get_broker_policyholder_list_from_json_op(dag, environment):
    return PythonOperator(
        task_id=f'get_broker_policyholder_list_from_json_task_{environment}',
        python_callable=get_broker_policyholder_list_from_json,
        op_kwargs={
            "environment": environment,
        },
        dag=dag)


def get_entities_from_email(name_entity, environment, task_instance):
    annotations_dump = task_instance.xcom_pull(
        task_ids=[
            f'initialization.initialization_mongo_annotation.get_uw_annotations_task_{environment}'])[0]
    annotations = json_util.loads(annotations_dump)
    entities = []
    for annotation in annotations:
        entities.extend([annot[0] for annot in annotation['body_annotations'] + annotation['attachment_annotation'] if
                         annot[1] == name_entity])
    return json_util.dumps([entity.lower() for entity in set(entities)])


def get_entities_from_email_op(dag, environment, name_entity):
    return PythonOperator(
        task_id=f'get_{name_entity}_from_email_{environment}',
        python_callable=get_entities_from_email,
        op_kwargs={
            "name_entity": name_entity,
            "environment": environment
        },
        dag=dag)


def get_added_entities(name_entity: str, environment, task_instance):
    tasks_dump = task_instance.xcom_pull(
        task_ids=[
            f'annotations.{name_entity}_annotations.get_{name_entity}_from_email_{environment}',
            f'initialization.get_broker_policyholder_list_from_json_task_{environment}'])

    new_entities = json_util.loads(tasks_dump[0])
    old_entities = json_util.loads(tasks_dump[1])
    old_entities_by_entity = old_entities.get(name_entity.lower())

    new_entities, old_entities = set(new_entities), set(old_entities_by_entity)
    added_entities = new_entities - (old_entities & new_entities)
    return json_util.dumps(list(added_entities))


def get_added_entities_op(dag, environment, name_entity):
    return PythonOperator(
        task_id=f'get_{name_entity}_added_{environment}',
        python_callable=get_added_entities,
        op_kwargs={
            "name_entity": name_entity,
            "environment": environment
        },
        dag=dag)


def create_rules(name_entity, environment, task_instance):
    added_entities_dump = task_instance.xcom_pull(
        task_ids=[
            f'annotations.{name_entity}_annotations.get_{name_entity}_added_{environment}'])
    entities = json_util.loads(added_entities_dump[0])
    list_pattern = []
    for entity in entities:
        entity = [e.text for e in nlp(entity)]
        pattern = '['
        for e in entity[:-1]:
            pattern += str({"LOWER": e.lower()}) + ","
        pattern += str({"LOWER": entity[-1].lower()}) + "]"
        list_pattern.append(pattern)
    return json_util.dumps([
        eval("{" + f""""label": "{name_entity}","pattern":{line},"id":"underwriter_rules" """ + "}")
        for line in list_pattern])


def create_rules_op(dag, environment, name_entity):
    return PythonOperator(
        task_id=f'create_{name_entity}_rules_{environment}',
        python_callable=create_rules,
        op_kwargs={
            "name_entity": name_entity,
            "environment": environment
        },
        dag=dag)


def add_roles_to_file(blob_client: BlobClient, local_temp_file_path, base_local_temp_dir, environment, entities,
                      task_instance):
    tasks_dump = task_instance.xcom_pull(
        task_ids=[
            f'annotations.{name_entity}_annotations.create_{name_entity}_rules_{environment}' for name_entity in
            entities])
    rules = list(itertools.chain.from_iterable([json_util.loads(rule_dump) for rule_dump in tasks_dump]))

    if not os.path.exists(base_local_temp_dir):
        os.makedirs(base_local_temp_dir, mode=0o777)
    os.chmod(base_local_temp_dir, mode=0o777)

    with open(local_temp_file_path, 'wb') as f:
        b = blob_client.download_blob()
        b.readinto(f)

    srsly.write_jsonl(append=True, path=local_temp_file_path, lines=rules)
    with open(local_temp_file_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    shutil.rmtree(base_local_temp_dir)


def add_roles_to_file_op(dag, environment, blob_client: BlobClient, local_temp_file_path, base_local_temp_dir,
                         entities):
    return PythonOperator(
        task_id=f'add_roles_to_file_task_{environment}',
        python_callable=add_roles_to_file,
        op_kwargs={
            "blob_client": blob_client,
            "local_temp_file_path": local_temp_file_path,
            "entities": entities,
            "base_local_temp_dir": base_local_temp_dir,
            "environment": environment
        },
        dag=dag)


def generate_dag(mongo_connection_id, mongo_db, property_mongo_collection,
                 user_mongo_collection, country_code, line_of_business,
                 environment='dev', scheduling_expression="@daily", tags=['property']
                 ):
    dag = DAG('user_update_model_{}'.format(environment), schedule_interval=scheduling_expression,
              default_args=args, tags=tags, start_date=days_ago(0.5), catchup=False)

    mongo_conn = MongoHook(conn_id=mongo_connection_id)
    storage_hook = WasbHook(wasb_conn_id=AZURE_BS_CONN_ID)
    bs_connection_string = storage_hook.get_connection(conn_id=storage_hook.conn_id).extra_dejson.get(
        "connection_string")
    blob_client = BlobServiceClient.from_connection_string(bs_connection_string)
    container_client = blob_client.get_container_client(container='base')

    file_name = 'patterns.jsonl'
    base_local_temp_dir = f'temp_{datetime.now().strftime("%Y_%m_%d_%H-%M-%S")}/'
    blob_folder_path = f'ml_models/entity_ruler/'

    blob_file_path = os.path.join(blob_folder_path, file_name)
    local_file_path = os.path.join(base_local_temp_dir, file_name)

    blob_client = container_client.get_blob_client(blob=blob_file_path)

    entities = ['BROKER', 'POLICYHOLDER']

    start_task = PythonOperator(
        task_id='start_task_{}'.format(environment),
        op_kwargs={'msg': 'Workflow started...'},
        python_callable=log,
        dag=dag
    )
    init_temp_file_and_copy_content_task = init_temp_file_and_copy_content_op(dag=dag,
                                                                              environment=environment,
                                                                              base_local_temp_dir=base_local_temp_dir,
                                                                              local_temp_file_path=local_file_path,
                                                                              blob_client=blob_client
                                                                              )
    with TaskGroup(group_id='initialization',
                   dag=dag) as initialization_task_group:
        with TaskGroup(group_id='initialization_mongo_annotation',
                       dag=dag):
            get_underwriter_users_emails_task = get_underwriter_users_emails_op(dag=dag,
                                                                                environment=environment,
                                                                                mongo_conn=mongo_conn,
                                                                                mongo_db=mongo_db,
                                                                                user_mongo_collection=user_mongo_collection,
                                                                                line_of_business=line_of_business,
                                                                                country_code=country_code)
            get_uw_annotations_task = get_uw_annotations_op(dag=dag,
                                                            environment=environment,
                                                            mongo_conn=mongo_conn,
                                                            mongo_db=mongo_db,
                                                            property_mongo_collection=property_mongo_collection
                                                            )

            get_underwriter_users_emails_task >> get_uw_annotations_task ## >> a parallel block

        get_broker_policyholder_list_from_json_op(dag=dag, #group_id='initialization_mongo_annotation' & this is op is at the same level hence they will be parallelly executed
                                                  environment=environment)

    with TaskGroup(group_id='annotations',
                   dag=dag) as annotations_task_group:
        for name_entity in entities:
            with TaskGroup(group_id=f'{name_entity}_annotations',
                           dag=dag):
                get_entities_from_email_task = get_entities_from_email_op(dag=dag,
                                                                          environment=environment,
                                                                          name_entity=name_entity)
                get_added_entities_task = get_added_entities_op(dag=dag,
                                                                environment=environment,
                                                                name_entity=name_entity)

                create_rules_task = create_rules_op(dag=dag,
                                                    environment=environment,
                                                    name_entity=name_entity)
                get_entities_from_email_task >> get_added_entities_task >> create_rules_task

    add_roles_to_file_task = add_roles_to_file_op(dag=dag,
                                                  environment=environment,
                                                  blob_client=blob_client,
                                                  local_temp_file_path=local_file_path,
                                                  base_local_temp_dir=base_local_temp_dir,
                                                  entities=entities)

    end_task = PythonOperator(
        task_id='end_task_{}'.format(environment),
        op_kwargs={'msg': 'Workflow ended...'},
        python_callable=log,
        dag=dag
    )

    start_task >> init_temp_file_and_copy_content_task \
               >> initialization_task_group \
               >> annotations_task_group \
               >> add_roles_to_file_task >> end_task
    return dag


AZURE_BS_CONN_ID = 'azure_storage' #connection id defined in the airflow UI. install necessary providers
DEV_MONGO_CONN_ID = 'mongo_dev'
UAT_MONGO_CONN_ID = 'mongo_uat'

uat_dag = generate_dag(mongo_connection_id=UAT_MONGO_CONN_ID,
                       mongo_db=MONGO_DB_NAME,
                       property_mongo_collection=PROPERTY_BE_MONGO_COLLECTION,
                       user_mongo_collection=USER_MONGO_COLLECTION,
                       country_code=COUNTRY_CODE,
                       line_of_business=LINE_OF_BUSINESS,
                       environment='uat',
                       tags=['property'])

dev_dag = generate_dag(mongo_connection_id=DEV_MONGO_CONN_ID,
                       mongo_db=MONGO_DB_NAME,
                       property_mongo_collection=PROPERTY_BE_MONGO_COLLECTION,
                       user_mongo_collection=USER_MONGO_COLLECTION,
                       country_code=COUNTRY_CODE,
                       line_of_business=LINE_OF_BUSINESS,
                       environment='dev',
                       tags=['property'],
                       scheduling_expression=timedelta(days=1, hours=1))
