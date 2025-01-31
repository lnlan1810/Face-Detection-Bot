import boto3
import ydb
import json
import requests
import os
import re
    

def get_s3_client():
    return boto3.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        region_name = 'ru-central1'
    )


def get_ydb_pool(db_endpoint, db_name):
    driver = ydb.Driver(
        endpoint=db_endpoint,
        database=db_name,
        credentials=ydb.iam.MetadataUrlCredentials(),
    )
    driver.wait(fail_fast=True, timeout=5)
    return ydb.SessionPool(driver)


def get_random_face_photo(pool, table_name):
    def call(session):
        result_set = session.transaction(ydb.SerializableReadWrite()).execute(
            f"SELECT * FROM {table_name} WHERE name IS NULL LIMIT 1;",
            commit_tx=True,
        )
        
        return result_set[0].rows
    return pool.retry_operation_sync(call)


def get_face_photos_by_name(pool, table_name, photo_name):
    def call(session):
        result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
            f"SELECT * FROM {table_name} WHERE name = '{photo_name}';",
            commit_tx=True,
        )
        
        return result_sets[0].rows
    return pool.retry_operation_sync(call)


def get_face_photo_by_tg_object_id(pool, table_name, tg_object_id):
    def call(session):
        result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
            f"SELECT * FROM {table_name} WHERE tg_object_id = '{tg_object_id}';",
            commit_tx=True,
        )
        
        return result_sets[0].rows
    return pool.retry_operation_sync(call)


def update_name_column(pool, table_name, key_id, photo_name):
    def call(session):
        session.transaction(ydb.SerializableReadWrite()).execute(
            f"UPDATE {table_name} SET name = '{photo_name}' WHERE key_id = '{key_id}';",
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
        
    return pool.retry_operation_sync(call)


def update_tg_object_id_column(pool, table_name, key_id, tg_object_id):
    def call(session):
        session.transaction(ydb.SerializableReadWrite()).execute(
            f"UPDATE {table_name} SET tg_object_id = '{tg_object_id}' WHERE key_id = '{key_id}';",
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
        
    return pool.retry_operation_sync(call)


def send_photo(tg_key, chat_id, photo_data):
    print(f'Send photo to chat = {chat_id}')
    response = requests.get(
        url=f'https://api.telegram.org/bot{tg_key}/sendPhoto',
        params={"chat_id": chat_id, "photo": photo_data}
    ).json()
    return response['result']['photo'][0]['file_id']


def send_message(tg_key, chat_id, message_id, text):
    print(f'Send message to chat = {chat_id}')
    requests.get(
        url=f'https://api.telegram.org/bot{tg_key}/sendMessage', 
        params={"chat_id": chat_id, "text": text, "reply_to_message_id": message_id}
    )


def handler(event, context):
    print('Start telegram bot trigger')

    tg_key = os.environ['TG_KEY']
    db_endpoint = os.environ['DB_API_ENDPOINT']
    db_name = os.environ['DB_NAME']
    table_name = os.environ['TABLE_NAME']
    face_storage_endpoint = os.environ['FACES_STORAGE_API_GATEWAY_ENDPOINT']
    ok_response = {'statusCode': 200}
    
    body = json.loads(event['body'])
    message = body['message']
    message_id = message['message_id']
    chat_id = message['chat']['id']

    ydb_pool = get_ydb_pool(db_endpoint, db_name)
    if 'text' in message:
        text = message['text']
        
        name = re.search(r"^\/face\/([a-zа-я0-9]{1,100})$", text)
        if name:
            photo_name = name.group(1)
            rows = get_face_photos_by_name(ydb_pool, table_name, photo_name)
            if rows:
                for row in rows:
                    key_id = row['key_id']
                    send_photo(tg_key, chat_id, face_storage_endpoint + key_id)
            else: 
                send_message(tg_key, chat_id, message_id, f'Фотографии с именем {photo_name} не найдены')
            return ok_response

        if 'reply_to_message' in message:
            reply_to_message = message['reply_to_message']
            if 'photo' in reply_to_message:
                file_id = reply_to_message['photo'][0]['file_id']
                new_name = message['text']
                photos = get_face_photo_by_tg_object_id(ydb_pool, table_name, file_id)
                if len(photos) > 0:
                    update_name_column(ydb_pool, table_name, photos[0]['key_id'], new_name)
            return ok_response

        if text == '/getface':
            rows = get_random_face_photo(ydb_pool, table_name)
            if len(rows) > 0:
                key_id = rows[0]['key_id']
                tg_object_id = send_photo(tg_key, chat_id, face_storage_endpoint + key_id)
                update_tg_object_id_column(ydb_pool, table_name, key_id, tg_object_id)
            else:
                send_message(tg_key, chat_id, message_id, f'Фотографии без имени не найдены')
            return ok_response

    send_message(tg_key, chat_id, message_id, 'Ошибка')

    return ok_response