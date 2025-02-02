import os
import json
import requests
import re
from database_utils import (
    get_ydb_pool,
    get_random_face_photo,
    get_face_photos_by_name,
    get_face_photo_by_tg_object_id,
    update_name_column,
    update_tg_object_id_column
)


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
