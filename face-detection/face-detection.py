import os
import json
import base64
import requests
from yandex_storage import get_image_from_bucket
from yandex_messaging import send_message_to_queue

def handler(event, context):
    print('Start trigger face detection processing')

    api_key_header = 'Api-Key ' + os.environ['API_KEY']
    ymq_queue_url = os.environ['YMQ_QUEUE_URL']

    messages = event['messages'][0]
    event_metadata = messages['event_metadata']
    object_details = messages['details']

    folder_id = event_metadata['folder_id']
    bucket_id = object_details['bucket_id']
    object_id = object_details['object_id']
    
    # Lấy ảnh từ Object Storage
    image_data = get_image_from_bucket(bucket_id, object_id)
    if image_data is None:
        print("Error: Image size exceeds 1MB")
        return "Error"

    encoded_image_data = base64.b64encode(image_data).decode()

    # Gửi request đến Yandex Vision API
    request_body = {
        "folderId": folder_id,
        "analyze_specs": [{
            "content": encoded_image_data,
            "features": [{
                "type": "FACE_DETECTION"
            }]
        }]
    }
    headers = {
        "Content-Type": "application/json", 
        "Authorization": api_key_header
    }
    response = requests.post(
        'https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze', 
        json=request_body, headers=headers
    ).json()

    # Xử lý kết quả nhận diện khuôn mặt
    face_boxes = response['results'][0]['results'][0]['faceDetection']['faces']
    for face_box in face_boxes:
        send_message_to_queue(ymq_queue_url, {
            "object_id": object_id,
            "vertices": face_box['boundingBox']['vertices']
        })

    return "Ok"
