import boto3
import base64
import requests
import os
import json


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
    
    s3 = boto3.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        region_name = 'ru-central1'
    )

    object_response = s3.get_object(Bucket=bucket_id, Key=object_id)

    image_size = int(object_response['ContentLength'])
    if image_size > 1048576:
        print("Image size exceeds 1 Mb")
        return "Error"

    image_data = object_response['Body'].read()
    encoded_image_data = base64.b64encode(image_data).decode()

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
    response = requests.post('https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze', json=request_body, headers=headers).json()
    
    sqs = boto3.client(
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net',
        region_name = 'ru-central1'
    )

    face_boxes = response['results'][0]['results'][0]['faceDetection']['faces']
    for face_box in face_boxes:
        sqs.send_message(
            QueueUrl=ymq_queue_url,
            MessageBody=json.dumps({
                "object_id": object_id,
                "vertices": face_box['boundingBox']['vertices']
            })
        )

    return "Ok"