import boto3

def get_image_from_bucket(bucket_id, object_id):
    s3 = boto3.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        region_name='ru-central1'
    )

    object_response = s3.get_object(Bucket=bucket_id, Key=object_id)
    
    image_size = int(object_response['ContentLength'])
    if image_size > 1048576:
        return None  # Ảnh quá lớn
    
    return object_response['Body'].read()
