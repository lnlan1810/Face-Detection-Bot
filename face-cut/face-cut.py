import boto3
import uuid
import json
import ydb
import ydb.iam
import os
import logging
from PIL import Image
from io import BytesIO

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Khởi tạo clients (s3, ydb) một lần
def get_s3_client():
    return boto3.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        region_name='ru-central1',
        config=boto3.session.Config(signature_version='s3v4')
    )

def get_ydb_pool(db_endpoint, db_name):
    driver = ydb.Driver(
        endpoint=db_endpoint,
        database=db_name,
        credentials=ydb.iam.MetadataUrlCredentials(),
    )
    driver.wait(fail_fast=True, timeout=5)
    return ydb.SessionPool(driver)

# Hàm cắt ảnh
def crop_image(image_data, x1, y1, x2, y2):
    try:
        image = Image.open(image_data)
        cropped_image = image.crop((x1, y1, x2, y2))
        cropped_image_data = BytesIO()
        cropped_image.save(cropped_image_data, format='JPEG')
        cropped_image_data.seek(0)  # Đặt con trỏ về đầu để đọc lại dữ liệu
        return cropped_image_data
    except Exception as e:
        logger.error(f"Error cropping image: {e}")
        raise

# Hàm chèn dữ liệu vào YDB
def insert_photo_face(pool, table_name, key_id, origin_photo_key_id):
    def call(session):
        query = f"""
        INSERT INTO {table_name} (key_id, origin_photo_key_id)
        VALUES ('{key_id}', '{origin_photo_key_id}');
        """
        session.transaction().execute(
            query,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    try:
        pool.retry_operation_sync(call)
    except Exception as e:
        logger.error(f"Error inserting into YDB: {e}")
        raise

# Hàm chính
def handler(event, context):
    logger.info('Start trigger face cut processing')

    # Lấy các biến môi trường
    db_endpoint = os.environ['DB_API_ENDPOINT']
    db_name = os.environ['DB_NAME']
    table_name = os.environ['TABLE_NAME']
    photos_bucket_id = os.environ['PHOTOS_BUCKET_ID']
    faces_bucket_id = os.environ['FACES_BUCKET_ID']

    # Khởi tạo clients
    s3 = get_s3_client()
    ydb_pool = get_ydb_pool(db_endpoint, db_name)

    try:
        # Xử lý từng message trong event
        for message in event['messages']:
            task = json.loads(message['details']['message']['body'])

            object_id = task['object_id']
            x1 = int(task['vertices'][0]['x'])
            y1 = int(task['vertices'][0]['y'])
            x2 = int(task['vertices'][2]['x'])
            y2 = int(task['vertices'][2]['y'])

            # Tải ảnh gốc từ S3
            try:
                logger.info(f"Attempting to get object with key: {object_id} from bucket: {photos_bucket_id}")
                object_response = s3.get_object(Bucket=photos_bucket_id, Key=object_id)
                image_data = BytesIO(object_response['Body'].read())
            except s3.exceptions.NoSuchKey:
                logger.error(f"Object with key {object_id} does not exist in bucket {photos_bucket_id}")
                continue  # Bỏ qua message này và tiếp tục xử lý message tiếp theo
            except Exception as e:
                logger.error(f"Error downloading image from S3: {e}")
                continue

            # Cắt ảnh
            try:
                cropped_image_data = crop_image(image_data, x1, y1, x2, y2)
            except Exception as e:
                logger.error(f"Error cropping image: {e}")
                continue

            # Tạo key mới và tải lên ảnh đã cắt
            new_object_id = f"{uuid.uuid4()}.jpg"
            try:
                logger.info(f"Uploading cropped image with size: {cropped_image_data.getbuffer().nbytes} bytes")
                s3.put_object(
                    Bucket=faces_bucket_id,
                    Key=new_object_id,
                    Body=cropped_image_data,  # Truyền trực tiếp BytesIO
                    ContentType='image/jpeg'
                )
                logger.info(f"Uploaded cropped image to S3: {new_object_id}")
            except Exception as e:
                logger.error(f"Error uploading cropped image to S3: {e}")
                continue

            # Chèn thông tin vào YDB
            try:
                insert_photo_face(ydb_pool, table_name, new_object_id, object_id)
                logger.info(f"Inserted photo face into YDB: {new_object_id}")
            except Exception as e:
                logger.error(f"Error inserting into YDB: {e}")
                continue

    except Exception as e:
        logger.error(f"Unexpected error in handler: {e}")
        raise

    return "OK"