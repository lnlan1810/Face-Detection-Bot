terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
    }
  }
  required_version = ">= 0.13"
}

provider "yandex" {
  service_account_key_file = "key.json"
  cloud_id                 = var.cloud_id
  folder_id                = var.folder_id
  zone                     = "ru-central1-a"
}

# IAM Service Account Keys
resource "yandex_iam_service_account_static_access_key" "sa-static-key" {
  service_account_id = var.sa_id
  description        = "Static access key for object storage"
}

resource "yandex_iam_service_account_api_key" "sa-api-key" {
  service_account_id = var.sa_id
  description        = "Api key for authorization"
}

# Object Storage Buckets
resource "yandex_storage_bucket" "photos-bucket" {
  access_key = yandex_iam_service_account_static_access_key.sa-static-key.access_key
  secret_key = yandex_iam_service_account_static_access_key.sa-static-key.secret_key
  bucket = var.photos_bucket_name
  acl    = "private"
}

resource "yandex_storage_bucket" "faces-bucket" {
  access_key = yandex_iam_service_account_static_access_key.sa-static-key.access_key
  secret_key = yandex_iam_service_account_static_access_key.sa-static-key.secret_key
  bucket = var.faces_bucket_name
  acl    = "private"
}

# Message Queue
resource "yandex_message_queue" "face-cut-task-queue" {
  name = var.face-cut-task-queue-name
  access_key = yandex_iam_service_account_static_access_key.sa-static-key.access_key
  secret_key = yandex_iam_service_account_static_access_key.sa-static-key.secret_key
}

# YDB Database & Table
resource "yandex_ydb_database_serverless" "db" {
  name                = var.ydb-name

  serverless_database {
    storage_size_limit = 5
  }
}

resource "yandex_ydb_table" "photo-face-table" {
  path = "photo_face"
  connection_string = yandex_ydb_database_serverless.db.ydb_full_endpoint

  column {
    name = "key_id"
    type = "Utf8"
    not_null = true
  }
  column {
    name = "origin_photo_key_id"
    type = "Utf8"
  }
  column {
    name = "name"
    type = "Utf8"
  }
  column {
    name = "tg_object_id"
    type = "Utf8"
  }

  primary_key = ["key_id"]
}

resource "yandex_api_gateway" "api-gateway" {
  name        = var.api-gateway-name
  spec = <<-EOT
    openapi: "3.0.0"
    info:
      version: 1.0.0
      title: HW API
    paths:
      /:
        get:
          summary: Get face photo
          operationId: face
          parameters:
            - name: face
              in: query
              description: Face photo key
              required: true
              schema:
                type: string
          x-yc-apigateway-integration:
            type: object_storage
            bucket: ${yandex_storage_bucket.faces-bucket.id}
            object: '{face}'
            service_account_id: ${var.sa_id}
  EOT
}

data "archive_file" "face-detection-zip" {
  type        = "zip"
  source_dir = "face-detection"
  output_path = "face-detection.zip"
}

resource "yandex_function" "face-detection-func" {
  name               = var.face-detection-func-name
  description        = "Face detection handler"
  user_hash = "v1.0.0"
  runtime            = "python311"
  entrypoint         = "face-detection.handler"
  memory             = "128"
  execution_timeout  = "10"
  service_account_id = var.sa_id
  content {
    zip_filename = "face-detection.zip"
  }
  environment = {
    AWS_ACCESS_KEY_ID = yandex_iam_service_account_static_access_key.sa-static-key.access_key
    AWS_SECRET_ACCESS_KEY = yandex_iam_service_account_static_access_key.sa-static-key.secret_key
    YMQ_QUEUE_URL = yandex_message_queue.face-cut-task-queue.id
    API_KEY = yandex_iam_service_account_api_key.sa-api-key.secret_key
  }
}

resource "yandex_function_trigger" "upload-photo-trigger" {
  name        = var.upload-photo-trigger-name
  description = "Upload photo trigger"
  object_storage {
     bucket_id = yandex_storage_bucket.photos-bucket.id
     suffix = ".jpg"
     create    = true
     batch_cutoff = 10
  }
  function {
    id                 = yandex_function.face-detection-func.id
    service_account_id = var.sa_id
  }
}

data "archive_file" "face-cut-zip" {
  type        = "zip"
  source_dir = "face-cut"
  output_path = "face-cut.zip"
}

resource "yandex_function" "face-cut-func" {
  name               = var.face-cut-func-name
  description        = "Face cut handler"
  user_hash = "v1.0.0"
  runtime            = "python311"
  entrypoint         = "face-cut.handler"
  memory             = "128"
  execution_timeout  = "10"
  service_account_id = var.sa_id
  content {
    zip_filename = "face-cut.zip"
  }
  environment = {
    AWS_ACCESS_KEY_ID = yandex_iam_service_account_static_access_key.sa-static-key.access_key
    AWS_SECRET_ACCESS_KEY = yandex_iam_service_account_static_access_key.sa-static-key.secret_key
    PHOTOS_BUCKET_ID = yandex_storage_bucket.photos-bucket.id
    FACES_BUCKET_ID = yandex_storage_bucket.faces-bucket.id
    DB_API_ENDPOINT = "grpcs://${yandex_ydb_database_serverless.db.ydb_api_endpoint}"
    DB_NAME = yandex_ydb_database_serverless.db.database_path
    TABLE_NAME = yandex_ydb_table.photo-face-table.path
  }
}

resource "yandex_function_trigger" "face-cut-task-queue-trigger" {
  name        = var.face-cut-task-queue-trigger-name
  description = "Face cut task queue trigger"
  message_queue {
    queue_id           = yandex_message_queue.face-cut-task-queue.arn
    service_account_id = var.sa_id
    batch_cutoff = "20"
    batch_size = "1"
  }
  function {
    id = yandex_function.face-cut-func.id
    service_account_id = var.sa_id
  }
}

data "archive_file" "telegram-bot-zip" {
  type        = "zip"
  source_dir = "telegram-bot"
  output_path = "telegram-bot.zip"
}

resource "yandex_function" "telegram-bot-func" {
  name               = var.telegram-bot-func-name
  description        = "Telegram bot function"
  user_hash          = "v1.0.0"
  runtime            = "python311"
  entrypoint         = "telegram-bot.handler"
  memory             = "128"
  execution_timeout  = "10"
  service_account_id = var.sa_id
  content {
    zip_filename = "telegram-bot.zip"
  }
  environment = {
    TG_KEY = var.tg_key
    FACES_STORAGE_API_GATEWAY_ENDPOINT = "https://${yandex_api_gateway.api-gateway.domain}/?face="
    DB_API_ENDPOINT = "grpcs://${yandex_ydb_database_serverless.db.ydb_api_endpoint}"
    DB_NAME = yandex_ydb_database_serverless.db.database_path
    TABLE_NAME = yandex_ydb_table.photo-face-table.path
  }
}

resource "yandex_function_iam_binding" "telegram-bot-func-iam" {
  function_id = yandex_function.telegram-bot-func.id
  role        = "serverless.functions.invoker"

  members = [
    "system:allUsers",
  ]
}

data "http" "telegram-bot-webhook" {
  url = "https://api.telegram.org/bot${var.tg_key}/setWebhook?url=https://functions.yandexcloud.net/${yandex_function.telegram-bot-func.id}"
}