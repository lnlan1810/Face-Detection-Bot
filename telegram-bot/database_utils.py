import ydb


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
