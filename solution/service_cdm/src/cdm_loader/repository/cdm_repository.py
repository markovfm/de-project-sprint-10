import uuid
from datetime import datetime
from lib.pg import PgConnect


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def insert_user_product_counter(self, user_id: uuid.UUID, product_id: uuid.UUID, product_name: str, order_cnt: int) -> None:
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO cdm.user_product_counters (user_id, product_id, product_name, order_cnt)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id, product_name) DO UPDATE
                SET order_cnt = cdm.user_product_counters.order_cnt + EXCLUDED.order_cnt;
            """, (user_id, product_id, product_name, order_cnt))
            self._db.commit()

    def insert_user_category_counter(self, user_id: uuid.UUID, category_id: uuid.UUID, category_name: str, order_cnt: int) -> None:
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO cdm.user_category_counters (user_id, category_id, category_name, order_cnt)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (category_id, category_name) DO UPDATE
                SET order_cnt = cdm.user_category_counters.order_cnt + EXCLUDED.order_cnt;
            """, (user_id, category_id, category_name, order_cnt))
            self._db.commit()
