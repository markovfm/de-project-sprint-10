import uuid
from datetime import datetime
from typing import Any, Dict, List
from lib.pg import PgConnect


class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def _generate_uuid(self):
        return str(uuid.uuid4())

    def _get_current_timestamp(self):
        return datetime.utcnow()

    def insert_h_user(self, user_id, load_src):
        h_user_pk = self._generate_uuid()
        load_dt = self._get_current_timestamp()
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id) DO NOTHING
            """, (h_user_pk, user_id, load_dt, load_src))
        self._db.commit()

    def insert_h_product(self, product_id, load_src):
        h_product_pk = self._generate_uuid()
        load_dt = self._get_current_timestamp()
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (product_id) DO NOTHING
            """, (h_product_pk, product_id, load_dt, load_src))
        self._db.commit()

    def insert_h_category(self, category_name, load_src):
        h_category_pk = self._generate_uuid()
        load_dt = self._get_current_timestamp()
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (category_name) DO NOTHING
            """, (h_category_pk, category_name, load_dt, load_src))
        self._db.commit()

    def insert_h_restaurant(self, restaurant_id, load_src):
        h_restaurant_pk = self._generate_uuid()
        load_dt = self._get_current_timestamp()
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (restaurant_id) DO NOTHING
            """, (h_restaurant_pk, restaurant_id, load_dt, load_src))
        self._db.commit()

    def insert_h_order(self, order_id, order_dt, load_src):
        h_order_pk = self._generate_uuid()
        load_dt = self._get_current_timestamp()
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
            """, (h_order_pk, order_id, order_dt, load_dt, load_src))
        self._db.commit()

    def insert_l_order_product(self, h_order_pk, h_product_pk, load_src):
        hk_order_product_pk = self._generate_uuid()
        load_dt = self._get_current_timestamp()
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (hk_order_product_pk) DO NOTHING
            """, (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src))
        self._db.commit()

    def insert_l_product_restaurant(self, h_restaurant_pk, h_product_pk, load_src):
        hk_product_restaurant_pk = self._generate_uuid()
        load_dt = self._get_current_timestamp()
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dds.l_product_restaurant (hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_dt, load_src)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (hk_product_restaurant_pk) DO NOTHING
            """, (hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_dt, load_src))
        self._db.commit()

    def insert_l_product_category(self, h_category_pk, h_product_pk, load_src):
        hk_product_category_pk = self._generate_uuid()
        load_dt = self._get_current_timestamp()
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dds.l_product_category (hk_product_category_pk, h_category_pk, h_product_pk, load_dt, load_src)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (hk_product_category_pk) DO NOTHING
            """, (hk_product_category_pk, h_category_pk, h_product_pk, load_dt, load_src))
        self._db.commit()

    def insert_l_order_user(self, h_order_pk, h_user_pk, load_src):
        hk_order_user_pk = self._generate_uuid()
        load_dt = self._get_current_timestamp()
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dds.l_order_user (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (hk_order_user_pk) DO NOTHING
            """, (hk_order_user_pk, h_order_pk, h_user_pk, load_dt, load_src))
        self._db.commit()

    def insert_s_user_names(self, h_user_pk, username, userlogin, load_src):
        hk_user_names_hashdiff = self._generate_uuid()
        load_dt = self._get_current_timestamp()
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dds.s_user_names (h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (h_user_pk, load_dt) DO NOTHING
            """, (h_user_pk, username, userlogin, load_dt, load_src, hk_user_names_hashdiff))
        self._db.commit()

    def insert_s_product_names(self, h_product_pk, name, load_src):
        hk_product_names_hashdiff = self._generate_uuid()
        load_dt = self._get_current_timestamp()
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dds.s_product_names (h_product_pk, name, load_dt, load_src, hk_product_names_hashdiff)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (h_product_pk, load_dt) DO NOTHING
            """, (h_product_pk, name, load_dt, load_src, hk_product_names_hashdiff))
        self._db.commit()

    def insert_s_restaurant_names(self, h_restaurant_pk, name, load_src):
        hk_restaurant_names_hashdiff = self._generate_uuid()
        load_dt = self._get_current_timestamp()
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dds.s_restaurant_names (h_restaurant_pk, name, load_dt, load_src, hk_restaurant_names_hashdiff)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (h_restaurant_pk, load_dt) DO NOTHING
            """, (h_restaurant_pk, name, load_dt, load_src, hk_restaurant_names_hashdiff))
        self._db.commit()

    def insert_s_order_cost(self, h_order_pk, cost, payment, load_src):
        hk_order_cost_hashdiff = self._generate_uuid()
        load_dt = self._get_current_timestamp()
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dds.s_order_cost (h_order_pk, cost, payment, load_dt, load_src, hk_order_cost_hashdiff)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (h_order_pk, load_dt) DO NOTHING
            """, (h_order_pk, cost, payment, load_dt, load_src, hk_order_cost_hashdiff))
        self._db.commit()

    def insert_s_order_status(self, h_order_pk, status, load_src):
        hk_order_status_hashdiff = self._generate_uuid()
        load_dt = self._get_current_timestamp()
        with self._db.cursor() as cursor:
            cursor.execute("""
                INSERT INTO dds.s_order_status (h_order_pk, status, load_dt, load_src, hk_order_status_hashdiff)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (h_order_pk, load_dt) DO NOTHING
            """, (h_order_pk, status, load_dt, load_src, hk_order_status_hashdiff))
        self._db.commit()
