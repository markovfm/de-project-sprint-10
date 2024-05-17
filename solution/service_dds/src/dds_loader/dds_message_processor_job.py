import json
from datetime import datetime
from typing import Dict
from logging import Logger
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository
import logging
from lib.config import AppConfig
from apscheduler.schedulers.background import BackgroundScheduler


class DDSMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = batch_size

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: Message received")

            payload = msg['payload']

            # Обработка данных и вставка в DDS
            self._process_and_insert_dds_data(
                payload, msg["object_id"], msg["object_type"], msg["sent_dttm"])

            # Формирование обогащенного сообщения
            enriched_msg = self._enrich_message(
                payload, msg["object_id"], msg["object_type"])

            # Отправка обогащенного сообщения в Kafka
            self._producer.produce(enriched_msg)
            self._logger.info(
                f"{datetime.utcnow()}: Enriched message sent to Kafka")

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _process_and_insert_dds_data(self, payload: Dict, object_id: str, object_type: str, sent_dttm: str) -> None:
        load_src = "dds_service"

        # Сохранение данных в соответствующие таблицы DDS
        user = payload["user"]
        self._dds_repository.insert_h_user(user["id"], load_src)
        self._dds_repository.insert_s_user_names(
            user["id"], user["name"], user["login"], load_src)

        restaurant = payload["restaurant"]
        self._dds_repository.insert_h_restaurant(restaurant["id"], load_src)
        self._dds_repository.insert_s_restaurant_names(
            restaurant["id"], restaurant["name"], load_src)

        order_id = payload["id"]
        order_dt = datetime.strptime(payload["date"], "%Y-%m-%dT%H:%M:%S")
        self._dds_repository.insert_h_order(order_id, order_dt, load_src)
        self._dds_repository.insert_s_order_cost(
            order_id, payload["cost"], payload["payment"], load_src)
        self._dds_repository.insert_s_order_status(
            order_id, payload["status"], load_src)
        self._dds_repository.insert_l_order_user(
            order_id, user["id"], load_src)

        for item in payload["products"]:
            product_id = item["id"]
            self._dds_repository.insert_h_product(product_id, load_src)
            self._dds_repository.insert_s_product_names(
                product_id, item["name"], load_src)
            self._dds_repository.insert_l_order_product(
                order_id, product_id, load_src)
            self._dds_repository.insert_l_product_restaurant(
                restaurant["id"], product_id, load_src)
            self._dds_repository.insert_l_product_category(
                item["category"], product_id, load_src)

    def _enrich_message(self, payload: Dict, object_id: str, object_type: str) -> Dict:
        enriched_msg = {
            "object_id": object_id,
            "object_type": object_type,
            "payload": {
                "id": payload["id"],
                "date": payload["date"],
                "cost": payload["cost"],
                "payment": payload["payment"],
                "status": payload["status"],
                "restaurant": payload["restaurant"],
                "user": payload["user"],
                "products": payload["products"]
            }
        }
        return enriched_msg
