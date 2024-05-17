import logging
from lib.app_config import AppConfig
from apscheduler.schedulers.background import BackgroundScheduler
import json
from datetime import datetime
from logging import Logger
from typing import List, Dict
from lib.kafka_connect import KafkaConsumer, KafkaProducer
from cdm_repository import CdmRepository


class CDMMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 cdm_repository: CdmRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._cdm_repository = cdm_repository
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

            # Процесс обработки данных и вставка в CDM
            self._process_and_insert_cdm_data(payload)

            # Формирование обогащенного сообщения
            enriched_msg = self._enrich_message(payload)

            # Отправка обогащенного сообщения в Kafka
            self._producer.produce(enriched_msg)
            self._logger.info(
                f"{datetime.utcnow()}: Enriched message sent to Kafka")

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.utcnow()}: FINISH")

    def _process_and_insert_cdm_data(self, payload: Dict) -> None:
        user_id = payload["user"]["id"]
        for product in payload["products"]:
            product_id = product["id"]
            product_name = product["name"]
            category_id = product["category"]
            order_cnt = product["quantity"]

            # Вставка в user_product_counters
            self._cdm_repository.insert_user_product_counter(
                user_id, product_id, product_name, order_cnt)

            # Вставка в user_category_counters
            self._cdm_repository.insert_user_category_counter(
                user_id, category_id, product["category_name"], order_cnt)

    def _enrich_message(self, payload: Dict) -> Dict:
        # Логика обогащения сообщения здесь
        enriched_msg = {
            "object_id": payload["id"],
            "object_type": "enriched_order",
            "payload": {
                "user_id": payload["user"]["id"],
                "products": payload["products"],
                "order_count": sum(item["quantity"] for item in payload["products"])
            }
        }
        return enriched_msg
