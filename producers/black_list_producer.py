import json
import logging
import random

from confluent_kafka import Producer
from config import producer_config as config, BLOCKED_USERS_TOPIC
from logging_config import setup_logging
from models import BlockedUsers
from utils import delivery_report



setup_logging()

logger = logging.getLogger(__name__)

# Функция для генерирования случайного списка заблокированных и отправки в топик BLOCKED_USERS_TOPIC
def produce_blocked_users():
    user_ids = list(range(1, 11)) # ID пользователей
    producer = Producer(config) # Объявление producer-а
    logger.info(f"Launched a producer to send information about blocked users to topic: {BLOCKED_USERS_TOPIC}")
    # перебираем список из ID пользователей
    for user_id in user_ids:
        # доступный список из ID с исключением самого ID пользователя
        available_users_for_blacklist = [uid for uid in user_ids if uid != user_id]

        # случайно выбираем два ID пользователя для помещения в в blacklist
        in_blacklist_users = random.sample(available_users_for_blacklist, 2)

        logger.info(f"Generated random black list for user_id {user_id}: {in_blacklist_users}")

        # формируем объект сообщения
        message = BlockedUsers(
            user_id=user_id,
            blacklist=in_blacklist_users
        )
        try:
            logger.info(
                f"Try to produce message: topic = {BLOCKED_USERS_TOPIC}, key = {str(user_id)}, message = {message}",
            )

            # отправляем сформированное сообщение
            producer.produce(
                topic=BLOCKED_USERS_TOPIC,
                key=str(user_id),
                value=message.serialize(),
                callback=delivery_report,
            )

            # проверяем на наличие callback
            producer.poll(0)

        except Exception as e:

            # обработка исключения
            logger.exception(
                f"Error producing message {e}: topic = {BLOCKED_USERS_TOPIC}, key = {str(user_id)}, message = {message}"
            )

    producer.flush(10)
