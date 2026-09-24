import logging

from confluent_kafka import Producer
from config import producer_config as config, FORBIDDEN_WORDS_ACTIONS_TOPIC, FORBIDDEN_WORDS
from logging_config import setup_logging
from models import ForbiddenWordActionRecord
from utils import delivery_report

setup_logging()

logger = logging.getLogger(__name__)

# Функция для отправки запрещенных слов в топик FORBIDDEN_WORDS_ACTIONS_TOPIC
def produce_forbidden_words():
    # объявляем producer
    producer = Producer(config)
    logger.info(f"Launched a producer to send forbidden words to topic: {FORBIDDEN_WORDS_ACTIONS_TOPIC}")

    # Перебираем запрещенные слова
    for word in FORBIDDEN_WORDS:
        action = "add"

        # Создаем объект ForbiddenWordActionRecord
        forbidden_word_action = ForbiddenWordActionRecord(
            action=action,
            word=word,
        )
        try:
            logger.info(
                f"Try to produce message: topic = {FORBIDDEN_WORDS_ACTIONS_TOPIC}, key = {action}, message = {forbidden_word_action.to_dict()}",
            )
            # Отправляем в топик FORBIDDEN_WORDS_ACTIONS_TOPIC
            producer.produce(
                topic=FORBIDDEN_WORDS_ACTIONS_TOPIC,
                key=action,
                value=forbidden_word_action.serialize(),
                callback=delivery_report,
            )

        except Exception as e:

            # Обработка исключения
            logger.exception(
                f"Error producing message {e}: topic = {FORBIDDEN_WORDS_ACTIONS_TOPIC}, key = {action}, message = {forbidden_word_action.to_dict()}"
            )

    producer.flush(10)