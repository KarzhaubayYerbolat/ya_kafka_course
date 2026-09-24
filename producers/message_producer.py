import logging
import random
import datetime

from confluent_kafka import Producer
from config import producer_config as config, MESSAGES_TOPIC, FORBIDDEN_WORDS
from logging_config import setup_logging
from models import Message
from utils import delivery_report



setup_logging()

logger = logging.getLogger(__name__)

# Фукнция для отправки случайного сообщения в топик MESSAGES_TOPIC
def produce_messages():
    # объявляем producer
    producer = Producer(config)
    # ID пользователей
    user_ids = list(range(1, 11))
    # Набор сообщений
    MESSAGES = [
        "Привет! Как дела?",
        "Доброе утро!",
        "Когда будешь свободен?",
        "Можешь посмотреть задачу?",
        "Я уже отправил документы.",
        "Давай созвонимся позже.",
        "Спасибо за помощь!",
        "Что сегодня по плану?",
        "Я закончил свою часть работы.",
        "Можешь проверить этот файл?",
        "Встреча переносится на завтра.",
        "Ты уже получил сообщение?",
        "Все работает нормально.",
        "Нужно немного подождать.",
        "Я сейчас занят, напишу позже.",
        "Когда будет готово?",
        "Давай обсудим это после обеда.",
        "Я отправил ссылку в чат.",
        "Проверь, пожалуйста, почту.",
        "У меня есть несколько вопросов.",
        "Все готово к запуску.",
        "Можешь прислать результат?",
        "Давай сделаем это сегодня.",
        "Я проверю и сообщу тебе.",
        "Нужно обновить конфигурацию.",
        "Сервис снова работает.",
        "У тебя есть доступ к серверу?",
        "Я нашел причину проблемы.",
        "Можешь перезапустить сервис?",
        "Логи выглядят нормально.",
        "Похоже, проблема на стороне сервера.",
        "Я посмотрю это через несколько минут.",
        "Давай пока оставим как есть.",
        "Нужно проверить соединение.",
        "Сообщи мне, когда закончишь.",
        "Я уже начал работу.",
        "Все изменения применились.",
        "Нужно согласовать это с командой.",
        "Когда будет следующая встреча?",
        "Я подготовлю необходимые данные.",
        "Можешь проверить настройки?",
        "Кажется, все исправлено.",
        "Давай попробуем еще раз.",
        "У меня пока нет информации.",
        "Я уточню этот вопрос.",
        "Спасибо, теперь все понятно.",
        "Можешь посмотреть последний лог?",
        "Система работает стабильно.",
        "Я напишу тебе после проверки.",
        "Хорошо, договорились.",
    ]

    logger.info(f"Launched a producer to send random messages to topic: {MESSAGES_TOPIC}")

    # Будем отправлять 100 сообщенгии
    for _ in range(100):

        # Случайно выбираем sender_user_id и recipient_user_id
        sender_user_id, recipient_user_id = random.sample(user_ids, 2)

        # Случайно выбираем message_body
        message_body = random.choice(MESSAGES)

        # Случайно добавляем запрещенное слово
        if random.choice([True, False]):
            message_body += random.choice(FORBIDDEN_WORDS)

        # Создаем объект Message
        message = Message(
            sender_user_id=sender_user_id,
            recipient_user_id=recipient_user_id,
            message=message_body,
            timestamp=datetime.datetime.now(datetime.UTC),
        )

        # Генерация ключа для сообщения по рекомендации в чате
        key = f"{min(sender_user_id, recipient_user_id)}-{max(sender_user_id, recipient_user_id)}"
        try:
            logger.info(
                f"Try to produce message: topic = messages, key = {key}, message = {message.to_dict()}",
            )

            # Отправляем в топик сформированное сообщение
            producer.produce(
                topic=MESSAGES_TOPIC,
                key=key,
                value=message.serialize(),
                callback=delivery_report,
            )

        except Exception as e:

            # Обработка исключения
            logger.exception(
                f"Error producing message {e}: topic = {MESSAGES_TOPIC}, key = {key}, message = {message.to_dict()}"
            )

    producer.flush(10)
