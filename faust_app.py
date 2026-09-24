import asyncio
import logging
import os
import sys

import faust
from dotenv import load_dotenv

from logging_config import setup_logging
from models import BlockedUsers, Message, Blacklist, ForbiddenWordAction, ForbiddenWords, ForbiddenWordActionRecord
from producers.black_list_producer import produce_blocked_users
from producers.forbidden_words_producer import produce_forbidden_words
from producers.message_producer import produce_messages
from utils import censor_forbidden_words

load_dotenv()
setup_logging()

logger = logging.getLogger(__name__)

# Подтягиваем BOOTSTRAP_SERVERS с переменного окружения
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

# Объявляем faust приложение
app = faust.App(
    "messenger",
    broker=BOOTSTRAP_SERVERS,
    topic_replication_factor=2,
)

# TOPIC грязных сообщении
messages_topic = app.topic(
    "messages",
    key_type=str,
    value_type=Message,
)

# TOPIC отфильтрованных сообщении
filtered_messages_topic = app.topic(
    "filtered_messages",
    key_type=str,
    value_type=Message,
)

# TOPIC по заблокированным пользователям
blocked_users_topic = app.topic(
    "blocked_users",
    key_type=str,
    value_type=BlockedUsers,
)

# TOPIC для управления списком запрещенных слов
forbidden_word_action_topic = app.topic(
    "forbidden_words_action",
    key_type=str,
    value_type=ForbiddenWordActionRecord,
)

# Talbe для хранения blacklist для каждого пользователя
users_black_list = app.Table(
    "users_black_list",
    key_type=str,
    value_type=Blacklist,
    default=lambda: Blacklist(users=[]),
    partitions=3,
)

# Talbe для хранения запрещенных слов
forbidden_words_table = app.Table(
    "forbidden_words",
    key_type=str,
    value_type=ForbiddenWords,
    default=lambda: ForbiddenWords(words=[]),
    partitions=3,
)

# Ключ для хранения списка запрещенных слов
FORBIDDEN_WORDS_KEY = "forbidden_words"


# Агент для обработки сообщении по заблокированным пользователям и сохранения в таблицу sers_black_list
@app.agent(blocked_users_topic)
async def update_blacklist(stream):
    async for message in stream:
        logger.info(
            "Received blocked users update: "
            "user_id=%s, blocked_users=%s",
            message.user_id,
            message.blacklist,
        )
        try:
            users_black_list[str(message.user_id)] = Blacklist(
                users=message.blacklist
            )
            logger.info(
                "Blacklist updated successfully: "
                "user_id=%s, blocked_users=%s",
                message.user_id,
                message.blacklist,
            )
        except Exception as exception:
            logger.exception(
                "Failed to update blacklist: "
                "user_id=%s, blocked_users=%s, error=%s",
                message.user_id,
                message.blacklist,
                exception,
            )


# Агент для обработки сообщении между пользователями с проверками на blacklist и экранированием и
# для дальнейшей отправки в filtered_messages
@app.agent(messages_topic)
async def filter_messages(stream):
    async for message in stream:

        sender = message.sender_user_id
        recipient = message.recipient_user_id
        logger.info(
            "Received message: sender=%s, recipient=%s",
            sender,
            recipient,
        )
        try:
            blacklist = users_black_list[str(recipient)]

            logger.debug(
                "Blacklist lookup: recipient=%s, blacklist=%s",
                recipient,
                blacklist.users,
            )

        except KeyError:
            logger.warning(
                "No blacklist found for recipient=%s. "
                "Message will be forwarded.",
                recipient,
            )
            blacklist = Blacklist(users=[])

        if sender in blacklist.users:
            logger.warning(
                "Message blocked: sender=%s, recipient=%s, "
                "sender is in blacklist=%s",
                sender,
                recipient,
                blacklist.users,
            )
            continue

        logger.info(
            "Message allowed: sender=%s, recipient=%s",
            sender,
            recipient,
        )

        key = f"{min(sender, recipient)}:{max(sender, recipient)}"

        forbidden_words = forbidden_words_table[FORBIDDEN_WORDS_KEY].words
        censored_text = censor_forbidden_words(message.message, forbidden_words)

        message.message = censored_text

        logger.debug(
            "Sending filtered message: key=%s, sender=%s, recipient=%s",
            key,
            sender,
            recipient,
        )

        await filtered_messages_topic.send(
            key=key,
            value=message,
        )

        logger.debug(
            "Filtered message sent successfully: key=%s",
            key,
        )

# Агент для динамического обновления списка запрещенных слов
@app.agent(forbidden_word_action_topic)
async def update_forbidden_words(stream):
    async for message in stream:
        logger.info(
            "Received forbidden word action: action=%s, word=%r",
            message.action,
            message.word,
        )
        current = forbidden_words_table[FORBIDDEN_WORDS_KEY]
        logger.debug(
            "Current forbidden words before update: %s",
            current.words,
        )
        if message.action == ForbiddenWordAction.ADD.value:
            if message.word in current.words:
                logger.warning(
                    "Forbidden word already exists: word=%r",
                    message.word,
                )
            else:
                current.words.append(message.word)

                logger.info(
                    "Forbidden word added: word=%r",
                    message.word,
                )

        elif message.action == ForbiddenWordAction.REMOVE.value:
            if message.word not in current.words:
                logger.warning(
                    "Forbidden word not found during removal: word=%r",
                    message.word,
                )
            else:
                current.words.remove(message.word)

                logger.info(
                    "Forbidden word removed: word=%r",
                    message.word,
                )
        else:
            logger.error(
                "Unknown forbidden word action: action=%r, word=%r",
                message.action,
                message.word,
            )
            continue

        forbidden_words_table[FORBIDDEN_WORDS_KEY] = current
        logger.info(
            "Forbidden words updated successfully: total=%d",
            len(current.words),
        )

        logger.debug(
            "Current forbidden words after update: %s",
            current.words,
        )

# Отправка тестовых данных и запуск faust приложения
def main():
    logger.info("Starting insert test data")

    produce_blocked_users()
    produce_forbidden_words()
    produce_messages()

    logger.info("Finished insert test data")

    sys.argv = [
        sys.argv[0],
        "worker",
        "-l",
        "info",
    ]

    app.main()

if __name__ == "__main__":
    main()