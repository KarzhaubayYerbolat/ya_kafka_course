import datetime
import json
from enum import Enum

import faust

# Объявляю базовый класс унаследовавшись от faust.Record с методами для использования вне faust
class BaseRecord(faust.Record):

    def to_dict(self):
        return {
            key: value.isoformat() if isinstance(value, datetime.datetime) else value
            for key, value in self.asdict().items()
        }

    def serialize(self):
        return json.dumps(self.to_dict()).encode("utf-8")

# Объект сообщения для топика messages
class Message(BaseRecord):
    sender_user_id: int
    recipient_user_id: int
    message: str
    timestamp: datetime.datetime

# Объект сообщения для топика blocked_users
class BlockedUsers(BaseRecord):
    user_id: int
    blacklist: list[int]

# Объект сообщения для таблицы users_black_list
class Blacklist(BaseRecord):
    users: list[int]

# Enum с действиями по запрещенным словам
class ForbiddenWordAction(str, Enum):
    ADD = "add"
    REMOVE = "remove"

# Объект сообщения для топика forbidden_words_action
class ForbiddenWordActionRecord(BaseRecord):
    action: str
    word: str

# Объект сообщения для таблицы forbidden_words_table
class ForbiddenWords(BaseRecord):
    words: list[str]


