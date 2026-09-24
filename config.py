import os

from dotenv import load_dotenv

load_dotenv()
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
MESSAGES_TOPIC = os.getenv("MESSAGES_TOPIC", "messages")
BLOCKED_USERS_TOPIC = os.getenv("BLOCKED_USERS_TOPIC", "blocked_users")
FORBIDDEN_WORDS_ACTIONS_TOPIC = os.getenv("FORBIDDEN_WORDS_ACTIONS_TOPIC", "forbidden_words_action")

producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 10
}

FORBIDDEN_WORDS = [
    "дурак",
    "идиот",
    "тупой",
    "дебил",
    "кретин",
    "мерзавец",
    "хам",
    "урод",
    "бестолочь",
    "лжец",
    "врун",
    "подлец",
    "сволочь",
    "негодяй",
    "черт",
    "проклятие",
    "ненависть",
    "оскорбление",
    "мат",
    "ругательство",
]