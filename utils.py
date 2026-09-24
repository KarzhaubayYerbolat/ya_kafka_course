import logging

from logging_config import setup_logging

setup_logging()

logger = logging.getLogger(__name__)


def delivery_report(err, msg):
    if err is not None:
        logger.error(
            "Delivery failed: %s",
            err
        )
        return

    logger.info(
        "Delivery successful: "
        "topic=%s, partition=%d, offset=%d",
        msg.topic(),
        msg.partition(),
        msg.offset(),
    )


def censor_forbidden_words(text, forbidden_words):
    original_text = text

    for forbidden_word in forbidden_words:
        if forbidden_word in text:
            logger.info(
                "Forbidden word found: word=%r, text=%r",
                forbidden_word,
                text,
            )

            text = text.replace(
                forbidden_word,
                "*" * len(forbidden_word),
            )

    if text != original_text:
        logger.info(
            "Text censored: original=%r, censored=%r",
            original_text,
            text,
        )

    return text
