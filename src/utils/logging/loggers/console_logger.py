import sys
from logging import DEBUG, WARNING, Formatter, Logger, StreamHandler, getLogger
from typing import Optional

from src.utils.logging.handlers import InMemoryHandler


class ConsoleLogger(Logger):
    _INSTANCE: Optional[Logger] = None

    def __new__(cls, *args, **kwargs):
        if cls._INSTANCE is None:
            cls._INSTANCE = cls._init_logger()

        return cls._INSTANCE

    @classmethod
    def get_in_memory_handler(cls) -> Optional[InMemoryHandler]:
        if cls._INSTANCE is None:
            return None

        return next(
            (handler for handler in cls._INSTANCE.handlers if isinstance(handler, InMemoryHandler)),
            None,
        )

    @staticmethod
    def _init_logger() -> Logger:
        logger = getLogger("main")
        logger.setLevel(DEBUG)
        log_format = "%(asctime)s [%(levelname)s] %(module)s.%(funcName)s:%(lineno)d: %(message)s"
        formatter = Formatter(log_format)

        console_handler = StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(DEBUG)
        logger.addHandler(console_handler)

        in_memory_handler = InMemoryHandler()
        in_memory_handler.setFormatter(formatter)
        in_memory_handler.setLevel(WARNING)
        logger.addHandler(in_memory_handler)
        return logger
