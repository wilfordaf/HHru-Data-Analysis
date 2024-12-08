from copy import copy
from logging import Handler, LogRecord
from typing import List


class InMemoryHandler(Handler):
    def __init__(self):
        super().__init__()
        self._saved_log_records: List[LogRecord] = []

    def get_saved_log_records(self) -> List[LogRecord]:
        saved_logs = copy(self._saved_log_records)
        self.flush()
        return saved_logs

    def emit(self, record: LogRecord) -> None:
        self._saved_log_records.append(record)

    def flush(self) -> None:
        self._saved_log_records = []
