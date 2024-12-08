from enum import Enum, auto


class DatasetTag(Enum):
    @staticmethod
    def _generate_next_value_(name, start, count, last_values):
        return name.lower()

    RAW = auto()
    INTERIM = auto()
    PROCESSED = auto()
    REFERENCES = auto()
    VERIFIED = auto()

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value
