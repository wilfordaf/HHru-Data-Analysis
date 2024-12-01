from enum import Enum, auto


class DatasetName(Enum):
    @staticmethod
    def _generate_next_value_(name, start, count, last_values):
        return name.lower()

    SOURCE_DATA = auto()
    PREPROCESSED_DATA = auto()
    PROCESSED_DATA = auto()
    VERIFIED_DATA = auto()

    def __str__(self) -> str:
        value: str = self.value
        return value

    def __repr__(self) -> str:
        value: str = self.value
        return value
