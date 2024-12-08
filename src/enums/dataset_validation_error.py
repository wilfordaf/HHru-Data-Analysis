from enum import Enum


class DatasetValidationError(Enum):
    PREPROCESSING_ERROR = "Во время предобработки произошла ошибка, валидация не была проведена"
    BAD_DATA_QUALITY_ERROR = "Датасет не прошёл простые проверки качества"
    TARGET_DISBALANCE_ERROR = "Обнаружен значительный дисбаланс целевой переменной в выборке"
    DISTRIBUTION_DEVIATION_ERROR = "Обнаружено значительное отклонение от целевого распределения"

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value
