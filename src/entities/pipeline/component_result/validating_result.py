from typing import Generic, Optional, TypeVar

from pydantic.dataclasses import dataclass

ErrorType = TypeVar("ErrorType")


@dataclass(config={"arbitrary_types_allowed": True})
class ValidatingResult(Generic[ErrorType]):
    success: bool = True
    error: Optional[ErrorType] = None
    message: str = ""

    def __post_init__(self):
        if self.success and self.error is not None:
            raise ValueError("Успешный результат не может содержать ошибку")

        if not self.success and self.error is None:
            raise ValueError("Проваленный результат обязан содержать ошибку")

    @property
    def is_success(self) -> bool:
        return self.success

    @property
    def is_failure(self) -> bool:
        return not self.is_success

    @property
    def error_message(self) -> str:
        message: str = "Не указана" if self.error is None else f"{self.error} {self.message}"
        return message
