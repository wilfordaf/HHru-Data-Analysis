from pydantic import BaseModel


class DataValidatingStepProperties(BaseModel):
    test_parameter: str
