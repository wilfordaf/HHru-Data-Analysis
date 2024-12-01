from pydantic import BaseModel


class ExtractionStepProperties(BaseModel):
    test_parameter: str
