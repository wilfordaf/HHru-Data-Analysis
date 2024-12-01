from pydantic import BaseModel


class PreprocessingStepProperties(BaseModel):
    unmatching_jobs_threshold: float
