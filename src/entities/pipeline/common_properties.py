from pydantic import BaseModel


class CommonProperties(BaseModel):
    utilize_clearml: bool
    provide_artifacts_to_project_dir: bool
