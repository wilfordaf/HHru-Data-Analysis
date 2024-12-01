from typing import Any, Callable

from src.pipeline.abstractions import AbstractPipelineController
from src.utils.artifact_publication import LocalLogger
from src.utils.file_managers import LocalFileManager


class LocalPipelineController(AbstractPipelineController):
    def __init__(self):
        super().__init__(LocalFileManager(), LocalLogger())

    def _get_decorated_step(self, step: Callable[..., Any]) -> Callable[..., Any]:
        return step
