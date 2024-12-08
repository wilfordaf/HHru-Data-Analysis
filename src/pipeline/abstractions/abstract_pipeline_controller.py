from typing import Any, Callable

from src import logger
from src.configuration.config_loaders import PipelineConfigLoader
from src.data_controlling.data_controller import DataController
from src.entities.pipeline.component_result import DataValidatingResult
from src.pipeline.steps import data_plot_creation_step, data_validating_step, extraction_step, preprocessing_step
from src.utils.artifact_publication.interfaces import ILogger
from src.utils.exceptions import ClearMLError
from src.utils.file_managers.interfaces import IFileManager


class AbstractPipelineController:
    def __init__(
        self,
        file_manager: IFileManager,
        target_logger: ILogger,
    ):
        self._pipeline_config = PipelineConfigLoader().get_config()

        self._data_controller = DataController(
            config=self._pipeline_config,
            file_manager=file_manager,
        )

        self._logger = target_logger

        self._step_kwargs = {
            "config": self._pipeline_config,
            "data_controller": self._data_controller,
            "target_logger": self._logger,
        }

    def assemble_pipeline(self) -> Callable[[], None]:
        def pipeline() -> None:
            logger.info("Пайплайн запущен...")

            logger.info("Начинается загрузка данных...")
            data_extracting_result = self._get_decorated_step(extraction_step)(**self._step_kwargs)
            if data_extracting_result:
                logger.info("Загрузка данных завершена!")

            logger.info("Начинается предобработка данных...")
            data_preprocessing_result = self._get_decorated_step(preprocessing_step)(
                **self._step_kwargs | {"extracting_result": data_extracting_result}
            )
            if data_preprocessing_result:
                logger.info("Предобработка данных завершена!")

            logger.info("Начинается валидация данных...")
            data_validating_result: DataValidatingResult = self._get_decorated_step(data_validating_step)(
                **self._step_kwargs | {"preprocessing_result": data_preprocessing_result}
            )

            data_validating_log = "Валидация данных завершена! Результат валидации данных: "
            if data_validating_result.is_failure:
                logger.critical(f"{data_validating_log} ошибка! Текст: {data_validating_result.error_message}")
                raise ClearMLError("Ошибка при валидации данных!")

            logger.info(f"{data_validating_log} успех!")

            logger.info("Начинается создание графиков...")
            data_plot_creation_result = self._get_decorated_step(data_plot_creation_step)(
                **self._step_kwargs | {"preprocessing_result": data_preprocessing_result}
            )
            if data_plot_creation_result:
                logger.info("Создание графиков выполнено!")

        return pipeline

    def _get_decorated_step(self, step: Callable[..., Any]) -> Callable[..., Any]:
        raise NotImplementedError()
