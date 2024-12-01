from pathlib import Path
from typing import Any, Callable, Dict, Optional

from clearml import PipelineDecorator, Task, TaskTypes

from src import name as clearml_project_name
from src.entities.pipeline import PipelineConfiguration
from src.pipeline.abstractions import AbstractPipelineController
from src.utils.artifact_publication.clearml_logger import ClearMLLogger
from src.utils.exceptions import ClearMLError
from src.utils.file_managers.clearml_file_manager import ClearMLFileManager
from src.utils.file_parsers import YamlFileParser
from src.utils.serialization.dill_serialization_methods import deserialize, serialize


class ClearMLPipelineController(AbstractPipelineController):
    CLEARML_CONFIGURATION_FILE_PATH = Path(__file__).parents[2].resolve() / "config/clearml_config.yaml"
    CLEARML_CONFIG_PARAMETERS_SECTION_NAME = "Args/parameters"

    _TASK_TYPES: Dict[str, TaskTypes] = {
        "extraction_step": TaskTypes.data_processing,
        "preprocessing_step": TaskTypes.data_processing,
        "data_validating_step": TaskTypes.qc,
        "data_plot_creation_step": TaskTypes.monitor,
    }

    def __init__(self):
        super().__init__(ClearMLFileManager(clearml_project_name), ClearMLLogger())
        self._clearml_config = YamlFileParser(self.CLEARML_CONFIGURATION_FILE_PATH).retrieve_data()

    @property
    def clearml_pipeline_parameters(self) -> Dict[str, Any]:
        return self._clearml_config

    def assemble_pipeline(self) -> Callable[[], None]:
        pipeline_logic = super().assemble_pipeline()

        def pipeline(parameters: Dict[str, Any] = self._pipeline_config.model_dump()) -> None:
            clearml_parameters = Task.current_task().get_parameters(cast=True)
            self._pipeline_config = self._update_pipeline_config_with_clearml_parameters(
                parameters,
                clearml_parameters,
            )

            pipeline_logic()

        decorated_pipeline: Callable[[], None] = PipelineDecorator.pipeline(
            _func=pipeline,
            project=clearml_project_name,
            artifact_serialization_function=serialize,
            artifact_deserialization_function=deserialize,
            **self._clearml_config["common_properties"],
        )

        return decorated_pipeline

    def _get_decorated_step(self, step: Callable[..., Any]) -> Callable[..., Any]:
        name = step.__name__
        task_type = self._TASK_TYPES[name]
        parameters = self._clearml_config["components"].get(f"{name}_properties", {})
        decorated_step: Callable[[], Any] = PipelineDecorator.component(
            _func=step,
            task_type=task_type,
            return_values=[f"{name}_return_value"],
            cache=False,
            repo=".",
            **parameters,
        )

        return decorated_step

    def _update_pipeline_config_with_clearml_parameters(
        self,
        config_parameters: Dict[str, Any],
        clearml_parameters: Dict[str, Any],
    ) -> PipelineConfiguration:
        for clearml_key, clearml_value in clearml_parameters.items():
            config_parameter = self._get_config_parameter_by_clearml_key(config_parameters, clearml_key)
            if config_parameter is None:
                continue

            if not isinstance(clearml_value, type(config_parameter["config_value"])):
                raise ClearMLError(
                    f"Не удалось применить настройки параметров ClearML к конфигурации. "
                    f"Неверный тип параметра {clearml_key}: {type(clearml_value)}"
                )

            try:
                config_parameter["config_key"][config_parameter["config_key_name"]] = clearml_value
            except Exception as e:
                raise ClearMLError(f"Не удалось применить настройки параметров ClearML к конфигурации: {e}!")

        config: PipelineConfiguration = PipelineConfiguration.model_validate(config_parameters)

        return config

    def _get_config_parameter_by_clearml_key(
        self,
        config_parameters: Dict[str, Any],
        clearml_dict_key: str,
    ) -> Optional[Dict[str, Any]]:
        if not clearml_dict_key.startswith(self.CLEARML_CONFIG_PARAMETERS_SECTION_NAME):
            return None

        clearml_config_keys = clearml_dict_key.split("/")
        config_key: Any = config_parameters
        for dict_key in clearml_config_keys[2:-1]:
            try:
                config_key = self._get_parameter_value(config_key, dict_key)
            except Exception as e:
                raise ClearMLError(f"Возникла ошибка при получении атрибута класса {clearml_dict_key}") from e

        config_key_name = clearml_config_keys[-1]
        config_value = self._get_parameter_value(config_key, config_key_name)

        return {
            "config_key": config_key,
            "config_value": config_value,
            "config_key_name": config_key_name,
        }

    @staticmethod
    def _get_parameter_value(config_key, dict_key):
        return config_key.get(type(list(config_key.keys())[0])(dict_key))
