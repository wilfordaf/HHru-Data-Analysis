from pathlib import Path
from typing import Any

import src.entities.pipeline as pipeline
from src.configuration.config_loaders.interfaces import IConfigLoader
from src.entities.pipeline import CommonProperties, Components, PipelineConfiguration
from src.entities.pipeline.data_properties import DataProperties
from src.utils.exceptions import ServiceError
from src.utils.file_parsers import YamlFileParser


class PipelineConfigLoader(IConfigLoader):
    _CONFIGURATION_FILE_PATH = Path(__file__).parents[3].resolve() / "config/pipeline.yaml"

    def __init__(self):
        try:
            self._config_data = YamlFileParser(self._CONFIGURATION_FILE_PATH).retrieve_data()
        except ServiceError as e:
            raise ServiceError(f"Ошибка с файлом конфигурации: {str(e)}") from e

    def get_config(self) -> PipelineConfiguration:
        pipeline_components = {
            key: self._get_component_instance(key, **value) for key, value in self._config_data["components"].items()
        }

        return PipelineConfiguration(
            common_properties=CommonProperties(**self._config_data["common_properties"]),
            components=Components(**pipeline_components),
            dataset={key: DataProperties(**value) for key, value in self._config_data["dataset"].items()},
        )

    @staticmethod
    def _get_component_instance(key: str, **kwargs) -> Any:
        class_name = "".join(key_part.title() for key_part in key.split("_"))  # camel_snake to CamelCase
        meta_class = getattr(pipeline, class_name)
        return meta_class(**kwargs)
