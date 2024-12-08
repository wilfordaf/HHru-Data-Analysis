import os.path
import shutil
from pathlib import Path

from src import logger
from src.utils.exceptions import ServiceError
from src.utils.file_providers.interfaces import IFileProvider


class LocalFileProvider(IFileProvider):
    def provide_file(self, original_path: Path, target_path: Path) -> Path:
        if not (os.path.isdir(original_path) or os.path.isfile(original_path)):
            raise ValueError(f"Указан неверный путь до оригинала {original_path}")
        if original_path == target_path:
            return target_path

        logger.debug(f"{original_path} -> {target_path}")
        message = f"Не удалось переместить файл {original_path} -> {target_path}"
        try:
            if target_path.is_dir():
                shutil.rmtree(target_path)

            self._copy_recursive(original_path, target_path)
            return target_path
        except FileNotFoundError as e:
            raise ServiceError(f"{message}, файл не найден: {str(e)}") from e
        except PermissionError as e:
            raise ServiceError(f"{message}, ошибка доступа: {str(e)}") from e
        except OSError as e:
            raise ServiceError(f"{message}, ошибка операционной системы: {str(e)}") from e
        except Exception as e:
            raise ServiceError(f"{message}, ошибка: {str(e)}") from e

    @staticmethod
    def _copy_recursive(source: Path, destination: Path) -> None:
        if not source.exists():
            raise FileNotFoundError(f"Источник {source} не существует")

        if source.is_file():
            if destination.is_dir():
                destination = destination / source.name

            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            return

        destination.mkdir(parents=True, exist_ok=True)
        for item in source.iterdir():
            src_item = source / item.name
            dest_item = destination / item.name
            if src_item.is_dir():
                LocalFileProvider._copy_recursive(src_item, dest_item)
            else:
                dest_item.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_item, dest_item)
