from pathlib import Path
from typing import Any, Dict

from plotly.graph_objects import Figure

from src import logger
from src.utils.artifact_publication.interfaces import ILogger


class LocalLogger(ILogger):
    _MAX_PLOT_POINTS = 10
    _PLOTS_SAVE_DIR_PATH = Path(__file__).parents[3] / "reports"

    def publish_dictionary_values(self, name: str, data_to_publish: Dict[str, Any]) -> None:
        keys = " | ".join(data_to_publish.keys())
        values = " | ".join(str(value) for value in data_to_publish.values())

        md_table = f"| {keys} |\n"
        md_table += f"| {' | '.join(['---'] * len(data_to_publish))} |\n"
        md_table += f"| {values} |\n"

        logger.info(f"Таблица: {name}")
        logger.info(md_table)

    def publish_plots(self, plots_to_publish: Dict[str, Figure]) -> None:
        for plot_name, plot_figure in plots_to_publish.items():
            title = plot_figure.layout.title.text if plot_figure.layout.title else "Без названия"

            if plot_figure.layout.xaxis.title:
                x_axis_label = plot_figure.layout.xaxis.title.text
            else:
                x_axis_label = "Без названия X оси"

            if plot_figure.layout.yaxis.title:
                y_axis_label = plot_figure.layout.yaxis.title.text
            else:
                y_axis_label = "Без названия Y оси"

            num_traces = len(plot_figure.data)

            logger.info(f"Имя графика: {plot_name}")
            logger.info(f"Название: {title}")
            logger.info(f"Ось X: {x_axis_label}")
            logger.info(f"Ось Y: {y_axis_label}")
            logger.info(f"Количество графиков: {num_traces}")
            logger.info("Схематическое представление графика:")
            logger.info(f"{'-' * 40}")
            logger.info(f"| {title} |")
            logger.info(f"| X: {x_axis_label} | Y: {y_axis_label} |")
            logger.info(f"{'-' * 40}\n")

            self._save_plot(plot_name, plot_figure)

    def _save_plot(self, plot_name: str, plot_figure: Figure) -> None:
        try:
            plot_save_path = self._PLOTS_SAVE_DIR_PATH / f"{plot_name}.html"
            plot_figure.write_html(plot_save_path.as_posix())
            logger.info(f"График успешно сохранён по пути: {plot_save_path}")
        except Exception as e:
            logger.error(f"Не удалось сохранить график {plot_name}: {e}")
