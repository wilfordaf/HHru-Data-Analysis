from typing import Any, Dict

import pandas as pd
from clearml import Logger
from plotly.graph_objects import Figure

from src import logger
from src.utils.artifact_publication.interfaces import ILogger


class ClearMLLogger(ILogger):
    def publish_dictionary_values(self, name: str, data_to_publish: Dict[str, Any]) -> None:
        current_logger = Logger.current_logger()

        for key, value in data_to_publish.items():
            data_to_publish[key] = [str(value)]

        try:
            current_logger.report_table(
                title=name,
                series=name,
                iteration=0,
                table_plot=pd.DataFrame(data_to_publish),
            )
        except Exception as e:
            logger.error(f"Не удалось опубликовать {data_to_publish} в ClearML: {e}")

    def publish_plots(self, plots_to_publish: Dict[str, Figure]) -> None:
        current_logger = Logger.current_logger()
        try:
            for plot_name, plot_figure in plots_to_publish.items():
                current_logger.report_plotly(title=plot_name, series=plot_name, figure=plot_figure)
        except Exception as e:
            logger.error(f"Не удалось опубликовать графики {plots_to_publish.keys()} в ClearML: {e}")
