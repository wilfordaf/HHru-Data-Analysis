from typing import Dict, List

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from clearml import Task

from src.data_controlling.interfaces import IDataController
from src.entities.pipeline import PipelineConfiguration
from src.entities.pipeline.component_result import DataPlotCreationResult, DataPreprocessingResult
from src.pipeline.data_plot_creation_components.interfaces import IDataPlotCreationComponent
from src.utils.artifact_publication.interfaces import ILogger
from src.utils.exceptions.service_error import ServiceError


class DataPlotCreationComponent(IDataPlotCreationComponent):
    _CLEARML_TASK_NAME = "Создание графиков"

    def __init__(
        self,
        config: PipelineConfiguration,
        data_controller: IDataController,
        preprocessing_result: DataPreprocessingResult,
        target_logger: ILogger,
    ):
        self._config = config
        self._preprocessing_result = preprocessing_result
        self._data_controller = data_controller
        self._target_logger = target_logger

    def create_plots(self) -> DataPlotCreationResult:
        step_parameters = self._config.components.data_plot_creation_step_properties
        if step_parameters is None:
            raise ServiceError("Пустые параметры шага загрузки данных")

        dataset_name = self._preprocessing_result.result["preprocessed_data"]
        dataset_parameters = self._data_controller.get_dataset_parameters(dataset_name).custom_properties
        if dataset_parameters is None:
            raise ServiceError(f"Обнаружены пустые параметры датасета {dataset_name}")

        dataset = self._data_controller.get_dataset(dataset_name)
        methods_to_run = [
            self._get_age_distribution_plot,
            self._get_city_distribution_plot,
            self._get_city_salary_plot,
            self._get_devops_salary_plot,
            self._get_employee_count_plot,
            self._get_golang_skills_plot,
            self._get_frontend_skills_plot,
            self._get_position_count_plot,
            self._get_hidden_salary_plot,
            self._get_mean_salary_plot,
            self._get_moscow_salary_plot,
            self._get_spb_salary_plot,
            self._get_university_salary_plot,
            self._get_top_skills_plot,
        ]

        plots: Dict[str, go.Figure] = {}
        for method in methods_to_run:
            plots |= method(dataset)

        self._target_logger.publish_plots(plots)

        if self._config.common_properties.utilize_clearml:
            self._remove_actual_tags()

        return DataPlotCreationResult(success=True)  # type: ignore

    def _remove_actual_tags(self) -> None:
        last_tasks: List[Task] = Task.get_tasks(task_name=self._CLEARML_TASK_NAME, tags=["actual"])
        for last_task in last_tasks[:-1]:
            last_task.set_tags([])

    def _get_position_count_plot(self, dataset: pd.DataFrame) -> Dict[str, go.Figure]:
        category_counts = dataset["Искомая позиция"].value_counts().reset_index()
        category_counts.columns = ["Искомая позиция", "Количество соискателей"]  # type: ignore

        fig = px.bar(
            category_counts,
            x="Количество соискателей",
            y="Искомая позиция",
            title="Количество соискателей по каждой позиции по всей России",
            labels={"ЗП": "Заработная плата", "Искомая позиция": "Искомая позиция"},
            text="Количество соискателей",
            height=700,
            width=1200,
            color="Количество соискателей",
            color_continuous_scale="rainbow",
        )

        fig.update_layout(
            xaxis_title="Искомая позиция",
            yaxis_title="Количество соискателей",
            title_x=0.5,
        )

        fig.update_traces(textposition="outside", textfont=dict(size=15, color="black", family="Arial"))
        fig.update_traces(marker=dict(line=dict(width=0.5, color="black")))

        return {"position_count": fig}

    def _get_employee_count_plot(self, dataset: pd.DataFrame) -> Dict[str, go.Figure]:
        category_counts = dataset["Искомая позиция"].value_counts().reset_index()
        category_counts.columns = ["Искомая позиция", "Количество соискателей"]  # type: ignore

        fig = px.bar(
            category_counts,
            x="Количество соискателей",
            y="Искомая позиция",
            title="Количество соискателей по каждой позиции по всей России",
            labels={"ЗП": "Заработная плата", "Искомая позиция": "Искомая позиция"},
            text="Количество соискателей",
            height=700,
            width=1200,
            color="Количество соискателей",
            color_continuous_scale="rainbow",
        )

        fig.update_layout(
            xaxis_title="Искомая позиция",
            yaxis_title="Количество соискателей",
            title_x=0.5,
        )

        fig.update_traces(textposition="outside", textfont=dict(size=15, color="black", family="Arial"))
        fig.update_traces(marker=dict(line=dict(width=0.5, color="black")))

        return {"employee_count": fig}

    def _get_hidden_salary_plot(self, dataset: pd.DataFrame) -> Dict[str, go.Figure]:
        num_salary_available = dataset["ЗП"].notna().sum()
        num_salary_unavailable = dataset.shape[0] - num_salary_available

        salary_num_df = pd.DataFrame(
            {
                "ЗП": ["Открыта", "Скрыта"],
                "Количество": [num_salary_available, num_salary_unavailable],
            }
        )

        fig = px.bar(
            salary_num_df,
            x="ЗП",
            y="Количество",
            text_auto="Количество",
            orientation="v",
            height=700,
            color="ЗП",
        )

        fig.update_layout(
            xaxis_title="Зарплата",
            yaxis_title="Количество",
            title_x=0.5,
        )

        fig.update_traces(
            text=salary_num_df["Количество"],
            textposition="outside",
            textfont=dict(size=18, color="black", family="Arial"),
        )

        fig.update_traces(marker=dict(line=dict(width=0.5, color="black")))

        return {"hidden_salary": fig}

    def _get_mean_salary_plot(self, dataset: pd.DataFrame) -> Dict[str, go.Figure]:
        median_salary_by_positions = dataset.groupby("Искомая позиция")["ЗП"].median().reset_index()
        median_salary_by_positions.rename(columns={"ЗП": "Медиана"}, inplace=True)

        fig = px.box(
            dataset[["Искомая позиция", "ЗП"]],
            x="ЗП",
            y="Искомая позиция",
            title="Boxplots ЗП по искомой позиции по всей России",
            labels={"ЗП": "Заработная плата", "Искомая позиция": "Искомая позиция"},
            height=700,
            color="Искомая позиция",
        )

        fig.update_layout(
            xaxis=dict(
                tickformat=".2s",
                title="Заработная плата",
            ),
            yaxis_title="Искомая позиция",
            title_x=0.5,
            showlegend=False,
        )

        for position in median_salary_by_positions["Искомая позиция"]:
            median_value = median_salary_by_positions.loc[
                median_salary_by_positions["Искомая позиция"] == position,
                "Медиана",
            ].values[0]
            fig.add_annotation(
                x=median_value,
                y=position,
                text=f"{median_value:.0f}",
                showarrow=False,
                font=dict(color="black", size=12),
                opacity=0.6,
                align="center",
            )

        return {"mean_salary": fig}

    def _get_moscow_salary_plot(self, dataset: pd.DataFrame) -> Dict[str, go.Figure]:
        median_salary_moscow_by_positions = (
            dataset[dataset["Город"] == "Москва"].groupby("Искомая позиция")["ЗП"].median().reset_index()
        )
        median_salary_moscow_by_positions.rename(columns={"ЗП": "Медиана"}, inplace=True)

        fig = px.box(
            dataset[dataset["Город"] == "Москва"][["Искомая позиция", "ЗП"]],
            x="ЗП",
            y="Искомая позиция",
            title="Boxplots ЗП по искомой позиции по Москве",
            labels={"ЗП": "Заработная плата", "Искомая позиция": "Искомая позиция"},
            height=700,
            color="Искомая позиция",
        )

        fig.update_layout(
            xaxis=dict(
                tickformat=".2s",
                title="Заработная плата",
            ),
            yaxis_title="Искомая позиция",
            title_x=0.5,
            showlegend=False,
        )

        for position in median_salary_moscow_by_positions["Искомая позиция"]:
            median_value = median_salary_moscow_by_positions.loc[
                median_salary_moscow_by_positions["Искомая позиция"] == position,
                "Медиана",
            ].values[0]
            fig.add_annotation(
                x=median_value,
                y=position,
                text=f"{median_value:.0f}",
                showarrow=False,
                font=dict(color="black", size=12),
                opacity=0.6,
                align="center",
            )

        return {"moscow_salary": fig}

    def _get_spb_salary_plot(self, dataset: pd.DataFrame) -> Dict[str, go.Figure]:
        median_salary_spb_by_positions = (
            dataset[dataset["Город"] == "Санкт-Петербург"].groupby("Искомая позиция")["ЗП"].median().reset_index()
        )
        median_salary_spb_by_positions.rename(columns={"ЗП": "Медиана"}, inplace=True)

        fig = px.box(
            dataset[dataset["Город"] == "Санкт-Петербург"][["Искомая позиция", "ЗП"]],
            x="ЗП",
            y="Искомая позиция",
            title="Boxplots ЗП по искомой позиции по Санкт-Петербургу",
            labels={"ЗП": "Заработная плата", "Искомая позиция": "Искомая позиция"},
            height=700,
            color="Искомая позиция",
        )

        fig.update_layout(
            xaxis=dict(
                tickformat=".2s",
                title="Заработная плата",
            ),
            yaxis_title="Искомая позиция",
            title_x=0.5,
            showlegend=False,
        )

        for position in median_salary_spb_by_positions["Искомая позиция"]:
            median_value = median_salary_spb_by_positions.loc[
                median_salary_spb_by_positions["Искомая позиция"] == position,
                "Медиана",
            ].values[0]
            fig.add_annotation(
                x=median_value,
                y=position,
                text=f"{median_value:.0f}",
                showarrow=False,
                font=dict(color="black", size=12),
                opacity=0.6,
                align="center",
            )

        return {"spb_salary": fig}

    def _get_city_distribution_plot(self, dataset: pd.DataFrame) -> Dict[str, go.Figure]:
        city_counts = dataset["Город"].value_counts().reset_index()
        city_counts.columns = ["Город", "Количество позиций"]  # type: ignore
        city_counts = city_counts[city_counts["Количество позиций"] > 50]
        city_counts = city_counts.sort_values("Количество позиций")[::-1]

        fig = px.bar(
            city_counts,
            x="Количество позиций",
            y="Город",
            orientation="h",
            title="Количество позиций по городам",
            height=700,
            labels={"Количество позиций": "Количество позиций", "Город": "Город"},
            text="Количество позиций",
            color="Количество позиций",
            color_continuous_scale="rainbow",
        )

        fig.update_traces(
            textposition="outside",
            texttemplate="%{text}",
            textfont=dict(
                size=18,
                color="black",
                family="Arial",
            ),
        )

        return {"city_distribution": fig}

    def _get_city_salary_plot(self, dataset: pd.DataFrame) -> Dict[str, go.Figure]:
        city_counts = dataset["Город"].value_counts().reset_index()
        city_counts.columns = ["Город", "Количество позиций"]  # type: ignore

        cities_to_include = city_counts[city_counts["Количество позиций"] > 50]
        cities_to_include = cities_to_include["Город"].unique()  # type: ignore

        filtered_cities = dataset[dataset["Город"].isin(cities_to_include)]

        median_salary_by_cities = filtered_cities.groupby("Город")["ЗП"].median().reset_index()
        median_salary_by_cities.rename(columns={"ЗП": "Медиана"}, inplace=True)

        fig = px.box(
            filtered_cities,
            x="ЗП",
            y="Город",
            title="Boxplots ЗП по городам",
            labels={"ЗП": "Заработная плата", "Город": "Город"},
            height=1200,
            color="Город",
        )

        fig.update_layout(
            xaxis=dict(
                tickformat=".2s",
                title="Заработная плата",
            ),
            yaxis_title="Искомая позиция",
            title_x=0.5,
            showlegend=False,
        )

        for position in median_salary_by_cities["Город"]:
            median_value = median_salary_by_cities.loc[
                median_salary_by_cities["Город"] == position,
                "Медиана",
            ].values[0]
            fig.add_annotation(
                x=median_value,
                y=position,
                text=f"{median_value:.0f}",
                showarrow=False,
                font=dict(color="black", size=12),
                opacity=0.6,
                align="center",
            )

        return {"city_salary": fig}

    def _get_top_skills_plot(self, dataset: pd.DataFrame) -> Dict[str, go.Figure]:
        skills_split = dataset["Навыки"].dropna().str.split(", ").explode().str.strip()
        skills_counts = skills_split.value_counts()
        skills_filtered = skills_counts[:30].reset_index()
        skills_filtered.columns = ["Навык", "Частота"]  # type: ignore

        fig = px.bar(
            skills_filtered,
            x="Частота",
            y="Навык",
            labels={"x": "Навык", "y": "Частота"},
            title="Частота встречаемости навыков у соискателей по всем позициям",
            text="Частота",
            height=700,
            color="Частота",
            color_continuous_scale="rainbow",
        )

        fig.update_traces(
            textposition="outside",
            texttemplate="%{text}",
            textfont=dict(
                size=18,
                color="black",
                family="Arial",
            ),
        )

        return {"top_skills": fig}

    def _get_golang_skills_plot(self, dataset: pd.DataFrame) -> Dict[str, go.Figure]:
        skills_split_golang = (
            dataset[dataset["Искомая позиция"] == "Golang Developer"]["Навыки"]
            .dropna()
            .str.split(", ")
            .explode()
            .str.strip()
        )
        skills_counts_golang = skills_split_golang.value_counts()
        skills_filtered_golang = skills_counts_golang[:30].reset_index()
        skills_filtered_golang.columns = ["Навык", "Частота"]  # type: ignore

        fig = px.bar(
            skills_filtered_golang,
            x="Частота",
            y="Навык",
            labels={"x": "Навык", "y": "Частота"},
            title="Частота встречаемости навыков у соискателей по позиции Golang Developer",
            text="Частота",
            height=700,
            color="Частота",
            color_continuous_scale="rainbow",
        )

        fig.update_traces(
            textposition="outside",
            texttemplate="%{text}",
            textfont=dict(
                size=18,
                color="black",
                family="Arial",
            ),
        )

        return {"golang_skills": fig}

    def _get_frontend_skills_plot(self, dataset: pd.DataFrame) -> Dict[str, go.Figure]:
        skills_split_frontend = (
            dataset[dataset["Искомая позиция"] == "Frontend Developer"]["Навыки"]
            .dropna()
            .str.split(", ")
            .explode()
            .str.strip()
        )
        skills_counts_frontend = skills_split_frontend.value_counts()
        skills_filtered_frontend = skills_counts_frontend[:30].reset_index()
        skills_filtered_frontend.columns = ["Навык", "Частота"]  # type: ignore

        fig = px.bar(
            skills_filtered_frontend,
            x="Частота",
            y="Навык",
            labels={"x": "Навык", "y": "Частота"},
            title="Частота встречаемости навыков у соискателей по позиции Frontend Developer",
            text="Частота",
            height=700,
            color="Частота",
            color_continuous_scale="rainbow",
        )

        fig.update_traces(
            textposition="outside",
            texttemplate="%{text}",
            textfont=dict(
                size=18,
                color="black",
                family="Arial",
            ),
        )

        return {"frontend_skills": fig}

    def _get_devops_salary_plot(self, dataset: pd.DataFrame) -> Dict[str, go.Figure]:
        skills_split_devops = (
            dataset[dataset["Искомая позиция"] == "DevOps Engineer"]["Навыки"]
            .dropna()
            .str.split(", ")
            .explode()
            .str.strip()
        )

        skills_counts_devops = skills_split_devops.value_counts()
        skills_filtered_devops = skills_counts_devops[:30].reset_index()
        skills_filtered_devops.columns = ["Навык", "Частота"]  # type: ignore

        fig = px.bar(
            skills_filtered_devops,
            x="Частота",
            y="Навык",
            labels={"x": "Навык", "y": "Частота"},
            title="Частота встречаемости навыков у соискателей по позиции DevOps Engineer",
            text="Частота",
            height=700,
            color="Частота",
            color_continuous_scale="rainbow",
        )

        fig.update_traces(
            textposition="outside",
            texttemplate="%{text}",
            textfont=dict(
                size=18,
                color="black",
                family="Arial",
            ),
        )

        return {"devops_skills": fig}

    def _get_age_distribution_plot(self, dataset: pd.DataFrame) -> Dict[str, go.Figure]:
        median_ages_by_position = dataset.groupby("Искомая позиция")["Возраст"].median().reset_index()
        median_ages_by_position.rename(columns={"Возраст": "Медиана"}, inplace=True)

        fig = px.box(
            dataset,
            x="Возраст",
            y="Искомая позиция",
            title="Возраст соискателей для каждой искомой позиции",
            labels={"Возраст": "Возраст", "Искомая позиция": "Искомая позиция"},
            height=700,
            color="Искомая позиция",
        )

        fig.update_layout(
            xaxis=dict(
                tickformat=".2s",
                title="Возраст",
            ),
            yaxis_title="Искомая позиция",
            title_x=0.5,
            showlegend=False,
        )

        for position in median_ages_by_position["Искомая позиция"]:
            median_value = median_ages_by_position.loc[
                median_ages_by_position["Искомая позиция"] == position,
                "Медиана",
            ].values[0]
            fig.add_annotation(
                x=median_value,
                y=position,
                text=f"{median_value:.0f}",
                showarrow=False,
                font=dict(color="black", size=12),
                opacity=0.5,
                align="center",
                xshift=10,
            )

        return {"age_distribution": fig}

    def _get_university_salary_plot(self, dataset: pd.DataFrame) -> Dict[str, go.Figure]:
        education_counts = dataset["Образование и ВУЗ"].value_counts()
        filtered_education = education_counts[:30].index
        filtered_df = dataset[dataset["Образование и ВУЗ"].isin(filtered_education)]

        median_salary_by_uni = filtered_df.groupby("Образование и ВУЗ")["ЗП"].median().reset_index()
        median_salary_by_uni.rename(columns={"ЗП": "Медиана"}, inplace=True)

        fig = px.box(
            filtered_df,
            y="Образование и ВУЗ",
            x="ЗП",
            title="Распределение зарплат соискателей по уровню образования",
            labels={"Образование и ВУЗ": "Образование и ВУЗ", "ЗП": "Заработная плата"},
            height=1000,
            width=1500,
            color="Образование и ВУЗ",
            orientation="h",
        )

        fig.update_layout(
            xaxis=dict(
                tickformat=".2s",
                title="Заработная плата",
            ),
            yaxis_title="ВУЗ",
            title_x=0.5,
            showlegend=False,
        )

        for position in median_salary_by_uni["Образование и ВУЗ"]:
            median_value = median_salary_by_uni.loc[
                median_salary_by_uni["Образование и ВУЗ"] == position,
                "Медиана",
            ].values[0]
            fig.add_annotation(
                x=median_value,
                y=position,
                text=f"{median_value:.0f}",
                showarrow=False,
                font=dict(color="black", size=12),
                opacity=0.5,
                align="center",
                xshift=10,
            )

        return {"university_salary": fig}
