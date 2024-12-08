from typing import Any, Dict, Optional, Sequence

import great_expectations as gx
import pandas as pd
from great_expectations.core.batch import Batch
from scipy.stats import ks_2samp

from src import logger
from src.enums.dataset_validation_error import DatasetValidationError
from src.pipeline.data_validating_components.interfaces import IDataValidator


class GreatExpectationsValidator(IDataValidator):
    _DATA_SOURCE_NAME = "superjob"
    _DATA_ASSET_NAME = "superjob_parsed_data"
    _BATCH_NAME = "batch_1"

    def __init__(
        self,
        verified_data: pd.DataFrame,
        extracted_data: pd.DataFrame,
        dataset_parameters: Dict[str, Any],
    ):
        self._dataset_parameters = dataset_parameters
        self._verified_data = verified_data
        self._extracted_data = extracted_data

        self._extracted_dataset: Optional[Batch] = None

        self._context = gx.get_context()

    def validate_data(self) -> Dict[str, Any]:
        self._preprocess_data()

        if not self._verify_values_in_set(self._dataset_parameters["bounds"]):
            logger.error("В данных есть образцы за корректными границами")
            return {
                "success": False,
                "message": "В данных есть образцы за корректными границами",
                "error": DatasetValidationError.BAD_DATA_QUALITY_ERROR,
            }

        # if not self._verify_kl_divergence_test(self._dataset_parameters["kl_divergence_threshold"]):
        #     logger.error("Провален тест Кульбака-Лейблера")
        #     return {
        #         "success": False,
        #         "message": "Провален тест Кульбака-Лейблера",
        #         "error": DatasetValidationError.DISTRIBUTION_DEVIATION_ERROR,
        #     }

        if not self._verify_z_score_test(self._dataset_parameters["z_score_threshold"]):
            logger.error("Провален тест z-score")
            return {
                "success": False,
                "message": "Провален тест z-score",
                "error": DatasetValidationError.DISTRIBUTION_DEVIATION_ERROR,
            }

        if not self._verify_ks_test(self._dataset_parameters["ks_test_p_value"]):
            logger.error("Провален тест Колмогорова-Смирнова")
            return {
                "success": False,
                "message": "Провален тест Колмогорова-Смирнова",
                "error": DatasetValidationError.DISTRIBUTION_DEVIATION_ERROR,
            }

        return {"success": True, "message": ""}

    def _preprocess_data(self) -> None:
        skills_column = "skills_count"

        self._extracted_data.dropna(subset=["ЗП"], inplace=True)
        logger.debug(f"{self._extracted_data.shape} {self._verified_data.shape}")

        self._verified_data[skills_column] = self._verified_data["Навыки"].str.count(",")
        self._verified_data.dropna(subset=[skills_column], inplace=True)
        self._extracted_data[skills_column] = self._extracted_data["Навыки"].str.count(",")
        self._extracted_data.dropna(subset=[skills_column], inplace=True)

        data_source = self._context.data_sources.add_pandas(name=self._DATA_SOURCE_NAME)
        data_asset = data_source.add_dataframe_asset(name=self._DATA_ASSET_NAME)
        batch_definition = data_asset.add_batch_definition_whole_dataframe(self._BATCH_NAME)
        batch_parameters = {"dataframe": self._extracted_data}

        self._extracted_dataset = batch_definition.get_batch(batch_parameters=batch_parameters)

    def _verify_values_in_set(self, bounds: Sequence[Any]) -> bool:
        if self._extracted_dataset is None:
            return False

        min_value, max_value = bounds

        expectation = gx.expectations.ExpectColumnValuesToBeBetween(
            column=self._dataset_parameters["target_column"],
            min_value=min_value,
            max_value=max_value,
        )

        validation_results = self._extracted_dataset.validate(expectation)
        success: bool = validation_results.success

        return success

    def _verify_kl_divergence_test(self, threshold: float) -> bool:
        if self._extracted_dataset is None:
            return False

        bins = [0, 50_000, 100_000, 250_000, 500_000]
        bin_labels = range(len(bins) - 1)
        self._verified_data["bin"] = pd.cut(
            self._verified_data["ЗП"],
            bins=bins,
            labels=bin_labels,
            include_lowest=True,
        )

        bin_counts = self._verified_data["bin"].value_counts(sort=False)
        total_values = bin_counts.sum()
        weights = (bin_counts / total_values).tolist()

        tail_weights = [0.0, 0.0]

        partition = {"bins": bins, "weights": weights, "tail_weights": tail_weights}

        expectation = gx.expectations.ExpectColumnKLDivergenceToBeLessThan(
            column=self._dataset_parameters["target_column"],
            partition_object=partition,
            threshold=threshold,
        )

        validation_results = self._extracted_dataset.validate(expectation)
        logger.debug(validation_results)
        success: bool = validation_results.success

        return success

    def _verify_z_score_test(self, threshold: float) -> bool:
        if self._extracted_dataset is None:
            return False

        expectation = gx.expectations.ExpectColumnValueZScoresToBeLessThan(
            column=self._dataset_parameters["target_column"],
            threshold=threshold,
            double_sided=True,
        )

        validation_results = self._extracted_dataset.validate(expectation)
        logger.debug(validation_results)
        success: bool = validation_results.success

        return success

    def _verify_ks_test(self, p_value: float) -> bool:
        _, p_value = ks_2samp(self._verified_data["skills_count"], self._extracted_data["skills_count"])
        logger.debug(p_value)
        result: bool = p_value >= self._dataset_parameters["ks_test_p_value"]
        return result
