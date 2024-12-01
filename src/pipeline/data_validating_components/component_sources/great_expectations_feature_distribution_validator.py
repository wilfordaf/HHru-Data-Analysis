from typing import Any, Dict

import great_expectations as gx
import numpy as np
import pandas as pd
from great_expectations.core.batch import Batch
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.validator.validator import Validator
from scipy.stats import ks_2samp

from src.pipeline.data_validating_components.interfaces import IDataValidator


class GreatExpectationsFeatureDistributionValidator(IDataValidator):
    def __init__(
        self,
        verified_data: pd.DataFrame,
        extracted_data: pd.DataFrame,
        dataset_parameters: Dict[str, Any],
    ):
        self._dataset_parameters = dataset_parameters

        self._gx_verified_dataframe: Validator = self._create_validator(verified_data)
        self._gx_dataframe: Validator = self._create_validator(extracted_data)

    def validate_data(self) -> Dict[str, Any]:
        skills_column = "skills_count"

        verified_df = self._gx_verified_dataframe.execution_engine.get_batch_data
        extracted_df = self._gx_dataframe.execution_engine.dataframe

        verified_df[skills_column] = verified_df["Навыки"].str.count(",")
        extracted_df[skills_column] = extracted_df["Навыки"].str.count(",")

        verified_skills_count = verified_df[skills_column]
        hist, bin_edges = np.histogram(verified_skills_count, bins="auto", density=False)
        weights = hist / hist.sum()
        partition_object = {
            "bins": bin_edges.tolist(),
            "weights": weights.tolist(),
        }

        if not self._verify_kl_divergence(
            skills_column,
            partition_object,
            self._dataset_parameters["kl_divergence_threshold"],
        ):
            return {"success": False, "message": "Провален тест Кульбака-Лейблера"}

        _, p_value = ks_2samp(verified_skills_count, extracted_df[skills_column])

        if p_value < self._dataset_parameters["bootstrapped_ks_test_p_value"]:
            return {"success": False, "message": "Провален тест Колмогорова-Смирнова"}

        return {"success": True, "message": ""}

    def _verify_kl_divergence(
        self,
        column_name: str,
        partition_object: Dict[str, Any],
        threshold: float,
    ) -> bool:
        result: bool = self._gx_dataframe.expect_column_kl_divergence_to_be_less_than(
            column=column_name,
            partition_object=partition_object,
            threshold=threshold,
        )["success"]

        return result

    @staticmethod
    def _create_validator(data: pd.DataFrame) -> Validator:
        execution_engine = PandasExecutionEngine()

        batch = Batch(data=data)

        validator = Validator(
            execution_engine=execution_engine,
            batches=[batch],
        )

        return validator
