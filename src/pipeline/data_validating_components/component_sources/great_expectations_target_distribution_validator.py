from typing import Any, Dict, Sequence

import great_expectations as gx
import pandas as pd

from src.pipeline.data_validating_components.interfaces import IDataValidator


class GreatExpectationsTargetDistributionValidator(IDataValidator):
    _DATA_SOURCE_NAME = "superjob"
    _DATA_ASSET_NAME = "superjob_parsed_data"
    _BATCH_NAME = "batch_1"

    def __init__(
        self,
        verified_data: pd.DataFrame,
        extracted_data: pd.DataFrame,
        target_column_name: str,
        dataset_parameters: Dict[str, Any],
    ):
        self._target_column_name = target_column_name
        self._dataset_parameters = dataset_parameters

        self._verified_data = verified_data

        context = gx.get_context()

        data_source = context.data_sources.add_pandas(name=self._DATA_SOURCE_NAME)
        data_asset = data_source.add_dataframe_asset(name=self._DATA_ASSET_NAME)
        batch_definition = data_asset.add_batch_definition_whole_dataframe(self._BATCH_NAME)
        batch_parameters = {"dataframe": extracted_data}

        self._batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    def validate_data(self) -> Dict[str, Any]:
        if not self._verify_values_in_set(self._dataset_parameters["bounds"]):
            return {"success": False, "message": "В данных есть образцы за корректными границами"}

        if not self._verify_kl_divergence_test(self._dataset_parameters["kl_divergence_threshold"]):
            return {"success": False, "message": "Провален тест Кульбака-Лейблера"}

        return {"success": True, "message": ""}

    def _verify_values_in_set(self, bounds: Sequence[Any]) -> bool:
        min_value, max_value = bounds

        expectation = gx.expectations.ExpectColumnValuesToBeBetween(
            column=self._target_column_name,
            min_value=min_value,
            max_value=max_value,
        )

        validation_results = self._batch.validate(expectation)
        print(validation_results)
        success: bool = validation_results.success

        return success

    def _verify_kl_divergence_test(self, p_value: float) -> bool:
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
            column=self._target_column_name,
            partition_object=partition,
            threshold=p_value,
        )

        validation_results = self._batch.validate(expectation)
        print(validation_results)
        success: bool = validation_results.success

        return success
