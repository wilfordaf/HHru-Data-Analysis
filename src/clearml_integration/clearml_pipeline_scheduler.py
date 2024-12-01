from typing import Any, Dict

from clearml import Task
from clearml.automation import TaskScheduler

from src.clearml_integration import SERVICE_PROJECT_NAME
from src.clearml_integration.clearml_pipeline_controller import ClearMLPipelineController


def create_scheduler(pipeline_task_name: str, schedule: Dict[str, Any], task_queue: str) -> None:
    scheduler_task_name = f"Регламентное задание для создания отчёта проекта: {pipeline_task_name}"

    task = Task.get_task(task_name=pipeline_task_name, allow_archived=False)

    scheduler = TaskScheduler(
        sync_frequency_minutes=30,
        force_create_task_project=SERVICE_PROJECT_NAME,
        force_create_task_name=scheduler_task_name,
    )
    scheduler.add_task(
        schedule_task_id=task.task_id,
        queue=task_queue,
        execute_immediately=False,
        **schedule,
    )

    scheduler.start_remotely(queue="services")


if __name__ == "__main__":
    clearml_pipeline_parameters = ClearMLPipelineController().clearml_pipeline_parameters
    create_scheduler(
        pipeline_task_name=clearml_pipeline_parameters["common_properties"]["name"],
        schedule=clearml_pipeline_parameters["schedule"],
        task_queue=clearml_pipeline_parameters["common_properties"]["pipeline_execution_queue"],
    )
