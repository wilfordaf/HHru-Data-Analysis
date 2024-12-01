import argparse

from clearml import PipelineDecorator

from src.clearml_integration.clearml_pipeline_controller import ClearMLPipelineController
from src.pipeline.abstractions.abstract_pipeline_controller import AbstractPipelineController
from src.pipeline.local_pipeline_controller import LocalPipelineController


def parse_args():
    parser = argparse.ArgumentParser(description="Выбор между локальным запуском и ClearML")
    parser.add_argument("--clearml", action="store_true", help="Взаимодействие пайплайна с ClearML")
    parser.add_argument("--local", action="store_true", help="Исполнение пайплайна на локальном ПК")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    pipeline: AbstractPipelineController
    if args.clearml and args.local:
        PipelineDecorator.run_locally()
        pipeline = ClearMLPipelineController()
    elif args.clearml:
        pipeline = ClearMLPipelineController()
    elif args.local:
        pipeline = LocalPipelineController()
    else:
        raise ValueError("Неверные аргументы командной строки")

    pipeline_method = pipeline.assemble_pipeline()
    pipeline_method()
