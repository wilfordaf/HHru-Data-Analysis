from typing import Any


def serialize(object: Any) -> bytes:
    import dill

    bytes_object: bytes = dill.dumps(object)
    return bytes_object


def deserialize(bytes_object: bytes) -> Any:
    import dill

    return dill.loads(bytes_object)
