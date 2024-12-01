class ServiceError(Exception):
    def __init__(self, message: str):
        super().__init__(message)

    def __str__(self):
        return self._stringify()

    def __repr__(self):
        return self._stringify()

    def _stringify(self):
        return Exception.__str__(self)
