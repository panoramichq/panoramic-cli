from typing import Any, Dict


class RowProxyDictMock:
    def __init__(self, values: Dict[str, Any]):
        self._values = values

    def items(self):
        return self._values.items()

    def __getattr__(self, attr):
        return self._values[attr]

    def __iter__(self):
        return iter(self.items())
