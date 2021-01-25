from typing import Any, Dict, List, Optional


class PanoTransform:
    API_VERSION = 'v1'

    name: str
    fields: List[str]
    target: str
    datasets: Optional[List[str]]
    filters: Optional[str]

    def __init__(
        self,
        name: str,
        fields: List[str],
        target: str,
        datasets: Optional[List[str]] = None,
        filters: Optional[str] = None,
    ):
        self.name = name
        self.fields = fields
        self.target = target
        self.datasets = datasets
        self.filters = filters

    @classmethod
    def from_dict(cls, inputs: Dict[str, Any]):
        return cls(
            name=inputs['name'],
            fields=inputs['fields'],
            target=inputs['target'],
            datasets=inputs.get('datasets'),
            filters=inputs.get('filters'),
        )

    def to_dict(self) -> Dict[str, Any]:
        data = {'api_version': self.API_VERSION, 'name': self.name, 'fields': self.fields, 'target': self.target}
        if self.filters is not None:
            data['filters'] = self.filters

        return data

    @property
    def connection_name(self) -> str:
        return self.target.split('.', 1)[0]

    @property
    def view_path(self) -> str:
        return self.target.split('.', 1)[1]


class CompiledTransform:
    transform: PanoTransform
    company_id: str
    compiled_query: str

    def __init__(self, transform: PanoTransform, company_id: str, compiled_query: str):
        self.transform = transform
        self.company_id = company_id
        self.compiled_query = compiled_query

    @property
    def correctness_query(self) -> str:
        return f'SELECT * from {self.transform.view_path} limit 1'
