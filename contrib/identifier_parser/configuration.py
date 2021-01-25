from typing import List

from pydantic import BaseModel, Field

from panoramic.cli.husky.federated.identifier_parser.enums import ColumnOverflowStrategy
from panoramic.cli.husky.federated.identifier_parser.exceptions import (
    MissingConfiguration,
)


class Configuration(BaseModel):
    # Algorithmic constraints
    max_dimension_combinations: int = Field(
        default=5,
        title='Maximum number of dimension combinations',
        description='Upper bound on how many combinations to explore (inclusive, min=3, max=7).',
        ge=3,
        le=7,
    )
    max_allowed_dimensions: int = Field(
        default=8,
        title='Maximum number of dimensions for processing',
        description='Sets the limit on the number of dimensions/columns, if the limit is exceeded, an error is raised.',
        ge=1,
        le=8,
    )
    column_overflow_strategy: ColumnOverflowStrategy = Field(
        default=ColumnOverflowStrategy.SLICE_COLUMNS,
        title='Strategy for handling overflowing columns',
        description='How to handle "wide" tables that contain more than `max_allowed_dimensions` columns.',
    )
    # Data retrieval and sampling
    sample_size: int = Field(
        default=10_000, title='Sample rows', description='Controls the input sample size (number of rows).', le=20_000
    )
    population_size: int = Field(
        default=10_000,
        title='Population size',
        description='Controls the population size (limit) before applying the sampling.',
        le=20_000,
    )
    ignored_column_names: List[str] = Field(
        default_factory=lambda: [
            # common junk present in ETL vendors such as Fivetran
            'updated_at',
            'created_at',
            'processed_at',
            'cancelled_at',
            'deleted_at',
            'last_modified',
            'last_updated',
            # https://community.dremio.com/t/does-dremio-supports-native-json-jsonb-types-in-postgres-source/2056
            'data',
        ],
        title='Ignored column names',
        description='Which column names are explicitly not treated as dimensions.',
    )
    # Throughput related
    table_retrieval_limit: int = Field(
        default=3,
        title='Number of tables to retrieve simultaneously',
        description='How many tables can be fetched at the same by all Dask workers. '
        'Useful for controlling the memory usage of Dask workers, '
        'as each table\'s dataset is pinned to the distributed memory, '
        'meaning it is copied to each running process of all worker(s).',
        ge=1,
    )
    table_processing_limit: int = Field(
        default=2,
        title='Number of tables to process simultaneously',
        description='How many tables can be processed at the same by all Dask workers. '
        'Useful for controlling the memory usage of Dask workers, '
        'as each table\'s dataset is pinned to the distributed memory, '
        'meaning it is copied to each running process of all worker(s).',
        ge=1,
    )
    task_timeout: float = Field(
        default=600.0,
        title='Timeout for a single task',
        description='Timeout when awaiting a single table processing task.',
        ge=0.0,
    )


class ParameterStore:
    def get_config(self) -> Configuration:
        raise MissingConfiguration()
