import logging
import time
from typing import Iterable, List, Set

import dask
from config.identifier_parser import (
    IDENTIFIER_TASK_TIMEOUT_SECS,
    IDENTIFIER_TASK_WAIT_SECS,
)
from dask.utils import parse_timedelta
from distributed import Semaphore, get_client, rejoin, secede
from federated.identifier_parser.configuration import (
    ColumnOverflowStrategy,
    Configuration,
    ParameterStore,
)
from federated.identifier_parser.exceptions import (
    ColumnMetadataNotAvailable,
    SampleJobError,
    SampleJobTimeout,
    ServiceUnavailable,
    TooManyColumns,
    UnknownStrategy,
)
from federated.identifier_parser.heuristics import ColumnFilter
from federated.identifier_parser.parser import (
    IdentifierParser,
    ParserJob,
    ParserJobStore,
)
from federated.identifier_parser.sampler import PhysicalDataSourceSampler
from federated.metadata.models import MetadataColumnsInput
from federated.metadata.scan import (
    async_get_job_results,
    async_get_job_status,
    async_select_metadata_columns,
)

from panoramic.cli.datacol.instrumentation.measurement import Measure

logger = logging.getLogger(__name__)


def _extract_explorable_columns(physical_data_source: str, table_name: str, params: Configuration) -> Iterable[str]:
    job_id = async_select_metadata_columns(
        MetadataColumnsInput(source_name=physical_data_source, table_filter=table_name)
    )
    timeout = IDENTIFIER_TASK_TIMEOUT_SECS
    wait_time = IDENTIFIER_TASK_WAIT_SECS

    status = None
    while status is None or not status.is_terminal:
        time.sleep(wait_time)
        status = async_get_job_status(job_id).job_status
        timeout -= wait_time
        if timeout <= 0:
            raise SampleJobTimeout(job_id)

    if not status.is_completed:
        raise SampleJobError(job_id)

    async_results = list(async_get_job_results(job_id))

    if len(async_results) == 0:
        raise ColumnMetadataNotAvailable(physical_data_source, table_name)

    found_dimensions = ColumnFilter(params).find_dimensions(async_results)

    yield from found_dimensions


def _find_identifiers(
    physical_data_source: str, table_name: str, columns_to_consider: List[str], params: Configuration
) -> Set[str]:
    _task_start = time.perf_counter()

    # Check if the scheduler is available
    if getattr(get_client(timeout=30), 'scheduler', None) is None:
        raise ServiceUnavailable(physical_data_source, table_name, columns_to_consider)

    # Limit the number of processed tables via a semaphore, this is mainly used to control the workers' memory usage
    # as the sampled tables are submitted to each worker's memory for faster processing (less ser/de overhead).
    # Keep in mind that a table with 8 columns and 10000 rows roughly takes up 150-250MB of memory.
    with Semaphore(max_leases=params.table_retrieval_limit, name='table_retrieval_limit'):
        logger.info(f'Starting fetching and sampling {physical_data_source}.{table_name} with {columns_to_consider}')

        table_sample = PhysicalDataSourceSampler(params).sample(physical_data_source, table_name, columns_to_consider)

        logger.info(
            f'Fetching and sampling done {physical_data_source}.{table_name} in {time.perf_counter() - _task_start:.2f}'
        )

        with Semaphore(max_leases=params.table_processing_limit, name='table_processing_limit'):
            logger.info(f'Running the identifier parser on {physical_data_source}.{table_name}')

            _task_start = time.perf_counter()

            identifiers = IdentifierParser(params).parse(table_sample)

            Measure.histogram(
                'idparser_runtime',
                tags={
                    'physical_data_source': physical_data_source,
                    'table_name': table_name,
                    'num_columns': len(columns_to_consider),
                    'num_ids': len(identifiers),
                },
            )(time.perf_counter() - _task_start)

            return identifiers


def parse_identifiers_task(job_id: str, physical_data_source: str, table_name: str):
    # Estimated maximum / pessimistic processing time of a single table is around 10 minutes,
    # therefore we need to ensure that the semaphore isn't automatically released (via timeout) if the task is idle.
    # See: https://docs.dask.org/en/latest/configuration-reference.html#distributed.scheduler.locks.lease-timeout
    assert parse_timedelta(dask.config.get('distributed.scheduler.locks.lease-timeout', default='0s')) >= 600
    assert dask.config.get('distributed.comm.retry.count', default=0) > 0

    # detach the task from worker's thread-pool as we're mostly just waiting for Dremio to return the sampled dataset,
    # also we don't want to take up "scheduling slots" from other (compute-heavy) tasks as this might lead to deadlocks.
    secede()
    start_time = time.perf_counter()

    parser_job = ParserJob(job_id=job_id)

    try:
        ParserJobStore.store(parser_job)
        params = ParameterStore().get_config()

        columns_to_consider = list(_extract_explorable_columns(physical_data_source, table_name, params))

        Measure.histogram(
            'idparser_num_columns_per_table',
            tags={'physical_data_source': physical_data_source, 'table_name': table_name},
        )(len(columns_to_consider))

        if len(columns_to_consider) > params.max_allowed_dimensions:
            if params.column_overflow_strategy == ColumnOverflowStrategy.RAISE_EXCEPTION:
                raise TooManyColumns(physical_data_source, table_name, columns_to_consider)
            elif params.column_overflow_strategy == ColumnOverflowStrategy.SLICE_COLUMNS:
                logger.warning(
                    f'Source {physical_data_source}.{table_name} has too many columns. '
                    f'Slicing first {params.max_allowed_dimensions}'
                )
                columns_to_consider = columns_to_consider[0 : params.max_allowed_dimensions]
            else:
                raise UnknownStrategy(
                    params.column_overflow_strategy, physical_data_source, table_name, columns_to_consider
                )

        # early return for the trivial case
        if len(columns_to_consider) <= 1:
            identifiers = set(columns_to_consider)
        else:
            identifiers = _find_identifiers(physical_data_source, table_name, columns_to_consider, params)

        parser_job.identifiers = identifiers
        parser_job.status = 'COMPLETED'

        logger.info(
            f'Success! Finished running the identifier script on {physical_data_source}.{table_name} '
            f'in {time.perf_counter() - start_time:0.3f} (identifiers: {identifiers})'
        )
    except Exception as e:
        parser_job.status = 'FAILED'
        raise e  # Re-raise exception so it's handled and reported via our Dask plugin
    finally:
        ParserJobStore.store(parser_job)
        rejoin()  # reuse the existing thread by returning it to the worker's thread-pool
