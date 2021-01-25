import itertools
import json
import logging
from datetime import timedelta
from typing import Any, Dict, List, Optional, Set, Tuple, cast
from uuid import uuid4

from dask.dataframe import DataFrame, from_pandas
from dask.dataframe.core import pd
from distributed import Client, Future, get_client, worker_client

from panoramic.cli.husky.federated.identifier_parser.configuration import (
    Configuration,
    ParameterStore,
)
from panoramic.cli.husky.federated.identifier_parser.exceptions import ParserJobNotFound
from panoramic.cli.husky.federated.identifier_parser.helpers import timed_block

ID_PARSER_REDIS_NAMESPACE = 'diesel-identifier-parser'
NON_NULL_VALUES_RATIO = 0.60

logger = logging.getLogger(__name__)
distributed_logger = logging.getLogger('distributed.worker.identifier_parser')


class IdentifierParser:
    def __init__(self, params: Optional[Configuration]):
        self.params = params if params is not None else ParameterStore().get_config()

    def parse(self, records: List[Dict[str, Any]]) -> Set[str]:
        with timed_block('[idparser] Processing candidate ids took {:.3f} seconds', logger, logging.INFO):
            candidate_ids = self._find_candidate_ids(records)

            if len(candidate_ids) == 0:
                return set()

            best_candidate = min(candidate_ids, key=lambda possible_dims: (-len(possible_dims), str(possible_dims)))

            return set(best_candidate)

    def _find_candidate_ids(self, records: List[Dict[str, Any]]) -> List[List[str]]:
        with timed_block('[idparser] Building local DataFrame took {:.3f} seconds', logger, logging.DEBUG):
            local_df = pd.DataFrame.from_records(records)

        with timed_block('[idparser] Converting to Dask DataFrame took {:.3f} seconds', logger, logging.DEBUG):
            df = from_pandas(local_df, chunksize=20_000, sort=False)

        with timed_block('[idparser] Creating a new client took {:.3f} seconds', logger, logging.DEBUG):
            client = get_client()

        with timed_block('[idparser] Submitting the sample to each worker took {:.3f} seconds', logger, logging.DEBUG):
            [distributed_df] = client.scatter([df], broadcast=True, direct=True, hash=False)
            distributed_df = cast(Future, distributed_df)

        with timed_block('[idparser] Submitting table task took {:.3f} seconds', logger, logging.DEBUG):
            future = client.submit(
                _process_table_identifiers,
                distributed_df,
                max_combination_length=self.params.max_dimension_combinations,
                pure=False,
                retries=1,
                # priority=10,
                key=f'process_table_ids_{str(uuid4())}',
            )

        with timed_block('[idparser] Waiting for table task to complete took {:.3f} seconds', logger, logging.DEBUG):
            candidate_ids: List[List[str]] = future.result(timeout=self.params.task_timeout)

        return candidate_ids


def _process_possible_identifier_combination(df: DataFrame, dimensions: List[str]) -> Tuple[List[str], int]:
    """
    Count how many duplicate groups (of a single dimension combination) are present within the table (DataFrame).
    If the table contains no duplicate groups, the set of provided dimensions is considered to uniquely identify each
    row of the given table, therefore it can be considered a primary key.

    NOTE: this method should only be used within the context of a Dask computation

    # Sample
    df =
    +------------+-------------+-------------+--------+
    | Account ID | Campaign ID | Objective   | Status |
    +============+=============+=============+========+
    | 1          | 1001        | link clicks | active |
    +------------+-------------+-------------+--------+
    | 1          | 1002        | video views | active |
    +------------+-------------+-------------+--------+
    | 2          | 1003        | link clicks | active |
    +------------+-------------+-------------+--------+

    ## Example 1
    dimensions = ['Account ID', 'Campaign ID']
    df.groupby(dimensions).size() = [
        [(1, 1001), 1],
        [(1, 1002), 1],
        [(1, 1003), 1],
    ]
    => num_duplicates = 0 (all groups are unique, their size is equal to 1)

    ## Example 2
    dimensions = ['Objective', 'Status']
    df.groupby(dimensions).size() = [
        [('link clicks', 'active'), 2],  # <= duplicate group
        [('video views', 'active'), 1],  # <= unique group
    ]
    => num_duplicates = 1 (some groups contain duplicates)

    """
    with timed_block(f'[idparser][{dimensions}] ' + 'combination took {:.3f} seconds', logger, logging.DEBUG):
        if len(dimensions) >= 1:
            num_duplicates = (df.groupby(dimensions).size() > 1).sum().compute()
        else:
            num_duplicates = 0

    return dimensions, num_duplicates


def _process_table_identifiers(
    pdf: DataFrame, dimension_combinations: Optional[List[List[str]]] = None, max_combination_length: int = 5
) -> List[List[str]]:
    """
    Dask wrapper around extracting identifiers from a single sampled table (pdf).

    This method submits multiple sub-tasks to identify possible identifier combinations, waits for them to complete
    and returns one or more dimension combinations.

    Note that the `worker_client` call forces the task to secede from the Worker's thread-pool, therefore it does not
    block any other computations and cannot cause a deadlock while waiting for sub-tasks to finish.
    """
    with timed_block('[idparser] Computing number of rows took {:.3f} seconds', logger, logging.DEBUG):
        num_rows = len(pdf)

    with timed_block('[idparser] Pruning columns took {:.3f} seconds', logger, logging.DEBUG):
        # filter out columns that contain at least X% null values - null values can't be parts of the primary key
        columns = [col for col, count in pdf.count().compute().items() if count / num_rows >= NON_NULL_VALUES_RATIO]

    with worker_client(separate_thread=True) as client:  # type: Client
        with timed_block('[idparser] Generating combinations took {:.3f} seconds', logger, logging.DEBUG):
            # explore all possible dimension combinations if none are provided
            if dimension_combinations is None:
                all_possible_combinations = itertools.chain.from_iterable(
                    itertools.combinations(columns, i) for i in range(1, min(max_combination_length, len(columns)) + 1)
                )
                generated_combinations: List[List[str]] = [
                    sorted(combination) for combination in all_possible_combinations
                ]
            else:
                generated_combinations = dimension_combinations

        with timed_block('[idparser] Waiting for all combination tasks took {:.3f} seconds', logger, logging.DEBUG):
            with timed_block('[idparser] Submitting all combination tasks took {:.3f} seconds', logger, logging.DEBUG):
                # submit "per dimension combination" tasks
                futures = client.map(
                    lambda combination: _process_possible_identifier_combination(pdf, combination),
                    generated_combinations,
                    key=[f'comb_{combination}_{str(uuid4())}' for combination in generated_combinations],
                    # priority=100,
                    # batch_size=32,
                    retries=2,
                )
            results = client.gather(futures)

    return [dimensions for dimensions, num_duplicates in results if num_duplicates == 0]


class ParserJob:
    def __init__(self, job_id: str, status: str = 'RUNNING', identifiers: Optional[Set[str]] = None):
        self.job_id = job_id
        self.status = status
        self.identifiers = identifiers

    def to_store_payload(self) -> Dict[str, Any]:
        if self.identifiers is None:
            return {'status': self.status}
        return {'status': self.status, 'identifiers': json.dumps(list(self.identifiers))}

    def to_dict(self) -> Dict[str, Any]:
        return {'job_id': self.job_id, 'status': self.status, 'identifiers': self.identifiers}

    def __eq__(self, other):
        return self.to_dict() == other.to_dict()

    def __repr__(self):
        return f'{self.job_id}|{self.status}'


class ParserJobStore:
    JOB_KEY = f'{ID_PARSER_REDIS_NAMESPACE}-jobs'

    @classmethod
    def store(cls, parser_job: ParserJob):
        logger.debug(f'Storing job {parser_job}')
        get_redis().hset(f'{cls.JOB_KEY}-{parser_job.job_id}', mapping=parser_job.to_store_payload())
        get_redis().expire(f'{cls.JOB_KEY}-{parser_job.job_id}', timedelta(days=1))

    @classmethod
    def get(cls, job_id: str) -> ParserJob:
        logger.debug(f'Retrieving job {job_id}')
        payload: Dict = get_redis().hgetall(f'{cls.JOB_KEY}-{job_id}')
        if not payload:
            raise ParserJobNotFound(job_id)

        payload = {k.decode('utf-8'): v.decode('utf-8') for k, v in payload.items()}

        return ParserJob(
            job_id=job_id,
            status=payload['status'],
            identifiers=None if payload.get('identifiers') is None else set(json.loads(payload['identifiers'])),
        )


class ParserJobAccess:
    _PERMISSIONS_KEY = f'{ID_PARSER_REDIS_NAMESPACE}-permissions'

    @classmethod
    def has_access(cls, principal: str, job_id: str) -> bool:
        logger.debug(f'Checking access to id parser job {job_id} for {principal}')
        stored_access = get_redis().get(f'{cls._PERMISSIONS_KEY}-{job_id}')
        stored_access = stored_access.decode() if stored_access else None
        return stored_access == principal

    @classmethod
    def store_access(cls, principal: str, job_id: str):
        logger.debug(f'Creating access to id parser job {job_id} for {principal}')
        get_redis().setex(f'{cls._PERMISSIONS_KEY}-{job_id}', timedelta(days=1), principal)
