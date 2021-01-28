import logging

from panoramic.cli.connection import Connection
from panoramic.cli.errors import ConnectionNotFound, TransformExecutionFailed
from panoramic.cli.transform.pano_transform import CompiledTransform

logger = logging.getLogger(__name__)


class TransformExecutor:
    @classmethod
    def execute(cls, compiled_transform: CompiledTransform):
        try:
            connection = Connection.get()
        except ValueError:
            raise ConnectionNotFound()

        try:
            logger.debug(f'Executing transform {compiled_transform.transform.name}')
            # TODO: Consider moving into SQL Executor class that manages connection state
            Connection.execute(sql=compiled_transform.compiled_query, connection=connection)

            logger.debug(f'Verifying transform {compiled_transform.transform.name}')
            Connection.execute(sql=compiled_transform.correctness_query, connection=connection)
        except Exception:
            raise TransformExecutionFailed(
                transform_name=compiled_transform.transform.name,
                compiled_sql=compiled_transform.compiled_query,
            )
