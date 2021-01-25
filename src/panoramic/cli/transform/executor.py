import logging

from panoramic.cli.connections import Connections
from panoramic.cli.errors import ConnectionNotFound, TransformExecutionFailed
from panoramic.cli.transform.pano_transform import CompiledTransform

logger = logging.getLogger(__name__)


class TransformExecutor:
    @classmethod
    def execute(cls, compiled_transform: CompiledTransform):
        connection_name = compiled_transform.transform.connection_name
        try:
            connection = Connections.get_by_name(connection_name)
        except ValueError:
            raise ConnectionNotFound(connection_name)

        try:
            logger.debug(f'Executing transform {compiled_transform.transform.name} on {connection_name}')
            # TODO: Consider moving into SQL Executor class that manages connection state
            Connections.execute(sql=compiled_transform.compiled_query, connection=connection)

            logger.debug(
                f'Verifying transform {compiled_transform.transform.name} on {compiled_transform.transform.connection_name}'
            )
            Connections.execute(sql=compiled_transform.correctness_query, connection=connection)
        except Exception:
            raise TransformExecutionFailed(
                transform_name=compiled_transform.transform.name,
                connection_name=connection_name,
                compiled_sql=compiled_transform.compiled_query,
            )
