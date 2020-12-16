import logging

from dbt.exceptions import FailedToConnectException

from panoramic.cli.connections import (
    Connections,
    fill_dbt_required_connection_keys,
    get_dialect_credentials,
)
from panoramic.cli.errors import (
    ConnectionFormatException,
    ConnectionNotFound,
    TransformExecutionFailed,
)
from panoramic.cli.transform.pano_transform import CompiledTransform

logger = logging.getLogger(__name__)


class TransformExecutor:
    @classmethod
    def execute(cls, compiled_transform: CompiledTransform):
        connection_name = compiled_transform.transform.connection_name
        try:
            connection_credentials_dict = Connections.get_by_name(connection_name)
        except ValueError:
            raise ConnectionNotFound(connection_name)

        connection_credentials_dict = fill_dbt_required_connection_keys(connection_credentials_dict)
        credentials, credential_error = get_dialect_credentials(connection_credentials_dict)

        if credential_error is not None:
            raise ConnectionFormatException(connection_name, credential_error)

        try:
            logger.debug(f'Executing transform {compiled_transform.transform.name} on {connection_name}')
            # TODO: Consider moving into SQL Executor class that manages connection state
            Connections.execute(sql=compiled_transform.compiled_query, credentials=credentials)

            logger.debug(
                f'Verifying transform {compiled_transform.transform.name} on {compiled_transform.transform.connection_name}'
            )
            Connections.execute(sql=compiled_transform.correctness_query, credentials=credentials)
        except FailedToConnectException:
            raise
        except Exception:
            raise TransformExecutionFailed(
                transform_name=compiled_transform.transform.name,
                connection_name=connection_name,
                compiled_sql=compiled_transform.compiled_query,
            )
