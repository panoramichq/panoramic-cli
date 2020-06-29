import logging
import time

from panoramic.cli.metadata import MetadataClient


logger = logging.getLogger(__name__)

TERMINAL_STATES = {'COMPLETED', 'CANCELED', 'FAILED'}


class Refresher:

    """Scans metadata for a given source and scope."""

    source_id: str
    client: MetadataClient

    def __init__(self, source_id: str, client: MetadataClient = None):
        self.source_id = source_id

        if client is None:
            self.client = MetadataClient()

    def refresh_table(self, table_name: str):
        """Scan columns for a given source and scope."""
        logger.debug(f'Starting refresh metadata job for table {table_name}')
        job_id = self.client.create_refresh_job(self.source_id, table_name)
        logger.debug(f'Refresh metadata job with id {job_id} started for table {table_name}')

        self._wait_for_terminal_state(job_id)

    def _wait_for_terminal_state(self, job_id: str):
        """Wait for job to reach terminal state."""
        while True:
            logger.debug(f'Getting status for job with id {job_id}')
            status = self.client.get_job_status(job_id)
            logger.debug(f'Got status {status} for job with id {job_id}')
            if status in TERMINAL_STATES:
                return
            time.sleep(1)
