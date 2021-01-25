import logging

from distributed import Worker
from distributed.http.utils import RequestHandler

logger = logging.getLogger("distributed.panoramic")


class TerminationHandler(RequestHandler):
    """
    Custom HTTP handler to trigger a graceful shutdown via Kubernetes's Lifecycle Hooks

    reference: https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/
    """

    def get(self):
        self.server: Worker  # add typing information (Worker extends distributed.core.Server)

        logger.info(f"Lifecycle hook triggered. Initiating graceful shutdown of {self.server.name}.")

        self.server.io_loop.add_callback(self.server.close_gracefully)

        self.write({'message': 'Shutting down...', 'extra': {'worker_name': self.server.name}})
        self.set_header("Content-Type", "application/json")


# Export the HTTP routes
#
# https://docs.dask.org/en/latest/configuration-reference.html#distributed.worker.http.routes
# https://distributed.dask.org/en/latest/http_services.html
routes = [('graceful-shutdown', TerminationHandler, {})]
