import logging
from functools import partial
from typing import Any, Dict, List, Optional

from distributed import Scheduler
from distributed.http.proxy import GlobalProxyHandler
from distributed.scheduler import WorkerState
from tornado.web import RequestHandler

logger = logging.getLogger("distributed.panoramic")


class WorkerOnlyProxyHandler(GlobalProxyHandler):
    """
    Endpoint that allows proxying from Scheduler's HTTP server to Workers' HTTP server.

    Useful to remotely shutdown a specific worker or access worker's metrics, logs and dashboards.
    """

    def initialize(self, dask_server: Optional[Scheduler] = None, extra: Optional[Dict[Any, Any]] = None):
        super().initialize(dask_server=dask_server, extra=extra)

        worker_hosts: List[str] = (
            [
                worker_state.host for worker_state in dask_server.workers.values()  # type: WorkerState
            ]
            if dask_server
            else []
        )

        # override the whitelist function to proxy only to workers
        self.host_whitelist = partial(self.whitelist_workers, worker_hosts=worker_hosts)

    @staticmethod
    def whitelist_workers(_handler: RequestHandler, host: str, *, worker_hosts: List[str]):
        return any(host in worker_host for worker_host in worker_hosts)


# Export the HTTP routes
#
# https://docs.dask.org/en/latest/configuration-reference.html#distributed.scheduler.http.routes
# https://distributed.dask.org/en/latest/http_services.html
routes = [(r"proxy/(\d+)/(.*?)/(.*)", WorkerOnlyProxyHandler, {})]
