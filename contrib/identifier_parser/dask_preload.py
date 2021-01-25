from typing import Union

import click
from config.identifier_parser import DASK_SCHEDULER_ADDRESS
from distributed import Client, Scheduler, Worker
from federated.identifier_parser.configuration import install_signal_handlers
from federated.identifier_parser.dask_plugins import SchedulerMonitor, WorkerMonitor


@click.command()
def dask_setup(service: Union[Scheduler, Worker]):
    """
    Preload scripts for both the Scheduler as well as the Worker

    References:
    * Lifecycle and CLI integration: https://docs.dask.org/en/latest/setup/custom-startup.html
    * Developing plugins: https://distributed.dask.org/en/latest/plugins.html
    """

    # Use pickle version 5 for faster ser/de
    # Note that Dask uses back-ported pickle5 by default when available, so leaving this here for clarity
    # see: https://github.com/dask/distributed/blob/2.25.0/distributed/protocol/pickle.py#L6-L12
    # register_serialization_family('pickle', pickle5_dumps, pickle5_loads)

    # Scheduler specific
    if isinstance(service, Scheduler):
        # Plugin for monitoring & metrics
        scheduler_monitor_plugin = SchedulerMonitor(scheduler=service, component_name='idparser')
        service.add_plugin(scheduler_monitor_plugin)

        # Gracefully shut down the scheduler
        # (FIXME: remove once https://github.com/dask/distributed/pull/3332 gets merged & released)
        async def on_signal(_signum):
            await service.close()

        install_signal_handlers(service.loop, cleanup=on_signal)

    # Worker specific
    elif isinstance(service, Worker):
        # Plugin for monitoring & metrics
        worker_monitor_plugin = WorkerMonitor()
        with Client(address=DASK_SCHEDULER_ADDRESS, timeout=30, name='worker-plugin-setup-client') as client:
            client.register_worker_plugin(worker_monitor_plugin, name='idparser-worker-monitor')
