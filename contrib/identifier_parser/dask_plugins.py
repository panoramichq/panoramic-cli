import logging
from types import TracebackType
from typing import Optional, cast

from common.exception_enums import ExceptionGroup
from common.exception_handler import ExceptionHandler
from dask.utils import funcname
from distributed import Scheduler, Worker
from distributed.diagnostics.plugin import SchedulerPlugin, WorkerPlugin

from panoramic.cli.datacol.instrumentation.measurement import Measure


class SchedulerMonitor(SchedulerPlugin):
    def __init__(self, scheduler: Scheduler, component_name: str = ''):
        super().__init__()
        self.component_name = component_name
        self.scheduler = scheduler

    def add_worker(self, scheduler: Optional[Scheduler] = None, worker: Optional[Worker] = None, **kwargs):
        if scheduler is not None:
            Measure.gauge('dask.workers', tags={'component': self.component_name})(len(scheduler.workers))

    def remove_worker(self, scheduler: Optional[Scheduler] = None, worker: Optional[Worker] = None, **kwargs):
        if scheduler is not None:
            Measure.gauge('dask.workers', tags={'component': self.component_name})(len(scheduler.workers))


class WorkerMonitor(WorkerPlugin):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger('dask.worker.worker_monitor')

    def setup(self, worker: Worker):
        self.worker = worker  # noqa

    def transition(self, key: str, start: str, finish: str, *args, **kwargs):
        if finish == 'error' and key in self.worker.exceptions and key in self.worker.tasks:
            serialized_exc = self.worker.exceptions[key]

            if hasattr(serialized_exc, 'data') and isinstance(serialized_exc.data, Exception):
                tb = self.worker.tracebacks.get(key)
                exc = (
                    serialized_exc.data.with_traceback(tb.data)
                    if (tb and hasattr(tb, 'data') and isinstance(tb.data, TracebackType))
                    else serialized_exc.data
                )
                exc = cast(Exception, exc)

                function, fn_args, fn_kwargs = self.worker.tasks[key]
                function_name = str(funcname(function))

                extra_data = {
                    "task_id": key,
                    "task_name": function_name,
                    "args": fn_args,
                    "kwargs": fn_kwargs,
                    'dask_worker_name': self.worker.name,
                }

                ExceptionHandler.track_exception(
                    exc,
                    exc_group=ExceptionGroup.UNHANDLED_DASK_TASK,
                    message=None,
                    ddog_tags={'dask_worker_name': self.worker.name, 'function_name': function_name},
                )
