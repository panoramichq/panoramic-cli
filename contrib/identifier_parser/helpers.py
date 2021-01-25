import logging
import pickle
import time
from contextlib import contextmanager
from typing import Any


@contextmanager
def timed_block(msg: str, logger: logging.Logger, log_level: Any = logging.INFO):
    start_time = time.perf_counter()

    try:
        yield start_time
    finally:
        logger.log(log_level, msg.format(time.perf_counter() - start_time))


# see: https://distributed.dask.org/en/latest/serialization.html#extend
def pickle5_dumps(x):
    header = {'serializer': 'pickle'}
    # TODO: use pickle5
    frames = [pickle.dumps(x)]
    return header, frames


def pickle5_loads(header, frames):
    if len(frames) > 1:
        frame = ''.join(frames)
    else:
        frame = frames[0]
    return pickle.loads(frame)
