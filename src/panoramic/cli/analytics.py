import logging
import platform
import shutil
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

# Unfortunate naming choice of Segment analytics python lib.
# May be fixed in future, follow: https://github.com/segmentio/analytics-python/issues/154 .
import analytics  # type: ignore

from panoramic.cli.__version__ import __version__
from panoramic.cli.config.analytics import get_write_key
from panoramic.cli.config.analytics import is_enabled as config_is_enabled
from panoramic.cli.config.auth import get_client_id
from panoramic.cli.config.storage import read_config, update_config
from panoramic.cli.file_utils import (
    append_json_line,
    read_json_lines,
    read_yaml,
    truncate_file,
    write_yaml,
)
from panoramic.cli.paths import Paths
from panoramic.cli.print import echo_info

logger = logging.getLogger(__name__)

MINIMAL_FLUSH_DURATION = timedelta(minutes=10)
MINIMAL_FLUSH_EVENTS = 10

"""
Usage: currently only way to track data is by using `write_command_event` after execution of each CLI command.
If we decide to extend analytics with extra information, implement your own method like `write_request_event`
that will track your data, but there is no need to call flush at the end.
Data will be stored to events journal and flushed based on conditions set here.
"""


def write_command_event(name: str, group: str, start_time: float, error: str = '') -> None:
    """
    Generate command execution context and store it in analytics events file as JSON line.
    Truncate blacklisted parameters, like password.
    Try to flush data to external source based on various conditions.
    """
    if not is_enabled():
        return

    command_name = ' '.join((group, name))
    properties = {
        'type': command_name,
        'name': command_name,
        'duration_seconds': time.time() - start_time,
        'error': error,
        'timestamp': datetime.now().isoformat(),
        'platform': platform.platform(),
        'app_version': __version__,
        'python_version': platform.python_version(),
        'user_id': _get_user_id(),
        'success': error == '',
    }

    # Store event into temporary JSON lines events file.
    append_json_line(Paths.analytics_events_file(), properties)

    # Do not crash the command because we are unable to connect with external source.
    try:
        _flush()
    except Exception as e:
        logger.debug("Analytics flush failed.", exc_info=e)


def is_enabled() -> bool:
    """Check if user opted out of usage metrics recording."""
    if not config_is_enabled():
        return False

    config = read_config('analytics')
    return config.get('enabled', False)


def opt_in_command() -> None:
    """CLI command to opt in to anonymous usage analytics."""
    update_config('analytics', {'enabled': True})


def opt_out_command() -> None:
    """CLI command to opt out off anonymous usage analytics."""
    update_config('analytics', {'enabled': False})

    if Paths.analytics_dir().is_dir():
        shutil.rmtree(Paths.analytics_dir())


def show_tracking_id_command() -> None:
    """CLI command to display users anonymous tracking id."""
    echo_info(_get_user_id())


def _get_user_id() -> str:
    """Get client_id from configuration step. If doesn't exist yet use just empty string."""
    try:
        return get_client_id()
    except KeyError:
        return ''

    # Old implementation, can be removed later when above solution if verified and released.
    # """Look for unique user ID that is used while storing events.
    # If no user ID was found, generate new id for user and store it in analytics metadata file.
    # """
    # metadata = _read_metadata()
    # if 'user_id' in metadata:
    #     return metadata['user_id']
    #
    # user_id = str(uuid.uuid4())
    # _update_metadata({'user_id': user_id})
    # return user_id


def _read_events() -> List[Dict[str, Any]]:
    """Read all events from JSON lines events file."""
    events_file = Paths.analytics_events_file()
    if not events_file.is_file():
        return []

    return read_json_lines(Paths.analytics_events_file())


def _read_metadata() -> Dict[str, Any]:
    """Read analytics metadata file containing information like last flush time or anonymous unique user ID."""
    metadata_file = Paths.analytics_metadata_file()
    if not metadata_file.is_file():
        return {}

    metadata = read_yaml(metadata_file)
    return metadata


def _update_metadata(data: Dict[str, Any]) -> None:
    """Update analytics metadata file. For example to update last flush time."""
    metadata = _read_metadata()
    metadata.update(data)
    write_yaml(Paths.analytics_metadata_file(), metadata)


def _get_last_flush_time() -> datetime:
    """Datetime of last flush."""
    metadata = _read_metadata()
    if 'last_flush_time' in metadata:
        return datetime.fromtimestamp(metadata['last_flush_time'])
    return datetime.now()


def _flush() -> None:
    """Perform flush to external system.
    This will iterate through all events from current interval and push them to external Analytics system.
    After all events were successfully sent it will truncate events and update last flush time interval.
    """
    proceed = False
    # Check if flush happen more than MINIMAL_FLUSH_DURATION ago.
    if _get_last_flush_time() < datetime.now() - MINIMAL_FLUSH_DURATION:
        proceed = True

    # Check if number of events since last flush if bigger than MINIMAL_FLUSH_EVENTS threshold.
    events = _read_events()
    if len(events) > MINIMAL_FLUSH_EVENTS:
        proceed = True

    if not proceed:
        return

    analytics.write_key = get_write_key()
    if analytics.write_key is None:
        return

    logger.debug("Flushing analytics events.")

    # Because sending of analytics is asynchronous, only way to get error is by adding an on_error callback,
    # As we don't want to delete events that weren't sent we specify analytics_sent_successfully
    # that can be overridden inside this callback to keep the events.
    analytics_sent_successfully = True

    def on_error(*args, **kwargs):
        logger.debug("Failed to send analytics events.", *args, **kwargs)
        nonlocal analytics_sent_successfully
        analytics_sent_successfully = False

    analytics.on_error = on_error
    for event in events:
        event_type = event.pop('type')
        timestamp = datetime.fromisoformat(event['timestamp'])
        analytics.track(user_id=_get_user_id(), event=event_type, properties=event, timestamp=timestamp)

    # Batching is automatic according to:
    # https://segment.com/docs/connections/sources/catalog/libraries/server/python/#batching
    analytics.flush()

    # Truncate events file after all events were flushed and store time of the flush.
    if analytics_sent_successfully:
        truncate_file(Paths.analytics_events_file())
        _update_metadata({'last_flush_time': time.time()})
