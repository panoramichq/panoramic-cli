import logging
import re
from collections import defaultdict
from copy import deepcopy
from typing import Dict, Optional

import click
from tqdm import tqdm

from panoramic.cli.config.storage import update_context
from panoramic.cli.connection import Connection
from panoramic.cli.diff import echo_diff
from panoramic.cli.errors import JoinException, ValidationError, ValidationErrorSeverity
from panoramic.cli.husky.common.enum import EnumHelper
from panoramic.cli.husky.federated.join_detection.detect import (
    detect_joins as detect_join_for_models,
)
from panoramic.cli.husky.federated.transform.exceptions import UnsupportedDialectError
from panoramic.cli.husky.service.types.enums import HuskyQueryRuntime
from panoramic.cli.local import get_state as get_local_state
from panoramic.cli.local.executor import LocalExecutor
from panoramic.cli.local.writer import FileWriter
from panoramic.cli.metadata.scanner import Scanner
from panoramic.cli.pano_model import PanoField, PanoModel, PanoModelJoin
from panoramic.cli.print import echo_error, echo_errors, echo_info, echo_warnings
from panoramic.cli.scan import scan_fields_for_errors
from panoramic.cli.state import Action, ActionList
from panoramic.cli.validate import (
    validate_context,
    validate_local_state,
    validate_missing_files,
    validate_orphaned_files,
)

logger = logging.getLogger(__name__)


def configure():
    """Just create an empty config file"""
    update_context('auth', {})


def validate() -> bool:
    """Check local files against schema."""
    errors = []

    try:
        validate_context()
    except ValidationError as e:
        errors.append(e)

    errors.extend(validate_local_state())

    errors_by_severity = defaultdict(list)
    for error in errors:
        errors_by_severity[error.severity].append(error)

    if len(errors_by_severity[ValidationErrorSeverity.WARNING]) > 0:
        echo_warnings(errors_by_severity[ValidationErrorSeverity.WARNING])
        echo_info('')

    if len(errors_by_severity[ValidationErrorSeverity.ERROR]) > 0:
        echo_errors(errors_by_severity[ValidationErrorSeverity.ERROR])
        return False

    echo_info("Success: All files are valid.")
    return True


def scan(filter_reg_ex: Optional[str] = None):
    """Scan all metadata for given source and filter."""

    connection_info = Connection.get()
    dialect_name = Connection.get_dialect_name(connection_info)

    query_runtime = EnumHelper.from_value_safe(HuskyQueryRuntime, dialect_name)
    if not query_runtime:
        raise UnsupportedDialectError(dialect_name)

    scanner_cls = Scanner.get_scanner(query_runtime)
    scanner = scanner_cls()

    echo_info('Started scanning the data source')
    scanner.scan(force_reset=True)
    echo_info('Finished scanning the data source')

    # apply regular expression as a filter on model names
    if filter_reg_ex:
        re_compiled = re.compile(filter_reg_ex)
        models = [model for model in scanner.models.values() if re_compiled.match(model.model_name)]
    else:
        models = list(scanner.models.values())

    if len(scanner.models) == 0:
        echo_info('No tables have been found')
        return

    progress_bar = tqdm(total=len(scanner.models))
    writer = FileWriter()
    for model in models:
        writer.write_scanned_model(model)
        progress_bar.write(f'Discovered model {model.model_name}')

        progress_bar.update()

    progress_bar.write(f'Scanned {progress_bar.total} tables')


def detect_joins(target_dataset: Optional[str] = None, diff: bool = False, overwrite: bool = False, yes: bool = False):
    echo_info('Loading local state...')
    local_state = get_local_state(target_dataset=target_dataset)

    if local_state.is_empty:
        echo_info('No datasets to detect joins on')
        return

    models_by_virtual_data_source: Dict[Optional[str], Dict[str, PanoModel]] = defaultdict(dict)
    for model in local_state.models:
        # Prepare a mapping for a quick access when reconciling necessary changes later
        models_by_virtual_data_source[model.virtual_data_source][model.model_name] = model

    action_list: ActionList[PanoModel] = ActionList()

    with tqdm(list(local_state.data_sources)) as bar:
        for dataset in bar:
            try:
                bar.write(f'Detecting joins for dataset {dataset.dataset_slug}')
                joins_by_model = detect_join_for_models([dataset.dataset_slug])

                for model_name, joins in joins_by_model.items():
                    if not joins:
                        bar.write(f'No joins detected for {model_name} under dataset {dataset.dataset_slug}')
                        continue

                    bar.write(f'Detected {len(joins)} joins for {model_name} under dataset {dataset.dataset_slug}')

                    detected_join_objects = [PanoModelJoin.from_dict(join_dict) for join_dict in joins]
                    current_model = models_by_virtual_data_source[dataset.dataset_slug][model_name]
                    desired_model = deepcopy(current_model)

                    if overwrite:
                        desired_model.joins = detected_join_objects
                    else:
                        for detected_join in detected_join_objects:
                            # Only append joins that are not already defined
                            if detected_join not in current_model.joins:
                                desired_model.joins.append(detected_join)

                    action_list.actions.append(Action(current=current_model, desired=desired_model))

            except JoinException as join_exception:
                bar.write(f'Error: {str(join_exception)}')
                logger.debug(str(join_exception), exc_info=True)
            except Exception:
                error_msg = f'An unexpected error occured when detecting joins for {dataset.dataset_slug}'
                bar.write(f'Error: {error_msg}')
                logger.debug(error_msg, exc_info=True)
            finally:
                bar.update()

    if action_list.is_empty:
        echo_info('No joins detected')
        return

    echo_diff(action_list)
    if diff:
        # User decided to see the diff only
        return

    if not yes and not click.confirm('Do you want to proceed?'):
        # User decided not to update local models based on join suggestions
        return

    echo_info('Updating local state...')

    executor = LocalExecutor()
    for action in action_list.actions:
        try:
            executor.execute(action)
        except Exception:
            echo_error(f'Error: Failed to execute action {action.description}')
        echo_info(f'Updated {executor.success_count}/{executor.total_count} models')


def delete_orphaned_fields(target_dataset: Optional[str] = None, yes: bool = False):
    """Delete orphaned field files."""
    echo_info('Loading local state...')
    state = get_local_state(target_dataset=target_dataset)

    action_list: ActionList[PanoField] = ActionList()

    for dataset, (fields, models) in state.get_objects_by_package().items():
        fields_by_slug = {f.slug: f for f in fields}
        for idx, error in enumerate(validate_orphaned_files(fields, models, package_name=dataset)):
            if idx == 0:
                echo_info(f'\nFields without calculation or reference in a model in dataset {dataset}:')
            echo_info(f'  {error.field_slug}')
            # Add deletion action
            action_list.add_action(Action(current=fields_by_slug[error.field_slug], desired=None))

    if action_list.is_empty:
        echo_info('No issues found')
        return

    echo_info('')
    if not yes and not click.confirm('You will not be able to query these fields. Do you want to remove them?'):
        # User decided not to fix issues
        return

    echo_info('Updating local state...')

    executor = LocalExecutor()
    for action in action_list.actions:
        try:
            executor.execute(action)
        except Exception:
            echo_error(f'Error: Failed to execute action {action.description}')
    echo_info(f'Updated {executor.success_count}/{executor.total_count} fields')


def scaffold_missing_fields(target_dataset: Optional[str] = None, yes: bool = False, no_remote: bool = True):
    """Scaffold missing field files."""
    echo_info('Loading local state...')
    state = get_local_state(target_dataset=target_dataset)

    errors = []

    for dataset, (fields, models) in state.get_objects_by_package().items():
        for idx, error in enumerate(validate_missing_files(fields, models, package_name=dataset)):
            if idx == 0:
                echo_info(f'\nFields referenced in models without definition in dataset {dataset}:')
            echo_info(f'  {error.field_slug}')
            errors.append(error)

    if len(errors) == 0:
        echo_info('No issues found')
        return

    echo_info('')
    if not yes and not click.confirm(
        'You will not be able to query these fields until you define them. Do you want to do that now?'
    ):
        # User decided not to fix issues
        return

    loaded_models: Dict[str, PanoModel] = {}
    if not no_remote:
        connection = Connection.get()
        dialect_name = Connection.get_dialect_name(connection)
        query_runtime = EnumHelper.from_value_safe(HuskyQueryRuntime, dialect_name)

        scanner_cls = Scanner.get_scanner(query_runtime)
        scanner = scanner_cls()

        echo_info('Scanning remote storage...')
        scanner.scan()
        echo_info('Finished scanning remote storage...')
        loaded_models = scanner.models

    echo_info('Scanning fields...')
    fields = scan_fields_for_errors(errors, loaded_models)
    action_list = ActionList(actions=[Action(desired=field) for field in fields])

    echo_info('Updating local state...')

    executor = LocalExecutor()
    for action in action_list.actions:
        try:
            executor.execute(action)
        except Exception:
            echo_error(f'Error: Failed to execute action {action.description}')
    echo_info(f'Updated {executor.success_count}/{executor.total_count} fields')
