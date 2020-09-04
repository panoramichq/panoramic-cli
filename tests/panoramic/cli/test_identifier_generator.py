from unittest.mock import Mock

import pytest
from requests.exceptions import RequestException

from panoramic.cli.errors import IdentifierException
from panoramic.cli.identifier.client import JobState
from panoramic.cli.identifier_generator import IdentifierGenerator


def test_generate_success():
    mock_client = Mock()
    generator = IdentifierGenerator('test--company', 'test-source', client=mock_client)

    mock_client.create_identifier_job.return_value = 'test-job-id'
    mock_client.wait_for_terminal_state.return_value = JobState.COMPLETED
    mock_client.get_job_results.return_value = {'identifiers': ['a', 'b']}

    assert ['a', 'b'] == generator.generate('test-table')


def test_generate_job_creation_failed():
    mock_client = Mock()
    generator = IdentifierGenerator('test--company', 'test-source', client=mock_client)

    mock_client.create_identifier_job.side_effect = RequestException('test')

    with pytest.raises(IdentifierException):
        generator.generate('test-table')


def test_generate_job_failed():
    mock_client = Mock()
    generator = IdentifierGenerator('test--company', 'test-source', client=mock_client)

    mock_client.create_identifier_job.return_value = 'test-job-id'
    mock_client.wait_for_terminal_state.return_value = JobState.FAILED

    with pytest.raises(IdentifierException):
        generator.generate('test-table')


def test_generate_job_results_failed():
    mock_client = Mock()
    generator = IdentifierGenerator('test--company', 'test-source', client=mock_client)

    mock_client.create_identifier_job.return_value = 'test-job-id'
    mock_client.wait_for_terminal_state.return_value = JobState.COMPLETED
    mock_client.get_job_results.side_effect = RequestException('test')

    with pytest.raises(IdentifierException):
        generator.generate('test-table')
