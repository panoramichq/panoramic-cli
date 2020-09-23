from unittest.mock import Mock

import pytest
from requests.exceptions import RequestException

from panoramic.cli.errors import JoinException
from panoramic.cli.join.client import JobState, JoinClient
from panoramic.cli.join_detector import JoinDetector


def test_detect_joins_success():
    mock_client = Mock(JoinClient, autospec=True)
    detector = JoinDetector('test-company', client=mock_client)

    mock_client.create_join_detection_job.return_value = 'test-job-id'
    mock_client.wait_for_terminal_state.return_value = JobState.COMPLETED
    mock_client.get_job_results.return_value = {'joins': {}}

    assert {} == detector.detect('dataset')


def test_detect_joins_job_creation_failed():
    mock_client = Mock(JoinClient, autospec=True)
    detector = JoinDetector('test-company', client=mock_client)

    mock_client.create_join_detection_job.side_effect = RequestException('test')

    with pytest.raises(JoinException):
        detector.detect('dataset')


def test_detect_joins_job_failed():
    mock_client = Mock(JoinClient, autospec=True)
    detector = JoinDetector('test-company', client=mock_client)

    mock_client.create_join_detection_job.return_value = 'test-job-id'
    mock_client.wait_for_terminal_state.return_value = JobState.FAILED

    with pytest.raises(JoinException):
        detector.detect('dataset')


def test_generate_job_results_failed():
    mock_client = Mock(JoinClient, autospec=True)
    detector = JoinDetector('test--company', client=mock_client)

    mock_client.create_join_detection_job.return_value = 'test-job-id'
    mock_client.wait_for_terminal_state.return_value = JobState.COMPLETED
    mock_client.get_job_results.side_effect = RequestException('test')

    with pytest.raises(JoinException):
        detector.detect('dataset')
