from unittest.mock import Mock, patch

import pytest
from requests.models import HTTPError

from panoramic.cli.errors import RefreshException, SourceNotFoundException
from panoramic.cli.metadata.client import TERMINAL_STATES, JobState
from panoramic.cli.refresh import Refresher


@patch('panoramic.cli.refresh.MetadataClient')
def test_refresher_completed(mock_client):
    mock_client.return_value.create_refresh_job.return_value = 'test-job-id'
    mock_client.return_value.wait_for_terminal_state.return_value = JobState.COMPLETED

    Refresher('test-company', 'test-source').refresh_table('table-name')


@pytest.mark.parametrize('final_state', list(TERMINAL_STATES - {JobState.COMPLETED}))
@patch('panoramic.cli.refresh.MetadataClient')
def test_refresher_non_completed(mock_client, final_state):
    mock_client.return_value.create_refresh_job.return_value = 'test-job-id'
    mock_client.return_value.wait_for_terminal_state.return_value = final_state

    with pytest.raises(RefreshException):
        Refresher('test-company', 'test-source').refresh_table('table-name')


@patch('panoramic.cli.refresh.MetadataClient')
def test_refresher_not_found(mock_client):
    mock_client.return_value.create_refresh_job.side_effect = HTTPError(response=Mock(status_code=404))

    with pytest.raises(SourceNotFoundException):
        Refresher('test-company', 'test-source').refresh_table('table-name')


@patch('panoramic.cli.refresh.MetadataClient')
def test_refresher_generic_error(mock_client):
    mock_client.return_value.create_refresh_job.side_effect = HTTPError(response=Mock(status_code=500))

    with pytest.raises(RefreshException):
        Refresher('test-company', 'test-source').refresh_table('table-name')
