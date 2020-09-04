import pytest
from requests.exceptions import RequestException
from requests.models import Response

from panoramic.cli.errors import RefreshException


def test_no_response_request_exception():
    with pytest.raises(
        RefreshException, match='^Metadata could not be refreshed for table some_table in data connection some_source$'
    ):
        e = RequestException('Failed to connect')
        raise RefreshException('some_source', 'some_table').extract_request_id(e)


def test_no_headers_request_exception():
    with pytest.raises(
        RefreshException, match='^Metadata could not be refreshed for table some_table in data connection some_source$'
    ):
        e = RequestException('Failed to connect', response={})
        raise RefreshException('some_source', 'some_table').extract_request_id(e)


def test_no_request_id_request_exception():
    with pytest.raises(
        RefreshException, match='^Metadata could not be refreshed for table some_table in data connection some_source$'
    ):
        e = RequestException('Failed to connect', response=Response())
        raise RefreshException('some_source', 'some_table').extract_request_id(e)


def test_valid_request_id_request_exception():
    with pytest.raises(
        RefreshException,
        match=r'^Metadata could not be refreshed for table some_table in data connection some_source \(RequestId\: some_request_id\)$',
    ):
        response = Response()
        response.headers['x-diesel-request-id'] = 'some_request_id'
        e = RequestException('Failed to connect', response=response)
        raise RefreshException('some_source', 'some_table').extract_request_id(e)
