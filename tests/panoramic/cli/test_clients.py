from unittest.mock import MagicMock

import pytest

from panoramic.cli.clients import VersionedClient


def test_versioned_client_with_session():
    mock_session = MagicMock()

    class TestClient(VersionedClient):
        def __init__(self):
            self.session = mock_session

    TestClient()

    assert mock_session.session.headers['User-Agent'] is not None


def test_versioned_client_with_no_session():
    class TestClient(VersionedClient):
        pass

    with pytest.raises(AssertionError):
        TestClient()
