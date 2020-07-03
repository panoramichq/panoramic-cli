import responses

from panoramic.cli.supported_version import URL, is_version_supported


@responses.activate
def test_not_found():
    responses.add(responses.GET, URL, json={'error': 'not found'}, status=404)
    assert is_version_supported("0.1.0") is False


@responses.activate
def test_empty_answer():
    responses.add(responses.GET, URL, json='')
    assert is_version_supported("0.1.0") is False


@responses.activate
def test_invalid_json():
    responses.add(responses.GET, URL, json='{"": }')
    assert is_version_supported("0.1.0") is False


@responses.activate
def test_success():
    responses.add(responses.GET, URL, json={"minimum_supported_version": "0.1.0"})
    assert is_version_supported("0.1.0") is True


@responses.activate
def test_success_with_cli_type():
    responses.add(responses.GET, URL, json={"minimum_supported_version": "0.1.0-python"})
    assert is_version_supported("0.1.0") is True


@responses.activate
def test_failure():
    responses.add(responses.GET, URL, json={"minimum_supported_version": "0.1.1"})
    assert is_version_supported("0.1.0") is False
