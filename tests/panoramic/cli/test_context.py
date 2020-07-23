from unittest.mock import Mock

import pytest
import yaml
from click.core import Context

from panoramic.cli.context import ContextAwareCommand, get_api_version, get_company_slug
from panoramic.cli.errors import (
    CriticalError,
    InvalidYamlFile,
    MissingContextFileException,
    MissingValueException,
)
from panoramic.cli.local.file_utils import PresetFileName


def test_context_aware_command_no_context(monkeypatch, tmpdir):
    """Check command fails when no context."""
    with pytest.raises(CriticalError):
        ContextAwareCommand(name='test-command').invoke(Mock())


def test_context_aware_command_context_exists(monkeypatch, tmpdir):
    """Check command succeeds when context exists."""
    monkeypatch.chdir(tmpdir)
    (tmpdir / PresetFileName.CONTEXT.value).ensure()

    def test_callback():
        return 10

    command = ContextAwareCommand(name='test-command', callback=test_callback)
    context = Context(command)

    assert command.invoke(context) == 10


def test_no_context_file(monkeypatch, tmpdir):
    monkeypatch.chdir(tmpdir)

    with pytest.raises(MissingContextFileException):
        get_api_version()

    with pytest.raises(MissingContextFileException):
        get_company_slug()


def test_invalid_context_file(monkeypatch, tmpdir):
    monkeypatch.chdir(tmpdir)

    with open(tmpdir / PresetFileName.CONTEXT.value, 'w') as f:
        f.write('api_version: some_value\ncompany_slug slug_but_missing_colon\n')

    with pytest.raises(
        InvalidYamlFile,
        match=f'while scanning a simple key\n  in "{tmpdir}/pano.yaml", line 2, column 1\ncould not find expected \':\'\n  in "{tmpdir}/pano.yaml", line 3, column 1',
    ):
        assert get_api_version()

    with pytest.raises(
        InvalidYamlFile,
        match=f'while scanning a simple key\n  in "{tmpdir}/pano.yaml", line 2, column 1\ncould not find expected \':\'\n  in "{tmpdir}/pano.yaml", line 3, column 1',
    ):
        assert get_company_slug()


def test_missing_value_context_file(monkeypatch, tmpdir):
    monkeypatch.chdir(tmpdir)

    with open(tmpdir / PresetFileName.CONTEXT.value, 'w') as f:
        f.write(yaml.dump(dict(company_slug='company_name_12fxs')))

    with pytest.raises(MissingValueException):
        get_api_version()

    monkeypatch.chdir(tmpdir)

    with open(tmpdir / PresetFileName.CONTEXT.value, 'w') as f:
        f.write(yaml.dump(dict(api_version='v2')))

    with pytest.raises(MissingValueException):
        get_company_slug()


def test_context_file(monkeypatch, tmpdir):
    monkeypatch.chdir(tmpdir)

    with open(tmpdir / PresetFileName.CONTEXT.value, 'w') as f:
        f.write(yaml.dump(dict(api_version='v2', company_slug='company_name_12fxs')))

    assert get_api_version() == 'v2'
    assert get_company_slug() == 'company_name_12fxs'
