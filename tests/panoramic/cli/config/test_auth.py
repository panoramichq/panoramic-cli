import os
import tempfile
from pathlib import Path

import pytest
import yaml

from panoramic.cli.config.auth import get_client_id, get_client_secret
from panoramic.cli.errors import (
    InvalidYamlFile,
    MissingConfigFileException,
    MissingValueException,
)
from tests.panoramic.cli.util import overwrite_env


def test_no_config_file():
    with tempfile.TemporaryDirectory() as tmpdirname, overwrite_env('HOME', tmpdirname):
        with pytest.raises(MissingConfigFileException):
            get_client_id()

        with pytest.raises(MissingConfigFileException):
            get_client_secret()


def test_invalid_config_file():
    with tempfile.TemporaryDirectory() as tmpdirname, overwrite_env('HOME', tmpdirname):
        os.mkdir(Path(tmpdirname) / '.pano')
        with open(Path(tmpdirname) / '.pano' / 'config', 'w') as f:
            f.write('client_id: some_value\nclient_secret slug_but_missing_colon\n')

        with pytest.raises(
            InvalidYamlFile,
            match=f'while scanning a simple key\n  in "{tmpdirname}/.pano/config", line 2, column 1\ncould not find expected \':\'\n  in "{tmpdirname}/.pano/config", line 3, column 1',
        ):
            assert get_client_id()

        with pytest.raises(
            InvalidYamlFile,
            match=f'while scanning a simple key\n  in "{tmpdirname}/.pano/config", line 2, column 1\ncould not find expected \':\'\n  in "{tmpdirname}/.pano/config", line 3, column 1',
        ):
            assert get_client_secret()


def test_missing_value_config_file():
    with tempfile.TemporaryDirectory() as tmpdirname, overwrite_env('HOME', tmpdirname):
        os.mkdir(Path(tmpdirname) / '.pano')
        with open(Path(tmpdirname) / '.pano' / 'config', 'w') as f:
            f.write(yaml.dump(dict(client_secret='some_random_value')))

        with pytest.raises(MissingValueException):
            get_client_id()

    with tempfile.TemporaryDirectory() as tmpdirname, overwrite_env('HOME', tmpdirname):
        os.mkdir(Path(tmpdirname) / '.pano')
        with open(Path(tmpdirname) / '.pano' / 'config', 'w') as f:
            f.write(yaml.dump(dict(client_id='another_random_value')))

        with pytest.raises(MissingValueException):
            get_client_secret()


def test_config_file():
    with tempfile.TemporaryDirectory() as tmpdirname, overwrite_env('HOME', tmpdirname):
        os.mkdir(Path(tmpdirname) / '.pano')
        with open(Path(tmpdirname) / '.pano' / 'config', 'w') as f:
            f.write(yaml.dump(dict(client_id='some_random_id', client_secret='some_random_secret')))

        assert get_client_id() == 'some_random_id'
        assert get_client_secret() == 'some_random_secret'
