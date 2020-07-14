from pathlib import Path
import tempfile
import pytest
import yaml
from panoramic.cli.errors import InvalidYamlFile, MissingContextFileException, MissingValueException
from tests.panoramic.cli.util import changedir

from panoramic.cli.context import get_api_version, get_company_slug


def test_no_context_file():
    with tempfile.TemporaryDirectory() as tmpdirname, changedir(tmpdirname):
        with pytest.raises(MissingContextFileException):
            get_api_version()

        with pytest.raises(MissingContextFileException):
            get_company_slug()


def test_invalid_context_file():
    with tempfile.TemporaryDirectory() as tmpdirname, changedir(tmpdirname):
        with open(Path(tmpdirname) / 'pano.yaml', 'w') as f:
            f.write('api_version: some_value\ncompany_slug slug_but_missing_colon\n')

        with pytest.raises(
            InvalidYamlFile,
            match=f'while scanning a simple key\n  in "{tmpdirname}/pano.yaml", line 2, column 1\ncould not find expected \':\'\n  in "{tmpdirname}/pano.yaml", line 3, column 1',
        ):
            assert get_api_version()

        with pytest.raises(
            InvalidYamlFile,
            match=f'while scanning a simple key\n  in "{tmpdirname}/pano.yaml", line 2, column 1\ncould not find expected \':\'\n  in "{tmpdirname}/pano.yaml", line 3, column 1',
        ):
            assert get_company_slug()


def test_missing_value_context_file():
    with tempfile.TemporaryDirectory() as tmpdirname, changedir(tmpdirname):
        with open(Path(tmpdirname) / 'pano.yaml', 'w') as f:
            f.write(yaml.dump(dict(company_slug='company_name_12fxs')))

        with pytest.raises(MissingValueException):
            get_api_version()

    with tempfile.TemporaryDirectory() as tmpdirname, changedir(tmpdirname):
        with open(Path(tmpdirname) / 'pano.yaml', 'w') as f:
            f.write(yaml.dump(dict(api_version='v2')))

        with pytest.raises(MissingValueException):
            get_company_slug()


def test_context_file():
    with tempfile.TemporaryDirectory() as tmpdirname, changedir(tmpdirname):
        with open(Path(tmpdirname) / 'pano.yaml', 'w') as f:
            f.write(yaml.dump(dict(api_version='v2', company_slug='company_name_12fxs')))

        assert get_api_version() == 'v2'
        assert get_company_slug() == 'company_name_12fxs'
