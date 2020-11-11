from unittest.mock import Mock

import pytest

from panoramic.cli.state import Action, VirtualState


def test_state_empty():
    assert VirtualState(data_sources=[], models=[], fields=[]).is_empty


def test_state_not_empty():
    assert not VirtualState(data_sources=[Mock()], models=[], fields=[]).is_empty
    assert not VirtualState(data_sources=[], models=[Mock()], fields=[]).is_empty
    assert not VirtualState(data_sources=[], models=[Mock()], fields=[Mock()]).is_empty


def test_empty_action_is_invalid():
    with pytest.raises(ValueError, match='cannot be None'):
        Action()


def test_get_objects_by_package():
    package1_model = Mock(package='package1')
    package2_model = Mock(package='package2')
    global_field = Mock(package=None)
    package1_field = Mock(package='package1')
    package3_field = Mock(package='package3')
    state = VirtualState(
        data_sources=[],
        models=[package1_model, package2_model],
        fields=[global_field, package1_field, package3_field],
    )

    assert state.get_objects_by_package() == {
        'package1': ([package1_field], [package1_model]),
        'package2': ([], [package2_model]),
        'package3': ([package3_field], []),
    }
