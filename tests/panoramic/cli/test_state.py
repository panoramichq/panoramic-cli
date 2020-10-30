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
