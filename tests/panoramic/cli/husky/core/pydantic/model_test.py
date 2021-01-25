from enum import Enum
from typing import Any, List, Optional

import pytest

from panoramic.cli.husky.core.pydantic.model import (
    PydanticModel,
    matches_choices,
    matches_regex,
    non_empty_list,
    non_empty_str,
)

raises_value_error = pytest.mark.xfail(raises=ValueError, strict=True)


# Test classes
class MyEnum(Enum):
    option1 = 'option1'
    option2 = 'option2'
    option3 = 'option3'
    option4 = 'option4'


class Inner(PydanticModel):
    required_primitive: str
    optional_primitive: Optional[str]


class Outer(PydanticModel):
    required_enum: MyEnum
    optional_enum: Optional[Enum]
    required_complex: Inner
    optional_complex: Optional[Inner]


class OuterList(PydanticModel):
    required_list_primitive: List[str]
    optional_list_primitive: List[Optional[str]]
    required_list_enum: List[MyEnum]
    optional_list_enum: List[Optional[MyEnum]]
    required_list_complex: List[Inner]
    optional_list_complex: List[Optional[Inner]]


class EdgeCase(PydanticModel):
    required_unknown: Any
    optional_unknown: Optional[Any]
    required_list_unknown: List[Any]
    optional_list_unknown: List[Optional[Any]]
    optional_list_entire: Optional[List[Any]]


def test_deep_construct_inner():
    inner = Inner.deep_construct(required_primitive='str', optional_primitive=None)
    assert inner.required_primitive == 'str'
    assert inner.optional_primitive == None


def test_deep_construct_outer():
    outer = Outer.deep_construct(
        required_enum='option1',
        optional_enum=None,
        required_complex=dict(required_primitive='str', optional_primitive=None),
        optional_complex=None,
    )
    assert outer.required_enum == MyEnum.option1
    assert outer.optional_enum == None
    assert outer.required_complex.required_primitive == 'str'
    assert outer.required_complex.optional_primitive == None
    assert outer.optional_complex == None


def test_deep_construct_outer_list():
    outer_list = OuterList.deep_construct(
        required_list_primitive=['str1', 'str2'],
        optional_list_primitive=['str3', 'str4', None],
        required_list_enum=['option1', 'option2'],
        optional_list_enum=['option3', 'option4', None],
        required_list_complex=[
            dict(required_primitive='str1', optional_primitive=None),
            dict(required_primitive='str2', optional_primitive=None),
        ],
        optional_list_complex=[
            dict(required_primitive='str3', optional_primitive=None),
            dict(required_primitive='str4', optional_primitive=None),
            None,
        ],
    )
    assert outer_list.required_list_primitive == ['str1', 'str2']
    assert outer_list.optional_list_primitive == ['str3', 'str4', None]
    assert outer_list.required_list_enum == [MyEnum.option1, MyEnum.option2]
    assert outer_list.optional_list_enum == [MyEnum.option3, MyEnum.option4, None]
    assert outer_list.required_list_complex[0].required_primitive == 'str1'
    assert outer_list.required_list_complex[0].optional_primitive == None
    assert outer_list.required_list_complex[1].required_primitive == 'str2'
    assert outer_list.required_list_complex[1].optional_primitive == None
    assert outer_list.optional_list_complex[0].required_primitive == 'str3'
    assert outer_list.optional_list_complex[0].optional_primitive == None
    assert outer_list.optional_list_complex[1].required_primitive == 'str4'
    assert outer_list.optional_list_complex[1].optional_primitive == None
    assert outer_list.optional_list_complex[2] == None


def test_deep_construct_edge_case():
    edge_case = EdgeCase.deep_construct(
        required_unknown='unknown',
        optional_unknown=None,
        required_list_unknown=['unknown1', 'unknown2'],
        optional_list_unknown=['unknown3', 'unknown4', None],
        optional_list_entire=None,
    )
    assert edge_case.required_unknown == 'unknown'
    assert edge_case.optional_unknown == None
    assert edge_case.required_list_unknown == ['unknown1', 'unknown2']
    assert edge_case.optional_list_unknown == ['unknown3', 'unknown4', None]
    assert edge_case.optional_list_entire == None


@pytest.mark.parametrize('value', [None, 's', 'string', pytest.param('', marks=raises_value_error)])
def test_non_empty_str_validator(value):
    assert non_empty_str(value) == value


@pytest.mark.parametrize('value', [None, [1], [1, 2, 3], pytest.param([], marks=raises_value_error)])
def test_non_empty_list_validator(value):
    assert non_empty_list(value) == value


@pytest.mark.parametrize('value', [None, 'help', 'helen', pytest.param('paris', marks=raises_value_error)])
def test_matches_regex_validator(value):
    assert matches_regex('^hel.*')(value) == value


@pytest.mark.parametrize('value', [None, 'iliad', 'odyssey', pytest.param('argonauts', marks=raises_value_error)])
def test_matches_choices_validator(value):
    assert matches_choices(['iliad', 'odyssey'])(value) == value
