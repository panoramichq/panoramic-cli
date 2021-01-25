import inspect
import re
from copy import deepcopy
from enum import EnumMeta
from typing import Any, Callable, List, Optional, Set, Type

from pydantic import BaseModel, root_validator, validator
from pydantic.fields import SHAPE_LIST

from panoramic.cli.husky.common.enum import EnumHelper


class PydanticModel(BaseModel):
    def is_set(self, property_name: str) -> bool:
        return property_name in self.__fields_set__

    @classmethod
    def deep_construct(cls: Type[BaseModel], _fields_set: Optional[Set[str]] = None, **values: Any):
        """
        Copied from pydantic BaseModel and modified to be able to construct models recursively from
        primitive data types and enum values. It can deserialize models inheriting from PydanticModel,
        including lists of models.

        WARNING:
        - Dictionaries and sets are copied without any changes, even if they contain Pydantic models
        - Invalid enum values are ignored and replaced with None (no exception is thrown)

        Creates a new model setting __dict__ and __fields_set__ from trusted or pre-validated data.
        Default values are respected, but no other validation is performed.
        """
        m = cls.__new__(cls)

        for field_name, field in m.__fields__.items():
            field_type = field.type_
            if field.shape == SHAPE_LIST:
                # Lists can have their actual types kinda hidden
                # Would need change if we have List[Union[TypeA,TypeB]].. but that is quite an edge case and
                # not sure pydantic even supports that
                list_field_type = field.sub_fields[0].type_
                if (
                    inspect.isclass(list_field_type)
                    and issubclass(list_field_type, PydanticModel)
                    and values.get(field_name) is not None
                ):
                    deserialized_list = []
                    for model in values[field_name]:
                        if model is not None:
                            deserialized_list.append(list_field_type.deep_construct(**model))
                        else:
                            deserialized_list.append(None)
                    values[field_name] = deserialized_list
                if issubclass(type(list_field_type), EnumMeta) and values.get(field_name) is not None:
                    deserialized_list = []
                    for enum in values[field_name]:
                        if enum is not None:
                            # Deserialize enum and replace invalid values with None, do not throw exception
                            deserialized_list.append(EnumHelper.from_value_safe(list_field_type, enum))
                        else:
                            deserialized_list.append(None)
                    values[field_name] = deserialized_list
            elif (
                inspect.isclass(field_type)
                and issubclass(field_type, PydanticModel)
                and values.get(field_name) is not None
            ):
                values[field_name] = field_type.deep_construct(**values[field_name])
            elif issubclass(type(field_type), EnumMeta) and values.get(field_name) is not None:
                # Deserialize enum and replace invalid values with None, do not throw exception
                values[field_name] = EnumHelper.from_value_safe(field_type, values[field_name])

        object.__setattr__(m, '__dict__', {**deepcopy(cls.__field_defaults__), **values})
        if _fields_set is None:
            _fields_set = set(values.keys())
        object.__setattr__(m, '__fields_set__', _fields_set)
        return m


def reuse_validator(*fields: str, each_item: bool = False) -> Callable:
    """
    Convenience method for applying validation rule on multiple pydantic model fields.
    """
    return validator(*fields, each_item=each_item, allow_reuse=True)


def reuse_root_validator() -> Callable:
    """
    Convenience method for applying root validation rule on multiple pydantic models.
    """
    return root_validator(allow_reuse=True)


def non_empty_str(value: Optional[str]) -> Optional[str]:
    if (value is not None) and (len(value) == 0):
        raise ValueError('string cannot be empty')
    return value


def non_empty_list(value: Optional[List]) -> Optional[List]:
    if (value is not None) and (len(value) == 0):
        raise ValueError('list cannot be empty')
    return value


def matches_regex(regex: str) -> Callable:
    def validate(value: Optional[str]) -> Optional[str]:
        if (value is not None) and (not re.match(regex, value)):
            raise ValueError(f'string must match regex: {regex}')
        return value

    return validate


def matches_choices(choices: List[str]) -> Callable:
    def validate(value: Optional[str]) -> Optional[str]:
        if (value is not None) and (value not in choices):
            raise ValueError(f'string must be one of {choices}')
        return value

    return validate
