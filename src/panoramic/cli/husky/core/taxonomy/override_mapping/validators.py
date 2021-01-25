from collections import defaultdict
from typing import DefaultDict

from pydantic import ValidationError
from pydantic.error_wrappers import ErrorWrapper

_MAX_MAPPING_CNT = 1000
"""Maximum number of mappings per single override mapping"""


def correct_length_mapping(cls, mapping):
    """Validates that each mapping contains only 2 values (original & changed) and at most 1000 values"""
    # if no mapping is provided (i.e. when updating only name), do nothing
    if not mapping:
        return mapping

    invalid_value_idx = []
    for idx, elem in enumerate(mapping):
        if len(elem) != 2:
            invalid_value_idx.append(idx)

    if invalid_value_idx:
        raise ValidationError(
            [
                ErrorWrapper(ValueError(f'This mapping contains {len(mapping[idx])} values instead of 2'), str(idx + 1))
                for idx in invalid_value_idx
            ],
            cls,
        )

    if len(mapping) > _MAX_MAPPING_CNT:
        raise ValueError(f'There is {len(mapping)} mappings, but Husky only supports {_MAX_MAPPING_CNT} mappings.')

    return mapping


def unique_mapping(mapping):
    """Validates that each mapping contains unique original value"""
    # if no mapping is provided (i.e. when updating only name), do nothing
    if not mapping:
        return mapping

    cnt_occurrence: DefaultDict[str, int] = defaultdict(int)
    for elem in mapping:
        cnt_occurrence[elem[0]] += 1

    multiple_usage = {('null' if elem is None else elem) for elem, cnt in cnt_occurrence.items() if cnt > 1}
    if multiple_usage:
        raise ValueError('Following original values are used more than once - ' + ', '.join(multiple_usage))

    return mapping
