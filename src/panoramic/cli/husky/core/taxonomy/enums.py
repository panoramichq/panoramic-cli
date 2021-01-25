from enum import Enum
from typing import Callable, Dict, Set

from sqlalchemy import asc, desc


class ValidationType(str, Enum):
    text = 'text'
    integer = 'integer'
    numeric = 'numeric'
    datetime = 'datetime'
    enum = 'enum'  # (aka predefined list)
    percent = 'percent'
    money = 'money'
    url = 'url'
    boolean = 'boolean'
    duration = 'duration'
    variant = 'variant'


class AggregationType(str, Enum):
    # simple aggregations
    sum = 'sum'
    avg = 'avg'
    min = 'min'
    max = 'max'
    count_all = 'count_all'

    # use list of dimensions
    count_distinct = 'count_distinct'

    # simple dimension aggregations
    group_by = 'group_by'

    # use time-dimension to calculate these aggregations
    first_by = 'first_by'
    last_by = 'last_by'

    # aggregation was not set
    not_set = 'not_set'

    # Other types are not supported yet.


DIMENSION_AGGREGATION_TYPES: Set[AggregationType] = {
    AggregationType.last_by,
    AggregationType.first_by,
    AggregationType.group_by,
    AggregationType.max,
    AggregationType.min,
}
"""Aggregation types which represent dimensions"""


class DisplayState(str, Enum):
    visible = 'visible'
    hidden = 'hidden'
    deleted = 'deleted'


class TaxonTypeEnum(str, Enum):
    # Use this instead of the string ^ when possible
    dimension = 'dimension'
    metric = 'metric'


class TaxonOrderType(Enum):
    asc = 'asc'
    desc = 'desc'


ORDER_BY_FUNCTIONS: Dict[TaxonOrderType, Callable] = {TaxonOrderType.asc: asc, TaxonOrderType.desc: desc}
