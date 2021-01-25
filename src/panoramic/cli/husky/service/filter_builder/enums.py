from enum import Enum


class FilterClauseType(Enum):
    GROUP = 'group'
    TAXON_VALUE = 'taxon_value'
    TAXON_TAXON = 'taxon_taxon'
    TAXON_ARRAY = 'taxon_array'


class LogicalOperator(Enum):
    AND = 'AND'
    OR = 'OR'


class SimpleFilterOperator(Enum):
    EQ = '='
    NEQ = '!='
    LT = '<'
    LTE = '<='
    GT = '>'
    GTE = '>='
    LIKE = 'LIKE'
    NOT_LIKE = 'NOT_LIKE'
    ILIKE = 'ILIKE'
    NOT_ILIKE = 'NOT_ILIKE'


class ArrayFilterOperator(Enum):
    IN = 'IN'
    NOT_IN = 'NOT_IN'
