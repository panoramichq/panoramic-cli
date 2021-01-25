import enum
from enum import Enum
from typing import Dict


class ValueQuantityType(enum.Enum):
    """
    Specifies the quantity of given value.
    """

    scalar = 'scalar'
    array = 'array'
    """
    Array, where order of values does not matter.
    """
    # Later this can be extended for other types, such as strict_array, where order will matter.


class HuskyModelType(Enum):

    ENTITY = 'entity'
    METRIC = 'metric'


class Relationship(Enum):
    one_to_one = 'one_to_one'
    one_to_many = 'one_to_many'
    many_to_one = 'many_to_one'
    many_to_many = 'many_to_many'

    def reverse(self):
        """
        Returns the reversed relationship type, such as, for many_to_one, return one_to_many.
        """
        if self in _relationship_reverse_map:
            return _relationship_reverse_map[self]
        raise RuntimeError(f'Cannot reverse the relationship {self}')


_relationship_reverse_map: Dict[Relationship, Relationship] = {
    Relationship.one_to_one: Relationship.one_to_one,
    Relationship.many_to_many: Relationship.many_to_many,
    Relationship.one_to_many: Relationship.many_to_one,
    Relationship.many_to_one: Relationship.one_to_many,
}


class JoinDirection(Enum):
    """
    Enum defining if a given join edge can be traversed from both linked models (model it is defined on and model it is
    referencing to), or if it can be only traversed from the defined model (outgoing) or referenced model (incoming)
    """

    both = 'both'
    outgoing = 'outgoing'
    incoming = 'incoming'


class JoinType(Enum):
    left = 'left'
    inner = 'inner'
    right = 'right'

    def reverse(self):
        """
        Returns the reversed join type, such as, for left, return right.
        """
        if self in _join_type_reverse_map:
            return _join_type_reverse_map[self]

        raise RuntimeError(f'Cannot reverse the join {self}')


_join_type_reverse_map: Dict[JoinType, JoinType] = {
    JoinType.left: JoinType.right,
    JoinType.right: JoinType.left,
    JoinType.inner: JoinType.inner,
}


class TimeGranularity(Enum):
    day = 'day'
    hour = 'hour'


class ModelVisibility(Enum):
    hidden = 'hidden'
    available = 'available'
    experimental = 'experimental'
