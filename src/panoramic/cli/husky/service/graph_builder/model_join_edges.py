from typing import Any, Dict, Optional, Set

from panoramic.cli.husky.common.util import serialize_class_with_props
from panoramic.cli.husky.core.model.enums import JoinType, Relationship


class ModelJoinEdge:
    """
    Data class with properties of a join edge between two models.
    """

    taxons: Set[str]
    relationship: Relationship
    join_type: JoinType

    _params: Optional[Dict[str, Any]]
    """Bind params for the join (if any)"""

    def __init__(
        self,
        taxons: Set[str],
        relationship: Relationship,
        join_type: JoinType,
        bind_params: Optional[Dict[str, Any]] = None,
    ):
        self.taxons = taxons
        self.relationship = relationship
        self.join_type = join_type
        self._params = bind_params

    def __repr__(self):
        return serialize_class_with_props(self)
