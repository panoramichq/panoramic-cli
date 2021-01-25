import logging
from collections import defaultdict
from itertools import combinations
from typing import Any, Dict, List

from panoramic.cli.husky.core.federated.model.models import (
    FdqModel,
    FdqModelJoin,
    FdqModelJoinRelationship,
)
from panoramic.cli.husky.core.model.enums import JoinType

logger = logging.getLogger(__name__)


def detect_joins(models: List[FdqModel]) -> Dict[str, List[Dict[str, Any]]]:
    joins = defaultdict(list)

    for _, (model, another_model) in enumerate(combinations(models, 2)):
        left_ids, right_ids = set(model.identifiers), set(another_model.identifiers)

        if len(left_ids) == 0:
            logger.warning(f'Model {model.model_name} has no ids defined')
            continue
        if len(right_ids) == 0:
            logger.warning(f'Model {model.model_name} has no ids defined')
            continue

        if left_ids == right_ids:
            logger.info(f'Found possible join: one {model.model_name} to one {another_model.model_name}')
            join = FdqModelJoin(
                to_model=another_model.model_name,
                relationship=FdqModelJoinRelationship.one_to_one,
                fields=list(left_ids),
                join_type=JoinType.left,
            )
            joins[model.model_name].append(join.dict(by_alias=True))
        elif left_ids.issubset(right_ids):
            logger.info(f'Found possible join: many {another_model.model_name} to one {model.model_name}')
            join = FdqModelJoin(
                to_model=model.model_name,
                relationship=FdqModelJoinRelationship.many_to_one,
                fields=list(left_ids),
                join_type=JoinType.left,
            )
            joins[another_model.model_name].append(join.dict(by_alias=True))
        elif right_ids.issubset(left_ids):
            logger.info(f'Found possible join: many {model.model_name} to one {another_model.model_name}')
            join = FdqModelJoin(
                to_model=another_model.model_name,
                relationship=FdqModelJoinRelationship.many_to_one,
                fields=list(right_ids),
                join_type=JoinType.left,
            )
            joins[model.model_name].append(join.dict(by_alias=True))

    return dict(joins)
