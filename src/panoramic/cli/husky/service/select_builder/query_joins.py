from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Optional, Sequence, Set

import networkx as nx

from panoramic.cli.husky.core.model.models import HuskyModel
from panoramic.cli.husky.core.sql_alchemy_util import safe_quote_identifier
from panoramic.cli.husky.service.context import (
    SNOWFLAKE_HUSKY_CONTEXT,
    HuskyQueryContext,
)
from panoramic.cli.husky.service.graph_builder.model_join_edges import ModelJoinEdge
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonSlugExpression


class QueryJoins(metaclass=ABCMeta):
    """Represents a node in a tree, that is result of a graph's BFS."""

    model: HuskyModel
    """The model itself, basically a node in a graph"""

    taxons_from_model: Set[TaxonSlugExpression]
    """Set of taxons that we want and that can be taken from this model."""

    def __init__(self, model: HuskyModel, select_from_model: Optional[Set[TaxonSlugExpression]] = None):
        self.model = model
        self.taxons_from_model = select_from_model or set()

    @abstractmethod
    def get_all_selectable_taxons(self) -> Set[TaxonSlugExpression]:
        """
        Returns set of all taxons in the query join
        """
        raise NotImplementedError('Not implemented')

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False

    def __hash__(self):
        return hash(self.model.unique_object_name(SNOWFLAKE_HUSKY_CONTEXT) + str(self.taxons_from_model))

    @abstractmethod
    def to_sql(self, ctx: HuskyQueryContext) -> str:
        """
        SQL representation of the join
        """
        raise NotImplementedError('Not implemented')

    @abstractmethod
    def to_primitive(self):
        """
        Gives very simple representation of query joins using python build-in data structures
        which is meant to be used for debugging purposes in Exceptions.
        """
        raise NotImplementedError('Not implemented')

    def bind_params(self) -> Dict[str, Any]:
        """
        Binding params for the SQL
        """
        return {}


class SimpleQueryJoins(QueryJoins):
    """Represents a node in a tree, that is result of a graph's BFS and uses only simple taxon joins."""

    join_to: List['SimpleQueryJoins']
    """Links to other nodes it can/should be joined to."""

    graph: nx.DiGraph
    """Model graph"""

    def __init__(
        self,
        graph: nx.DiGraph,
        model: HuskyModel,
        join_to: List['SimpleQueryJoins'] = None,
        select_from_model: Optional[Set[TaxonSlugExpression]] = None,
    ):
        super().__init__(model, select_from_model)
        self.graph = graph
        self.join_to = join_to or []

    def get_all_selectable_taxons(self) -> Set[TaxonSlugExpression]:
        """
        Recursively travels full QueryJoins tree, and return a set that is union of all taxons_from_models sets.
        Useful for getting the all taxons in a subtree, that is needed for subtree pruning.
        """
        result = set()
        result.update(self.taxons_from_model)
        for query_join in self.join_to:
            result.update(query_join.get_all_selectable_taxons())
        return result

    def to_sql(self, ctx: HuskyQueryContext) -> str:
        #  Iterate all query joins nodes as they were ordered.
        if not self.model:
            return ''

        join_from_model: HuskyModel = self.model
        join_query = []
        for join_to in self.join_to:
            if not join_to.model:
                continue

            join_to_model: HuskyModel = join_to.model
            # Iterate all join edges.
            node_join: ModelJoinEdge = self.graph.get_edge_data(join_from_model.name, join_to_model.name)['join']
            # Add the 'JOIN table ON' part
            join_query.extend([f'{node_join.join_type.value.upper()} JOIN', join_to.model.full_object_name(ctx), 'AS'])

            # add "AS"
            if join_to.model.table_alias is None:
                safe_name = safe_quote_identifier(join_to.model.full_object_name(ctx), ctx.dialect)
                join_query.append(safe_name)
            else:
                join_query.append(join_to.model.table_alias)

            join_query.append('ON')
            join_conditions = []
            for join_taxon in node_join.taxons:
                # Iterate taxons to join on. Get the from and to full column names.
                join_condition = (
                    f''
                    f'{join_from_model.taxon_sql_accessor(ctx, join_taxon)}'
                    f' = '
                    f'{join_to_model.taxon_sql_accessor(ctx, join_taxon)}'
                )
                join_conditions.append(join_condition)
            # Join the join conditions with AND operator and add to the join_query list.
            join_query.append(' AND '.join(join_conditions))

        return ' '.join(join_query)

    def to_primitive(self):
        """
        Gives very simple representation of query joins using python build-in data structures
        which is meant to be used for debugging purposes in Exceptions.
        """
        return {
            'model': self.model.to_primitive(),
            'join_to': [join.to_primitive() for join in self.join_to],
            'taxons_from_model': {taxon_slug_expression.slug for taxon_slug_expression in self.taxons_from_model},
        }

    def __hash__(self):
        return hash(self.model.unique_object_name(SNOWFLAKE_HUSKY_CONTEXT) + str(self.taxons_from_model))

    def bind_params(self) -> Dict[str, Any]:
        """
        Binding params for the SQL
        """
        return {}


def get_bfs_ordered(root: SimpleQueryJoins) -> Sequence[QueryJoins]:
    """
    Helper functions that returns ordered QueryJoins in the same way as BFS finds them.
    Useful for composing select queries, where taxon should be selected from the soonest model that has that taxon.
    """
    visited_nodes: Set[SimpleQueryJoins] = set()
    queue = [root]
    ordered_nodes = []
    while queue:
        current_node = queue.pop()
        visited_nodes.add(current_node)
        ordered_nodes.append(current_node)
        for next_join in current_node.join_to:
            if next_join not in visited_nodes and next_join not in queue:
                queue.append(next_join)

    return ordered_nodes
