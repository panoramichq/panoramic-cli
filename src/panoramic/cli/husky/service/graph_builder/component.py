from typing import Dict, List, Optional, Set

import networkx as nx

from panoramic.cli.husky.common.exception_enums import (
    ExceptionErrorCode,
    ExceptionGroup,
    ExceptionSeverity,
)
from panoramic.cli.husky.core.model.enums import JoinDirection, Relationship
from panoramic.cli.husky.core.model.models import HuskyModel
from panoramic.cli.husky.service.graph_builder.model_join_edges import ModelJoinEdge
from panoramic.cli.husky.service.utils.exceptions import (
    HuskyException,
    MissingJoinTaxons,
)


class Graph:
    def __init__(self, model_graph: nx.DiGraph, name_to_model: Dict[str, HuskyModel]):
        self.model_graph: nx.DiGraph = model_graph
        self.name_to_model: Dict[str, HuskyModel] = name_to_model


class GraphBuilder:
    def __init__(self, models: List[HuskyModel]):
        self.models = models

    def build_graph(self) -> Graph:
        """
        Builds a models graph based on given args.
        :return: Tuple.
            First value is used for performing graph search (networkx graph)
            Second value can be used for visualization (pyvis graph)
            Third value is mapping from name to model object
        """
        name_to_model: Dict[str, HuskyModel] = {m.graph_name: m for m in self.models}

        graph = nx.DiGraph()
        for model in self.models:
            # First add all nodes to the graph
            graph.add_node(model.graph_name)

        for model_from in self.models:
            for join in model_from.joins_memoized:
                model_to = name_to_model.get(join.to_model_memoized)
                if model_to:
                    # The to model is available for given scope.
                    if not join.taxons_memoized:
                        raise MissingJoinTaxons()

                    join_direction: JoinDirection
                    if join.direction_memoized:
                        # Join has explicitly defined direction, use that
                        join_direction = join.direction_memoized
                    else:
                        # No explicit direction found, derive it from relationship type
                        if join.relationship_memoized == Relationship.one_to_one:
                            join_direction = JoinDirection.both
                        elif join.relationship_memoized == Relationship.many_to_one:
                            join_direction = JoinDirection.outgoing
                        elif join.relationship_memoized == Relationship.one_to_many:
                            join_direction = JoinDirection.incoming
                        else:
                            raise RuntimeError(f'Undefined join relationship {join.relationship_memoized}')
                    model_to_graph_name = model_to.graph_name
                    model_from_graph_name = model_from.graph_name
                    if join_direction == JoinDirection.both:
                        node_join = ModelJoinEdge(
                            set(join.taxons_memoized), join.relationship_memoized, join.join_type_memoized
                        )
                        graph.add_edge(model_from_graph_name, model_to_graph_name, join=node_join)

                        # One to one relationship can be traversed in both ways, so here we add it in opposite
                        # direction and with reversed join
                        node_join = ModelJoinEdge(
                            set(join.taxons_memoized), join.relationship_memoized, join.join_type_memoized.reverse()
                        )
                        graph.add_edge(model_to_graph_name, model_from_graph_name, join=node_join)
                    elif join_direction == JoinDirection.outgoing:
                        node_join = ModelJoinEdge(
                            set(join.taxons_memoized), join.relationship_memoized, join.join_type_memoized
                        )
                        graph.add_edge(model_from_graph_name, model_to_graph_name, join=node_join)
                    elif join_direction == JoinDirection.incoming:
                        node_join = ModelJoinEdge(
                            set(join.taxons_memoized),
                            join.relationship_memoized.reverse(),
                            join.join_type_memoized.reverse(),
                        )
                        graph.add_edge(model_to_graph_name, model_from_graph_name, join=node_join)
                    else:
                        raise RuntimeError(f'Undefined join_direction {join_direction}')

        return Graph(graph, name_to_model)

    @classmethod
    def create_with_models(cls, models: List[HuskyModel]) -> Graph:
        """
        Method that returns a graph from the given models.
        """
        return cls(models).build_graph()


class MultipleDataSources(HuskyException):
    """
    Exception covering case when Graph builder is tasked to generate graph
    for multiple data sources at once (not supported at the moment)
    """

    def __init__(self, data_sources: Set[str], specific_model_name: Optional[str] = None):
        """
        Constructor

        :param data_sources: Set of data sources
        :param specific_model_name: Specific name of model (in case it is entered)
        """
        super().__init__(
            ExceptionErrorCode.MULTIPLE_DATA_SOURCES_GRAPH,
            'Graph builder can be used only for one exact data source.',
            exception_group=ExceptionGroup.UNSUPPORTED,
        )
        self._severity = ExceptionSeverity.info
