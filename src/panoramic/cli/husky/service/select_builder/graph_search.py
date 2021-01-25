from operator import itemgetter
from typing import Dict, Iterable, List, Optional, Set

from panoramic.cli.husky.core.model.enums import Relationship, TimeGranularity
from panoramic.cli.husky.core.model.models import HuskyModel, ModelAttribute
from panoramic.cli.husky.service.graph_builder.component import ModelJoinEdge
from panoramic.cli.husky.service.select_builder.exceptions import (
    ImpossibleTaxonCombination,
)
from panoramic.cli.husky.service.select_builder.query_joins import SimpleQueryJoins
from panoramic.cli.husky.service.utils.taxon_slug_expression import TaxonSlugExpression

TIME_GRANULARITY_RANK: Dict[Optional[TimeGranularity], int] = {TimeGranularity.hour: 0, TimeGranularity.day: 1}

DEFAULT_TIME_GRANULARITY_RANK = len(TIME_GRANULARITY_RANK)


def get_time_granularity_rank(time_granularity: Optional[TimeGranularity]) -> int:
    return TIME_GRANULARITY_RANK.get(time_granularity, DEFAULT_TIME_GRANULARITY_RANK)


def generate_model_taxon_rank(models: Iterable[HuskyModel], taxons: Set[TaxonSlugExpression]) -> Dict[HuskyModel, int]:
    """
    Returns a dict where key is a model, and value is number of taxons on that model that we want to select.
    """
    models_rank = dict()
    for model in models:
        model_taxon_slugs = {attribute.taxon_memoized for attribute in model.attributes_memoized.values()}
        models_rank[model] = len([taxon_slug for taxon_slug in taxons if taxon_slug.graph_slug in model_taxon_slugs])
    return models_rank


def sort_models_with_heuristic(models: Iterable[HuskyModel], taxons: Set[TaxonSlugExpression]) -> List[HuskyModel]:
    """
    Returns the models in which order we should try to search the graph. It skips tag models,
    because those cannot be used as root models.

    """
    models_score = dict()
    for model, taxon_rank in generate_model_taxon_rank(models, taxons).items():
        models_score[model] = (
            taxon_rank,  # Most matching taxons.
            -1 * model.number_of_identifiers,  # Less identifiers means smaller table
            get_time_granularity_rank(model.time_granularity),
            entity_level_rank(model.identifier_attributes),  # Higher entity means smaller table.
        )

    sorted_models_by_rank = sorted(list(models_score.items()), key=itemgetter(1), reverse=True)

    return [model for model, _ in sorted_models_by_rank]


def entity_level_rank(attributes: Set[ModelAttribute]) -> int:
    # TODO later this should use taxons parent relationship.
    str_taxons = set(map(lambda attr: attr.taxon, attributes))
    if 'ad_id' in str_taxons:
        return 100
    if 'adgroup_id' in str_taxons:
        return 1000
    if 'campaign_id' in str_taxons:
        return 10000
    return 0


class GraphSearch:
    """
    Class for performing bfs graph search, pruning and returning optimal QueryJoins tree.
    """

    def __init__(
        self,
        name_to_model: Dict[str, HuskyModel],
        query_taxons: Set[TaxonSlugExpression],
        graph,
        data_source: Optional[str] = None,
    ):
        self.name_to_model: Dict[str, HuskyModel] = name_to_model
        self.query_taxons: Set[TaxonSlugExpression] = query_taxons
        self.graph = graph
        self.data_source = data_source

    def find_join_tree(self) -> SimpleQueryJoins:
        # We just started, sort all available models.
        sorted_best_models = sort_models_with_heuristic(self.name_to_model.values(), self.query_taxons)

        for model in sorted_best_models:
            # run bfs to find all accessible models via a join
            query_join = self._bfs(model, self.query_taxons)
            #  calculate which taxons are covered by all joins
            taxons = query_join.get_all_selectable_taxons()
            # are all requested taxons covered?
            if self.query_taxons.issubset(taxons):
                # yes, so cut off all redundant query joins
                self._prune_useless_joins(query_join, self.query_taxons)
                return query_join
        raise ImpossibleTaxonCombination(self.query_taxons, self.data_source)

    def find_all_full_join_trees(self) -> List[SimpleQueryJoins]:
        """
        Finds all available join combinations that contain requested taxons
        """
        query_joins: List[SimpleQueryJoins] = []
        models = self.name_to_model.values()

        for model in models:
            query_join = self._bfs(model, self.query_taxons)
            taxons = query_join.get_all_selectable_taxons()
            if self.query_taxons.issubset(taxons):
                query_joins.append(query_join)

        if not query_joins:
            raise ImpossibleTaxonCombination(self.query_taxons, self.data_source)

        return query_joins

    @staticmethod
    def validate_next_time_granularity(
        contains_time_granularity: Set[TimeGranularity], next_time_granularity: TimeGranularity
    ) -> bool:
        """
        Checks what time granularities can be used together
        Currently none can be mixed together
        """
        next_contains_time_granularity = {*contains_time_granularity, next_time_granularity}
        return len(next_contains_time_granularity) == 1

    def _bfs(self, root_model: HuskyModel, query_taxons: Set[TaxonSlugExpression]) -> SimpleQueryJoins:
        """
        Performs a BFS, returning QueryJoins tree with >all< accessible models.
        :param root_model: model to start the BFS from.
        :param query_taxons: Taxons we want to select.
        """
        visited_models = {root_model}
        """Set with all already visited models, or models in a queue."""

        queue = [root_model]

        query_joins_by_model = dict()
        """Dict for keeping QueryJoin structures. The QueryJoin.join_to is extended as we traverse the graph."""

        root_taxon_slugs = {taxon_slug for taxon_slug in query_taxons if taxon_slug.graph_slug in root_model.taxons}
        query_joins_by_model[root_model] = SimpleQueryJoins(self.graph, root_model, [], root_taxon_slugs)

        # checks for time granularity of found models
        contains_time_granularity: Set[TimeGranularity] = set()

        while queue:
            current_model = queue.pop()
            current_model_query_join: SimpleQueryJoins = query_joins_by_model[current_model]

            next_model_names = set(self.graph.successors(current_model.name))
            for next_model_name in next_model_names:  # Iterate all successors
                node_join: ModelJoinEdge = self.graph.get_edge_data(current_model.name, next_model_name)['join']
                if node_join.relationship in [Relationship.one_to_many, Relationship.many_to_many]:
                    # Can't allow joins to_many, since that could result in duplicated rows.
                    continue
                next_model = self.name_to_model[next_model_name]
                if next_model in visited_models:
                    # Already visited that model, skip.
                    continue

                # do not allow mixing models with different time granularity
                if next_model.time_granularity is not None:
                    if not self.validate_next_time_granularity(contains_time_granularity, next_model.time_granularity):
                        continue

                    contains_time_granularity.add(next_model.time_granularity)

                queue.append(next_model)
                visited_models.add(next_model)

                # Create QueryJoin for the next model and add it to the query join dict
                next_model_taxon_slugs = {
                    taxon_slug for taxon_slug in query_taxons if taxon_slug.graph_slug in next_model.taxons
                }
                next_model_query_join = SimpleQueryJoins(self.graph, next_model, [], next_model_taxon_slugs)
                query_joins_by_model[next_model] = next_model_query_join

                # Add the next model QueryJoin to current models query join structure.
                current_model_query_join.join_to.append(next_model_query_join)

        # Return the root query join
        return query_joins_by_model[root_model]

    def _prune_useless_joins(self, query_join: SimpleQueryJoins, needed_taxons: Set[TaxonSlugExpression]):
        """
        Removes join sub-trees that do not bring any taxons that we need, or bring taxons we already have.
        Called recursively on each node in query join tree.
        It mutates the query joins.
        """
        if len(query_join.join_to) == 0:
            return
        # Taxons on current query join we already have, so remove them from needed taxons.
        currently_needed_taxons = {taxon.graph_slug for taxon in needed_taxons.difference(query_join.taxons_from_model)}

        # Sort the join subtrees based on total number of taxons they bring and we still need.
        sorted_by_taxon_size = sorted(
            query_join.join_to,
            key=lambda x: len(
                [
                    taxon_slug
                    for taxon_slug in x.get_all_selectable_taxons()
                    if taxon_slug.graph_slug in currently_needed_taxons
                ]
            ),
            reverse=True,
        )

        effective_joins = []  # New list of only effective joins.

        # TODO v1 check if we need this, or we are fine with updating needed_taxons only
        effective_joins_taxons: Set[str] = set()
        for join in sorted_by_taxon_size:
            # Get taxons that this join brings and we dont have yet
            join_taxon_slugs = {taxon_slug.graph_slug for taxon_slug in join.get_all_selectable_taxons()}
            extra_taxons = join_taxon_slugs.intersection(currently_needed_taxons).difference(effective_joins_taxons)
            # If not empty, use that join. Otherwise, omit (prune) that subtree
            if len(extra_taxons) > 0:
                effective_joins.append(join)
                effective_joins_taxons.update(extra_taxons)

        # Set only effective_joins on the current query join.
        query_join.join_to = effective_joins
        new_needed_taxons = {
            taxon_slug for taxon_slug in needed_taxons if taxon_slug not in query_join.taxons_from_model
        }
        for join in query_join.join_to:
            # Call recursively on all sub trees.
            self._prune_useless_joins(join, new_needed_taxons)
