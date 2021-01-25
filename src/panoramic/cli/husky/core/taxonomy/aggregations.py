from typing import List, Optional, Sequence, Set, Union

from pydantic import Field, root_validator, validator

from panoramic.cli.husky.common.enum import EnumHelper
from panoramic.cli.husky.core.pydantic.model import PydanticModel, non_empty_str
from panoramic.cli.husky.core.taxonomy.enums import (
    AggregationType,
    TaxonOrderType,
    TaxonTypeEnum,
)


class AggregationParamsCountDistinct(PydanticModel):
    """Parameters for count distinct aggregation (requiring list of non-empty strings)"""

    relevant_fields: List[str] = Field([], alias='fields')
    """List of non-empty strings is supported"""

    _validate_non_empty_fields = validator('relevant_fields', each_item=True, allow_reuse=True)(non_empty_str)

    def __repr__(self):
        relevant_fields = ', '.join(f"'{field}'" for field in self.relevant_fields)
        return f'AggregationParamsCountDistinct(relevant_fields=[{relevant_fields}])'

    def used_taxon_slugs(self) -> Set[str]:
        """
        Container with used taxon for the params
        """
        return set(self.relevant_fields)


class AggregationOrderByTaxon(PydanticModel):
    """Substructure representing a taxon for order by clause"""

    taxon: str
    order_by: Optional[TaxonOrderType]

    _validate_nonempty_taxon = validator('taxon', allow_reuse=True)(non_empty_str)

    def __repr__(self):
        order_by = f'TaxonOrderType.{self.order_by.value}' if self.order_by else 'None'
        return f'AggregationOrderByTaxon(taxon={self.taxon}, order_by={order_by})'


class AggregationParamsSortDimension(PydanticModel):
    """Parameters for aggregations which require sort dimension taxons"""

    sort_dimensions: List[AggregationOrderByTaxon]
    """Taxon slugs for "order by" clause of the window function"""

    def __repr__(self):
        repr_dimensions = [repr(dim) for dim in self.sort_dimensions]
        return f'AggregationParamsSortDimension(sort_dimensions=[{", ".join(repr_dimensions)}])'

    def used_taxon_slugs(self) -> Set[str]:
        """
        Container with used taxon for the params
        """
        return {dim.taxon for dim in self.sort_dimensions}


class AggregationDefinition(PydanticModel):
    """Definition of aggregation type with additional parameters"""

    _SIMPLE_AGGS: Set[AggregationType] = {
        AggregationType.sum,
        AggregationType.avg,
        AggregationType.min,
        AggregationType.max,
        # dimensions using one field
        AggregationType.count_all,
        AggregationType.group_by,
    }
    """Set with all simple aggregations - requiring only one taxon"""

    _WITH_SORT_DIMENSION_AGGS: Set[AggregationType] = {AggregationType.first_by, AggregationType.last_by}
    """Set with all aggregations which require sort dimension taxons"""

    type: AggregationType
    """Type of the aggregation function"""

    params: Optional[Union[AggregationParamsSortDimension, AggregationParamsCountDistinct]]
    """Additional parameters for the aggregation function"""

    @root_validator(pre=True)
    def validate_params(cls, values):
        """Make sure that all params for given aggregation type are set and valid"""
        agg_type = EnumHelper.from_value_safe(AggregationType, values.get('type'))
        if not agg_type:
            return values

        if values['type'] == AggregationType.not_set or values['type'] in cls._SIMPLE_AGGS:
            values['params'] = None
            return values
        elif agg_type == AggregationType.count_distinct.value:
            if 'params' not in values or values['params'] is None:
                # this is a temporary solution for FE
                # TODO: remove this once FE starts using field "aggregation" instead of field "aggregation_type"
                values['params'] = AggregationParamsCountDistinct()
            else:
                values['params'] = AggregationParamsCountDistinct(**values['params'])

            return values

        if 'params' not in values:
            raise ValueError('Missing "params" field')

        if agg_type in cls._WITH_SORT_DIMENSION_AGGS:
            values['params'] = AggregationParamsSortDimension(**values['params'])
        else:
            raise ValueError(f'Unsupported aggregation type - {values["type"]}')

        return values

    def __repr__(self):
        repr_params = repr(self.params) if self.params else 'None'
        return f'AggregationDefinition(type=AggregationType.{self.type.value}, params={repr_params})'

    def used_taxon_slugs(self) -> Set[str]:
        """
        Determine which taxon slugs are required for the aggregation definition
        """
        if not self.params:
            return set()
        else:
            return self.params.used_taxon_slugs()

    @classmethod
    def common_defined_definition(
        cls,
        agg_definitions: Sequence[Optional['AggregationDefinition']],
        use_fallback_aggregations: bool = False,
        calculation_type: TaxonTypeEnum = TaxonTypeEnum.dimension,
    ) -> Optional['AggregationDefinition']:
        """
        Finds the common defined aggregation definition among the provided iterable of definitions.

        Returns:
        - None, if a single aggregation definition cannot be deduced (i.e. there's multiple different defined definitions or there is no concrete definition)
        - "not_set", if all aggregation definitions are not set
        - a single aggregation definition - if all aggregation definitions are either "not_set" or have the same aggregation type

        Fallback aggregation logic:
        - if "calculation_type" = 'dimension' && "aggregation_type" = 'not_set' --> 'group_by'
        - if "calculation_type" = 'metric' && "aggregation_type" = 'not_set' --> 'sum'

        :param agg_definitions: List of aggregation definitions
        :param use_fallback_aggregations:   Whether we should fallback to default aggregation definitions for 'not_set"
        :param calculation_type:  Defines type of the calculation for including this type (used for fallback aggregation logic)
        """
        # at least one of the types is invalid so we cannot reliably deduce the definition
        agg_types = {agg.type if agg else None for agg in agg_definitions}
        if None in agg_types:
            return None

        defined_definitions = [agg for agg in agg_definitions if agg and agg.type is not AggregationType.not_set]
        unique_defined_types = {agg_type for agg_type in agg_types if agg_type is not AggregationType.not_set}

        # all types are "not_set" or the list is empty
        if len(unique_defined_types) == 0:
            if use_fallback_aggregations:
                if calculation_type is TaxonTypeEnum.dimension:
                    return AggregationDefinition(type=AggregationType.group_by)
                else:
                    return AggregationDefinition(type=AggregationType.sum)
            else:
                return AggregationDefinition(type=AggregationType.not_set)

        # the list of provided aggregation definitions contains only aggregation type "not_set" or one common type
        if len(unique_defined_types) == 1:
            # use the common aggregation type
            # TODO: update this code to work with complex aggregation definition (merging them)
            return defined_definitions[0]

        # there's multiple different defined aggregation types
        return None
