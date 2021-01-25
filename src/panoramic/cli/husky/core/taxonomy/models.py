from typing import Any, Dict, List, Mapping, Optional, Tuple

from pydantic import root_validator

from panoramic.cli.husky.common.util import serialize_class_with_dict_props
from panoramic.cli.husky.core.pydantic.model import PydanticModel
from panoramic.cli.husky.core.sql_alchemy_util import safe_identifier
from panoramic.cli.husky.core.taxonomy.aggregations import AggregationDefinition
from panoramic.cli.husky.core.taxonomy.enums import (
    DIMENSION_AGGREGATION_TYPES,
    AggregationType,
    DisplayState,
    ValidationType,
)
from panoramic.cli.husky.core.tel.tel_phases import TelPhase


class TaxonTelMetadata(PydanticModel):
    """
    Tel Metadata, used for tracking dependencies between taxons, based on their calculations.
    """

    used_data_sources: List[Optional[str]] = []
    required_raw_taxons: List[str] = []
    optional_raw_taxons: List[str] = []
    used_taxons: List[str] = []
    phase: TelPhase = TelPhase.any

    can_compute_comparison: bool = False
    """
    Flag which determines whether TEL detected enough hints to believe that this taxon can be used in comparison query
    as a metric.
    """

    aggregation_definition: Optional[AggregationDefinition] = None
    """
    Definition of aggregation function based on taxon's calculation
    """


TaxonsToTelMetadata = Mapping[Tuple[str, str], TaxonTelMetadata]
"""
Contains mapping from (TaxonExpressionStr, company_id) to metadata object.
Cannot import TaxonExpressionStr here coz of circular dependency
"""


class Taxon(PydanticModel):
    slug: str
    """
    Automatically generated unique taxon identifier. Can include alphanumerics, underscores and colon characters.
    More precisely it looks as two taxon slugs joined together with dividing colon operator as "taxon_1:taxon_2"
    or a single slug such as "taxon_1".
    """

    display_name: str
    data_source: Optional[str]
    """
    Data source used to namespace the taxon. If not set, taxon is generic.
    """

    taxon_description: Optional[str]
    taxon_group: str
    taxon_type: str

    calculation: Optional[str]
    validation_type: ValidationType
    company_id: str

    aggregation: Optional[AggregationDefinition]
    """
    Definition of aggregation function including its parameters.
    """
    settings: Optional[Dict[str, Any]]
    display_state: DisplayState
    display_settings: Optional[Dict[str, Any]]

    comparison_taxon_slug_origin: Optional[str]
    """
    Any computed metric can be comparison, and have a slug in form of 'comparison@<metric_slug>'.
    Whenever a taxon is a comparison, it will have this property set to the slug of original metric it references to.
    In case of comparison@cpm, comparison_taxon_slug_origin would equal to 'cpm'.
    """

    acronym: Optional[str]

    tel_metadata: Optional[TaxonTelMetadata]

    @root_validator
    def _validate_aggregations(cls, values):
        if values.get('aggregation') and values.get('calculation'):
            raise ValueError('Computed taxons cannot have aggregation definition')

        if not values.get('aggregation') and not values.get('calculation'):
            raise ValueError('Raw taxons require aggregation definition')

        return values

    @classmethod
    def create(
        cls,
        slug: str,
        display_name: str,
        taxon_description: str,
        taxon_group: str,
        taxon_type: str,
        validation_type: str,
        company_id: str,
        settings: Optional[Dict[str, Any]],
        display_state: str,
        display_settings: Optional[Dict[str, Any]],
        comparison_taxon_slug_origin: Optional[str] = None,
        data_source: Optional[str] = None,
        acronym: Optional[str] = None,
        tel_metadata: Optional[Dict[str, Any]] = None,
        calculation: Optional[str] = None,
        aggregation: Optional[Dict[str, Any]] = None,
    ):

        obj = cls.parse_obj(
            dict(
                slug=slug,
                display_name=display_name,
                data_source=data_source,
                taxon_description=taxon_description,
                taxon_group=taxon_group,
                taxon_type=taxon_type,
                calculation=calculation,
                validation_type=validation_type,
                settings=settings or {},
                display_state=display_state,
                display_settings=display_settings or {},
                comparison_taxon_slug_origin=comparison_taxon_slug_origin,
                acronym=acronym,
                tel_metadata=tel_metadata,
                aggregation=aggregation,
                company_id=company_id,
            )
        )

        return obj

    def __repr__(self):
        return serialize_class_with_dict_props(self, self.dict())

    @property
    def is_computed_metric(self) -> bool:
        return self.calculation is not None

    @property
    def is_comparison_taxon(self) -> bool:
        """
        Determines whether this taxon is comparison
        """
        return self.comparison_taxon_slug_origin is not None

    @property
    def is_dimension(self) -> bool:
        agg_type = self.tel_metadata_aggregation_type
        return agg_type is not None and agg_type in DIMENSION_AGGREGATION_TYPES

    @property
    def is_metric(self) -> bool:
        return not self.is_dimension

    @property
    def tel_metadata_aggregation_type(self) -> Optional[AggregationType]:
        """
        Aggregation type defined in TEL metadata
        """
        if self.tel_metadata and self.tel_metadata.aggregation_definition:
            return self.tel_metadata.aggregation_definition.type
        else:
            if self.aggregation:
                return self.aggregation.type

            return None

    @property
    def can_have_comparison(self):
        """
        Return true if this taxon can be used for comparisons.
        """
        return self.tel_metadata and self.tel_metadata.can_compute_comparison

    @property
    def slug_expr(self):
        """
        Same like slug, but type any to not need to cast it everywhere.
        Ideally, return type would be TaxonExpressionStr, but that now causes quite difficult circular dependency :/
        """
        return self.slug

    @property
    def slug_safe_sql_identifier(self):
        """
        Returns slug that is safe to use on any database, especially on BigQuery.
        :return:
        """
        return safe_identifier(self.slug)


class RequestTempTaxon(Taxon):
    """
    Special taxon created only for the life of a single request, when querying formula (TEL expr starting with '=')
    """

    @classmethod
    def create_temp_taxon(cls, slug: str, calculation: str, taxon_type: str, company_id: str):
        return cls.construct(  # type: ignore
            slug=slug,
            metric_calculation=calculation,
            company_id=company_id,
            calculation=calculation,
            taxon_type=taxon_type,
        )


def taxon_slug_to_sql_friendly_slug(slug: str):
    """
    SQL identifiers cannot - in no way - contain double quotes. They can occur for taxonless querying.
    This fn replaces " by $, so it can be used as sql identifier.
    Not returning type, since TaxonExprStr is not available here.. :(
    """
    return slug.replace('"', "**")
