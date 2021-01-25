from collections import Counter
from enum import Enum
from typing import Dict, List, Optional, Set

from pydantic import Field, root_validator
from pydantic.error_wrappers import ErrorWrapper, ValidationError

from panoramic.cli.husky.core.enums import DbDataType
from panoramic.cli.husky.core.federated.model.tel.data_structures import (
    AttributeValidationTelVisitorParams,
)
from panoramic.cli.husky.core.federated.model.tel.exceptions import (
    ModelTelCyclicReferenceException,
)
from panoramic.cli.husky.core.federated.model.tel.visitor import (
    AttributeValidationTelVisitor,
)
from panoramic.cli.husky.core.model.enums import JoinType, ModelVisibility, Relationship
from panoramic.cli.husky.core.pydantic.model import PydanticModel
from panoramic.cli.husky.core.tel.tel_dialect import ModelTelDialect


class FdqModelJoinRelationship(Enum):
    one_to_one = Relationship.one_to_one.value
    many_to_one = Relationship.many_to_one.value


class FdqModelAttribute(PydanticModel):
    data_type: Optional[DbDataType]
    """
    Type of the column this attribute points to.
    """

    data_reference: str = Field(..., min_length=1)
    """
    Transformation applied to the model attribute
    """

    field_map: List[str]
    """
    List of taxon slugs
    """


class FdqModelJoin(PydanticModel):
    """
    Description of model join from API contract
    """

    join_type: JoinType
    """
    Join type
    """

    to_model: str = Field(..., min_length=1)
    """
    Model name from the right part of the join
    """

    relationship: FdqModelJoinRelationship
    """
    Join relationship (1:1, N:1)
    """

    taxons: List[str] = Field(..., min_items=1, alias='fields')
    """
    List of joining taxon slugs
    """


class FdqModel(PydanticModel):
    model_name: str = Field(..., min_length=1)
    """
    Name of the model. API will add pano_ds| prefix.
    """

    data_source: str = Field(..., min_length=1)
    """
    Full table name in form of
    [physical_data_source].[db].[schema].table (db and schema is optional, depending on federated db type)
    Backend must verify, that company has access to physical_data_source.
    """

    attributes: List[FdqModelAttribute] = Field(..., alias='fields')
    """
    List of attributes. Note that this is an array, not a dict anymore.
    Dict does not help with anything, and we will eventually remove it from HuskyModel as well.
    Backend will add pano_ds| in front of each taxon slug, and create it if needed.
    """

    joins: List[FdqModelJoin] = []
    """
    List of possible joins on other models
    """

    visibility: ModelVisibility

    identifiers: List[str] = []
    """
    List of taxons that are identifiers of the model
    """

    @property
    def physical_data_source(self) -> str:
        return self.data_source.split('.')[0]

    @classmethod
    def _get_available_attrs_taxon_slugs(cls, attributes: List[FdqModelAttribute]) -> List[str]:
        """
        Gets list of available taxon slugs for given attributes
        """
        available_taxon_slugs: List[str] = []
        for attr in attributes:
            available_taxon_slugs.extend(attr.field_map)

        return available_taxon_slugs

    @root_validator
    def validate_unique_taxon_slugs(cls, values):
        """
        Validate that each taxon slug is used at most once in the list of attributes
        """
        if 'attributes' in values:
            # count occurrence of each taxon slug in attributes
            attributes: List[FdqModelAttribute] = values['attributes']
            taxon_slugs = cls._get_available_attrs_taxon_slugs(attributes)

            taxon_slugs_counter = Counter(taxon_slugs)

            multiple_taxon_slugs = [
                taxon_slug for taxon_slug, occurrence in taxon_slugs_counter.items() if occurrence > 1
            ]
            if len(multiple_taxon_slugs):
                raise ValueError('Following fields are mapped more than once - ' + ','.join(multiple_taxon_slugs))

        return values

    @root_validator
    def validate_model_attributes_tel(cls, values):
        """
        Validates that model attributes contain correct TEL expressions without cyclic dependencies
        """
        if 'attributes' in values:
            attributes: List[FdqModelAttribute] = values['attributes']

            # get set of available taxon slugs
            available_taxon_slugs = set(cls._get_available_attrs_taxon_slugs(attributes))
            # our TEL visitor only verifies here that all available TEL transformations are valid
            # we dont care about the SQL output here

            invalid_taxons_attr = {}
            for attribute in attributes:
                try:
                    # we dont really care from which taxon we start
                    # if there is cyclic reference somewhere, we will run into it eventually
                    taxon_slug = next(filter(None, attribute.field_map))
                except StopIteration:
                    # we dont care, if we dont find the taxon - this check is performed somewhere else
                    continue

                try:
                    # initialize options for the visitor
                    visitor_parameters = AttributeValidationTelVisitorParams(taxon_slug, attributes)

                    tree = ModelTelDialect.parse(attribute.data_reference)
                    visitor = AttributeValidationTelVisitor(visitor_parameters)
                    visitor.visit(tree)

                    # check whether this TEL transformation uses any taxons which arent available in this model
                    additional_taxons = visitor.result.used_taxon_slugs - available_taxon_slugs
                    if len(additional_taxons) > 0:
                        invalid_taxons_attr[attribute.data_reference] = additional_taxons

                except ModelTelCyclicReferenceException:
                    # there's no point in checking other attributes when we run into cyclic reference
                    raise ValueError(
                        f'Data reference "{attribute.data_reference}" contains TEL transformation with cyclic reference'
                    )

            # if we found any attribute with missing taxons, output them all in one error message
            if invalid_taxons_attr:
                attribute_msgs = [
                    f'Data reference "{attr_key}": {", ".join(taxon_slugs)} not available in this model'
                    for attr_key, taxon_slugs in invalid_taxons_attr.items()
                ]

                raise ValidationError([ErrorWrapper(ValueError(msg), 'attributes') for msg in attribute_msgs], cls)

        return values

    @root_validator
    def validate_joins_correct_taxons(cls, values):
        """Check list of fields in joins against all available taxons on model."""
        if 'attributes' in values and 'joins' in values:
            attributes: List[FdqModelAttribute] = values['attributes']
            joins: List[FdqModelJoin] = values['joins']

            # get set of available taxon slugs
            available_taxon_slugs = set(cls._get_available_attrs_taxon_slugs(attributes))

            # for each join, verify that all its taxons are available in this model
            invalid_joins: Dict[int, Set[str]] = {}
            for idx, join in enumerate(joins):
                missing_taxons = set(join.taxons) - available_taxon_slugs
                if len(missing_taxons):
                    invalid_joins[idx] = missing_taxons

            if invalid_joins:
                # report invalid joins
                raise ValidationError(
                    [
                        ErrorWrapper(
                            ValueError(f'Join {idx + 1} contains missing fields {",".join(taxon_slugs)}'), 'joins'
                        )
                        for idx, taxon_slugs in invalid_joins.items()
                    ],
                    cls,
                )

        return values

    @root_validator
    def validate_identifiers_correct_taxons(cls, values):
        """Check list of identifiers against all available taxons on model."""
        if 'attributes' in values and 'identifiers' in values:
            attributes: List[FdqModelAttribute] = values['attributes']
            identifiers: List[str] = values['identifiers']

            # get set of available taxon slugs
            available_taxon_slugs = set(cls._get_available_attrs_taxon_slugs(attributes))

            # verify that all identifier taxons are available in this model
            invalid_ids = set(identifiers) - available_taxon_slugs

            if len(invalid_ids) > 0:
                raise ValueError(f'Identifier(s) {", ".join(invalid_ids)} are not present as fields on the model')

        return values
