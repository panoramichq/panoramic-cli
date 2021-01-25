from typing import Dict, List, Optional, Set

from schematics.types import BooleanType, DictType, ListType, ModelType, StringType

from panoramic.cli.husky.common.exception_enums import ExceptionErrorCode
from panoramic.cli.husky.common.my_memoize import memoized_property
from panoramic.cli.husky.common.util import serialize_class_with_props
from panoramic.cli.husky.core.model.enums import (
    HuskyModelType,
    JoinDirection,
    JoinType,
    ModelVisibility,
    Relationship,
    TimeGranularity,
    ValueQuantityType,
)
from panoramic.cli.husky.core.model.exceptions import GenericModelException
from panoramic.cli.husky.core.schematics.model import (
    EnumType,
    NonEmptyStringType,
    SchematicsModel,
)
from panoramic.cli.husky.core.sql_alchemy_util import compile_query, quote_identifier
from panoramic.cli.husky.core.tel.tel_dialect import ModelTelDialect
from panoramic.cli.husky.service.context import (
    SNOWFLAKE_HUSKY_CONTEXT,
    HuskyQueryContext,
)


class ModelAttribute(SchematicsModel):
    taxon: str = NonEmptyStringType(required=True)
    identifier: bool = BooleanType(default=False)

    tel_transformation: str = NonEmptyStringType(required=True)
    """
    Model attribute transformation written in TEL.
    """

    quantity_type: ValueQuantityType = EnumType(ValueQuantityType, default=ValueQuantityType.scalar)

    column_sql_type: Optional[str] = NonEmptyStringType(default=None)
    """
    Type of the column this attribute points to.
    """

    @memoized_property
    def taxon_memoized(self):
        """
        Note the memoized property. It is cached on the instance after the first access.
        :return:
        """
        return self.taxon

    @memoized_property
    def identifier_memoized(self):
        """
        Note the memoized property. It is cached on the instance after the first access.
        :return:
        """
        return self.identifier

    def __repr__(self):
        return serialize_class_with_props(self)

    def __hash__(self):
        return hash(str(self.taxon_memoized))


class AttributeNotFound(RuntimeError):
    pass


class ModelJoin(SchematicsModel):
    join_type: JoinType = EnumType(JoinType, required=True)
    relationship: Relationship = EnumType(Relationship, required=True)
    direction: Optional[JoinDirection] = EnumType(JoinDirection, required=False)
    """
    Allows to explicitly define in which direction a join edge can be traversed.
    ModelJoin is defined on a model and references to a model.
    If direction is not defined, system will use relationship type to infer the allowed direction (for backward
    compatibility issues)
    - 'both', the join edge can be traversed from both models (defined and referenced)
    - 'outgoing', the join edge can be traversed from defined model only
    - 'incoming', the join edge can be traversed from referenced model to defined model only

    """

    to_model = NonEmptyStringType(required=True)

    taxons: Optional[List[str]] = ListType(NonEmptyStringType)
    """
    List of taxons on which the two models should be joined.
    Later, we can set more customizable joins, even joining different taxons on each other.
    """

    @memoized_property
    def join_type_memoized(self):
        """
        Note the memoized property. It is cached on the instance after the first access.
        :return:
        """
        return self.join_type

    @memoized_property
    def relationship_memoized(self):
        """
        Note the memoized property. It is cached on the instance after the first access.
        :return:
        """
        return self.relationship

    @memoized_property
    def direction_memoized(self):
        """
        Note the memoized property. It is cached on the instance after the first access.
        :return:
        """
        return self.direction

    @memoized_property
    def to_model_memoized(self):
        """
        Note the memoized property. It is cached on the instance after the first access.
        :return:
        """
        return self.to_model

    @memoized_property
    def taxons_memoized(self):
        """
        Note the memoized property. It is cached on the instance after the first access.
        :return:
        """
        return self.taxons


class HuskyModel(SchematicsModel):

    name: str = StringType(required=True, min_length=3)

    attributes: Dict[str, ModelAttribute] = DictType(ModelType(ModelAttribute), default=dict())
    """
    Key is name of the attribute. For usage, use attributes_memoized or attributes_by_taxon_memoized.
    """

    joins: List[ModelJoin] = ListType(ModelType(ModelJoin), default=[])
    """
    List of possible joins on other models
    """

    data_sources: List[str] = ListType(StringType(min_length=3), default=[], max_size=1)
    """
    Explicitly defined data sources.
    """

    model_type: Optional[HuskyModelType] = EnumType(HuskyModelType)
    """
    Optional attribute which defines type of the model explicitly
    """

    visibility: ModelVisibility = EnumType(ModelVisibility, default=ModelVisibility.hidden)

    company_id: str = NonEmptyStringType(required=True)

    project_id: Optional[str] = NonEmptyStringType(required=False)

    _alias: Optional[str] = None
    """
    Unique alias used in SQL for this model (if None, use full object name to reference columns)
    """

    time_granularity: Optional[TimeGranularity] = EnumType(TimeGranularity)
    """
    Optional attribute which sets time granularity explicitly (in case it cannot be inferred from model's name)
    """

    fully_qualified_name_parts: Optional[List[str]] = ListType(NonEmptyStringType(required=True))
    """
    All parts of the fully qualified name. Can contain 2..N values, depending on the actual federated database.

    Example:
    - physical data source (always first)
    - database name
    - schema name
    - table name
    """

    def __repr__(self):
        return serialize_class_with_props(self)

    @property
    def number_of_identifiers(self) -> int:
        return len(self.identifier_attributes)

    @property
    def identifier_attributes(self) -> Set[ModelAttribute]:
        return {attr for attr in self.attributes_memoized.values() if attr.identifier_memoized}

    @property
    def identifier_taxon_slugs(self) -> Set[str]:
        return {attr.taxon for attr in self.identifier_attributes}

    @property
    def physical_data_source(self) -> str:
        """
        Gets name of physical data source.
        """
        if self.fully_qualified_name_parts is None:
            raise ValueError('Missing physical data source')
        else:
            return self.fully_qualified_name_parts[0]

    def full_object_name(self, ctx: HuskyQueryContext) -> str:
        """
        Full name of the database object, including db and schema name.
        """
        assert self.fully_qualified_name_parts

        full_object_name = '.'.join(
            [quote_identifier(part, ctx.dialect) for part in self.fully_qualified_name_parts[1:]]
        )

        # sanity check that we have ANY name
        if not full_object_name:
            raise GenericModelException(
                'You are working with federated model so you need to turn on the appropriate feature flag',
                self.name,
                ExceptionErrorCode.FDQ_FLAG_REQUIRED,
            )

        return full_object_name

    @property
    def table_alias(self) -> Optional[str]:
        """
        Optional table alias to keep reference to the model unique (in case it is joined multiple times in query)
        """
        return self._alias

    @table_alias.setter
    def table_alias(self, alias: str):
        """
        Setter for optional table alias

        :param alias: New table alias
        """
        self._alias = alias

    @table_alias.deleter
    def table_alias(self):
        """
        Removes table alias
        """
        self._alias = None

    def unique_object_name(self, ctx: HuskyQueryContext) -> str:
        """
        Unique model reference within query
        :param ctx:
        """
        identifier = self.full_object_name(ctx) if self.table_alias is None else self.table_alias
        return identifier

    @property
    def graph_name(self) -> str:
        """
        Unique name in graph
        """
        return self.name if self.table_alias is None else self.table_alias

    @property
    def is_entity(self) -> bool:
        """
        Returns if the model is entity or not. Derived from model name at runtime.
        """
        if self.model_type is not None:
            return self.model_type is HuskyModelType.ENTITY

        return 'entity' in self.name.lower()

    @memoized_property
    def data_source(self) -> str:
        """
        Returns data source. Newly, all models have exactly one data source.
        """
        return self.data_sources[0]

    def get_attribute_by_taxon(self, taxon_slug: str) -> ModelAttribute:
        attribute = self.attributes_by_taxon_memoized.get(taxon_slug)
        if attribute:
            return attribute
        else:
            raise AttributeNotFound(f'Attribute with taxon {taxon_slug} not found.')

    def has_taxon(self, taxon: str) -> bool:
        try:
            self.get_attribute_by_taxon(taxon)
            return True
        except AttributeNotFound:
            return False

    @memoized_property
    def attributes_memoized(self):
        """
        Note the memoized property. It is cached on the instance after the first access.
        :return:
        """
        return self.attributes

    @memoized_property
    def attributes_by_taxon_memoized(self) -> Dict[str, ModelAttribute]:
        """
        Note the memoized property. It is cached on the instance after the first access.
        :return:
        """
        return {attr.taxon_memoized: attr for attr in self.attributes_memoized.values()}

    @memoized_property
    def joins_memoized(self):
        """
        Note the memoized property. It is cached on the instance after the first access.
        :return:
        """
        return self.joins

    @property
    def taxons(self) -> Set[str]:
        return set(self.attributes_by_taxon_memoized.keys())

    def taxon_sql_accessor(
        self,
        ctx: HuskyQueryContext,
        taxon_slug: str,
        cast_array: bool = False,
        model_tel_dialect: Optional[ModelTelDialect] = None,
    ) -> str:
        """
        Helper function that returns full sql accessor to given taxon on the model

        :param ctx:                     Husky query context
        :param taxon_slug               Original taxon slug
        :param cast_array               Automatically handle arrays by casting them to string (default is False)
        :param model_tel_dialect        Initialized model TEL dialect, if there is one (we use it to check for cyclic reference).

        """
        attribute = self.get_attribute_by_taxon(taxon_slug)

        # let TEL grammar to render the SQL transformation
        # on purpose, we dont use 'column' variable here, because we dont really rely on column_name attribute here
        tel_dialect = model_tel_dialect
        if tel_dialect is None:
            # no initialized tel visitor is provided so create a generic one
            tel_dialect = ModelTelDialect(
                unique_object_name=self.unique_object_name(ctx),
                virtual_data_source=self.data_sources[0],
                model=self,
            )

        # render the TEL transformation
        parsed_expression = tel_dialect.render(attribute.tel_transformation, ctx, {})
        sql_accessor = compile_query(parsed_expression.sql(ctx.dialect), ctx.dialect)

        # we cast arrays to varchar, if requested
        if cast_array and attribute.quantity_type is ValueQuantityType.array:
            sql_accessor = f'CAST({sql_accessor} AS VARCHAR)'

        return sql_accessor

    def __hash__(self):
        return hash(self.unique_object_name(SNOWFLAKE_HUSKY_CONTEXT))

    def add_attribute(self, model_attribute: ModelAttribute):
        """
        Adds attribute to a model. It should be used only in very edge-cases.

        One of the use cases is dynamically adding attributes when working with normalized values.

        :param model_attribute: Model attribute
        """
        self.attributes[model_attribute.taxon] = model_attribute
        self.attributes_memoized[model_attribute.taxon] = model_attribute
        self.attributes_by_taxon_memoized[model_attribute.taxon] = model_attribute

    def remove_attribute(self, taxon_slug: str):
        """
        Removes attribute from model by taxon slug
        :param taxon_slug: Taxon.slug
        """
        self.attributes.pop(taxon_slug, None)
        self.attributes_memoized.pop(taxon_slug, None)
        self.attributes_by_taxon_memoized.pop(taxon_slug, None)

    def add_join(self, model_join: ModelJoin):
        self.joins.append(model_join)
