from typing import Any, Dict, List, Optional, Set

from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from sqlalchemy import literal_column, null
from sqlalchemy.engine import default
from sqlalchemy.sql import ClauseElement

from panoramic.cli.husky.core.sql_alchemy_util import compile_query, safe_identifier
from panoramic.cli.husky.core.taxonomy.aggregations import AggregationDefinition
from panoramic.cli.husky.core.taxonomy.enums import AggregationType
from panoramic.cli.husky.core.taxonomy.models import Taxon
from panoramic.cli.husky.core.taxonomy.override_mapping.types import (
    OverrideMappingTelData,
)
from panoramic.cli.husky.core.tel.sql_formula import SqlFormulaTemplate, SqlTemplate
from panoramic.cli.husky.core.tel.tel_phases import TelPhase
from panoramic.cli.husky.core.tel.types.tel_types import TelDataType, TelType
from panoramic.cli.husky.service.utils.taxon_slug_expression import (
    TaxonExpressionStr,
    TaxonMap,
)


class PreFormula:
    """
    Contains sql formula for dimension and preaggregation phases.
    """

    formula: ClauseElement
    """
    The sql formula
    """
    label: str
    """
    Temporary column name, to be able to reference the query from post formula
    """

    aggregation: AggregationDefinition
    """
    Definition of aggregation function
    """

    def __init__(
        self,
        formula: ClauseElement,
        label: str,
        aggregation: Optional[AggregationDefinition] = None,
        data_source: Optional[str] = None,
    ):
        self.formula = formula
        self.label = safe_identifier(label)
        self.aggregation = aggregation or AggregationDefinition(type=AggregationType.sum)
        self.data_source = data_source

    def __repr__(self):
        # Repr as python code os it is easy copy paste to tests.
        ds_string = f"'{self.data_source}'" if self.data_source else None
        return f"PreFormula('''{compile_query(self.formula, SnowflakeDialect())}''','''{self.label}''', {repr(self.aggregation)}, {ds_string})"

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.__dict__ == other.__dict__


class PostFormula:
    """
    Contains sql formula for post-aggregation phase with optional template parameters.
    It is used to render dimensions for PARTITION BY clause of cumulative window function,
    which requires all husky request dimensions only available after TelPlan is created.
    """

    DIMENSION_SLUGS_TEMPLATE_PARAM = 'dimension_slugs'
    """
    Optional template parameter to render a list of dimension taxon slugs
    """

    _sql: ClauseElement
    """
    SQL formula
    """

    _template: Optional[ClauseElement]
    """
    Optional SQL formula template, used in case there are some dimension slugs to render
    """

    _exclude_slugs: Set[TaxonExpressionStr]
    """
    Taxon slugs to exclude from DIMENSION_SLUGS_TEMPLATE_PARAM when rendering the final SQL formula
    """

    def __init__(
        self,
        sql: ClauseElement,
        template: Optional[ClauseElement] = None,
        exclude_slugs: Optional[Set[TaxonExpressionStr]] = None,
    ):
        self._sql = sql
        self._template = template
        self._exclude_slugs = exclude_slugs or set()

    def render_formula(
        self, dialect: default.DefaultDialect, dimension_slugs: Optional[Set[TaxonExpressionStr]] = None
    ) -> ClauseElement:
        """
        Render the final SQL formula by replacing DIMENSION_SLUGS_TEMPLATE_PARAM with the comma separated
        list of provided dimension_slugs. Any slugs in the _exclude_slugs attribute will not be included.
        If the final set of dimension slugs is empty, then the unchanged _sql formula is returned instead.
        """

        valid_dimension_slugs = (dimension_slugs or set()).difference(self._exclude_slugs)
        if self._template is not None and valid_dimension_slugs:
            template = SqlTemplate(compile_query(self._template, dialect))
            template_mapping = {self.DIMENSION_SLUGS_TEMPLATE_PARAM: ', '.join(sorted(valid_dimension_slugs))}
            return literal_column(template.substitute(template_mapping))

        return self._sql

    def __repr__(self) -> str:
        return f"PostFormula('''{compile_query(self._sql)}''', template={compile_query(self._template) if self._template is not None else None}, exclude_slugs=set({repr(sorted(self._exclude_slugs))}))"

    def __eq__(self, other) -> bool:
        if isinstance(other, self.__class__):
            return all(
                [
                    self._sql == other._sql,
                    self._template == other._template,
                    self._exclude_slugs == other._exclude_slugs,
                ]
            )
        return False

    @property
    def template(self):
        return self._template

    @property
    def exclude_slugs(self):
        return self._exclude_slugs


class UsedTaxonsContainer:
    """
    Container for required and optional taxons
    """

    def __init__(self):
        self.required_taxons: TaxonMap = dict()
        self.optional_taxons: TaxonMap = dict()

    @staticmethod
    def optional(taxon: Taxon):
        result = UsedTaxonsContainer()
        result.add_taxon(taxon, True)
        return result

    @staticmethod
    def required(taxon: Taxon):
        result = UsedTaxonsContainer()
        result.add_taxon(taxon, False)
        return result

    def update_from(self, other: 'UsedTaxonsContainer'):
        self.optional_taxons.update(other.optional_taxons)
        self.required_taxons.update(other.required_taxons)

    def add_taxon(self, taxon: Taxon, optional: bool = False):
        if optional:
            self.optional_taxons[taxon.slug_expr] = taxon
        else:
            self.required_taxons[taxon.slug_expr] = taxon

    def has_some(self):
        return len(self.optional_taxons) > 0 or len(self.required_taxons) > 0

    @property
    def all_taxons(self) -> TaxonMap:
        return {**self.optional_taxons, **self.required_taxons}

    @property
    def required_taxon_slugs(self) -> Set[TaxonExpressionStr]:
        return set(self.required_taxons.keys())

    @property
    def optional_taxon_slugs(self) -> Set[TaxonExpressionStr]:
        return set(self.optional_taxons.keys())

    @staticmethod
    def merge(left: 'UsedTaxonsContainer', right: 'UsedTaxonsContainer') -> 'UsedTaxonsContainer':
        result = UsedTaxonsContainer()
        result.required_taxons.update(left.required_taxons)
        result.required_taxons.update(right.required_taxons)
        result.optional_taxons.update(left.optional_taxons)
        result.optional_taxons.update(right.optional_taxons)
        return result


class UsedTaxonSlugsContainer:
    """
    Container for required and optional slugs
    """

    def __init__(self):
        self.required_slugs: Set[TaxonExpressionStr] = set()
        self.optional_slugs: Set[TaxonExpressionStr] = set()

    def update_from(self, other: 'UsedTaxonSlugsContainer'):
        self.optional_slugs.update(other.optional_slugs)
        self.required_slugs.update(other.required_slugs)

    def add_slug(self, slug: TaxonExpressionStr, optional: bool = False):
        if optional:
            self.optional_slugs.add(slug)
        else:
            self.required_slugs.add(slug)

    def has_some(self) -> bool:
        return len(self.optional_slugs) > 0 or len(self.required_slugs) > 0

    @property
    def all_slugs(self) -> Set[TaxonExpressionStr]:
        return self.optional_slugs | self.required_slugs

    @classmethod
    def create_from_taxons(cls, taxon_container: UsedTaxonsContainer):
        new = UsedTaxonSlugsContainer()
        new.optional_slugs = set(taxon_container.optional_taxons.keys())
        new.required_slugs = set(taxon_container.required_taxons.keys())
        return new

    def __eq__(self, other):
        return self.all_slugs == other.all_slugs

    def __hash__(self):
        return hash(self.all_slugs)

    def __repr__(self):
        return f'UsedTaxonSlugsContainer(optional_slugs={repr(self.optional_slugs)}, required_slugs={repr(self.required_slugs)})'


class ExprResult:
    pre_formulas: List[PreFormula]
    """
    List of final formulas (columns) to be done before (pre) aggregation phase.
    """
    post_formula: Optional[PostFormula]
    """
    Formula to be used in post aggregation phase, e.g. division of summed taxons.
    """
    _output: List[str]
    """
    As long as a result can be done pre aggregation, it is stored only to _output.
    Once we need to also add post_formula, the expression cannot be solely computed in pre aggregation, and
    thus must_be_post is set to True. Then this property becomes irrelevant.
    """
    used_taxons: UsedTaxonSlugsContainer
    """
    Set of all taxons used in the expression, recursively.
    """
    invalid_value: bool
    """
    Flag if the result has no valid value, typically when referencing to taxons from which none are available.
    """

    phase: TelPhase = TelPhase.any

    return_data_sources: Set[Optional[str]]
    """
    Set of taxon namespaces this result is referencing to.
    Return data sources are set only when rendering sql.
    """

    return_type: TelType
    """
    Type of this result with information about the data type and
    whether it is a constant. It is set to ANY data type by default
    and checked only when rendering SQL.
    """

    dimension_formulas: List[PreFormula]
    """
    List of final column formulas for dimension phase.
    """

    data_source_formula_templates: List[SqlFormulaTemplate]
    """
    Dim formulas that are pushed down to single husky, per data source.
    """

    template_slugs: Set[TaxonExpressionStr]
    """
    Set of taxons used in a template formula. To be used when rendering the template.
    """

    override_mappings: OverrideMappingTelData
    """
    List of override mappings referenced in the result
    """

    debug_info: Optional[Any]
    """
    Optional debug info, from processing the Tel AST
    """

    def __init__(
        self,
        pre_formulas: List[PreFormula] = None,
        post_formula=None,
        dimension_formulas: List[PreFormula] = None,
        data_source_formula_templates: List[SqlFormulaTemplate] = None,
        phase: TelPhase = TelPhase.any,
        used_taxons: Optional[UsedTaxonSlugsContainer] = None,
        invalid_value: bool = False,
        return_data_sources: Set[Optional[str]] = None,
        return_type: Optional[TelType] = None,
        template_slugs: Set[TaxonExpressionStr] = None,
        debug_info: Optional[Any] = None,
        override_mappings: Optional[OverrideMappingTelData] = None,
    ):
        self.pre_formulas = [] if pre_formulas is None else pre_formulas
        self.dimension_formulas = [] if dimension_formulas is None else dimension_formulas
        self.post_formula = post_formula
        self._output = []
        self.used_taxons = used_taxons or UsedTaxonSlugsContainer()
        self.invalid_value = invalid_value
        self.return_data_sources = return_data_sources or set()
        self.return_type = return_type or TelType(TelDataType.ANY, is_constant=True)
        self.phase = phase
        self.data_source_formula_templates = data_source_formula_templates or []
        self.template_slugs = template_slugs or set()
        self.debug_info = debug_info
        self.override_mappings = override_mappings or set()

    def add_to_output(self, value: str, position=None):
        assert self.post_formula is None, 'Adding to output when post_formula is set'
        if position is not None:
            self._output.insert(position, value)
        else:
            self._output.append(value)

    @property
    def has_pre_formulas(self) -> bool:
        return len(self.pre_formulas) > 0

    def merge_with(self, other: 'ExprResult', with_type: bool = True):
        """
        Helper fn to copy various values from other result to self.
        """
        if self.invalid_value is False and other.invalid_value is False:
            # Copy the values only if source and target results are valid.
            self.used_taxons.update_from(other.used_taxons)
            self.data_source_formula_templates.extend(other.data_source_formula_templates)
            self.dimension_formulas.extend(other.dimension_formulas)
            self.pre_formulas.extend(other.pre_formulas)
            self.return_data_sources.update(other.return_data_sources)
            self.template_slugs.update(other.template_slugs)
            self.override_mappings.update(other.override_mappings)

            if with_type:
                self.return_type = TelType.return_common_type([self.return_type, other.return_type])

    def sql(self, dialect: default.DefaultDialect) -> ClauseElement:
        """
        Returns this results sql statement, which is either post formula or the generate output.
        :return:
        """
        if self.post_formula and not self.invalid_value:
            return self.post_formula.render_formula(dialect)

        return null()

    def __repr__(self):
        return (
            f'ExprResult(post_formula={repr(self.post_formula)}, '
            f'invalid_value={repr(self.invalid_value)}, '
            f'sql={self.sql(SnowflakeDialect())}, '
            f'phase={repr(self.phase.name)}, '
            f'return_type={repr(self.return_type.data_type.name)}, '
            f'return_data_sources={repr(self.return_data_sources)}, '
            f'pre_formulas={repr(self.pre_formulas)}, '
            f'dimension_formulas={repr(self.dimension_formulas)}, '
            f'data_source_formula_templates={repr(self.data_source_formula_templates)}, '
            f'template_slugs={repr(self.template_slugs)})'
        )


TaxonToTemplate = Dict[TaxonExpressionStr, SqlFormulaTemplate]
