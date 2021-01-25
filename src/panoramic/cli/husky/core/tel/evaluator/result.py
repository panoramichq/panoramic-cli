from typing import List, Optional, Set, Tuple

from sqlalchemy.engine import default
from sqlalchemy.sql import ClauseElement

from panoramic.cli.husky.core.sql_alchemy_util import safe_quote_identifier
from panoramic.cli.husky.core.taxonomy.override_mapping.types import (
    OverrideMappingTelData,
)
from panoramic.cli.husky.core.tel.result import PreFormula
from panoramic.cli.husky.core.tel.sql_formula import SqlFormulaTemplate


class TelQueryResult:
    """
    Result of the AST expression evaluation.
    """

    _aggregations: List[PreFormula]
    """
    List of final formulas (columns) to be done before (pre) aggregation phase.
    """

    _dimension_formulas: List[PreFormula]
    """
    List of final column formulas for dimension phase.
    """

    _data_source_formula_templates: List[SqlFormulaTemplate]
    """
    Dim formulas that are pushed down to single husky, per data source.
    """

    _override_mappings: OverrideMappingTelData
    """
    List of override mappings referenced in the result
    """

    _exclude_slugs: Set[str]
    """
    Optional taxon slugs to exclude when rendering the post-aggregation formula template.
    It is used to render dimensions for PARTITION BY clause of cumulative window function,
    which requires all husky request dimensions only available after TelPlan is created.
    """

    _template: Optional[ClauseElement]
    """
    Optional result template, rendered in case there are some husky request dimensions
    available after the TelPlan is created. If there are no dimensions, then the str()
    function should be used on the result instead.
    """

    _sql: ClauseElement
    _label: Optional[str]
    _dialect: default.DefaultDialect

    def __init__(
        self,
        sql: ClauseElement,
        dialect: default.DefaultDialect,
        aggregations: List[PreFormula] = None,
        dimension_formulas: List[PreFormula] = None,
        data_source_formula_templates: List[SqlFormulaTemplate] = None,
        label: Optional[str] = None,
        override_mappings: Optional[OverrideMappingTelData] = None,
        exclude_slugs: Set[str] = None,
        template: Optional[ClauseElement] = None,
    ):
        self._sql = sql
        self._dialect = dialect
        self._aggregations = aggregations or []
        self._dimension_formulas = dimension_formulas or []
        self._data_source_formula_templates = data_source_formula_templates or []
        self._label = safe_quote_identifier(label, dialect) if label else None
        self._override_mappings = override_mappings or set()
        self._exclude_slugs = exclude_slugs or set()
        self._template = template

    @staticmethod
    def merge(
        sql: ClauseElement,
        dialect: default.DefaultDialect,
        *others: Optional['TelQueryResult'],
        label: Optional[str] = None,
        template: Optional[ClauseElement] = None,
    ) -> 'TelQueryResult':
        new_aggregations = []
        new_dimension_formulas = []
        new_data_source_formula_templates = []
        new_override_mappings = set()
        new_excluded_slugs = set()

        for other in others:
            if other:
                new_aggregations.extend(other._aggregations)
                new_dimension_formulas.extend(other._dimension_formulas)
                new_data_source_formula_templates.extend(other._data_source_formula_templates)
                new_override_mappings.update(other._override_mappings)
                new_excluded_slugs.update(other._exclude_slugs)

        return TelQueryResult(
            sql,
            dialect,
            aggregations=new_aggregations,
            dimension_formulas=new_dimension_formulas,
            data_source_formula_templates=new_data_source_formula_templates,
            label=label,
            override_mappings=new_override_mappings,
            exclude_slugs=new_excluded_slugs,
            template=template,
        )

    @property
    def sql(self) -> ClauseElement:
        return self._sql

    @property
    def aggregations(self) -> List[PreFormula]:
        return self._aggregations

    @property
    def dimension_formulas(self) -> List[PreFormula]:
        return self._dimension_formulas

    @property
    def data_source_formula_templates(self) -> List[SqlFormulaTemplate]:
        return self._data_source_formula_templates

    @property
    def label(self) -> Optional[str]:
        return self._label

    @property
    def override_mappings(self) -> OverrideMappingTelData:
        return self._override_mappings

    @property
    def exclude_slugs(self) -> Set[str]:
        return self._exclude_slugs

    @property
    def template(self) -> Optional[ClauseElement]:
        return self._template

    @property
    def template_or_sql(self) -> ClauseElement:
        return self._template if self._template is not None else self._sql

    @property
    def has_template(self):
        return self._template is not None

    def update(
        self,
        sql: Optional[ClauseElement] = None,
        aggregations: List[PreFormula] = None,
        dimension_formulas: List[PreFormula] = None,
        data_source_formula_templates: List[SqlFormulaTemplate] = None,
        label: Optional[str] = None,
        override_mappings: OverrideMappingTelData = None,
        excluded_slugs: Set[str] = None,
        template: Optional[ClauseElement] = None,
    ) -> 'TelQueryResult':
        return TelQueryResult(
            sql=sql if sql is not None else self._sql,
            dialect=self._dialect,
            aggregations=self._aggregations + (aggregations or []),
            dimension_formulas=self._dimension_formulas + (dimension_formulas or []),
            data_source_formula_templates=self._data_source_formula_templates + (data_source_formula_templates or []),
            label=label or self._label,
            override_mappings=override_mappings or self._override_mappings,
            exclude_slugs=self.exclude_slugs | (excluded_slugs or set()),
            template=template if template is not None else self._template,
        )


def result_with_template(fun, **results: Optional[TelQueryResult]) -> Tuple[ClauseElement, ClauseElement]:
    """
    Apply the function to each result's `.sql` and `.template`, returning a tuple of both results.
    :param fun: function accepting as many ClauseElements as there are results
    :param results: variable length list of results or Nones
    :return: Tuple containing ClauseElement for sql and template, to be used in the new Result
    """
    sql = fun(**{key: result.sql if result else None for key, result in results.items()})
    template = fun(**{key: result.template_or_sql if result else None for key, result in results.items()})
    return sql, template
