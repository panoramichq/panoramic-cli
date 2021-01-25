from collections import namedtuple
from typing import Callable, Iterable, List, Optional, Set

from sqlalchemy.engine import default

from panoramic.cli.husky.core.taxonomy.aggregations import AggregationDefinition
from panoramic.cli.husky.core.taxonomy.enums import TaxonTypeEnum
from panoramic.cli.husky.core.taxonomy.models import Taxon
from panoramic.cli.husky.core.tel.exceptions import TelExpressionException
from panoramic.cli.husky.service.utils.taxon_slug_expression import (
    TaxonExpressionStr,
    TaxonMap,
)


class LabelMaker:
    """
    Provider of unique labels for a Tel expression.
    """

    def __init__(self, prefix=''):
        """
        :param prefix: Prefix for helper columns. Should be the taxon slug of rendered taxon.
        """
        self._unique_counter = 0
        self._prefix = prefix

    def next(self) -> str:
        """
        Returns a string to be used as unique temp column name. If prefix contains special characters, the string must
        be properly quoted before using in sql.
        """
        self._unique_counter += 1
        return f'__{self._prefix}{str(self._unique_counter)}'

    @property
    def prefix(self):
        return self._prefix


ParserLocation = namedtuple('ParserLocation', ['position', 'line', 'text'])


class TelValidationContext:
    """
    Context containing semantic errors.
    """

    _errors: List[str]

    def __init__(self, root_context: 'TelRootContext'):
        self._root_context = root_context
        self._errors = []

    @property
    def errors(self) -> Iterable[str]:
        return frozenset(self._errors)

    @property
    def root_context(self):
        return self._root_context

    def with_error(self, error: str, location: ParserLocation) -> 'TelValidationContext':
        self._errors.append(
            f'{error}. Occurred at position {location.position}, line {location.line} in expression "{location.text}"'
        )

        return self

    def raise_for_errors(self):
        if self._errors:
            raise self.exception

    @property
    def exception(self):
        return TelExpressionException('\n'.join(self._errors))

    @property
    def has_errors(self) -> bool:
        return len(self._errors) > 0


def node_id_maker():
    node_id = 0

    def next_node_id():
        nonlocal node_id
        result = node_id
        node_id += 1
        return result

    return next_node_id


class TelRootContext:
    """
    Root Tel context providing necessary options to generate correct result.
    """

    _validation_context: Optional[TelValidationContext]

    def __init__(
        self,
        husky_context,  # HuskyQueryContext
        tel_dialect,  # Type[TelDialect]
        allowed_data_sources: Optional[Iterable[str]],
        taxon_map: TaxonMap,
        next_node_id: Callable[[], int],
        depth=0,
        label_maker: Optional[LabelMaker] = None,
        is_benchmark=False,
        taxon_type: TaxonTypeEnum = TaxonTypeEnum.metric,
        taxon_slug: str = '',
        aggregation: Optional[AggregationDefinition] = None,
        subrequest_only: bool = False,
    ):
        """
        Renders full given expression, including referenced computed taxons, recursively.
        :param is_benchmark: Set to true if rendering sql for comparison dataframe.
        :param allowed_data_sources: If set, only render or crawl namespaced taxons that have data source in this set.
        :param taxon_map: Cached taxon map that must contain all taxons used
        :param taxon_type. Type of the taxon we are generating TEL for.
        :param taxon_slug: Taxon slug we are generating the query for. Used for naming temporal and final SQL columns.
        :param aggregation: Definition of aggregation function for the calculation
        """

        self._husky_context = husky_context
        self._tel_dialect = tel_dialect
        self._allowed_data_sources = set(allowed_data_sources) if allowed_data_sources is not None else None
        self._taxon_map = taxon_map
        self._depth = depth
        self._label_maker = label_maker or LabelMaker(taxon_slug)
        self._is_benchmark = is_benchmark
        self._taxon_type = taxon_type
        self._taxon_slug = taxon_slug
        self._aggregation = aggregation
        self._subrequest_only = subrequest_only
        self._validation_context = None
        self.next_node_id = next_node_id

    def __repr__(self):
        return f"""TelRootContext(
allowed_data_sources={repr(self._allowed_data_sources)},
depth={repr(self.depth)},
is_benchmark={repr(self._is_benchmark)},
taxon_type={repr(self._taxon_type.value)},
taxon_slug={repr(self._taxon_slug)},
aggregation={repr(self._aggregation)},
subrequest_only={repr(self._subrequest_only)},
)
"""

    def is_data_source_allowed(self, data_source: str) -> bool:
        if self._allowed_data_sources is None:
            return True
        else:
            return data_source in self._allowed_data_sources

    def taxon_from_slug(self, slug: TaxonExpressionStr) -> Optional[Taxon]:
        return self._taxon_map.get(slug)

    @property
    def depth(self) -> int:
        return self._depth

    @property
    def new_label(self) -> str:
        return self._label_maker.next()  # noqa

    @property
    def label_maker(self) -> LabelMaker:
        return self._label_maker

    @property
    def nested(self):
        new = TelRootContext(
            husky_context=self._husky_context,
            tel_dialect=self.tel_dialect,
            allowed_data_sources=self._allowed_data_sources,
            taxon_map=self._taxon_map,
            next_node_id=self.next_node_id,
            depth=self._depth + 1,
            label_maker=self._label_maker,
            is_benchmark=self._is_benchmark,
            taxon_type=self._taxon_type,
            taxon_slug=self._taxon_slug,
            aggregation=self._aggregation,
            subrequest_only=self._subrequest_only,
        )
        new.validation_context = self._validation_context

        return new

    @property
    def validation_context(self):
        return self._validation_context

    @validation_context.setter
    def validation_context(self, validation_context):
        self._validation_context = validation_context

    @property
    def is_benchmark(self):
        return self._is_benchmark

    @property
    def taxon_type(self):
        return self._taxon_type

    @property
    def aggregation(self):
        return self._aggregation

    @property
    def subrequest_only(self):
        return self._subrequest_only

    @property
    def allowed_data_sources(self) -> Optional[Set[str]]:
        return self._allowed_data_sources if self._allowed_data_sources is not None else None

    @property
    def taxon_slug(self):
        return self._taxon_slug

    @property
    def husky_context(self):
        return self._husky_context

    @property
    def husky_dialect(self) -> default.DefaultDialect:
        return self._husky_context.dialect

    @property
    def tel_dialect(self):
        return self._tel_dialect
