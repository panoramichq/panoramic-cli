from typing import List, Optional

import pytest

from panoramic.cli.husky.core.taxonomy.aggregations import AggregationDefinition
from panoramic.cli.husky.core.taxonomy.enums import AggregationType, TaxonTypeEnum


def get_case_id_common_defined_agg(val):
    if isinstance(val, list):
        return ', '.join(agg.type if agg else 'None' for agg in val)
    elif isinstance(val, AggregationDefinition):
        return str(val.type.value if val else None)
    else:
        return str(val)


@pytest.mark.parametrize(
    "agg_definitions, fallback, taxon_type, expected",
    [
        [
            [AggregationDefinition(type=AggregationType.sum), AggregationDefinition(type=AggregationType.sum)],
            False,
            None,
            AggregationDefinition(type=AggregationType.sum),
        ],
        [
            [AggregationDefinition(type=AggregationType.not_set), AggregationDefinition(type=AggregationType.avg)],
            False,
            None,
            AggregationDefinition(type=AggregationType.avg),
        ],
        [
            [AggregationDefinition(type=AggregationType.not_set), AggregationDefinition(type=AggregationType.not_set)],
            False,
            None,
            AggregationDefinition(type=AggregationType.not_set),
        ],
        [
            [AggregationDefinition(type=AggregationType.min), None, AggregationDefinition(type=AggregationType.min)],
            False,
            None,
            None,
        ],
        [
            [AggregationDefinition(type=AggregationType.not_set), AggregationDefinition(type=AggregationType.not_set)],
            True,
            TaxonTypeEnum.dimension,
            AggregationDefinition(type=AggregationType.group_by),
        ],
        [
            [AggregationDefinition(type=AggregationType.not_set), AggregationDefinition(type=AggregationType.not_set)],
            True,
            TaxonTypeEnum.metric,
            AggregationDefinition(type=AggregationType.sum),
        ],
        [[], True, TaxonTypeEnum.metric, AggregationDefinition(type=AggregationType.sum)],
        [[], True, TaxonTypeEnum.dimension, AggregationDefinition(type=AggregationType.group_by)],
        [[], False, None, AggregationDefinition(type=AggregationType.not_set, params=None)],
        [
            [
                AggregationDefinition(type=AggregationType.sum),
                AggregationDefinition(type=AggregationType.not_set),
                AggregationDefinition(type=AggregationType.avg),
            ],
            False,
            None,
            None,
        ],
        [
            [
                AggregationDefinition(type=AggregationType.sum),
                AggregationDefinition(type=AggregationType.not_set),
                AggregationDefinition(type=AggregationType.avg),
            ],
            True,
            TaxonTypeEnum.metric,
            None,
        ],
        [
            [
                AggregationDefinition(type=AggregationType.group_by),
                AggregationDefinition(type=AggregationType.not_set),
                AggregationDefinition(type=AggregationType.min),
            ],
            True,
            TaxonTypeEnum.dimension,
            None,
        ],
    ],
    ids=get_case_id_common_defined_agg,
)
def test_common_defined_definition(
    agg_definitions: List[AggregationDefinition],
    fallback: bool,
    taxon_type: Optional[TaxonTypeEnum],
    expected: Optional[AggregationDefinition],
):
    if taxon_type is None or fallback is None:
        actual = AggregationDefinition.common_defined_definition(agg_definitions)
    else:
        actual = AggregationDefinition.common_defined_definition(agg_definitions, fallback, taxon_type)

    if expected is None:
        assert actual is None
    else:
        assert actual
        assert actual.dict() == expected.dict()
