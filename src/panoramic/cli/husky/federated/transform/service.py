from collections import defaultdict
from typing import Dict, Optional, Tuple

from sqlalchemy import literal_column, select
from sqlalchemy.sql import ClauseElement

from panoramic.cli.husky.core.federated.transform.models import TransformRequest
from panoramic.cli.husky.core.sql_alchemy_util import (
    UNSAFE_IDENTIFIER_CHARS_REGEXP,
    compile_query,
    safe_quote_identifier,
)
from panoramic.cli.husky.core.taxonomy.getters import fetch_all_used_taxons_map
from panoramic.cli.husky.federated.transform.exceptions import UnsupportedDialectError
from panoramic.cli.husky.service.blending.query_builder import QueryBuilder
from panoramic.cli.husky.service.context import HuskyQueryContext
from panoramic.cli.husky.service.types.api_data_request_types import (
    ApiDataRequest,
    BlendingDataRequest,
    DataRequestOrigin,
)
from panoramic.cli.husky.service.types.enums import HuskyQueryRuntime
from panoramic.cli.husky.service.types.types import Dataframe


class TransformService:
    """Service layer for Federated Transform API"""

    @classmethod
    def _correct_column_aliases(cls, query_context: HuskyQueryContext, df: Dataframe) -> ClauseElement:
        used_column_names: Dict[str, int] = defaultdict(int)
        mapping: Dict[str, str] = {}

        if query_context.query_runtime is HuskyQueryRuntime.snowflake:
            # for SF query runtime, we can output everything "as-is"
            mapping = {
                safe_quote_identifier(slug_name, query_context.dialect): slug_name for slug_name in df.slug_to_column
            }
        elif query_context.query_runtime is HuskyQueryRuntime.bigquery:
            for slug_name in df.slug_to_column:
                new_column_name = UNSAFE_IDENTIFIER_CHARS_REGEXP.sub('_', slug_name)
                # cannot start with number
                if new_column_name[0].isdigit():
                    new_column_name = f'_{new_column_name}'

                if new_column_name in used_column_names:
                    # if this column name is already used, add number as an alias to it
                    correct_column_name = f'{new_column_name}_{used_column_names[new_column_name]}'
                else:
                    # otherwise, just use the safe version
                    correct_column_name = new_column_name

                used_column_names[correct_column_name] += 1
                mapping[safe_quote_identifier(slug_name, query_context.dialect)] = correct_column_name
        else:
            # unsupported dialect
            raise UnsupportedDialectError(query_context.query_runtime)

        query = select([literal_column(slug).label(label) for slug, label in mapping.items()]).select_from(df.query)

        return query

    @classmethod
    def compile_transformation_request(
        cls, req: TransformRequest, company_id: str, physical_data_source: Optional[str]
    ) -> Tuple[str, HuskyQueryRuntime]:
        """
        Compiles Transform request to its SQL representation

        :param req: Input request
        :param company_id: Company ID
        :param physical_data_source: Restrict the model graph traversal only on provided PDS

        :return: SQL and type of dialect
        """
        sorted_fields = sorted(req.requested_fields)
        # prepare origin description
        origin = DataRequestOrigin(
            {
                'system': 'FDQ',
                'extra': {
                    'purpose': 'taxonomy.transform.compile',
                },
            }
        )

        # get all used taxons in the request
        used_taxons_map = fetch_all_used_taxons_map(company_id, sorted_fields)

        # figure out set of all virtual data sources covered by the taxons in the request
        used_vds = {taxon.data_source for taxon in used_taxons_map.values() if taxon.data_source}

        # generate subrequest for each virtual data source
        # this will allow Husky to push the taxons into relevant subrequests
        subrequests = []
        for vds in sorted(used_vds):
            subrequest = ApiDataRequest({'scope': {'company_id': company_id}, 'properties': {'data_sources': [vds]}})

            subrequests.append(subrequest)

        # finalize the blending husky request
        husky_request_dict = {'data_subrequests': subrequests, 'taxons': req.requested_fields, 'origin': origin}

        if physical_data_source:
            husky_request_dict['physical_data_sources'] = [physical_data_source]

        husky_request = BlendingDataRequest(husky_request_dict)
        context = HuskyQueryContext.from_request(husky_request)

        husky_dataframe = QueryBuilder.validate_data_request(context, husky_request)

        # add another layer of query to use correct names
        final_query = cls._correct_column_aliases(context, husky_dataframe)

        return compile_query(final_query, context.dialect), context.query_runtime
