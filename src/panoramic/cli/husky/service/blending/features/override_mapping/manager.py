from typing import Dict, List, Optional

from sqlalchemy.sql import Select

from panoramic.cli.husky.core.taxonomy.override_mapping.constants import (
    MAX_OVERRIDE_MAPPINGS,
)
from panoramic.cli.husky.core.taxonomy.override_mapping.exceptions import (
    OverrideMappingGenericError,
    TooManyOverrideMappings,
)
from panoramic.cli.husky.core.taxonomy.override_mapping.models import OverrideMapping
from panoramic.cli.husky.core.taxonomy.override_mapping.service import (
    OverrideMappingService,
)
from panoramic.cli.husky.core.taxonomy.override_mapping.types import (
    OverrideMappingSlug,
    OverrideMappingTelData,
)
from panoramic.cli.husky.service.blending.features.override_mapping.sql import (
    OverrideMappingSql,
)


class OverrideMappingManager:
    """This class handles management of override mappings and all resources relevant to them"""

    def __init__(
        self,
        override_mapping_tel_data: Optional[OverrideMappingTelData] = None,
        comparison_override_mapping_tel_data: Optional[OverrideMappingTelData] = None,
        mappings: Optional[List[OverrideMapping]] = None,
        mappings_cte_map: Optional[Dict[OverrideMappingSlug, Select]] = None,
    ):
        self.override_mapping_tel_data = override_mapping_tel_data or set()
        self.comparison_override_mapping_tel_data = comparison_override_mapping_tel_data or set()
        self.mappings = mappings or []
        self.cte_map = mappings_cte_map or {}

    @staticmethod
    def initialize(
        company_id: Optional[str],
        override_mapping_tel_data: OverrideMappingTelData,
        comparison_override_mapping_tel_data: OverrideMappingTelData,
    ) -> 'OverrideMappingManager':
        """
        Initializes the manager - loads the mappings and creates necessary SQL primitives

        :param company_id: Company ID
        :param override_mapping_tel_data:   Override mapping TEL data
        :param comparison_override_mapping_tel_data:    Override mapping TEL data for comparison subqueries
        """

        # merge TEL data from both sources (just to be sure)
        all_mapping_data = override_mapping_tel_data | comparison_override_mapping_tel_data

        mapping_slugs = {om.override_mapping_slug for om in all_mapping_data}

        if len(mapping_slugs) == 0:
            # no mappings are required so initialize an empty manager
            return OverrideMappingManager()

        if company_id is None:
            raise OverrideMappingGenericError(
                'Field "company_id" is a required field in subrequest scope when using override mappings.',
                mapping_slugs,
            )

        # make sure that we dont request too many override mappings in one request
        if len(mapping_slugs) > MAX_OVERRIDE_MAPPINGS:
            raise TooManyOverrideMappings(mapping_slugs)

        # load relevant override mappings, if there are any
        mappings = OverrideMappingService.get_by_slugs_list(mapping_slugs, company_id, True)

        # generate all CTEs for now (we only support direct override mapping at the moment)
        main_ctes = {
            om.slug: OverrideMappingSql.render_direct_mapping(om).cte(OverrideMappingSql.generate_cte_name(om.slug))
            for om in mappings
        }

        mappings_cte_map = {}
        for om in all_mapping_data:
            identifier = OverrideMappingSql.generate_identifier(
                om.column, om.override_mapping_slug, om.include_missing_values
            )
            mappings_cte_map[identifier] = main_ctes[om.override_mapping_slug].alias(identifier)

        return OverrideMappingManager(
            override_mapping_tel_data, comparison_override_mapping_tel_data, mappings, mappings_cte_map
        )
