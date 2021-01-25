from collections import namedtuple
from typing import List, Optional, Set, Tuple

OverrideMappingSlug = str

MappingDefinition = List[Tuple[Optional[str], Optional[str]]]
"""
Definition of direct override mapping.
Direct mapping is defined as list of tuples with 2 values - original and changed. Both original and changed value
can be None, because we allow overriding NULL values in result.
"""

OverrideMappingTelInformation = namedtuple(
    'OverrideMappingTelInformation', ['column', 'override_mapping_slug', 'include_missing_values']
)
"""Override mapping information from TEL formula"""

OverrideMappingTelData = Set[OverrideMappingTelInformation]
"""Set with all required override mappings in TEL plan"""
