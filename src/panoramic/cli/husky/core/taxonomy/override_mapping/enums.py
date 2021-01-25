from enum import Enum


class MappingSourceType(str, Enum):
    """Supported types of mapping source"""

    DIRECT = 'direct'
    """Direct mapping (original -> changed)"""
