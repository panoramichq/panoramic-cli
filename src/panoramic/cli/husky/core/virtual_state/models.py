from typing import List

from panoramic.cli.husky.core.model.models import HuskyModel
from panoramic.cli.husky.core.taxonomy.models import Taxon
from panoramic.cli.husky.core.virtual_data_sources.models import VirtualDataSource


class VirtualState:
    """Husky representation of virtual state"""

    virtual_data_sources: List[VirtualDataSource]
    """List of virtual data sources in the state"""

    models: List[HuskyModel]
    """List of Husky models in the state"""

    taxons: List[Taxon]
    """List of taxons in the state"""

    def __init__(self, vds: List[VirtualDataSource], models: List[HuskyModel], taxons: List[Taxon]):
        """Create internal representation of virtual state"""
        self.virtual_data_sources = vds
        self.models = models
        self.taxons = taxons
