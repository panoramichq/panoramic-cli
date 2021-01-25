from typing import Optional

from panoramic.cli.husky.core.model.enums import ValueQuantityType


class TaxonModelInfo:
    """
    Contains all the info about taxon extracted from the Model that maps it, and that is used in the query.
    """

    quantity_type: ValueQuantityType
    "Marks if the taxon is stored as scalar or list value"
    taxon_sql_accessor: str
    "Full sql path that can be used to select the taxon in case the taxon is not present in the select clause"

    model_name: Optional[str]
    """HuskyModel name the taxon is sourced from"""

    def __init__(
        self, taxon_sql_accessor: str, model_name: Optional[str] = None, data_type: Optional[ValueQuantityType] = None
    ):
        self.taxon_sql_accessor = taxon_sql_accessor
        self.model_name = model_name
        self.quantity_type = data_type or ValueQuantityType.scalar

    @property
    def is_array(self) -> bool:
        return self.quantity_type == ValueQuantityType.array
