import logging

from typing import Dict, Iterable

from panoramic.cli.table_model import TableModel


logger = logging.getLogger(__name__)


def load_scanned_table(raw_columns: Iterable[Dict]) -> TableModel:
    """
    Load result of metadata table columns scan into Table Model
    """
    return TableModel()


def unload_scanned_table(table_model: TableModel) -> Dict:
    """
    Unload Table Model into yaml dict
    """
    return dict()
