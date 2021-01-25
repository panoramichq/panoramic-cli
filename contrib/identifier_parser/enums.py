from enum import Enum


class ColumnOverflowStrategy(str, Enum):
    SLICE_COLUMNS = 'SLICE_COLUMNS'
    RAISE_EXCEPTION = 'RAISE_EXCEPTION'
