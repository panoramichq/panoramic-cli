from enum import Enum
from typing import Set


class DbTimeUnit(str, Enum):
    """
    Enum with valid time units
    """

    SECOND = 'SECOND'
    MINUTE = 'MINUTE'
    HOUR = 'HOUR'
    DAY = 'DAY'
    WEEK = 'WEEK'
    MONTH = 'MONTH'
    YEAR = 'YEAR'

    @classmethod
    def allowed_in_date_trunc(cls, val: 'DbTimeUnit') -> bool:
        """
        Checks whether the time unit is allowed in DATE_TRUNC function
        """
        return val in {
            DbTimeUnit.SECOND,
            DbTimeUnit.MINUTE,
            DbTimeUnit.HOUR,
            DbTimeUnit.DAY,
            DbTimeUnit.MONTH,
            DbTimeUnit.YEAR,
        }


DATE_DIFF_VALUES: Set[DbTimeUnit] = {
    DbTimeUnit.SECOND,
    DbTimeUnit.MINUTE,
    DbTimeUnit.HOUR,
    DbTimeUnit.DAY,
    DbTimeUnit.WEEK,
    DbTimeUnit.MONTH,
    DbTimeUnit.YEAR,
}
"""
The set of allowed values in DATE_DIFF TEL function
"""


class SourceType(str, Enum):
    SNOWFLAKE = 'SNOWFLAKE'
    BIGQUERY = 'BIGQUERY'
