from enum import Enum
from typing import Set


class DbDataType(str, Enum):
    """
    Data types in database engines
    """

    BOOLEAN = 'BOOLEAN'
    SMALLINT = 'SMALLINT'
    INTEGER = 'INTEGER'
    BIGINT = 'BIGINT'
    TINYINT = 'TINYINT'

    FLOAT = 'FLOAT'
    DOUBLE = 'DOUBLE'
    DECIMAL = 'DECIMAL'

    CHARACTER_VARYING = 'CHARACTER VARYING'
    CHARACTER = 'CHARACTER'
    NATIONAL_CHARACTER_VARYING = 'NATIONAL CHARACTER VARYING'
    NATIONAL_CHARACTER = 'NATIONAL CHARACTER'
    BINARY_VARYING = 'BINARY VARYING'
    BINARY = 'BINARY'

    DATE = 'DATE'
    TIME = 'TIME'
    TIME_WITH_TIME_ZONE = 'TIME WITH TIME ZONE'
    TIMESTAMP = 'TIMESTAMP'
    TIMESTAMP_WITH_TIME_ZONE = 'TIMESTAMP WITH TIME ZONE'
    INTERVAL_YEAR_TO_MONTH = 'INTERVAL YEAR TO MONTH'
    INTERVAL_DAY_TO_SECOND = 'INTERVAL DAY TO SECOND'
    INTERVAL = 'INTERVAL'

    MAP = 'MAP'
    ANY = 'ANY'
    NULL = 'NULL'
    UNION = 'UNION'
    JAVA_OBJECT = 'JAVA_OBJECT'


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
