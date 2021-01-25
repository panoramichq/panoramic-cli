from enum import Enum

from pybigquery.sqlalchemy_bigquery import BigQueryDialect
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from sqlalchemy.dialects.mysql.base import MySQLDialect
from sqlalchemy.dialects.postgresql.base import PGDialect


class HuskyQueryRuntime(Enum):
    """
    Query runtime
    """

    snowflake = SnowflakeDialect.name
    bigquery = BigQueryDialect.name
    mysql = MySQLDialect.name
    postgres = PGDialect.name


class HuskyRequestMode(Enum):
    """
    Mode of requesting data
    """

    SYNC = 'sync'
    ASYNC = 'async'
