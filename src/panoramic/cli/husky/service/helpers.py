from pybigquery.sqlalchemy_bigquery import BigQueryDialect
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from sqlalchemy.dialects.mysql.base import MySQLDialect
from sqlalchemy.dialects.postgresql.base import PGDialect

from panoramic.cli.husky.service.types.enums import HuskyQueryRuntime

RUNTIME_DIALECTS = {
    HuskyQueryRuntime.snowflake: SnowflakeDialect(),
    HuskyQueryRuntime.bigquery: BigQueryDialect(),
    HuskyQueryRuntime.mysql: MySQLDialect(),
    HuskyQueryRuntime.postgres: PGDialect(),
}
