from typing import Iterable, Tuple

from panoramic.cli.husky.core.enums import DbDataType
from panoramic.cli.husky.federated.identifier_parser.configuration import Configuration

NUMERIC_COLUMN_TYPES = {
    DbDataType.BOOLEAN,
    DbDataType.SMALLINT,
    DbDataType.INTEGER,
    DbDataType.BIGINT,
    DbDataType.TINYINT,
    DbDataType.FLOAT,
    DbDataType.DOUBLE,
    DbDataType.DECIMAL,
}

STRING_COLUMN_TYPES = {
    DbDataType.CHARACTER_VARYING,
    DbDataType.CHARACTER,
    DbDataType.NATIONAL_CHARACTER_VARYING,
    DbDataType.NATIONAL_CHARACTER,
    DbDataType.BINARY_VARYING,
    DbDataType.BINARY,
}

COMPLEX_COLUMN_TYPES = {
    DbDataType.MAP,
    DbDataType.ANY,
    DbDataType.NULL,
    DbDataType.UNION,
    DbDataType.JAVA_OBJECT,
}


class ColumnFilter:
    def __init__(self, params: Configuration):
        self.params = params

    def _score_all_dimensions(self, columns) -> Iterable[Tuple[int, str]]:
        for column in columns:
            tel_transformation = column.tel_transformation
            data_type = column.data_type

            if not tel_transformation or not data_type:
                continue  # not enough information to apply heuristics

            # ignore certain columns by default
            if tel_transformation in self.params.ignored_column_names or tel_transformation.startswith('"_'):
                continue

            score = 0
            already_promoted = False

            # prioritize and yield ids directly
            if (
                tel_transformation == 'id'
                or tel_transformation.endswith('_id"')
                or tel_transformation.startswith('"id_')
            ):
                score += 1_000_000 + (1 if tel_transformation == '"id"' else 0)
                yield score, tel_transformation
                continue

            # maybe promote dimensions
            if data_type in STRING_COLUMN_TYPES:
                score += 1_000
                already_promoted = True

            # maybe promote dates that might be part of the primary key
            if tel_transformation in {
                '"date"',
                '"year"',
                '"month"',
                '"day"',
                '"hour"',
                '"seconds"',
                '"quarter"',
                '"week"',
                '"start_date"',
            }:
                score += 10_000
                already_promoted = True

            # ignore metrics and complex types
            if not already_promoted and (data_type in NUMERIC_COLUMN_TYPES or data_type in COMPLEX_COLUMN_TYPES):
                score = -1_000_000

            # ignore random dates and timestamps
            if not already_promoted and (
                data_type in {DbDataType.TIMESTAMP, DbDataType.TIMESTAMP_WITH_TIME_ZONE}
                or tel_transformation.endswith('_at')
                or tel_transformation.endswith('_date')
            ):
                score = -1_000_000

            yield score, tel_transformation

    def find_dimensions(self, columns) -> Iterable[str]:
        for score, column_name in sorted(
            self._score_all_dimensions(columns),
            # sort by score desc (highest first) and column name asc (alphabetically)
            key=lambda score_and_col: (-score_and_col[0], score_and_col[1]),
        ):
            if score < 0:
                continue

            yield column_name
