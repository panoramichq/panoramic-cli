from typing import Tuple

from sqlalchemy.sql.elements import ColumnClause

from panoramic.cli.husky.service.types.types import DataframeColumn

ColumnAndDataframeColumn = Tuple[ColumnClause, DataframeColumn]
