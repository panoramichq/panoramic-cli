from datetime import date, datetime, time
from typing import cast

from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql.type_api import TypeEngine
from tqdm import tqdm

from panoramic.cli.connection import Connection
from panoramic.cli.husky.core.taxonomy.enums import ValidationType
from panoramic.cli.metadata.engines.with_connection import WithConnection
from panoramic.cli.pano_model import PanoModel, PanoModelField


class InspectorScanner(WithConnection):
    """Metadata scanner using SQLAlchemy inspector"""

    _DATA_TYPES_MAP = {
        float: ValidationType.numeric,
        int: ValidationType.integer,
        str: ValidationType.text,
        bool: ValidationType.boolean,
        bytes: ValidationType.variant,
        datetime: ValidationType.datetime,
        date: ValidationType.datetime,
        time: ValidationType.datetime,
    }

    def scan(self, *, force_reset: bool = False):
        connection = self._get_connection()

        if force_reset:
            self.reset()

        engine = Connection.get_connection_engine(connection)
        inspector = Inspector.from_engine(engine)
        # list all available tables
        for schema_name in tqdm(inspector.get_schema_names()):
            for table_name in tqdm(inspector.get_table_names(schema=schema_name)):
                model_name = table_name

                for column in tqdm(inspector.get_columns(table_name=table_name)):
                    column_name = column['name']
                    data_type_raw = column['type']

                    if model_name not in self._models:
                        # create a new model, if no model with the name is found
                        model = PanoModel(model_name=model_name, fields=[], joins=[], identifiers=[])
                        self._models[model_name] = model

                    # determine data type
                    data_type = self._DATA_TYPES_MAP.get(
                        cast(TypeEngine, data_type_raw).python_type, ValidationType.text
                    )

                    # create the attribute
                    field = PanoModelField(
                        field_map=[column_name.lower()], data_reference=f'"{column_name}"', data_type=data_type.value
                    )
                    if column_name not in self._model_fields:
                        self._model_fields[column_name] = field

                    self._models[model_name].fields.append(field)
