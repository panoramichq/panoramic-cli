from typing import Dict

from tqdm import tqdm

from panoramic.cli.connection import Connection
from panoramic.cli.husky.core.taxonomy.enums import ValidationType
from panoramic.cli.metadata.engines.with_connection import WithConnection
from panoramic.cli.pano_model import PanoModel, PanoModelField


class SnowflakeScanner(WithConnection):
    """Snowflake metadata scanner"""

    _SF_DATA_TYPES_MAP: Dict[str, ValidationType] = {
        # numbers
        'NUMBER': ValidationType.numeric,
        'DECIMAL': ValidationType.numeric,
        'NUMERIC': ValidationType.numeric,
        'INT': ValidationType.numeric,
        'INTEGER': ValidationType.numeric,
        'BIGINT': ValidationType.numeric,
        'SMALLINT': ValidationType.numeric,
        'FLOAT': ValidationType.numeric,
        'FLOAT4': ValidationType.numeric,
        'FLOAT8': ValidationType.numeric,
        'DOUBLE': ValidationType.numeric,
        'DOUBLE PRECISION': ValidationType.numeric,
        'REAL': ValidationType.numeric,
        # date & datetimes
        'DATE': ValidationType.datetime,
        'DATETIME': ValidationType.datetime,
        'TIME': ValidationType.datetime,
        'TIMESTAMP': ValidationType.datetime,
        'TIMESTAMP_LTZ': ValidationType.datetime,
        'TIMESTAMP_NTZ': ValidationType.datetime,
        'TIMESTAMP_TZ': ValidationType.datetime,
        # boolean
        'BOOLEAN': ValidationType.boolean,
        # variant
        'VARIANT': ValidationType.variant,
        'OBJECT': ValidationType.variant,
        'ARRAY': ValidationType.variant,
        # everything else is text
    }
    """Map of Snowflake data types and their respective validation types"""

    def scan(self, *, force_reset: bool = False):
        """Scan Snowflake storage"""
        connection = self._get_connection()

        if force_reset:
            self.reset()

        # list all available databases
        dbs = Connection.execute('SHOW DATABASES', connection)

        for db_row in tqdm(dbs):
            db_name = db_row['name']

            # prepare the query to fetch metadata about all tables
            query = f'''
                SELECT
                    table_schema, table_name, column_name, data_type
                FROM
                    {db_name}.INFORMATION_SCHEMA.COLUMNS
                ORDER BY 
                    table_schema, table_name, column_name
                '''

            rows = Connection.execute(query, connection)

            for col_row in tqdm(rows):
                # generate correct model name
                model_name = '.'.join([db_name, col_row['table_schema'], col_row['table_name']])
                column_name = col_row['column_name']
                data_type_raw = col_row['data_type']

                if model_name not in self._models:
                    # create a new model, if no model with the name is found
                    model = PanoModel(model_name=model_name, fields=[], joins=[], identifiers=[])
                    self._models[model_name] = model

                # determine data type
                data_type = self._SF_DATA_TYPES_MAP.get(data_type_raw, ValidationType.text)

                # create the attribute
                field = PanoModelField(
                    field_map=[column_name.lower()], data_reference=f'"{column_name}"', data_type=data_type.value
                )
                if column_name not in self._model_fields:
                    self._model_fields[column_name] = field

                self._models[model_name].fields.append(field)
