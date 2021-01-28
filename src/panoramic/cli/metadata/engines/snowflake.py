from panoramic.cli.connections import Connections
from panoramic.cli.metadata.engines.base import BaseScanner
from panoramic.cli.pano_model import PanoModel, PanoModelField


class SnowflakeScanner(BaseScanner):
    """Snowflake metadata scanner"""

    def scan(self, *, force_reset: bool = False):
        """Scan Snowflake storage"""
        connection = self._get_connection()

        if force_reset:
            self.reset()

        # list all available databases
        dbs = Connections.execute('SHOW DATABASES', connection)

        for db_row in dbs:
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

            rows = Connections.execute(query, connection)

            for col_row in rows:
                # generate correct model name
                model_name = '.'.join([self._connection_name, db_name, col_row['table_schema'], col_row['table_name']])
                column_name = col_row['column_name']
                data_type = col_row['data_type']

                if model_name not in self._models:
                    # create a new model, if no model with the name is found
                    model = PanoModel(
                        model_name=model_name, data_source=model_name, fields=[], joins=[], identifiers=[]
                    )
                    self._models[model_name] = model

                # create the attribute
                field = PanoModelField(
                    field_map=[column_name.lower()], data_reference=f'"{column_name}"', data_type=data_type
                )
                if column_name not in self._model_fields:
                    self._model_fields[column_name] = field

                self._models[model_name].fields.append(field)
