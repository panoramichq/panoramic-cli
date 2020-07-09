from panoramic.cli.parser import _remove_source_from_path, load_scanned_tables


def test_remove_source_from_path():
    assert _remove_source_from_path('sc1') == ''
    assert _remove_source_from_path('sc1.sch') == 'sch'
    assert _remove_source_from_path('sc1.db.sch') == 'db.sch'


def test_load_scanned_tables():
    expected = [
        {
            'api_version': 'v1',
            'data_source': {'data_source_type': 'sql', 'path': 'schema1.table1'},
            'fields': [
                {'data_type': 'str', 'field_map': ['schema_1_table_1_id'], 'transformation': 'id'},
                {'data_type': 'int', 'field_map': ['schema_1_table_1_value'], 'transformation': 'value'},
            ],
            'identifiers': [],
            'joins': [],
        }
    ]
    output = [
        item.to_dict()
        for item in load_scanned_tables(
            [
                {'table_schema': 'source.schema1', 'table_name': 'table1', 'column_name': 'id', 'data_type': 'str'},
                {'table_schema': 'source.schema1', 'table_name': 'table1', 'column_name': 'value', 'data_type': 'int'},
            ],
            'v1',
        )
    ]
    assert output == expected
