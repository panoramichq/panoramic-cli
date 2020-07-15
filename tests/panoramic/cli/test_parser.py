from panoramic.cli.parser import _remove_source_from_path, load_scanned_tables


def test_remove_source_from_path():
    assert _remove_source_from_path('sc1.sch') == 'sch'
    assert _remove_source_from_path('sc1.db.sch') == 'db.sch'


def test_load_scanned_tables():
    expected = [
        {
            'model_name': 'schema1.table1',
            'data_source': 'schema1.table1',
            'fields': [
                {'data_type': 'str', 'field_map': ['schema1.table1.id'], 'transformation': 'id'},
                {'data_type': 'int', 'field_map': ['schema1.table1.value'], 'transformation': 'value'},
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
        )
    ]

    assert output == expected
