from panoramic.cli.parser import load_scanned_tables


def test_load_scanned_tables():
    expected = [
        {
            'api_version': 'v1',
            'model_name': 'source.schema1.table1',
            'data_source': 'source.schema1.table1',
            'fields': [
                {'uid': 'source.schema1.table1.id', 'data_type': 'str', 'field_map': ['id'], 'data_reference': 'id',},
                {
                    'uid': 'source.schema1.table1.value',
                    'data_type': 'int',
                    'field_map': ['value'],
                    'data_reference': 'value',
                },
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
