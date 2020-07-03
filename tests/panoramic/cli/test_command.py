from panoramic.cli.command import scan


def test_scan():
    scan('test-source', 'test-filter')
