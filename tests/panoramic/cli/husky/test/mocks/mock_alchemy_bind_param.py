from unittest.mock import patch

RANDOM_PREFIX_IMPORT_PATH = 'panoramic.cli.husky.core.sql_alchemy_util._random_bind_param_prefix'


def mock_alchemy_bind_param(fn):
    """
    Helper decorator for mocking _random_bind_param_prefix, to ensure deterministic bind param prefixes in tests.
    Having it as decorator does makes it easier to use.. instead of adding the mock argument to test fn, we just need
    to put this decorator on given test fn.
    """

    def wrapper(*args, **kws):
        counter = 0

        def side_effect():
            nonlocal counter
            counter += 1
            return str(counter) + '_'

        patcher = patch(RANDOM_PREFIX_IMPORT_PATH)
        mock_foo = patcher.start()
        mock_foo.side_effect = side_effect
        result = fn(*args, **kws)
        patcher.stop()
        return result

    return wrapper
