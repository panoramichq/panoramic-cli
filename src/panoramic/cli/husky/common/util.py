import random
import string
import traceback


class TerminalStyle:
    BLACK = lambda x: '\033[30m' + str(x)
    RED = lambda x: '\033[31m' + str(x)
    GREEN = lambda x: '\033[32m' + str(x)
    YELLOW = lambda x: '\033[33m' + str(x)
    BLUE = lambda x: '\033[34m' + str(x)
    MAGENTA = lambda x: '\033[35m' + str(x)
    CYAN = lambda x: '\033[36m' + str(x)
    WHITE = lambda x: '\033[37m' + str(x)
    UNDERLINE = lambda x: '\033[4m' + str(x)
    RESET = lambda x='': '\033[0m' + str(x)


def exception_to_string_with_traceback(exception: Exception):
    return ''.join(traceback.format_exception(etype=type(exception), value=exception, tb=exception.__traceback__))


def random_string(size: int = 5):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(size))


def random_lowercase_string(size: int = 8):
    return ''.join((random.choice(string.ascii_lowercase + string.digits) for _ in range(size)))


def random_alphabet_string(size: int = 5):
    return ''.join(random.choice(string.ascii_uppercase) for _ in range(size))


def random_int(max_int=2 * 1000 * 1000000):
    return random.randint(0, max_int)


def serialize_class_with_dict_props(class_instance, dict_props):
    return f'{class_instance.__class__}({dict_props!r})'


def serialize_class_with_props(class_instance):
    return serialize_class_with_dict_props(class_instance, class_instance.__dict__)


class FilterEmpty:
    """
    Each empty value in nested object is filtered out
    """

    @classmethod
    def _is_not_filtered(cls, val):
        if val or val is False:
            return True
        else:
            return False

    @classmethod
    def _filter_dict(cls, val):
        return {key: cls.filter_empty(item) for key, item in val.items() if cls._is_not_filtered(item)}

    @classmethod
    def _filter_list(cls, val):
        return [cls.filter_empty(item) for item in val if cls._is_not_filtered(item)]

    @classmethod
    def filter_empty(cls, val):
        if isinstance(val, dict):
            return cls._filter_dict(val)
        elif isinstance(val, list):
            return cls._filter_list(val)
        else:
            return val


def chunkify(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]
